"""Command line interface: ``eta serve | worker | consume | db | validate | dlq | runs``."""

from __future__ import annotations

import asyncio
import json
import signal
import sys
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from typing import Any, TypeVar

import click

from eta.config import Settings, get_settings
from eta.errors import ConfigError
from eta.logs import configure_logging
from eta.models import DeadLetterStatus, ReplayMode, RunStatus

T = TypeVar("T")


def _echo_json(data: Any) -> None:
    click.echo(json.dumps(data, indent=2, default=str))


def _run(coro_fn: Callable[[], Awaitable[T]]) -> T:
    try:
        return asyncio.run(coro_fn())  # type: ignore[arg-type]
    except ConfigError as exc:
        raise click.ClickException(f"configuration error: {exc}") from exc


def _load_dotenv() -> None:
    """Export ``.env`` into the process environment (without overriding).

    Settings read ``ETA_*`` keys themselves, but webhook signing secrets
    (``secret_env``) and ``ETA_SECRET_*`` template secrets are read from
    ``os.environ``.
    """
    from dotenv import load_dotenv

    load_dotenv(".env", override=False)


def _settings() -> Settings:
    _load_dotenv()
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    return settings


async def _run_until_signal(
    start: Callable[[], None], shutdown: Callable[[], Awaitable[None]], stop: asyncio.Event
) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    start()
    await stop.wait()
    await shutdown()


@click.group()
@click.version_option(package_name="event-triggered-automation-agent")
def main() -> None:
    """Event-Triggered Automation Agent."""


# ------------------------------------------------------------------ serving


@main.command()
@click.option("--worker/--no-worker", default=False, help="Also run a worker in this process.")
@click.option("--consumers/--no-consumers", default=False, help="Also run queue consumers.")
@click.option("--migrate/--no-migrate", default=False, help="Apply database migrations first.")
def serve(worker: bool, consumers: bool, migrate: bool) -> None:
    """Run the HTTP API (webhooks, admin API, metrics)."""
    import uvicorn

    from eta.api.app import create_app
    from eta.db.migrate import upgrade
    from eta.runtime import Runtime

    settings = _settings()
    if migrate:
        upgrade(settings.database_url)
    try:
        rt = Runtime.build(settings)
    except ConfigError as exc:
        raise click.ClickException(f"configuration error: {exc}") from exc
    app = create_app(rt, run_worker=worker, run_consumers=consumers)
    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        log_config=None,
        timeout_graceful_shutdown=int(settings.shutdown_grace_seconds) + 5,
    )


@main.command()
@click.option("--concurrency", type=int, default=None, help="Override ETA_WORKER_CONCURRENCY.")
def worker(concurrency: int | None) -> None:
    """Run a worker that executes queued workflow runs."""
    from eta.runtime import Runtime

    settings = _settings()
    if concurrency:
        settings = settings.model_copy(update={"worker_concurrency": concurrency})

    async def go() -> None:
        rt = Runtime.build(settings)
        await _run_until_signal(rt.start_worker, rt.shutdown, rt.stop_event)

    _run(go)


@main.command()
def consume() -> None:
    """Run queue consumers (Redis Streams) for all queue-backed sources."""
    from eta.runtime import Runtime

    settings = _settings()

    async def go() -> None:
        rt = Runtime.build(settings)

        def start() -> None:
            if rt.start_consumers() == 0:
                raise click.ClickException("no enabled sources with redis_stream configuration")

        await _run_until_signal(start, rt.shutdown, rt.stop_event)

    _run(go)


# ----------------------------------------------------------------- database


@main.group()
def db() -> None:
    """Database migrations."""


@db.command("upgrade")
@click.argument("revision", default="head")
def db_upgrade(revision: str) -> None:
    from eta.db.migrate import upgrade

    upgrade(_settings().database_url, revision)
    click.echo(f"database upgraded to {revision}")


@db.command("downgrade")
@click.argument("revision")
def db_downgrade(revision: str) -> None:
    from eta.db.migrate import downgrade

    downgrade(_settings().database_url, revision)
    click.echo(f"database downgraded to {revision}")


# --------------------------------------------------------------- validation


@main.command()
def validate() -> None:
    """Validate sources and workflow definitions in ETA_CONFIG_DIR."""
    from eta.actions import default_registry, load_plugins
    from eta.workflows.registry import load_config

    _load_dotenv()
    settings = get_settings()
    try:
        load_plugins(settings.plugin_module_list)
        cfg = load_config(settings.config_dir, default_registry)
    except ConfigError as exc:
        click.secho(f"✗ {exc}", fg="red", err=True)
        sys.exit(1)
    click.secho(
        f"✓ {len(cfg.sources)} source(s), {len(cfg.workflows)} workflow(s) valid", fg="green"
    )
    for src in cfg.sources.values():
        kind = "redis_stream" if src.redis_stream else "webhook"
        click.echo(f"  source   {src.name:<24} {kind:<13} verify={src.verify.type}")
    for wf in cfg.workflows:
        triggers = ", ".join(f"{t.source}:{'|'.join(t.event)}" for t in wf.triggers)
        click.echo(f"  workflow {wf.name:<24} v{wf.version} steps={len(wf.steps)} on {triggers}")


# ------------------------------------------------------------------ operate


def _store() -> Any:
    from eta.db.store import Store

    return Store.from_url(_settings().database_url, pool_size=2)


@main.group()
def dlq() -> None:
    """Inspect and act on the dead-letter queue."""


@dlq.command("list")
@click.option("--status", type=click.Choice([s.value for s in DeadLetterStatus]), default="open")
@click.option("--workflow", default=None)
@click.option("--limit", type=int, default=50)
def dlq_list(status: str, workflow: str | None, limit: int) -> None:
    async def go() -> None:
        store = _store()
        try:
            items = await store.list_dead_letters(
                status=DeadLetterStatus(status), workflow=workflow, limit=limit
            )
            _echo_json([asdict(i) for i in items])
        finally:
            await store.close()

    _run(go)


@dlq.command("replay")
@click.argument("dlq_ids", nargs=-1, required=True)
@click.option("--mode", type=click.Choice([m.value for m in ReplayMode]), default="resume")
def dlq_replay(dlq_ids: tuple[str, ...], mode: str) -> None:
    """Re-queue dead-lettered runs (resume from the failed step, or restart)."""

    async def go() -> None:
        store = _store()
        try:
            for dlq_id in dlq_ids:
                try:
                    run_id = await store.replay_dead_letter(dlq_id, mode=ReplayMode(mode))
                    click.echo(f"replayed {dlq_id} -> run {run_id}")
                except (KeyError, ValueError) as exc:
                    click.secho(f"skipped {dlq_id}: {exc}", fg="yellow", err=True)
        finally:
            await store.close()

    _run(go)


@dlq.command("discard")
@click.argument("dlq_ids", nargs=-1, required=True)
def dlq_discard(dlq_ids: tuple[str, ...]) -> None:
    async def go() -> None:
        store = _store()
        try:
            for dlq_id in dlq_ids:
                ok = await store.discard_dead_letter(dlq_id)
                click.echo(f"{'discarded' if ok else 'not open'} {dlq_id}")
        finally:
            await store.close()

    _run(go)


@main.group()
def runs() -> None:
    """Inspect workflow runs."""


@runs.command("list")
@click.option("--status", type=click.Choice([s.value for s in RunStatus]), default=None)
@click.option("--workflow", default=None)
@click.option("--limit", type=int, default=20)
def runs_list(status: str | None, workflow: str | None, limit: int) -> None:
    async def go() -> None:
        store = _store()
        try:
            items = await store.list_runs(
                status=RunStatus(status) if status else None, workflow=workflow, limit=limit
            )
            _echo_json([asdict(r) for r in items])
        finally:
            await store.close()

    _run(go)


@runs.command("show")
@click.argument("run_id")
def runs_show(run_id: str) -> None:
    async def go() -> None:
        store = _store()
        try:
            run = await store.get_run(run_id)
            if run is None:
                raise click.ClickException("run not found")
            _echo_json(
                {
                    **asdict(run),
                    "steps": [asdict(s) for s in await store.get_steps(run_id)],
                    "history": [asdict(h) for h in await store.get_history(run_id)],
                }
            )
        finally:
            await store.close()

    _run(go)


if __name__ == "__main__":
    main()
