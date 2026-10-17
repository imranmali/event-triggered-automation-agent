from __future__ import annotations

from pathlib import Path

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from click.testing import CliRunner
from sqlalchemy import inspect, text

from eta.actions import default_registry
from eta.cli import main
from eta.db.migrate import downgrade, upgrade_async
from eta.db.schema import metadata
from eta.db.store import create_engine
from eta.workflows.registry import load_config

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE_ENV = {
    "GITHUB_WEBHOOK_SECRET": "x",
    "STRIPE_WEBHOOK_SECRET": "x",
    "GENERIC_WEBHOOK_SECRET": "x",
}


async def _reset(url: str) -> None:
    engine = create_engine(url)
    async with engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
        await conn.execute(text("DROP TABLE IF EXISTS alembic_version"))
    await engine.dispose()


async def test_migrations_match_models(db_url):
    await _reset(db_url)
    await upgrade_async(db_url)
    engine = create_engine(db_url)
    async with engine.connect() as conn:
        diff = await conn.run_sync(
            lambda c: compare_metadata(
                MigrationContext.configure(c, opts={"compare_type": False}), metadata
            )
        )
        tables = await conn.run_sync(lambda c: set(inspect(c).get_table_names()))
    await engine.dispose()
    assert diff == []
    assert {
        "events",
        "runs",
        "step_results",
        "dead_letters",
        "run_history",
        "workflow_versions",
    } <= tables


async def test_downgrade_to_base(db_url):
    import asyncio

    await _reset(db_url)
    await upgrade_async(db_url)
    await asyncio.to_thread(downgrade, db_url, "base")
    engine = create_engine(db_url)
    async with engine.connect() as conn:
        tables = await conn.run_sync(lambda c: set(inspect(c).get_table_names()))
    await engine.dispose()
    assert tables <= {"alembic_version"}


def test_example_config_is_valid():
    cfg = load_config(ROOT / "config", default_registry, environ=EXAMPLE_ENV)
    assert {"github", "stripe", "generic", "orders"} <= set(cfg.sources)
    assert len(cfg.workflows) >= 5
    # The internal chain is wired: stripe fulfilment emits what the follow-up listens to.
    matched = cfg.workflows.match("internal", "order.paid", {}, {})
    assert [m.name for m in matched] == ["order_paid_followup"]


def test_cli_validate(monkeypatch):
    for k, v in EXAMPLE_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("ETA_CONFIG_DIR", str(ROOT / "config"))
    from eta.config import get_settings

    get_settings.cache_clear()
    result = CliRunner().invoke(main, ["validate"])
    get_settings.cache_clear()
    assert result.exit_code == 0, result.output
    assert "workflow(s) valid" in result.output


def test_cli_validate_reports_errors(monkeypatch, tmp_path):
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows" / "bad.yaml").write_text(
        "name: bad\ntrigger: {source: x}\nsteps: [{id: a, action: nope}]\n"
    )
    monkeypatch.setenv("ETA_CONFIG_DIR", str(tmp_path))
    from eta.config import get_settings

    get_settings.cache_clear()
    result = CliRunner().invoke(main, ["validate"])
    get_settings.cache_clear()
    assert result.exit_code == 1
    assert "unknown action" in result.output


@pytest.mark.parametrize(
    "cmd", [["--help"], ["dlq", "--help"], ["runs", "--help"], ["db", "--help"]]
)
def test_cli_help(cmd):
    assert CliRunner().invoke(main, cmd).exit_code == 0
