from __future__ import annotations

import os
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import text

from eta.actions import default_registry
from eta.config import Settings
from eta.db.schema import metadata
from eta.db.store import Store
from eta.runtime import Runtime
from eta.workflows.registry import LoadedConfig, WorkflowRegistry, parse_sources, parse_workflow

PG_URL = os.environ.get("ETA_TEST_DATABASE_URL")
BACKENDS = ["sqlite", *(["postgres"] if PG_URL else [])]


@pytest.fixture(params=BACKENDS)
def db_url(request: pytest.FixtureRequest, tmp_path: Path) -> str:
    if request.param == "sqlite":
        return f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    assert PG_URL
    return PG_URL


@pytest.fixture
async def store(db_url: str) -> AsyncIterator[Store]:
    s = Store.from_url(db_url, pool_size=20)
    if s.dialect == "postgresql":
        async with s.engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
            await conn.execute(text("DROP TABLE IF EXISTS alembic_version"))
    await s.create_all()
    yield s
    await s.close()


class Recorder:
    """Mock HTTP downstream that records requests and replays scripted responses."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.responses: list[httpx.Response | Exception] = []
        self.default = httpx.Response(200, json={"ok": True})

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.responses:
            nxt = self.responses.pop(0)
            if isinstance(nxt, Exception):
                raise nxt
            return nxt
        return self.default

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handler))


@pytest.fixture
def recorder() -> Recorder:
    return Recorder()


FAST_RETRY = {"max_attempts": 3, "initial_delay": 0, "max_delay": 0, "jitter": "none"}


def make_config(
    workflows: list[dict[str, Any]], sources: dict[str, Any] | None = None
) -> LoadedConfig:
    return LoadedConfig(
        sources=parse_sources({"sources": sources or {}}, environ={"SECRET": "s3cr3t"}),
        workflows=WorkflowRegistry(parse_workflow(w, default_registry) for w in workflows),
    )


RuntimeFactory = Callable[..., Runtime]


@pytest.fixture
def make_runtime(store: Store, recorder: Recorder, tmp_path: Path) -> RuntimeFactory:
    def factory(
        workflows: list[dict[str, Any]],
        sources: dict[str, Any] | None = None,
        **settings: Any,
    ) -> Runtime:
        base: dict[str, Any] = {
            "config_dir": tmp_path,
            "worker_id": "test-worker",
            "poll_interval": 0.05,
            "lease_seconds": 5,
            "heartbeat_interval": 1,
            "reap_interval": 0.1,
            "shutdown_grace_seconds": 1,
        }
        base.update(settings)
        return Runtime.build(
            Settings(**base),
            store=store,
            config=make_config(workflows, sources),
            http=recorder.client(),
            environ={"SECRET": "s3cr3t", "ETA_SECRET_API_URL": "https://api.example.com"},
        )

    return factory
