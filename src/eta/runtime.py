"""Wires settings, storage, configuration and engine components together."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass, field

import httpx

from eta.actions import ActionRegistry, default_registry, load_plugins
from eta.config import Settings, load_secrets
from eta.db.store import Store
from eta.engine.executor import Executor
from eta.engine.worker import Worker, WorkerConfig
from eta.ingress.service import IngestService
from eta.workflows.registry import LoadedConfig, load_config

log = logging.getLogger(__name__)


@dataclass
class Runtime:
    settings: Settings
    store: Store
    config: LoadedConfig
    actions: ActionRegistry
    ingest: IngestService
    http: httpx.AsyncClient
    executor: Executor
    worker: Worker
    environ: Mapping[str, str] = field(default_factory=lambda: os.environ)
    _background: list[asyncio.Task[None]] = field(default_factory=list)
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)

    @classmethod
    def build(
        cls,
        settings: Settings,
        *,
        store: Store | None = None,
        config: LoadedConfig | None = None,
        actions: ActionRegistry | None = None,
        http: httpx.AsyncClient | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> Runtime:
        actions = actions or default_registry
        if actions is default_registry:
            load_plugins(settings.plugin_module_list)
        config = config or load_config(settings.config_dir, actions, environ=environ)
        store = store or Store.from_url(settings.database_url, pool_size=settings.db_pool_size)
        http = http or httpx.AsyncClient(follow_redirects=False)
        ingest = IngestService(store, config.workflows)
        executor = Executor(
            store,
            actions,
            http=http,
            emit=ingest.emit,
            secrets=load_secrets(environ),
        )
        worker = Worker(
            store,
            executor,
            WorkerConfig(
                worker_id=settings.worker_id,
                concurrency=settings.worker_concurrency,
                poll_interval=settings.poll_interval,
                lease_seconds=settings.lease_seconds,
                heartbeat_interval=settings.heartbeat_interval,
                shutdown_grace_seconds=settings.shutdown_grace_seconds,
                reap_interval=settings.reap_interval,
            ),
        )
        # Same-process ingest wakes the worker immediately instead of waiting a poll.
        ingest.on_runs_created = worker.notify
        return cls(
            settings,
            store,
            config,
            actions,
            ingest,
            http,
            executor,
            worker,
            environ=os.environ if environ is None else environ,
        )

    def start_worker(self) -> None:
        self._background.append(
            asyncio.create_task(self.worker.run(self.stop_event), name="eta-worker")
        )

    def start_consumers(self) -> int:
        from redis.asyncio import Redis

        from eta.ingress.redis_streams import RedisStreamConsumer

        queue_sources = [s for s in self.config.sources.values() if s.enabled and s.redis_stream]
        if not queue_sources:
            return 0
        redis = Redis.from_url(self.settings.redis_url)
        for source in queue_sources:
            consumer = RedisStreamConsumer(
                redis, source, self.ingest, self.store, consumer_name=self.settings.worker_id
            )
            self._background.append(
                asyncio.create_task(
                    consumer.run(self.stop_event), name=f"eta-consumer-{source.name}"
                )
            )
        return len(queue_sources)

    async def wait(self) -> None:
        await asyncio.gather(*self._background)

    async def shutdown(self) -> None:
        self.stop_event.set()
        if self._background:
            await asyncio.gather(*self._background, return_exceptions=True)
        with contextlib.suppress(Exception):
            await self.http.aclose()
        await self.store.close()
