#!/usr/bin/env python3
"""Throughput benchmark: N events -> N runs (3 steps each) through W workers.

    python scripts/benchmark.py --db postgresql+asyncpg://eta:eta@localhost:5432/eta --events 2000 --workers 4

Verifies exactly-once execution afterwards (every run succeeded, every step
attempted once).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import httpx

from eta.actions import default_registry
from eta.db.schema import metadata
from eta.db.store import Store
from eta.engine.executor import Executor
from eta.engine.worker import Worker, WorkerConfig
from eta.ingress.service import IngestService
from eta.workflows.registry import WorkflowRegistry, parse_workflow

WORKFLOW = {
    "name": "bench",
    "trigger": {"source": "bench"},
    "steps": [
        {"id": "a", "action": "transform", "with": {"output": "{{ event.payload.n * 2 }}"}},
        {
            "id": "b",
            "action": "http_request",
            "with": {"url": "http://sink.local/", "json": {"v": "{{ steps.a.output }}"}},
        },
        {"id": "c", "action": "transform", "with": {"output": "{{ steps.b.output.status }}"}},
    ],
}


async def main(db: str, events: int, workers: int, concurrency: int, processes: int) -> None:
    logging.disable(logging.WARNING)
    store = Store.from_url(db, pool_size=workers * concurrency + 5)
    async with store.engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
        await conn.run_sync(metadata.create_all)
    registry = WorkflowRegistry([parse_workflow(WORKFLOW, default_registry)])
    ingest = IngestService(store, registry)
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={})))

    t0 = time.perf_counter()
    sem = asyncio.Semaphore(32)

    async def one(i: int) -> None:
        async with sem:
            await ingest.ingest(
                source="bench", event_type="e", payload={"n": i}, idempotency_key=f"k{i}"
            )

    await asyncio.gather(*(one(i) for i in range(events)))
    ingest_s = time.perf_counter() - t0

    t1 = time.perf_counter()
    if processes > 1:
        import multiprocessing as mp

        ctx = mp.get_context("spawn")
        procs = [
            ctx.Process(target=_worker_process, args=(db, workers, concurrency, events, i))
            for i in range(processes)
        ]
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join()
        await _report(
            store,
            events,
            ingest_s,
            time.perf_counter() - t1,
            f"{processes}proc x {workers}x{concurrency}",
        )
        await http.aclose()
        await store.close()
        return

    pool = [
        Worker(
            store,
            Executor(store, default_registry, http=http, emit=ingest.emit),
            WorkerConfig(
                worker_id=f"w{i}",
                concurrency=concurrency,
                poll_interval=0.05,
                heartbeat_interval=10,
                lease_seconds=60,
            ),
        )
        for i in range(workers)
    ]
    stop = asyncio.Event()
    tasks = [asyncio.create_task(w.run(stop)) for w in pool]
    while (await store.stats())["runs_by_status"]["succeeded"] < events:
        await asyncio.sleep(0.1)
    exec_s = time.perf_counter() - t1
    stop.set()
    await asyncio.gather(*tasks)

    await _report(store, events, ingest_s, exec_s, f"{workers}x{concurrency}")
    await http.aclose()
    await store.close()


async def _report(store: Store, events: int, ingest_s: float, exec_s: float, shape: str) -> None:
    stats = await store.stats()
    print(f"backend={store.dialect} events={events} workers={shape}")
    print(f"ingest : {events / ingest_s:8.0f} events/s ({ingest_s:.2f}s)")
    print(f"execute: {events / exec_s:8.0f} runs/s   ({exec_s:.2f}s, 3 steps/run)")
    print(f"result : {stats['runs_by_status']}")


def _worker_process(db: str, workers: int, concurrency: int, events: int, index: int) -> None:
    """One OS process running ``workers`` worker loops until all runs are done."""

    async def go() -> None:
        logging.disable(logging.WARNING)
        store = Store.from_url(db, pool_size=workers * concurrency + 2)
        registry = WorkflowRegistry([parse_workflow(WORKFLOW, default_registry)])
        ingest = IngestService(store, registry)
        http = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json={}))
        )
        pool = [
            Worker(
                store,
                Executor(store, default_registry, http=http, emit=ingest.emit),
                WorkerConfig(
                    worker_id=f"p{index}w{i}",
                    concurrency=concurrency,
                    poll_interval=0.05,
                    heartbeat_interval=10,
                    lease_seconds=60,
                ),
            )
            for i in range(workers)
        ]
        stop = asyncio.Event()
        tasks = [asyncio.create_task(w.run(stop)) for w in pool]
        while (await store.stats())["runs_by_status"]["succeeded"] < events:
            await asyncio.sleep(0.1)
        stop.set()
        await asyncio.gather(*tasks)
        await store.close()

    asyncio.run(go())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="sqlite+aiosqlite:///./bench.db")
    p.add_argument("--events", type=int, default=1000)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--processes", type=int, default=1, help="worker OS processes")
    a = p.parse_args()
    asyncio.run(main(a.db, a.events, a.workers, a.concurrency, a.processes))
