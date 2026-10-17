from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import subprocess
import time
from collections.abc import AsyncIterator, Iterator

import pytest
from redis.asyncio import Redis

from eta.ingress.redis_streams import RedisStreamConsumer
from eta.models import DeadLetterKind, RunStatus
from eta.runtime import Runtime

REDIS_URL = os.environ.get("ETA_TEST_REDIS_URL")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture(scope="module")
def redis_url() -> Iterator[str]:
    if REDIS_URL:
        yield REDIS_URL
        return
    binary = shutil.which("redis-server")
    if not binary:
        pytest.skip("redis-server not available (set ETA_TEST_REDIS_URL)")
    port = _free_port()
    proc = subprocess.Popen(
        [binary, "--port", str(port), "--save", "", "--appendonly", "no"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
            break
        except OSError:
            time.sleep(0.05)
    yield f"redis://127.0.0.1:{port}/0"
    proc.terminate()
    proc.wait(5)


@pytest.fixture
async def redis(redis_url: str) -> AsyncIterator[Redis]:
    client = Redis.from_url(redis_url)
    await client.flushdb()
    yield client
    await client.aclose()


SOURCES = {
    "orders": {
        "redis_stream": {
            "stream": "orders",
            "group": "eta",
            "idempotency_field": "order_id",
            "max_deliveries": 3,
            "claim_idle": "100ms",
        }
    }
}
WORKFLOWS = [
    {
        "name": "on_order",
        "trigger": {"source": "orders", "event": "order.created"},
        "steps": [
            {"id": "t", "action": "transform", "with": {"output": "{{ event.payload.total }}"}}
        ],
    }
]


def consumer(rt: Runtime, redis: Redis, name: str = "c1") -> RedisStreamConsumer:
    return RedisStreamConsumer(
        redis, rt.config.sources["orders"], rt.ingest, rt.store, consumer_name=name, block_ms=50
    )


async def add(redis: Redis, **fields) -> str:
    return (await redis.xadd("orders", fields)).decode()


async def test_consume_ingest_ack_and_execute(make_runtime, redis):
    rt = make_runtime(WORKFLOWS, SOURCES)
    c = consumer(rt, redis)
    await c.ensure_group()
    await add(redis, event_type="order.created", order_id="o-1", payload=json.dumps({"total": 42}))
    assert await c.poll_once() == 1

    assert (await redis.xpending("orders", "eta"))["pending"] == 0  # acked
    [ev] = await rt.store.list_events()
    assert ev.idempotency_key == "o-1" and ev.ingress == "redis_stream"
    await rt.worker.drain_ready()
    [run] = await rt.store.list_runs()
    assert run.status == RunStatus.SUCCEEDED
    assert (await rt.store.get_steps(run.id))[0].output == 42


async def test_duplicate_messages_are_deduplicated(make_runtime, redis):
    rt = make_runtime(WORKFLOWS, SOURCES)
    c = consumer(rt, redis)
    await c.ensure_group()
    for _ in range(3):
        await add(redis, event_type="order.created", order_id="o-dup", payload="{}")
    await c.poll_once()
    assert len(await rt.store.list_events()) == 1
    assert len(await rt.store.list_runs()) == 1
    assert (await redis.xpending("orders", "eta"))["pending"] == 0


async def test_message_id_used_when_no_idempotency_field(make_runtime, redis):
    rt = make_runtime(WORKFLOWS, SOURCES)
    c = consumer(rt, redis)
    await c.ensure_group()
    msg_id = await add(redis, event_type="order.created", total="5")
    await c.poll_once()
    [ev] = await rt.store.list_events()
    assert ev.idempotency_key == f"orders:{msg_id}"
    assert ev.payload == {"total": "5"}  # flat fields become the payload


@pytest.mark.parametrize(
    "fields",
    [{"payload": "{broken"}, {"event_type": "order.created", "payload": "{broken"}],
)
async def test_poison_messages_are_dead_lettered_and_acked(make_runtime, redis, fields):
    rt = make_runtime(WORKFLOWS, SOURCES)
    c = consumer(rt, redis)
    await c.ensure_group()
    await add(redis, **fields)
    await c.poll_once()
    assert (await redis.xpending("orders", "eta"))["pending"] == 0
    [dl] = await rt.store.list_dead_letters()
    assert dl.kind == DeadLetterKind.INGRESS and dl.source == "orders"
    assert dl.payload["fields"] == fields


async def test_crashed_consumer_messages_are_reclaimed(make_runtime, redis):
    rt = make_runtime(WORKFLOWS, SOURCES)
    dead, alive = consumer(rt, redis, "dead"), consumer(rt, redis, "alive")
    await dead.ensure_group()
    await add(redis, event_type="order.created", order_id="o-9", payload="{}")
    # "dead" reads the message but crashes before ingesting/acking.
    await redis.xreadgroup("eta", "dead", {"orders": ">"}, count=10)
    assert (await redis.xpending("orders", "eta"))["pending"] == 1

    await asyncio.sleep(0.15)  # > claim_idle
    assert await alive.reclaim_stale() == 1
    assert (await redis.xpending("orders", "eta"))["pending"] == 0
    assert len(await rt.store.list_events()) == 1


async def test_ingest_failure_leaves_message_pending_then_poisons(make_runtime, redis, monkeypatch):
    rt = make_runtime(WORKFLOWS, SOURCES)
    c = consumer(rt, redis)
    await c.ensure_group()

    async def down(**_kw):
        raise ConnectionError("db down")

    monkeypatch.setattr(rt.ingest, "ingest", down)
    await add(redis, event_type="order.created", order_id="o-5", payload="{}")
    await c.poll_once()
    assert (await redis.xpending("orders", "eta"))["pending"] == 1  # not acked -> will retry

    for _ in range(3):  # redelivered until max_deliveries is exceeded
        await asyncio.sleep(0.15)
        await c.reclaim_stale()
    assert (await redis.xpending("orders", "eta"))["pending"] == 0
    [dl] = await rt.store.list_dead_letters()
    assert "delivered 4 times" in dl.error


async def test_run_loop_stops_cleanly(make_runtime, redis):
    rt = make_runtime(WORKFLOWS, SOURCES)
    c = consumer(rt, redis)
    stop = asyncio.Event()
    task = asyncio.create_task(c.run(stop))
    await add(redis, event_type="order.created", order_id="o-loop", payload="{}")
    for _ in range(50):
        if await rt.store.list_events():
            break
        await asyncio.sleep(0.02)
    stop.set()
    await asyncio.wait_for(task, 2)
    assert len(await rt.store.list_events()) == 1
