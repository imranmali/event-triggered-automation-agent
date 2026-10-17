from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import httpx
from tests.conftest import FAST_RETRY, make_config

from eta.actions import ActionRegistry
from eta.actions.builtin import register_builtins
from eta.config import Settings
from eta.engine.executor import Executor
from eta.engine.worker import Worker, WorkerConfig
from eta.errors import PermanentError, RetryableError
from eta.models import DeadLetterStatus, RunStatus, StepStatus, utcnow
from eta.runtime import Runtime

API = "https://api.example.com/hook"


def workflow(name="wf", steps=None, retry=None, **kw):
    return {
        "name": name,
        "trigger": {"source": "test", "event": kw.pop("event", "thing.happened")},
        "retry": retry or FAST_RETRY,
        "steps": steps
        or [{"id": "call", "action": "http_request", "with": {"url": API, "json": {"a": 1}}}],
        **kw,
    }


async def publish(rt: Runtime, key="evt-1", payload=None, event_type="thing.happened"):
    return await rt.ingest.ingest(
        source="test", event_type=event_type, payload=payload or {"n": 2}, idempotency_key=key
    )


async def run_status(rt: Runtime, run_id: str) -> RunStatus:
    return (await rt.store.get_run(run_id)).status


# ---------------------------------------------------------------- happy path


async def test_multi_step_workflow_passes_outputs_between_steps(make_runtime, recorder):
    recorder.default = httpx.Response(201, json={"id": "remote-1"})
    rt = make_runtime(
        [
            workflow(
                steps=[
                    {
                        "id": "compute",
                        "action": "transform",
                        "with": {"output": {"double": "{{ event.payload.n * 2 }}"}},
                    },
                    {
                        "id": "call",
                        "action": "http_request",
                        "with": {
                            "url": "{{ secrets.API_URL }}/x",
                            "json": {"value": "{{ steps.compute.output.double }}"},
                        },
                    },
                    {
                        "id": "after",
                        "action": "transform",
                        "with": {"output": "{{ steps.call.output.body.id }}"},
                    },
                ]
            )
        ]
    )
    res = await publish(rt)
    await rt.worker.drain_ready()

    [run_id] = res.run_ids
    assert await run_status(rt, run_id) == RunStatus.SUCCEEDED
    [req] = recorder.requests
    assert str(req.url) == "https://api.example.com/x"
    assert json.loads(req.content) == {"value": 4}
    steps = {s.step_id: s for s in await rt.store.get_steps(run_id)}
    assert steps["after"].output == "remote-1"
    assert all(s.attempts == 1 for s in steps.values())


async def test_fan_out_one_event_many_workflows(make_runtime, recorder):
    rt = make_runtime([workflow("a"), workflow("b"), workflow("c", event="other")])
    res = await publish(rt)
    await rt.worker.drain_ready()
    assert len(res.run_ids) == 2
    assert len(recorder.requests) == 2


# --------------------------------------------------------- retry semantics


async def test_transient_failure_retried_then_succeeds(make_runtime, recorder):
    recorder.responses = [httpx.Response(503), httpx.ConnectError("refused")]
    rt = make_runtime([workflow()])
    [run_id] = (await publish(rt)).run_ids
    await rt.worker.drain_ready()

    assert await run_status(rt, run_id) == RunStatus.SUCCEEDED
    assert len(recorder.requests) == 3
    # Same idempotency key on every attempt so the downstream can dedupe.
    keys = {r.headers["idempotency-key"] for r in recorder.requests}
    assert keys == {f"{run_id}:0:call"}
    run = await rt.store.get_run(run_id)
    assert run.attempts == 3
    kinds = [h.kind for h in await rt.store.get_history(run_id)]
    assert kinds.count("retry_scheduled") == 2


async def test_retries_exhausted_goes_to_dlq(make_runtime, recorder):
    recorder.default = httpx.Response(500, text="down")
    rt = make_runtime([workflow()])
    [run_id] = (await publish(rt)).run_ids
    await rt.worker.drain_ready()

    assert await run_status(rt, run_id) == RunStatus.DEAD_LETTERED
    assert len(recorder.requests) == FAST_RETRY["max_attempts"]
    [dl] = await rt.store.list_dead_letters()
    assert dl.run_id == run_id and dl.reason == "retries_exhausted"
    assert "HTTP 500" in dl.error and "attempt 3/3" in dl.error


async def test_permanent_error_skips_retries(make_runtime, recorder):
    recorder.default = httpx.Response(422, json={"error": "invalid"})
    rt = make_runtime([workflow()])
    [run_id] = (await publish(rt)).run_ids
    await rt.worker.drain_ready()

    assert await run_status(rt, run_id) == RunStatus.DEAD_LETTERED
    assert len(recorder.requests) == 1
    [dl] = await rt.store.list_dead_letters()
    assert dl.reason == "permanent_error"


async def test_retry_after_header_is_honoured(make_runtime, recorder):
    recorder.responses = [httpx.Response(429, headers={"Retry-After": "120"})]
    rt = make_runtime([workflow()])
    [run_id] = (await publish(rt)).run_ids
    before = utcnow()
    await rt.worker.drain_ready()
    run = await rt.store.get_run(run_id)
    assert run.status == RunStatus.QUEUED
    assert run.next_attempt_at >= before + timedelta(seconds=119)


async def test_backoff_schedules_future_attempt(make_runtime, recorder):
    recorder.default = httpx.Response(503)
    rt = make_runtime(
        [workflow(retry={"max_attempts": 5, "initial_delay": "10s", "jitter": "none"})]
    )
    [run_id] = (await publish(rt)).run_ids
    before = utcnow()
    await rt.worker.drain_ready()
    run = await rt.store.get_run(run_id)
    assert run.status == RunStatus.QUEUED
    assert before + timedelta(seconds=9) <= run.next_attempt_at <= utcnow() + timedelta(seconds=10)
    assert len(recorder.requests) == 1  # not retried before its time


async def test_step_level_retry_policy_overrides_workflow(make_runtime, recorder):
    recorder.default = httpx.Response(503)
    rt = make_runtime(
        [
            workflow(
                steps=[
                    {
                        "id": "call",
                        "action": "http_request",
                        "retry": {**FAST_RETRY, "max_attempts": 5},
                        "with": {"url": API},
                    }
                ]
            )
        ]
    )
    await publish(rt)
    await rt.worker.drain_ready()
    assert len(recorder.requests) == 5


async def test_step_timeout_is_retryable(make_runtime):
    rt = make_runtime(
        [
            workflow(
                retry={**FAST_RETRY, "max_attempts": 2},
                steps=[
                    {"id": "slow", "action": "delay", "timeout": "50ms", "with": {"seconds": 5}}
                ],
            )
        ]
    )
    [run_id] = (await publish(rt)).run_ids
    await rt.worker.drain_ready()
    [dl] = await rt.store.list_dead_letters()
    assert dl.reason == "retries_exhausted" and "timed out" in dl.error
    assert (await rt.store.get_steps(run_id))[0].attempts == 2


# ------------------------------------------------------ idempotent execution


async def test_completed_steps_are_not_re_executed_on_retry(make_runtime, recorder):
    """Step 1 has a side effect; step 2 fails twice. Step 1 must run exactly once."""
    rt = make_runtime(
        [
            workflow(
                steps=[
                    {"id": "charge", "action": "http_request", "with": {"url": API}},
                    {
                        "id": "flaky",
                        "action": "fail",
                        "with": {"mode": "retryable", "succeed_on_attempt": 3},
                    },
                ]
            )
        ]
    )
    [run_id] = (await publish(rt)).run_ids
    await rt.worker.drain_ready()
    assert await run_status(rt, run_id) == RunStatus.SUCCEEDED
    assert len(recorder.requests) == 1
    steps = {s.step_id: s for s in await rt.store.get_steps(run_id)}
    assert steps["charge"].attempts == 1 and steps["flaky"].attempts == 3


async def test_duplicate_event_delivery_executes_once(make_runtime, recorder):
    rt = make_runtime([workflow()])
    results = await asyncio.gather(*[publish(rt, key="same") for _ in range(5)])
    assert sum(not r.duplicate for r in results) == 1
    await rt.worker.drain_ready()
    assert len(recorder.requests) == 1


async def test_resume_replay_after_fix_continues_from_failed_step(make_runtime, recorder):
    rt = make_runtime(
        [
            workflow(
                steps=[
                    {"id": "charge", "action": "http_request", "with": {"url": API}},
                    {"id": "notify", "action": "http_request", "with": {"url": API + "/notify"}},
                ]
            )
        ]
    )
    recorder.responses = [httpx.Response(200), httpx.Response(400, text="bad template")]
    [run_id] = (await publish(rt)).run_ids
    await rt.worker.drain_ready()
    [dl] = await rt.store.list_dead_letters()

    await rt.store.replay_dead_letter(dl.id)
    await rt.worker.drain_ready()
    assert await run_status(rt, run_id) == RunStatus.SUCCEEDED
    urls = [str(r.url) for r in recorder.requests]
    assert urls == [API, API + "/notify", API + "/notify"]  # charge not repeated
    assert (await rt.store.get_dead_letter(dl.id)).status == DeadLetterStatus.REPLAYED


async def test_restart_replay_uses_new_idempotency_keys(make_runtime, recorder):
    rt = make_runtime([workflow(retry={**FAST_RETRY, "max_attempts": 1})])
    recorder.responses = [httpx.Response(500)]
    [run_id] = (await publish(rt)).run_ids
    await rt.worker.drain_ready()
    [dl] = await rt.store.list_dead_letters()
    from eta.models import ReplayMode

    await rt.store.replay_dead_letter(dl.id, mode=ReplayMode.RESTART)
    await rt.worker.drain_ready()
    keys = [r.headers["idempotency-key"] for r in recorder.requests]
    assert keys == [f"{run_id}:0:call", f"{run_id}:1:call"]


# ----------------------------------------------------- control flow features


async def test_when_condition_skips_step(make_runtime, recorder):
    rt = make_runtime(
        [
            workflow(
                steps=[
                    {
                        "id": "big",
                        "action": "http_request",
                        "when": "event.payload.n > 10",
                        "with": {"url": API},
                    },
                    {
                        "id": "after",
                        "action": "transform",
                        "with": {"output": "{{ steps.big.status }}"},
                    },
                ]
            )
        ]
    )
    [run_id] = (await publish(rt, payload={"n": 1})).run_ids
    await rt.worker.drain_ready()
    steps = {s.step_id: s for s in await rt.store.get_steps(run_id)}
    assert steps["big"].status == StepStatus.SKIPPED
    assert steps["after"].output == "skipped"
    assert recorder.requests == []


async def test_continue_on_error(make_runtime, recorder):
    recorder.responses = [httpx.Response(404)]
    rt = make_runtime(
        [
            workflow(
                steps=[
                    {
                        "id": "optional",
                        "action": "http_request",
                        "continue_on_error": True,
                        "with": {"url": API},
                    },
                    {
                        "id": "after",
                        "action": "transform",
                        "with": {"output": "{{ steps.optional.output.reason }}"},
                    },
                ]
            )
        ]
    )
    [run_id] = (await publish(rt)).run_ids
    await rt.worker.drain_ready()
    assert await run_status(rt, run_id) == RunStatus.SUCCEEDED
    steps = {s.step_id: s for s in await rt.store.get_steps(run_id)}
    assert steps["optional"].status == StepStatus.FAILED_CONTINUED
    assert steps["after"].output == "permanent_error"


async def test_template_error_is_permanent(make_runtime):
    rt = make_runtime(
        [
            workflow(
                steps=[
                    {
                        "id": "t",
                        "action": "transform",
                        "with": {"output": "{{ event.payload.nope }}"},
                    }
                ]
            )
        ]
    )
    await publish(rt)
    await rt.worker.drain_ready()
    [dl] = await rt.store.list_dead_letters()
    assert dl.reason == "permanent_error" and "template error" in dl.error


async def test_emit_event_chains_workflows_idempotently(make_runtime, recorder):
    rt = make_runtime(
        [
            workflow(
                "parent",
                steps=[
                    {
                        "id": "emit",
                        "action": "emit_event",
                        "with": {
                            "event_type": "child.go",
                            "payload": {"x": "{{ event.payload.n }}"},
                        },
                    },
                    {"id": "flaky", "action": "fail", "with": {"succeed_on_attempt": 2}},
                ],
            ),
            {
                "name": "child",
                "trigger": {"source": "internal", "event": "child.go"},
                "steps": [
                    {
                        "id": "call",
                        "action": "http_request",
                        "with": {"url": API, "json": "{{ event.payload }}"},
                    }
                ],
            },
        ]
    )
    await publish(rt)
    await rt.worker.drain_ready()
    child_runs = await rt.store.list_runs(workflow="child")
    assert len(child_runs) == 1 and child_runs[0].status == RunStatus.SUCCEEDED
    assert [json.loads(r.content) for r in recorder.requests] == [{"x": 2}]


async def test_custom_python_action_plugin(store, recorder, tmp_path):
    registry = ActionRegistry()
    register_builtins(registry)
    calls = []

    @registry.action("enrich")
    async def enrich(params, ctx):
        calls.append((params, ctx.idempotency_key, ctx.attempt))
        if len(calls) == 1:
            raise RetryableError("cold cache")
        return {"vip": params["customer"] == "acme"}

    @registry.action("reject")
    async def reject(params, ctx):
        raise PermanentError("nope")

    from eta.workflows.registry import LoadedConfig, WorkflowRegistry, parse_workflow

    cfg = LoadedConfig(
        sources={},
        workflows=WorkflowRegistry(
            [
                parse_workflow(
                    workflow(steps=[{"id": "e", "action": "enrich", "with": {"customer": "acme"}}]),
                    registry,
                )
            ]
        ),
    )
    rt = Runtime.build(
        Settings(config_dir=tmp_path, worker_id="w"),
        store=store,
        config=cfg,
        actions=registry,
        http=recorder.client(),
    )
    [run_id] = (await publish(rt)).run_ids
    await rt.worker.drain_ready()
    assert [c[2] for c in calls] == [1, 2]
    assert (await rt.store.get_steps(run_id))[0].output == {"vip": True}


# --------------------------------------------- crash recovery & concurrency


async def test_reaper_recovers_run_from_crashed_worker(make_runtime, recorder):
    rt = make_runtime([workflow()])
    [run_id] = (await publish(rt)).run_ids
    # Worker "crashes" right after claiming and starting the step.
    [claimed] = await rt.store.claim_runs("dead-worker", limit=1, lease_seconds=0.2)
    await rt.store.start_step(run_id, claimed.lease_token, "call")
    await asyncio.sleep(0.3)

    assert await rt.worker.reap_expired() == 1
    run = await rt.store.get_run(run_id)
    assert run.status == RunStatus.QUEUED and "lease expired" in run.last_error
    await rt.worker.drain_ready()
    assert await run_status(rt, run_id) == RunStatus.SUCCEEDED
    assert (await rt.store.get_steps(run_id))[0].attempts == 2  # the crash counted


async def test_crash_loop_ends_in_dlq(make_runtime):
    rt = make_runtime([workflow(retry={**FAST_RETRY, "max_attempts": 2})])
    [run_id] = (await publish(rt)).run_ids
    for _ in range(2):
        [c] = await rt.store.claim_runs("crashy", limit=1, lease_seconds=0.05)
        await rt.store.start_step(run_id, c.lease_token, "call")
        await asyncio.sleep(0.1)
        await rt.worker.reap_expired()
    assert await run_status(rt, run_id) == RunStatus.DEAD_LETTERED
    [dl] = await rt.store.list_dead_letters()
    assert "lease expired" in dl.error


async def test_zombie_worker_cannot_complete_reclaimed_run(make_runtime, recorder):
    """A worker that stalls past its lease must not clobber the new owner."""
    gate = asyncio.Event()

    async def slow_handler(request: httpx.Request) -> httpx.Response:
        await gate.wait()
        return httpx.Response(200)

    rt = make_runtime([workflow()])
    rt.executor.http = httpx.AsyncClient(transport=httpx.MockTransport(slow_handler))
    [run_id] = (await publish(rt)).run_ids
    [zombie_claim] = await rt.store.claim_runs("zombie", limit=1, lease_seconds=0.1)
    zombie = asyncio.create_task(rt.executor.execute(zombie_claim))
    await asyncio.sleep(0.2)
    await rt.worker.reap_expired()  # lease expired -> requeued
    gate.set()

    from eta.errors import LeaseLostError

    try:
        await zombie
        raise AssertionError("zombie write should have been fenced")
    except LeaseLostError:
        pass
    await rt.worker.drain_ready()
    assert await run_status(rt, run_id) == RunStatus.SUCCEEDED


async def test_heartbeat_loss_cancels_execution(make_runtime):
    rt = make_runtime(
        [
            workflow(
                steps=[{"id": "slow", "action": "delay", "timeout": "10s", "with": {"seconds": 3}}]
            )
        ],
        lease_seconds=5,
        heartbeat_interval=0.1,
    )
    [run_id] = (await publish(rt)).run_ids
    stop = asyncio.Event()
    task = asyncio.create_task(rt.worker.run(stop))
    for _ in range(100):  # wait until the worker is inside the slow step
        if (await rt.store.get_run(run_id)).current_step == "slow":
            break
        await asyncio.sleep(0.02)
    # Simulate another worker stealing the run (e.g. after a network partition).
    from sqlalchemy import update

    from eta.db import schema as t

    async with rt.store.tx() as conn:
        await conn.execute(update(t.runs).where(t.runs.c.id == run_id).values(lease_token="stolen"))
    for _ in range(100):
        if rt.worker.inflight == 0:
            break
        await asyncio.sleep(0.02)
    assert rt.worker.inflight == 0  # abandoned well before the 3s step would finish
    assert (await rt.store.get_run(run_id)).status == RunStatus.RUNNING  # untouched by us
    stop.set()
    await task


async def test_graceful_shutdown_releases_inflight_runs(make_runtime):
    rt = make_runtime(
        [
            workflow(
                steps=[{"id": "slow", "action": "delay", "timeout": "10s", "with": {"seconds": 5}}]
            )
        ],
        shutdown_grace_seconds=0.1,
    )
    [run_id] = (await publish(rt)).run_ids
    stop = asyncio.Event()
    task = asyncio.create_task(rt.worker.run(stop))
    for _ in range(100):
        if (await rt.store.get_run(run_id)).current_step == "slow":
            break
        await asyncio.sleep(0.02)
    assert rt.worker.inflight == 1
    stop.set()
    await asyncio.wait_for(task, 3)
    run = await rt.store.get_run(run_id)
    assert run.status == RunStatus.QUEUED
    assert run.attempts == 0  # released without penalty
    assert (await rt.store.get_steps(run_id))[0].attempts == 0


async def test_many_workers_execute_each_run_exactly_once(store, recorder, tmp_path):
    cfg = make_config([workflow()])
    workers = []
    for i in range(4):
        executor = Executor(store, _registry(), http=recorder.client(), emit=_no_emit)
        workers.append(
            Worker(
                store,
                executor,
                WorkerConfig(
                    worker_id=f"w{i}",
                    concurrency=5,
                    poll_interval=0.02,
                    heartbeat_interval=1,
                    lease_seconds=10,
                ),
            )
        )
    from eta.ingress.service import IngestService

    ingest = IngestService(store, cfg.workflows)
    for i in range(60):
        await ingest.ingest(
            source="test", event_type="thing.happened", payload={}, idempotency_key=f"e{i}"
        )

    stop = asyncio.Event()
    tasks = [asyncio.create_task(w.run(stop)) for w in workers]
    for _ in range(200):
        stats = await store.stats()
        if stats["runs_by_status"]["succeeded"] == 60:
            break
        await asyncio.sleep(0.05)
    stop.set()
    await asyncio.gather(*tasks)

    assert (await store.stats())["runs_by_status"]["succeeded"] == 60
    keys = [r.headers["idempotency-key"] for r in recorder.requests]
    assert len(keys) == 60 and len(set(keys)) == 60  # every run executed exactly once
    owners = set()
    for run in await store.list_runs(limit=100):
        owners.update(
            h.detail.get("worker") for h in await store.get_history(run.id) if h.kind == "claimed"
        )
    assert len(owners) > 1  # work was actually spread across workers


async def test_worker_wakes_immediately_on_ingest(make_runtime, recorder):
    rt = make_runtime([workflow()], poll_interval=30)
    stop = asyncio.Event()
    task = asyncio.create_task(rt.worker.run(stop))
    await asyncio.sleep(0.05)
    [run_id] = (await publish(rt)).run_ids
    for _ in range(50):
        if await run_status(rt, run_id) == RunStatus.SUCCEEDED:
            break
        await asyncio.sleep(0.02)
    stop.set()
    await task
    assert await run_status(rt, run_id) == RunStatus.SUCCEEDED


def _registry() -> ActionRegistry:
    r = ActionRegistry()
    register_builtins(r)
    return r


async def _no_emit(*_args):
    raise AssertionError("emit not expected")
