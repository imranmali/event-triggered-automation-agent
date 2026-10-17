from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from eta.db.store import Store
from eta.errors import LeaseLostError
from eta.models import (
    DeadLetterStatus,
    NewEvent,
    ReplayMode,
    RunStatus,
    StepStatus,
    WorkflowRef,
    utcnow,
)

WF = WorkflowRef("wf", "v1", {"name": "wf"})
WF2 = WorkflowRef("wf2", "v1", {"name": "wf2"})


def ev(key: str = "k1", **kw) -> NewEvent:
    return NewEvent(source="src", event_type="t", payload={"a": 1}, idempotency_key=key, **kw)


async def test_ingest_creates_event_and_runs(store: Store):
    res = await store.ingest_event(ev(), [WF, WF2])
    assert not res.duplicate and len(res.run_ids) == 2
    runs = await store.list_runs(event_id=res.event_id)
    assert {r.workflow_name for r in runs} == {"wf", "wf2"}
    assert all(r.status == RunStatus.QUEUED for r in runs)


async def test_ingest_is_idempotent(store: Store):
    first = await store.ingest_event(ev(), [WF])
    again = await store.ingest_event(ev(), [WF, WF2])  # even if config changed meanwhile
    assert again.duplicate
    assert again.event_id == first.event_id
    assert again.run_ids == first.run_ids
    assert len(await store.list_runs()) == 1


async def test_same_key_different_source_is_not_duplicate(store: Store):
    await store.ingest_event(ev(), [WF])
    other = NewEvent(source="other", event_type="t", payload=None, idempotency_key="k1")
    assert not (await store.ingest_event(other, [WF])).duplicate


async def test_concurrent_duplicate_ingest_creates_one_event(store: Store):
    results = await asyncio.gather(*[store.ingest_event(ev("same"), [WF]) for _ in range(10)])
    assert sum(not r.duplicate for r in results) == 1
    assert len({r.event_id for r in results}) == 1
    assert len(await store.list_runs()) == 1


async def test_claim_is_exclusive(store: Store):
    for i in range(20):
        await store.ingest_event(ev(f"k{i}"), [WF])
    batches = await asyncio.gather(
        *[store.claim_runs(f"w{i}", limit=5, lease_seconds=30) for i in range(8)]
    )
    claimed = [c.run.id for b in batches for c in b]
    assert len(claimed) == 20
    assert len(set(claimed)) == 20  # no run handed to two workers


async def test_claim_respects_next_attempt_at(store: Store):
    await store.ingest_event(ev(), [WF])
    [c] = await store.claim_runs("w", limit=10, lease_seconds=30)
    await store.schedule_retry(c.run.id, c.lease_token, delay=3600, error="boom")
    assert await store.claim_runs("w", limit=10, lease_seconds=30) == []


async def test_claim_payload(store: Store):
    await store.ingest_event(ev(), [WF])
    [c] = await store.claim_runs("w", limit=1, lease_seconds=30)
    assert c.run.status == RunStatus.RUNNING
    assert c.run.attempts == 1
    assert c.event.payload == {"a": 1}
    assert c.workflow.spec == {"name": "wf"}
    assert c.steps == {}


async def test_fencing_rejects_stale_token(store: Store):
    await store.ingest_event(ev(), [WF])
    [c] = await store.claim_runs("w", limit=1, lease_seconds=30)
    with pytest.raises(LeaseLostError):
        await store.start_step(c.run.id, "not-the-token", "a")
    with pytest.raises(LeaseLostError):
        await store.complete_run(c.run.id, "not-the-token")
    await store.complete_run(c.run.id, c.lease_token)
    with pytest.raises(LeaseLostError):  # terminal runs can't be touched either
        await store.heartbeat(c.run.id, c.lease_token, 30)


async def test_step_checkpoints_and_attempt_counting(store: Store):
    await store.ingest_event(ev(), [WF])
    [c] = await store.claim_runs("w", limit=1, lease_seconds=30)
    assert await store.start_step(c.run.id, c.lease_token, "a") == 1
    await store.schedule_retry(c.run.id, c.lease_token, delay=0, error="x", step_id="a")
    [c2] = await store.claim_runs("w", limit=1, lease_seconds=30)
    assert c2.steps["a"].status == StepStatus.FAILED
    assert c2.steps["a"].attempts == 1
    assert await store.start_step(c2.run.id, c2.lease_token, "a") == 2
    await store.finish_step(c2.run.id, c2.lease_token, "a", status=StepStatus.SUCCEEDED, output={"x": 1})
    steps = await store.get_steps(c.run.id)
    assert steps[0].output == {"x": 1} and steps[0].attempts == 2


async def test_expired_lease_detection_requires_expiry(store: Store):
    now = [utcnow()]
    store.clock = lambda: now[0]
    await store.ingest_event(ev(), [WF])
    [c] = await store.claim_runs("w", limit=1, lease_seconds=10)
    await store.start_step(c.run.id, c.lease_token, "a")
    assert await store.find_expired_leases() == []
    with pytest.raises(LeaseLostError):  # not expired yet: reaper must not steal it
        await store.schedule_retry(c.run.id, c.lease_token, delay=0, error="x", require_expired=True)

    now[0] += timedelta(seconds=11)
    [lease] = await store.find_expired_leases()
    assert lease.current_step == "a" and lease.step_attempts == 1 and lease.lease_owner == "w"
    await store.schedule_retry(
        c.run.id, lease.lease_token, delay=0, error="lost", step_id="a", require_expired=True
    )
    # The zombie worker wakes up and tries to finish: rejected.
    with pytest.raises(LeaseLostError):
        await store.finish_step(c.run.id, c.lease_token, "a", status=StepStatus.SUCCEEDED)


async def test_heartbeat_extends_lease(store: Store):
    now = [utcnow()]
    store.clock = lambda: now[0]
    await store.ingest_event(ev(), [WF])
    [c] = await store.claim_runs("w", limit=1, lease_seconds=10)
    now[0] += timedelta(seconds=8)
    await store.heartbeat(c.run.id, c.lease_token, 10)
    now[0] += timedelta(seconds=8)
    assert await store.find_expired_leases() == []


async def test_release_does_not_consume_attempts(store: Store):
    await store.ingest_event(ev(), [WF])
    [c] = await store.claim_runs("w", limit=1, lease_seconds=30)
    await store.start_step(c.run.id, c.lease_token, "a")
    await store.release_run(c.run.id, c.lease_token, step_id="a")
    [c2] = await store.claim_runs("w", limit=1, lease_seconds=30)
    assert c2.run.attempts == 1
    assert c2.steps["a"].attempts == 0


async def test_dead_letter_and_resume_replay(store: Store):
    await store.ingest_event(ev(), [WF])
    [c] = await store.claim_runs("w", limit=1, lease_seconds=30)
    await store.start_step(c.run.id, c.lease_token, "a")
    await store.finish_step(c.run.id, c.lease_token, "a", status=StepStatus.SUCCEEDED, output=1)
    await store.start_step(c.run.id, c.lease_token, "b")
    dlq_id = await store.dead_letter_run(
        c.run.id, c.lease_token, reason="permanent_error", error="bad", step_id="b"
    )
    run = await store.get_run(c.run.id)
    assert run.status == RunStatus.DEAD_LETTERED and run.last_error == "bad"
    [dl] = await store.list_dead_letters()
    assert dl.id == dlq_id and dl.workflow_name == "wf" and dl.reason == "permanent_error"

    assert await store.replay_dead_letter(dlq_id) == c.run.id
    assert (await store.get_dead_letter(dlq_id)).status == DeadLetterStatus.REPLAYED
    [c2] = await store.claim_runs("w", limit=1, lease_seconds=30)
    assert set(c2.steps) == {"a"}  # succeeded checkpoint kept, failed step reset
    assert c2.run.generation == 0

    with pytest.raises(ValueError):  # can't replay twice
        await store.replay_dead_letter(dlq_id)


async def test_restart_replay_clears_checkpoints_and_bumps_generation(store: Store):
    await store.ingest_event(ev(), [WF])
    [c] = await store.claim_runs("w", limit=1, lease_seconds=30)
    await store.start_step(c.run.id, c.lease_token, "a")
    await store.finish_step(c.run.id, c.lease_token, "a", status=StepStatus.SUCCEEDED)
    dlq_id = await store.dead_letter_run(c.run.id, c.lease_token, reason="r", error="e")
    new_wf = WorkflowRef("wf", "v2", {"name": "wf", "fixed": True})
    await store.replay_dead_letter(dlq_id, mode=ReplayMode.RESTART, workflow=new_wf)
    [c2] = await store.claim_runs("w", limit=1, lease_seconds=30)
    assert c2.steps == {}
    assert c2.run.generation == 1
    assert c2.workflow.version == "v2" and c2.workflow.spec["fixed"] is True


async def test_discard_and_cancel(store: Store):
    await store.ingest_event(ev("a"), [WF])
    await store.ingest_event(ev("b"), [WF])
    [c] = await store.claim_runs("w", limit=1, lease_seconds=30)
    dlq_id = await store.dead_letter_run(c.run.id, c.lease_token, reason="r", error="e")
    assert await store.discard_dead_letter(dlq_id)
    assert not await store.discard_dead_letter(dlq_id)
    assert await store.list_dead_letters() == []

    [queued] = await store.list_runs(status=RunStatus.QUEUED)
    assert await store.cancel_run(queued.id)
    assert not await store.cancel_run(queued.id)
    assert not await store.cancel_run(c.run.id)


async def test_ingress_dead_letter(store: Store):
    dlq_id = await store.record_ingress_dead_letter(
        source="orders", reason="poison_message", error="bad json", payload={"raw": "{"}
    )
    dl = await store.get_dead_letter(dlq_id)
    assert dl.kind == "ingress" and dl.run_id is None
    with pytest.raises(ValueError):
        await store.replay_dead_letter(dlq_id)


async def test_history_and_stats(store: Store):
    await store.ingest_event(ev(), [WF, WF2])
    [c, _] = await store.claim_runs("w", limit=2, lease_seconds=30)
    await store.complete_run(c.run.id, c.lease_token)
    kinds = [h.kind for h in await store.get_history(c.run.id)]
    assert kinds == ["created", "claimed", "succeeded"]
    stats = await store.stats()
    assert stats["runs_by_status"]["succeeded"] == 1
    assert stats["runs_by_status"]["running"] == 1
    assert stats["ready"] == 0
    assert stats["dead_letters_open"] == 0


async def test_stats_queue_lag(store: Store):
    now = [utcnow()]
    store.clock = lambda: now[0]
    await store.ingest_event(ev(), [WF])
    now[0] += timedelta(seconds=42)
    stats = await store.stats()
    assert stats["ready"] == 1
    assert 41 <= stats["queue_lag_seconds"] <= 43
