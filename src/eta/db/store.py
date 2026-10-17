"""Persistence layer: events, runs (the work queue), checkpoints and the DLQ.

Concurrency model
-----------------
* Workers *claim* runs with a single ``UPDATE ... RETURNING`` statement. On
  Postgres the candidate rows are selected ``FOR UPDATE SKIP LOCKED`` so many
  workers can claim in parallel without blocking each other; on SQLite the
  database-level write lock (``BEGIN IMMEDIATE``) gives the same atomicity.
* A claim stamps the run with a random ``lease_token`` (a fencing token).
  Every subsequent transition for that run is conditional on the token, so a
  worker whose lease expired and was reclaimed (a "zombie") can never
  overwrite the new owner's progress — its writes raise ``LeaseLostError``.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import Select, Table, and_, delete, event, func, insert, or_, select, update
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from eta.db import schema as t
from eta.errors import LeaseLostError
from eta.models import (
    ClaimedRun,
    DeadLetter,
    DeadLetterKind,
    DeadLetterStatus,
    Event,
    HistoryEntry,
    IngestResult,
    NewEvent,
    ReplayMode,
    Run,
    RunStatus,
    StepResult,
    StepStatus,
    WorkflowRef,
    new_id,
    utcnow,
)

log = logging.getLogger(__name__)

Clock = Callable[[], datetime]


@dataclass(slots=True)
class ExpiredLease:
    run_id: str
    lease_token: str
    lease_owner: str | None
    current_step: str | None
    workflow: WorkflowRef
    step_attempts: int


def create_engine(url: str, *, pool_size: int = 10, echo: bool = False) -> AsyncEngine:
    if url.startswith("sqlite"):
        engine = create_async_engine(url, echo=echo, connect_args={"timeout": 30})

        @event.listens_for(engine.sync_engine, "connect")
        def _sqlite_connect(dbapi_conn: Any, _record: Any) -> None:
            # Take over transaction control so we can issue BEGIN IMMEDIATE.
            dbapi_conn.isolation_level = None
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA busy_timeout=30000")
            cur.execute("PRAGMA foreign_keys=ON")
            cur.execute("PRAGMA synchronous=NORMAL")
            cur.close()

        @event.listens_for(engine.sync_engine, "begin")
        def _sqlite_begin(conn: Any) -> None:
            # Acquire the write lock up front: avoids SQLITE_BUSY lock-upgrade
            # deadlocks between concurrent read-then-write transactions.
            conn.exec_driver_sql("BEGIN IMMEDIATE")

        return engine
    return create_async_engine(
        url, echo=echo, pool_size=pool_size, max_overflow=pool_size, pool_pre_ping=True
    )


class Store:
    def __init__(self, engine: AsyncEngine, *, clock: Clock = utcnow) -> None:
        self.engine = engine
        self.clock = clock
        self.dialect = engine.dialect.name

    @classmethod
    def from_url(cls, url: str, *, pool_size: int = 10, clock: Clock = utcnow) -> Store:
        return cls(create_engine(url, pool_size=pool_size), clock=clock)

    async def create_all(self) -> None:
        """Create tables directly (tests/dev). Production uses ``eta db upgrade``."""
        async with self.engine.begin() as conn:
            await conn.run_sync(t.metadata.create_all)

    async def close(self) -> None:
        await self.engine.dispose()

    async def ping(self) -> None:
        async with self.engine.connect() as conn:
            await conn.execute(select(1))

    @asynccontextmanager
    async def tx(self) -> AsyncIterator[AsyncConnection]:
        async with self.engine.begin() as conn:
            yield conn

    def _insert_ignore(self, table: Table) -> Any:
        dialect_insert = postgresql.insert if self.dialect == "postgresql" else sqlite.insert
        return dialect_insert(table)

    # ------------------------------------------------------------------ ingest

    async def ingest_event(self, ev: NewEvent, workflows: Sequence[WorkflowRef]) -> IngestResult:
        """Persist an event and one run per matching workflow, atomically.

        Idempotent on ``(source, idempotency_key)``: a redelivered event returns
        the original event id with ``duplicate=True`` and creates nothing.
        """
        now = self.clock()
        async with self.tx() as conn:
            stmt = (
                self._insert_ignore(t.events)
                .values(
                    id=ev.id,
                    source=ev.source,
                    event_type=ev.event_type,
                    idempotency_key=ev.idempotency_key,
                    payload=ev.payload,
                    headers=ev.headers,
                    ingress=ev.ingress,
                    received_at=now,
                )
                .on_conflict_do_nothing(index_elements=["source", "idempotency_key"])
                .returning(t.events.c.id)
            )
            inserted = (await conn.execute(stmt)).scalar_one_or_none()
            if inserted is None:
                existing = (
                    await conn.execute(
                        select(t.events.c.id).where(
                            t.events.c.source == ev.source,
                            t.events.c.idempotency_key == ev.idempotency_key,
                        )
                    )
                ).scalar_one()
                run_ids = (
                    (await conn.execute(select(t.runs.c.id).where(t.runs.c.event_id == existing)))
                    .scalars()
                    .all()
                )
                return IngestResult(event_id=existing, duplicate=True, run_ids=list(run_ids))

            run_ids = []
            for wf in workflows:
                await self._save_workflow_version(conn, wf, now)
                run_id = new_id()
                await conn.execute(
                    insert(t.runs).values(
                        id=run_id,
                        event_id=ev.id,
                        workflow_name=wf.name,
                        workflow_version=wf.version,
                        status=RunStatus.QUEUED,
                        attempts=0,
                        generation=0,
                        next_attempt_at=now,
                        created_at=now,
                        updated_at=now,
                    )
                )
                await self._history(conn, run_id, "created", {"event_id": ev.id})
                run_ids.append(run_id)
            return IngestResult(event_id=ev.id, duplicate=False, run_ids=run_ids)

    async def _save_workflow_version(
        self, conn: AsyncConnection, wf: WorkflowRef, now: datetime
    ) -> None:
        await conn.execute(
            self._insert_ignore(t.workflow_versions)
            .values(name=wf.name, version=wf.version, spec=wf.spec, created_at=now)
            .on_conflict_do_nothing(index_elements=["name", "version"])
        )

    async def record_ingress_dead_letter(
        self, *, source: str, reason: str, error: str, payload: Any
    ) -> str:
        """Park a message that could not be ingested at all (poison message)."""
        dlq_id = new_id()
        async with self.tx() as conn:
            await conn.execute(
                insert(t.dead_letters).values(
                    id=dlq_id,
                    kind=DeadLetterKind.INGRESS,
                    status=DeadLetterStatus.OPEN,
                    source=source,
                    reason=reason,
                    error=error,
                    payload=payload,
                    replay_count=0,
                    created_at=self.clock(),
                )
            )
        return dlq_id

    # ------------------------------------------------------------- claiming

    async def claim_runs(
        self, worker_id: str, *, limit: int, lease_seconds: float
    ) -> list[ClaimedRun]:
        if limit <= 0:
            return []
        now = self.clock()
        token = new_id()
        candidates = (
            select(t.runs.c.id)
            .where(t.runs.c.status == RunStatus.QUEUED, t.runs.c.next_attempt_at <= now)
            .order_by(t.runs.c.next_attempt_at)
            .limit(limit)
        )
        if self.dialect == "postgresql":
            # A MATERIALIZED CTE guarantees the LIMIT/SKIP LOCKED selection runs
            # exactly once; an ``IN (subquery)`` may be re-evaluated by the
            # planner and claim more than ``limit`` rows.
            cte = (
                candidates.with_for_update(skip_locked=True)
                .cte("candidates")
                .prefix_with("MATERIALIZED")
            )
            target = t.runs.c.id == cte.c.id
        else:
            target = t.runs.c.id.in_(candidates.scalar_subquery())
        stmt = (
            update(t.runs)
            .where(target)
            .where(t.runs.c.status == RunStatus.QUEUED)
            .values(
                status=RunStatus.RUNNING,
                lease_owner=worker_id,
                lease_token=token,
                lease_expires_at=now + timedelta(seconds=lease_seconds),
                attempts=t.runs.c.attempts + 1,
                updated_at=now,
            )
            .returning(*t.runs.c)
        )
        async with self.tx() as conn:
            rows = (await conn.execute(stmt)).all()
            if not rows:
                return []
            claimed_runs = [_run(r) for r in rows]
            ids = [r.id for r in claimed_runs]
            for r in claimed_runs:
                await self._history(
                    conn, r.id, "claimed", {"worker": worker_id, "attempt": r.attempts}
                )

            ev_rows = (
                await conn.execute(
                    select(t.events).where(t.events.c.id.in_({r.event_id for r in claimed_runs}))
                )
            ).all()
            evs = {row.id: _event(row) for row in ev_rows}
            wf_rows = (
                await conn.execute(
                    select(t.workflow_versions).where(
                        or_(
                            *[
                                and_(
                                    t.workflow_versions.c.name == r.workflow_name,
                                    t.workflow_versions.c.version == r.workflow_version,
                                )
                                for r in claimed_runs
                            ]
                        )
                    )
                )
            ).all()
            wfs = {(w.name, w.version): WorkflowRef(w.name, w.version, w.spec) for w in wf_rows}
            step_rows = (
                await conn.execute(select(t.step_results).where(t.step_results.c.run_id.in_(ids)))
            ).all()
            steps: dict[str, dict[str, StepResult]] = {i: {} for i in ids}
            for s in step_rows:
                steps[s.run_id][s.step_id] = _step(s)

        return [
            ClaimedRun(
                run=r,
                lease_token=token,
                event=evs[r.event_id],
                workflow=wfs[(r.workflow_name, r.workflow_version)],
                steps=steps[r.id],
            )
            for r in claimed_runs
        ]

    # ------------------------------------------------- fenced transitions

    async def _fence(
        self,
        conn: AsyncConnection,
        run_id: str,
        token: str,
        *,
        require_expired: bool = False,
        **values: Any,
    ) -> None:
        """Assert (and lock) ownership of a running run; optionally update it."""
        now = self.clock()
        conds = [
            t.runs.c.id == run_id,
            t.runs.c.lease_token == token,
            t.runs.c.status == RunStatus.RUNNING,
        ]
        if require_expired:
            conds.append(t.runs.c.lease_expires_at < now)
        result = await conn.execute(update(t.runs).where(*conds).values(updated_at=now, **values))
        if result.rowcount != 1:
            raise LeaseLostError(f"lease lost for run {run_id}")

    async def heartbeat(self, run_id: str, token: str, lease_seconds: float) -> None:
        async with self.tx() as conn:
            await self._fence(
                conn,
                run_id,
                token,
                lease_expires_at=self.clock() + timedelta(seconds=lease_seconds),
            )

    async def start_step(self, run_id: str, token: str, step_id: str) -> int:
        """Checkpoint that a step is about to run; returns its attempt number.

        Attempts are counted *before* executing so that a worker crash inside a
        step still consumes an attempt (protects against crash loops).
        """
        now = self.clock()
        async with self.tx() as conn:
            await self._fence(conn, run_id, token, current_step=step_id)
            existing = (
                await conn.execute(
                    select(t.step_results.c.attempts).where(
                        t.step_results.c.run_id == run_id, t.step_results.c.step_id == step_id
                    )
                )
            ).scalar_one_or_none()
            attempts = (existing or 0) + 1
            if existing is None:
                await conn.execute(
                    insert(t.step_results).values(
                        run_id=run_id,
                        step_id=step_id,
                        status=StepStatus.RUNNING,
                        attempts=attempts,
                        started_at=now,
                    )
                )
            else:
                await conn.execute(
                    update(t.step_results)
                    .where(t.step_results.c.run_id == run_id, t.step_results.c.step_id == step_id)
                    .values(
                        status=StepStatus.RUNNING,
                        attempts=attempts,
                        started_at=now,
                        finished_at=None,
                    )
                )
            await self._history(
                conn, run_id, "step_started", {"step": step_id, "attempt": attempts}
            )
            return attempts

    async def finish_step(
        self,
        run_id: str,
        token: str,
        step_id: str,
        *,
        status: StepStatus,
        output: Any = None,
        error: str | None = None,
    ) -> None:
        now = self.clock()
        async with self.tx() as conn:
            await self._fence(conn, run_id, token, current_step=None)
            values = {"status": status, "output": output, "error": error, "finished_at": now}
            result = await conn.execute(
                update(t.step_results)
                .where(t.step_results.c.run_id == run_id, t.step_results.c.step_id == step_id)
                .values(**values)
            )
            if result.rowcount == 0:
                # Skipped steps are never "started".
                await conn.execute(
                    insert(t.step_results).values(
                        run_id=run_id, step_id=step_id, attempts=0, started_at=now, **values
                    )
                )
            detail: dict[str, Any] = {"step": step_id}
            if error:
                detail["error"] = error
            await self._history(conn, run_id, f"step_{status.value}", detail)

    async def complete_run(self, run_id: str, token: str) -> None:
        async with self.tx() as conn:
            await self._fence(
                conn,
                run_id,
                token,
                status=RunStatus.SUCCEEDED,
                finished_at=self.clock(),
                last_error=None,
                **_cleared_lease(),
            )
            await self._history(conn, run_id, "succeeded", {})

    async def schedule_retry(
        self,
        run_id: str,
        token: str,
        *,
        delay: float,
        error: str,
        step_id: str | None = None,
        require_expired: bool = False,
    ) -> datetime:
        """Fail the current step (if any) and requeue the run, in one transaction."""
        next_at = self.clock() + timedelta(seconds=delay)
        async with self.tx() as conn:
            await self._fence(
                conn,
                run_id,
                token,
                require_expired=require_expired,
                status=RunStatus.QUEUED,
                next_attempt_at=next_at,
                last_error=error,
                **_cleared_lease(),
            )
            await self._mark_step_failed(conn, run_id, step_id, error)
            await self._history(
                conn,
                run_id,
                "retry_scheduled",
                {"delay_seconds": round(delay, 3), "error": error},
            )
        return next_at

    async def dead_letter_run(
        self,
        run_id: str,
        token: str,
        *,
        reason: str,
        error: str,
        step_id: str | None = None,
        require_expired: bool = False,
    ) -> str:
        """Fail the current step (if any) and park the run in the DLQ, atomically."""
        now = self.clock()
        dlq_id = new_id()
        async with self.tx() as conn:
            await self._fence(
                conn,
                run_id,
                token,
                require_expired=require_expired,
                status=RunStatus.DEAD_LETTERED,
                finished_at=now,
                last_error=error,
                **_cleared_lease(),
            )
            await self._mark_step_failed(conn, run_id, step_id, error)
            wf_name = (
                await conn.execute(select(t.runs.c.workflow_name).where(t.runs.c.id == run_id))
            ).scalar_one()
            await conn.execute(
                insert(t.dead_letters).values(
                    id=dlq_id,
                    kind=DeadLetterKind.RUN,
                    status=DeadLetterStatus.OPEN,
                    run_id=run_id,
                    workflow_name=wf_name,
                    reason=reason,
                    error=error,
                    replay_count=0,
                    created_at=now,
                )
            )
            await self._history(
                conn, run_id, "dead_lettered", {"reason": reason, "error": error, "dlq_id": dlq_id}
            )
        return dlq_id

    async def release_run(self, run_id: str, token: str, *, step_id: str | None) -> None:
        """Hand a run back to the queue without penalty (graceful shutdown)."""
        async with self.tx() as conn:
            await self._fence(
                conn,
                run_id,
                token,
                status=RunStatus.QUEUED,
                next_attempt_at=self.clock(),
                attempts=t.runs.c.attempts - 1,
                **_cleared_lease(),
            )
            if step_id is not None:
                await conn.execute(
                    update(t.step_results)
                    .where(
                        t.step_results.c.run_id == run_id,
                        t.step_results.c.step_id == step_id,
                        t.step_results.c.status == StepStatus.RUNNING,
                    )
                    .values(
                        attempts=t.step_results.c.attempts - 1,
                        status=StepStatus.FAILED,
                        error="released during shutdown",
                    )
                )
            await self._history(conn, run_id, "released", {"step": step_id})

    async def find_expired_leases(self, limit: int = 100) -> list[ExpiredLease]:
        now = self.clock()
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(
                        t.runs.c.id,
                        t.runs.c.lease_token,
                        t.runs.c.lease_owner,
                        t.runs.c.current_step,
                        t.workflow_versions.c.name,
                        t.workflow_versions.c.version,
                        t.workflow_versions.c.spec,
                        t.step_results.c.attempts,
                    )
                    .join(
                        t.workflow_versions,
                        and_(
                            t.workflow_versions.c.name == t.runs.c.workflow_name,
                            t.workflow_versions.c.version == t.runs.c.workflow_version,
                        ),
                    )
                    .outerjoin(
                        t.step_results,
                        and_(
                            t.step_results.c.run_id == t.runs.c.id,
                            t.step_results.c.step_id == t.runs.c.current_step,
                        ),
                    )
                    .where(t.runs.c.status == RunStatus.RUNNING, t.runs.c.lease_expires_at < now)
                    .limit(limit)
                )
            ).all()
        return [
            ExpiredLease(
                run_id=r.id,
                lease_token=r.lease_token,
                lease_owner=r.lease_owner,
                current_step=r.current_step,
                workflow=WorkflowRef(r.name, r.version, r.spec),
                step_attempts=r.attempts or 0,
            )
            for r in rows
        ]

    # ---------------------------------------------------------- operations

    async def cancel_run(self, run_id: str) -> bool:
        async with self.tx() as conn:
            result = await conn.execute(
                update(t.runs)
                .where(t.runs.c.id == run_id, t.runs.c.status == RunStatus.QUEUED)
                .values(
                    status=RunStatus.CANCELLED, finished_at=self.clock(), updated_at=self.clock()
                )
            )
            if result.rowcount != 1:
                return False
            await self._history(conn, run_id, "cancelled", {})
            return True

    async def replay_dead_letter(
        self,
        dlq_id: str,
        *,
        mode: ReplayMode = ReplayMode.RESUME,
        workflow: WorkflowRef | None = None,
    ) -> str:
        """Re-queue the run behind an open dead letter. Returns the run id.

        ``resume`` keeps succeeded step checkpoints (no duplicate side effects);
        ``restart`` clears them and bumps the run *generation*, which changes the
        idempotency keys handed to actions so downstream systems re-execute.
        ``workflow`` optionally re-pins the run to a newer workflow version
        (e.g. after deploying a fix).
        """
        now = self.clock()
        async with self.tx() as conn:
            dl = (
                await conn.execute(select(t.dead_letters).where(t.dead_letters.c.id == dlq_id))
            ).one_or_none()
            if dl is None:
                raise KeyError(dlq_id)
            if dl.kind != DeadLetterKind.RUN or dl.status != DeadLetterStatus.OPEN:
                raise ValueError(f"dead letter {dlq_id} is not an open run dead letter")
            values: dict[str, Any] = {
                "status": RunStatus.QUEUED,
                "next_attempt_at": now,
                "finished_at": None,
                "updated_at": now,
                "current_step": None,
            }
            if workflow is not None:
                await self._save_workflow_version(conn, workflow, now)
                values["workflow_version"] = workflow.version
            if mode == ReplayMode.RESTART:
                values["generation"] = t.runs.c.generation + 1
                await conn.execute(
                    delete(t.step_results).where(t.step_results.c.run_id == dl.run_id)
                )
            else:
                await conn.execute(
                    delete(t.step_results).where(
                        t.step_results.c.run_id == dl.run_id,
                        t.step_results.c.status.notin_([s.value for s in StepStatus if s.done]),
                    )
                )
            result = await conn.execute(
                update(t.runs)
                .where(t.runs.c.id == dl.run_id, t.runs.c.status == RunStatus.DEAD_LETTERED)
                .values(**values)
            )
            if result.rowcount != 1:
                raise ValueError(f"run {dl.run_id} is not dead-lettered")
            await conn.execute(
                update(t.dead_letters)
                .where(t.dead_letters.c.id == dlq_id)
                .values(
                    status=DeadLetterStatus.REPLAYED,
                    resolved_at=now,
                    replay_count=t.dead_letters.c.replay_count + 1,
                )
            )
            await self._history(
                conn,
                dl.run_id,
                "replayed",
                {
                    "dlq_id": dlq_id,
                    "mode": mode.value,
                    "workflow_version": values.get("workflow_version"),
                },
            )
            return str(dl.run_id)

    async def discard_dead_letter(self, dlq_id: str) -> bool:
        async with self.tx() as conn:
            result = await conn.execute(
                update(t.dead_letters)
                .where(
                    t.dead_letters.c.id == dlq_id, t.dead_letters.c.status == DeadLetterStatus.OPEN
                )
                .values(status=DeadLetterStatus.DISCARDED, resolved_at=self.clock())
            )
            return bool(result.rowcount == 1)

    # ---------------------------------------------------------------- reads

    async def get_event(self, event_id: str) -> Event | None:
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(select(t.events).where(t.events.c.id == event_id))
            ).one_or_none()
        return _event(row) if row else None

    async def list_events(
        self, *, source: str | None = None, limit: int = 50, offset: int = 0
    ) -> list[Event]:
        q = select(t.events).order_by(t.events.c.received_at.desc()).limit(limit).offset(offset)
        if source:
            q = q.where(t.events.c.source == source)
        async with self.engine.connect() as conn:
            return [_event(r) for r in (await conn.execute(q)).all()]

    async def get_run(self, run_id: str) -> Run | None:
        async with self.engine.connect() as conn:
            row = (await conn.execute(select(t.runs).where(t.runs.c.id == run_id))).one_or_none()
        return _run(row) if row else None

    async def list_runs(
        self,
        *,
        status: RunStatus | None = None,
        workflow: str | None = None,
        event_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Run]:
        q: Select[Any] = (
            select(t.runs).order_by(t.runs.c.created_at.desc()).limit(limit).offset(offset)
        )
        if status:
            q = q.where(t.runs.c.status == status)
        if workflow:
            q = q.where(t.runs.c.workflow_name == workflow)
        if event_id:
            q = q.where(t.runs.c.event_id == event_id)
        async with self.engine.connect() as conn:
            return [_run(r) for r in (await conn.execute(q)).all()]

    async def get_steps(self, run_id: str) -> list[StepResult]:
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(t.step_results)
                    .where(t.step_results.c.run_id == run_id)
                    .order_by(t.step_results.c.started_at)
                )
            ).all()
        return [_step(r) for r in rows]

    async def get_history(self, run_id: str) -> list[HistoryEntry]:
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(t.run_history)
                    .where(t.run_history.c.run_id == run_id)
                    .order_by(t.run_history.c.id)
                )
            ).all()
        return [HistoryEntry(ts=r.ts, kind=r.kind, detail=r.detail) for r in rows]

    async def get_dead_letter(self, dlq_id: str) -> DeadLetter | None:
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(select(t.dead_letters).where(t.dead_letters.c.id == dlq_id))
            ).one_or_none()
        return _dead_letter(row) if row else None

    async def list_dead_letters(
        self,
        *,
        status: DeadLetterStatus | None = DeadLetterStatus.OPEN,
        workflow: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[DeadLetter]:
        q = (
            select(t.dead_letters)
            .order_by(t.dead_letters.c.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        if status:
            q = q.where(t.dead_letters.c.status == status)
        if workflow:
            q = q.where(t.dead_letters.c.workflow_name == workflow)
        async with self.engine.connect() as conn:
            return [_dead_letter(r) for r in (await conn.execute(q)).all()]

    async def stats(self) -> dict[str, Any]:
        """Queue depth / status counts, used for metrics and the admin API."""
        now = self.clock()
        async with self.engine.connect() as conn:
            by_status: dict[str, int] = dict(
                (
                    await conn.execute(
                        select(t.runs.c.status, func.count()).group_by(t.runs.c.status)
                    )
                ).all()
            )
            ready = (
                await conn.execute(
                    select(func.count()).where(
                        t.runs.c.status == RunStatus.QUEUED, t.runs.c.next_attempt_at <= now
                    )
                )
            ).scalar_one()
            oldest_ready = (
                await conn.execute(
                    select(func.min(t.runs.c.next_attempt_at)).where(
                        t.runs.c.status == RunStatus.QUEUED, t.runs.c.next_attempt_at <= now
                    )
                )
            ).scalar_one()
            dlq_open = (
                await conn.execute(
                    select(func.count()).where(t.dead_letters.c.status == DeadLetterStatus.OPEN)
                )
            ).scalar_one()
        if isinstance(oldest_ready, str):  # SQLite aggregate loses the column type
            oldest_ready = datetime.fromisoformat(oldest_ready)
        if oldest_ready is not None and oldest_ready.tzinfo is None:
            oldest_ready = oldest_ready.replace(tzinfo=UTC)
        lag = (now - oldest_ready).total_seconds() if oldest_ready else 0.0
        return {
            "runs_by_status": {s.value: int(by_status.get(s.value, 0)) for s in RunStatus},
            "ready": int(ready),
            "queue_lag_seconds": max(lag, 0.0),
            "dead_letters_open": int(dlq_open),
        }

    async def _mark_step_failed(
        self, conn: AsyncConnection, run_id: str, step_id: str | None, error: str
    ) -> None:
        if step_id is None:
            return
        await conn.execute(
            update(t.step_results)
            .where(t.step_results.c.run_id == run_id, t.step_results.c.step_id == step_id)
            .values(status=StepStatus.FAILED, error=error, finished_at=self.clock())
        )
        await self._history(conn, run_id, "step_failed", {"step": step_id, "error": error})

    async def _history(
        self, conn: AsyncConnection, run_id: str, kind: str, detail: dict[str, Any]
    ) -> None:
        await conn.execute(
            insert(t.run_history).values(run_id=run_id, ts=self.clock(), kind=kind, detail=detail)
        )


def _cleared_lease() -> dict[str, Any]:
    return {
        "lease_owner": None,
        "lease_token": None,
        "lease_expires_at": None,
        "current_step": None,
    }


def _run(row: Any) -> Run:
    return Run(
        id=row.id,
        event_id=row.event_id,
        workflow_name=row.workflow_name,
        workflow_version=row.workflow_version,
        status=RunStatus(row.status),
        attempts=row.attempts,
        generation=row.generation,
        next_attempt_at=row.next_attempt_at,
        lease_owner=row.lease_owner,
        lease_expires_at=row.lease_expires_at,
        current_step=row.current_step,
        last_error=row.last_error,
        created_at=row.created_at,
        updated_at=row.updated_at,
        finished_at=row.finished_at,
    )


def _event(row: Any) -> Event:
    return Event(
        id=row.id,
        source=row.source,
        event_type=row.event_type,
        idempotency_key=row.idempotency_key,
        payload=row.payload,
        headers=row.headers,
        ingress=row.ingress,
        received_at=row.received_at,
    )


def _step(row: Any) -> StepResult:
    return StepResult(
        step_id=row.step_id,
        status=StepStatus(row.status),
        attempts=row.attempts,
        output=row.output,
        error=row.error,
        started_at=row.started_at,
        finished_at=row.finished_at,
    )


def _dead_letter(row: Any) -> DeadLetter:
    return DeadLetter(
        id=row.id,
        kind=DeadLetterKind(row.kind),
        status=DeadLetterStatus(row.status),
        run_id=row.run_id,
        workflow_name=row.workflow_name,
        source=row.source,
        reason=row.reason,
        error=row.error,
        payload=row.payload,
        replay_count=row.replay_count,
        created_at=row.created_at,
        resolved_at=row.resolved_at,
    )
