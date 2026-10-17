"""Worker: claims runs, executes them concurrently, keeps leases alive and
recovers runs abandoned by crashed workers.

Lifecycle of a claimed run inside the worker::

    claim ──► execute ──► complete / retry / dead-letter   (normal path)
       │         │
       │         └── heartbeat every N s extends the lease; if the lease is
       │             lost (we were presumed dead) execution is cancelled
       └── on shutdown: wait up to the grace period, then cancel and *release*
           the run back to the queue without consuming an attempt
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
from dataclasses import dataclass

from eta import metrics
from eta.db.store import ExpiredLease, Store
from eta.engine.executor import ExecutionState, Executor, Outcome
from eta.errors import LeaseLostError
from eta.logs import log_context
from eta.models import ClaimedRun
from eta.workflows.spec import WorkflowSpec

log = logging.getLogger(__name__)


@dataclass(slots=True)
class WorkerConfig:
    worker_id: str
    concurrency: int = 8
    poll_interval: float = 1.0
    lease_seconds: float = 60.0
    heartbeat_interval: float = 15.0
    shutdown_grace_seconds: float = 30.0
    reap_interval: float = 10.0

    def __post_init__(self) -> None:
        if self.heartbeat_interval >= self.lease_seconds:
            raise ValueError("heartbeat_interval must be shorter than lease_seconds")
        if self.concurrency < 1:
            raise ValueError("concurrency must be >= 1")


class Worker:
    def __init__(self, store: Store, executor: Executor, config: WorkerConfig) -> None:
        self.store = store
        self.executor = executor
        self.config = config
        self._wakeup = asyncio.Event()
        self._inflight: dict[str, asyncio.Task[None]] = {}

    def notify(self) -> None:
        """Wake the poll loop early (new runs were just enqueued in-process)."""
        self._wakeup.set()

    @property
    def inflight(self) -> int:
        return len(self._inflight)

    # ------------------------------------------------------------- main loop

    async def run(self, stop: asyncio.Event) -> None:
        log.info(
            "worker started",
            extra={"worker_id": self.config.worker_id, "concurrency": self.config.concurrency},
        )
        reaper = asyncio.create_task(self._reaper_loop(stop), name="eta-reaper")
        error_backoff = self.config.poll_interval
        try:
            while not stop.is_set():
                try:
                    claimed = await self._claim()
                    error_backoff = self.config.poll_interval
                except Exception:
                    log.exception("claim failed; backing off", extra={"backoff": error_backoff})
                    await self._sleep(stop, error_backoff)
                    error_backoff = min(error_backoff * 2, 30.0)
                    continue
                if claimed and self.inflight < self.config.concurrency:
                    continue  # there may be more ready work; claim again immediately
                await self._wait_for_work(stop)
        finally:
            await self._drain()
            reaper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reaper
            log.info("worker stopped", extra={"worker_id": self.config.worker_id})

    async def _claim(self) -> int:
        free = self.config.concurrency - self.inflight
        if free <= 0:
            return 0
        runs = await self.store.claim_runs(
            self.config.worker_id, limit=free, lease_seconds=self.config.lease_seconds
        )
        for claimed in runs:
            task = asyncio.create_task(self._process(claimed), name=f"run-{claimed.run.id}")
            self._inflight[claimed.run.id] = task
            task.add_done_callback(functools.partial(self._on_done, claimed.run.id))
        metrics.INFLIGHT.set(self.inflight)
        return len(runs)

    def _on_done(self, run_id: str, _task: asyncio.Task[None] | None = None) -> None:
        self._inflight.pop(run_id, None)
        metrics.INFLIGHT.set(self.inflight)
        self._wakeup.set()  # a slot freed up

    async def _wait_for_work(self, stop: asyncio.Event) -> None:
        self._wakeup.clear()
        waiters = [asyncio.ensure_future(self._wakeup.wait()), asyncio.ensure_future(stop.wait())]
        try:
            await asyncio.wait(
                waiters, timeout=self.config.poll_interval, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for w in waiters:
                w.cancel()

    @staticmethod
    async def _sleep(stop: asyncio.Event, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=seconds)

    async def _drain(self) -> None:
        if not self._inflight:
            return
        log.info(
            "draining in-flight runs",
            extra={"inflight": self.inflight, "grace": self.config.shutdown_grace_seconds},
        )
        tasks = list(self._inflight.values())
        _done, pending = await asyncio.wait(tasks, timeout=self.config.shutdown_grace_seconds)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    # ------------------------------------------------------- processing a run

    async def _process(self, claimed: ClaimedRun) -> None:
        run = claimed.run
        state = ExecutionState()
        with log_context(
            run_id=run.id, workflow=run.workflow_name, event_id=run.event_id, attempt=run.attempts
        ):
            exec_task = asyncio.current_task()
            assert exec_task is not None
            lease_lost = asyncio.Event()
            heartbeat = asyncio.create_task(self._heartbeat(claimed, exec_task, lease_lost))
            try:
                await self.executor.execute(claimed, state)
            except LeaseLostError:
                metrics.RUNS_FINISHED.labels(run.workflow_name, "lease_lost").inc()
                log.warning("lease lost; abandoning run (another worker owns it now)")
            except asyncio.CancelledError:
                if lease_lost.is_set():
                    metrics.RUNS_FINISHED.labels(run.workflow_name, "lease_lost").inc()
                    log.warning("lease lost during step; execution cancelled")
                    return
                await self._release(claimed, state)
                raise
            except Exception:
                # Engine bug or database outage: leave the run leased; the reaper
                # will recover it once the lease expires.
                log.exception("unexpected error while executing run")
            finally:
                heartbeat.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat

    async def _heartbeat(
        self, claimed: ClaimedRun, exec_task: asyncio.Task[object], lease_lost: asyncio.Event
    ) -> None:
        while True:
            await asyncio.sleep(self.config.heartbeat_interval)
            try:
                await self.store.heartbeat(
                    claimed.run.id, claimed.lease_token, self.config.lease_seconds
                )
            except LeaseLostError:
                lease_lost.set()
                exec_task.cancel()
                return
            except Exception:
                log.exception("heartbeat failed; will retry")

    async def _release(self, claimed: ClaimedRun, state: ExecutionState) -> None:
        try:
            await asyncio.shield(
                self.store.release_run(
                    claimed.run.id, claimed.lease_token, step_id=state.current_step
                )
            )
            metrics.RUNS_FINISHED.labels(claimed.run.workflow_name, "released").inc()
            log.info("run released back to queue (shutdown)", extra={"step": state.current_step})
        except Exception:
            log.exception("failed to release run; it will be recovered after lease expiry")

    # ---------------------------------------------------------------- reaper

    async def _reaper_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.reap_expired()
            except Exception:
                log.exception("reaper iteration failed")
            await self._sleep(stop, self.config.reap_interval)

    async def reap_expired(self) -> int:
        """Recover runs whose worker died. Safe to run on every worker."""
        recovered = 0
        for lease in await self.store.find_expired_leases():
            try:
                await self._recover(lease)
                recovered += 1
            except LeaseLostError:
                continue  # another reaper (or a late heartbeat) won the race
        if recovered:
            metrics.LEASES_REAPED.inc(recovered)
            self.notify()
        return recovered

    async def _recover(self, lease: ExpiredLease) -> None:
        wf = WorkflowSpec.model_validate(lease.workflow.spec)
        policy = wf.retry_for(lease.current_step)
        error = (
            f"lease expired: worker {lease.lease_owner} lost "
            f"while running step {lease.current_step}"
        )
        with log_context(run_id=lease.run_id, workflow=wf.name):
            if lease.current_step is not None and policy.exhausted(lease.step_attempts):
                await self.store.dead_letter_run(
                    lease.run_id,
                    lease.lease_token,
                    reason="retries_exhausted",
                    error=error,
                    step_id=lease.current_step,
                    require_expired=True,
                )
                metrics.DEAD_LETTERED.labels("run", "retries_exhausted").inc()
                metrics.RUNS_FINISHED.labels(wf.name, Outcome.DEAD_LETTERED).inc()
                log.error("expired lease recovered; retries exhausted, dead-lettered")
                return
            delay = policy.delay(max(lease.step_attempts, 1), rng=self.executor.rng)
            await self.store.schedule_retry(
                lease.run_id,
                lease.lease_token,
                delay=delay,
                error=error,
                step_id=lease.current_step,
                require_expired=True,
            )
            log.warning(
                "expired lease recovered; retry scheduled", extra={"delay": round(delay, 3)}
            )

    # ----------------------------------------------------------------- tests

    async def drain_ready(self, *, max_rounds: int = 100) -> None:
        """Process ready runs until none are left (used by tests and the CLI)."""
        for _ in range(max_rounds):
            claimed = await self._claim()
            if self._inflight:
                await asyncio.gather(*list(self._inflight.values()), return_exceptions=True)
            if not claimed and not self._inflight:
                return
