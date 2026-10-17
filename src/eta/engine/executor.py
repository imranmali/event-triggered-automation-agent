"""Executes one claimed run, step by step, with durable checkpoints.

Per step:

1. Skip it if it is already checkpointed (succeeded / skipped / failed-continued
   on a previous attempt) — this is what makes retries and DLQ resumes free of
   duplicate side effects.
2. Evaluate ``when``; record a skip if false.
3. ``start_step`` (consumes an attempt *before* running, so crashes count).
4. Render inputs, invoke the action under a timeout.
5. On success checkpoint the output. On failure decide, using the step's retry
   policy and the error class: schedule a retry, continue (``continue_on_error``)
   or dead-letter.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import httpx

from eta import metrics
from eta.actions.base import ActionContext, ActionRegistry, EmitFn
from eta.db.store import Store
from eta.errors import LeaseLostError, PermanentError, RetryableError, StepError
from eta.models import ClaimedRun, StepStatus
from eta.workflows.spec import StepSpec, WorkflowSpec
from eta.workflows.templating import evaluate_condition, render

log = logging.getLogger(__name__)

MAX_RETRY_AFTER_SECONDS = 86_400.0


class Outcome(StrEnum):
    SUCCEEDED = "succeeded"
    RETRY = "retry"
    DEAD_LETTERED = "dead_lettered"


@dataclass(slots=True)
class ExecutionState:
    """Mutable view of an in-flight run, shared with the worker."""

    current_step: str | None = None


def idempotency_key(run_id: str, generation: int, step_id: str) -> str:
    return f"{run_id}:{generation}:{step_id}"


class Executor:
    def __init__(
        self,
        store: Store,
        actions: ActionRegistry,
        *,
        http: httpx.AsyncClient,
        emit: EmitFn,
        secrets: dict[str, str] | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.store = store
        self.actions = actions
        self.http = http
        self.emit = emit
        self.secrets = secrets or {}
        self.rng = rng or random.Random()

    def _context(self, claimed: ClaimedRun, wf: WorkflowSpec) -> dict[str, Any]:
        ev, run = claimed.event, claimed.run
        return {
            "event": {
                "id": ev.id,
                "source": ev.source,
                "type": ev.event_type,
                "payload": ev.payload,
                "headers": ev.headers,
                "received_at": ev.received_at.isoformat(),
                "idempotency_key": ev.idempotency_key,
            },
            "run": {
                "id": run.id,
                "attempt": run.attempts,
                "generation": run.generation,
                "workflow": wf.name,
                "workflow_version": claimed.workflow.version,
            },
            "steps": {
                sid: {"status": res.status.value, "output": res.output}
                for sid, res in claimed.steps.items()
                if res.status.done
            },
            "secrets": self.secrets,
        }

    async def execute(self, claimed: ClaimedRun, state: ExecutionState | None = None) -> Outcome:
        state = state or ExecutionState()
        run, token = claimed.run, claimed.lease_token
        wf = WorkflowSpec.model_validate(claimed.workflow.spec)
        context = self._context(claimed, wf)

        for step in wf.steps:
            previous = claimed.steps.get(step.id)
            if previous is not None and previous.status.done:
                continue

            if step.when is not None:
                try:
                    should_run = evaluate_condition(step.when, context)
                except StepError as exc:
                    return await self._dead_letter(
                        claimed, wf, step.id, "permanent_error", str(exc), started=False
                    )
                if not should_run:
                    await self.store.finish_step(run.id, token, step.id, status=StepStatus.SKIPPED)
                    context["steps"][step.id] = {"status": StepStatus.SKIPPED.value, "output": None}
                    continue

            state.current_step = step.id
            attempt = await self.store.start_step(run.id, token, step.id)
            started = time.perf_counter()
            try:
                output = await self._run_step(step, wf, claimed, context, attempt)
            except StepError as exc:
                state.current_step = None
                self._observe(wf, step, started, "error")
                outcome = await self._handle_failure(claimed, wf, step, attempt, exc, context)
                if outcome is None:  # continue_on_error
                    continue
                return outcome
            self._observe(wf, step, started, "ok")
            await self.store.finish_step(
                run.id, token, step.id, status=StepStatus.SUCCEEDED, output=output
            )
            state.current_step = None
            context["steps"][step.id] = {"status": StepStatus.SUCCEEDED.value, "output": output}

        await self.store.complete_run(run.id, token)
        metrics.RUNS_FINISHED.labels(wf.name, Outcome.SUCCEEDED).inc()
        log.info("run succeeded")
        return Outcome.SUCCEEDED

    async def _run_step(
        self,
        step: StepSpec,
        wf: WorkflowSpec,
        claimed: ClaimedRun,
        context: dict[str, Any],
        attempt: int,
    ) -> Any:
        """Run the action, normalising every failure into a ``StepError``."""
        action = self.actions.get(step.action)
        params = render(step.with_, context)
        ctx = ActionContext(
            run_id=claimed.run.id,
            workflow_name=wf.name,
            step_id=step.id,
            attempt=attempt,
            idempotency_key=idempotency_key(claimed.run.id, claimed.run.generation, step.id),
            event=claimed.event,
            http=self.http,
            emit=self.emit,
            logger=logging.getLogger(f"eta.action.{step.action}"),
        )
        timeout = step.timeout if step.timeout is not None else wf.step_timeout
        try:
            async with asyncio.timeout(timeout):
                output = await action.invoke(params, ctx)
        except TimeoutError as exc:
            raise RetryableError(f"step timed out after {timeout:g}s") from exc
        except (StepError, LeaseLostError, asyncio.CancelledError):
            raise
        except Exception as exc:  # unknown errors are assumed transient
            raise RetryableError(f"{type(exc).__name__}: {exc}") from exc
        try:
            json.dumps(output)
        except (TypeError, ValueError) as exc:
            raise PermanentError(f"step output is not JSON serialisable: {exc}") from exc
        return output

    async def _handle_failure(
        self,
        claimed: ClaimedRun,
        wf: WorkflowSpec,
        step: StepSpec,
        attempt: int,
        exc: StepError,
        context: dict[str, Any],
    ) -> Outcome | None:
        run, token = claimed.run, claimed.lease_token
        policy = wf.retry_for(step.id)
        error = f"[{step.id} attempt {attempt}/{policy.max_attempts}] {exc}"

        if exc.retryable and not policy.exhausted(attempt):
            delay = policy.delay(attempt, rng=self.rng)
            if exc.retry_after is not None:
                delay = max(delay, min(exc.retry_after, MAX_RETRY_AFTER_SECONDS))
            await self.store.schedule_retry(
                run.id, token, delay=delay, error=error, step_id=step.id
            )
            metrics.RETRIES_SCHEDULED.labels(wf.name, step.id).inc()
            metrics.RUNS_FINISHED.labels(wf.name, Outcome.RETRY).inc()
            log.warning(
                "step failed; retry scheduled",
                extra={"step": step.id, "delay": round(delay, 3), "error": str(exc)},
            )
            return Outcome.RETRY

        reason = "retries_exhausted" if exc.retryable else "permanent_error"
        if step.continue_on_error:
            output = {"error": str(exc), "reason": reason}
            await self.store.finish_step(
                run.id,
                token,
                step.id,
                status=StepStatus.FAILED_CONTINUED,
                output=output,
                error=error,
            )
            context["steps"][step.id] = {
                "status": StepStatus.FAILED_CONTINUED.value,
                "output": output,
            }
            log.warning(
                "step failed; continuing (continue_on_error)",
                extra={"step": step.id, "error": str(exc)},
            )
            return None
        return await self._dead_letter(claimed, wf, step.id, reason, error, started=True)

    async def _dead_letter(
        self,
        claimed: ClaimedRun,
        wf: WorkflowSpec,
        step_id: str,
        reason: str,
        error: str,
        *,
        started: bool,
    ) -> Outcome:
        dlq_id = await self.store.dead_letter_run(
            claimed.run.id,
            claimed.lease_token,
            reason=reason,
            error=error,
            step_id=step_id if started else None,
        )
        metrics.DEAD_LETTERED.labels("run", reason).inc()
        metrics.RUNS_FINISHED.labels(wf.name, Outcome.DEAD_LETTERED).inc()
        log.error(
            "run dead-lettered",
            extra={"step": step_id, "reason": reason, "error": error, "dlq_id": dlq_id},
        )
        return Outcome.DEAD_LETTERED

    @staticmethod
    def _observe(wf: WorkflowSpec, step: StepSpec, started: float, outcome: str) -> None:
        metrics.STEP_DURATION.labels(wf.name, step.id, step.action, outcome).observe(
            time.perf_counter() - started
        )
