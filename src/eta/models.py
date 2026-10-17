"""Domain records shared by the store, engine and API."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


def utcnow() -> datetime:
    return datetime.now(UTC)


def new_id() -> str:
    return uuid.uuid4().hex


class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    DEAD_LETTERED = "dead_lettered"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in {RunStatus.SUCCEEDED, RunStatus.DEAD_LETTERED, RunStatus.CANCELLED}


class StepStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    FAILED_CONTINUED = "failed_continued"  # failed, but continue_on_error let the run proceed
    SKIPPED = "skipped"

    @property
    def done(self) -> bool:
        """Checkpointed: never re-executed when the run is retried or resumed."""
        return self in {StepStatus.SUCCEEDED, StepStatus.SKIPPED, StepStatus.FAILED_CONTINUED}


class DeadLetterKind(StrEnum):
    RUN = "run"  # a workflow run exhausted retries / failed permanently
    INGRESS = "ingress"  # a message could not even be ingested (poison message)


class DeadLetterStatus(StrEnum):
    OPEN = "open"
    REPLAYED = "replayed"
    DISCARDED = "discarded"


class ReplayMode(StrEnum):
    RESUME = "resume"  # keep succeeded step checkpoints, retry from the failed step
    RESTART = "restart"  # drop all checkpoints and execute every step again


@dataclass(slots=True)
class NewEvent:
    source: str
    event_type: str
    payload: Any
    idempotency_key: str
    headers: dict[str, str] = field(default_factory=dict)
    ingress: str = "webhook"
    id: str = field(default_factory=new_id)


@dataclass(slots=True)
class Event:
    id: str
    source: str
    event_type: str
    idempotency_key: str
    payload: Any
    headers: dict[str, str]
    ingress: str
    received_at: datetime


@dataclass(slots=True)
class WorkflowRef:
    """An immutable, versioned workflow definition a run is pinned to."""

    name: str
    version: str
    spec: dict[str, Any]


@dataclass(slots=True)
class IngestResult:
    event_id: str
    duplicate: bool
    run_ids: list[str]


@dataclass(slots=True)
class StepResult:
    step_id: str
    status: StepStatus
    attempts: int
    output: Any
    error: str | None
    started_at: datetime | None
    finished_at: datetime | None


@dataclass(slots=True)
class Run:
    id: str
    event_id: str
    workflow_name: str
    workflow_version: str
    status: RunStatus
    attempts: int
    generation: int
    next_attempt_at: datetime | None
    lease_owner: str | None
    lease_expires_at: datetime | None
    current_step: str | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None


@dataclass(slots=True)
class ClaimedRun:
    """Everything a worker needs to execute a run it has leased."""

    run: Run
    lease_token: str
    event: Event
    workflow: WorkflowRef
    steps: dict[str, StepResult]


@dataclass(slots=True)
class HistoryEntry:
    ts: datetime
    kind: str
    detail: dict[str, Any]


@dataclass(slots=True)
class DeadLetter:
    id: str
    kind: DeadLetterKind
    status: DeadLetterStatus
    run_id: str | None
    workflow_name: str | None
    source: str | None
    reason: str
    error: str | None
    payload: Any
    replay_count: int
    created_at: datetime
    resolved_at: datetime | None
