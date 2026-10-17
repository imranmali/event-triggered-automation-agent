"""Declarative configuration models: sources (ingress) and workflows."""

from __future__ import annotations

import functools
import hashlib
import json
import random
import re
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h)?\s*$")
_UNITS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, None: 1.0}


def parse_duration(value: Any) -> float:
    """Parse ``"500ms"``, ``"2s"``, ``"5m"``, ``"1h"`` or a number of seconds."""
    if isinstance(value, bool):
        raise ValueError("invalid duration")
    if isinstance(value, int | float):
        seconds = float(value)
    else:
        match = _DURATION_RE.match(str(value))
        if not match:
            raise ValueError(f"invalid duration {value!r}; use e.g. 500ms, 2s, 5m, 1h")
        seconds = float(match.group(1)) * _UNITS[match.group(2)]
    if seconds < 0:
        raise ValueError("duration must be >= 0")
    return seconds


Duration = Annotated[float, BeforeValidator(parse_duration)]

IDENT = r"^[A-Za-z_][A-Za-z0-9_\-]*$"


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, frozen=True)


# --------------------------------------------------------------------- retry


class RetryPolicy(_Model):
    """Exponential backoff with jitter.

    ``max_attempts`` counts the first execution, so ``max_attempts: 1`` means
    "never retry". ``jitter: full`` (the default) spreads retries uniformly in
    ``[0, backoff]`` which avoids thundering herds after a downstream outage.
    """

    max_attempts: int = Field(default=5, ge=1, le=100)
    initial_delay: Duration = 1.0
    max_delay: Duration = 300.0
    multiplier: float = Field(default=2.0, ge=1.0)
    jitter: Literal["full", "equal", "none"] = "full"

    def backoff(self, failed_attempts: int) -> float:
        """Deterministic upper bound of the delay before the next attempt."""
        exp = self.initial_delay * (self.multiplier ** max(failed_attempts - 1, 0))
        return float(min(exp, self.max_delay))

    def delay(self, failed_attempts: int, *, rng: random.Random | None = None) -> float:
        cap = self.backoff(failed_attempts)
        r = rng or random
        if self.jitter == "full":
            return r.uniform(0, cap)
        if self.jitter == "equal":
            return cap / 2 + r.uniform(0, cap / 2)
        return cap

    def exhausted(self, attempts: int) -> bool:
        return attempts >= self.max_attempts


# ------------------------------------------------------------------ triggers

FilterOp = Literal[
    "eq",
    "ne",
    "in",
    "not_in",
    "exists",
    "not_exists",
    "contains",
    "startswith",
    "endswith",
    "matches",
    "gt",
    "gte",
    "lt",
    "lte",
]


class FilterCondition(_Model):
    path: str = Field(description="Dotted path into the event, e.g. payload.ref or headers.x-foo")
    op: FilterOp = "eq"
    value: Any = None

    @model_validator(mode="after")
    def _check(self) -> FilterCondition:
        if self.op in {"in", "not_in"} and not isinstance(self.value, list):
            raise ValueError(f"op {self.op!r} requires a list value")
        if self.op == "matches":
            re.compile(str(self.value))
        return self


class TriggerSpec(_Model):
    source: str = Field(description="Source name or glob, e.g. github or *")
    event: list[str] = Field(default_factory=lambda: ["*"])
    filter: list[FilterCondition] = Field(default_factory=list)

    @field_validator("event", mode="before")
    @classmethod
    def _as_list(cls, v: Any) -> Any:
        return [v] if isinstance(v, str) else v


# --------------------------------------------------------------------- steps


class StepSpec(_Model):
    id: str = Field(pattern=IDENT)
    action: str
    with_: dict[str, Any] = Field(default_factory=dict, alias="with")
    when: str | None = None
    timeout: Duration | None = None
    retry: RetryPolicy | None = None
    continue_on_error: bool = False


class WorkflowSpec(_Model):
    name: str = Field(pattern=IDENT)
    description: str = ""
    enabled: bool = True
    triggers: list[TriggerSpec] = Field(alias="trigger", min_length=1)
    retry: RetryPolicy = Field(default_factory=RetryPolicy)
    step_timeout: Duration = 30.0
    steps: list[StepSpec] = Field(min_length=1)

    @field_validator("triggers", mode="before")
    @classmethod
    def _triggers_as_list(cls, v: Any) -> Any:
        return [v] if isinstance(v, dict) else v

    @model_validator(mode="after")
    def _unique_steps(self) -> WorkflowSpec:
        seen: set[str] = set()
        for step in self.steps:
            if step.id in seen:
                raise ValueError(f"duplicate step id {step.id!r}")
            seen.add(step.id)
        return self

    def dump(self) -> dict[str, Any]:
        return self.model_dump(mode="json", by_alias=True)

    @property
    def version(self) -> str:
        """Content hash: identical definitions share a version."""
        canonical = json.dumps(self.dump(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]

    def step(self, step_id: str) -> StepSpec:
        for s in self.steps:
            if s.id == step_id:
                return s
        raise KeyError(step_id)

    def retry_for(self, step_id: str | None) -> RetryPolicy:
        if step_id is not None:
            try:
                override = self.step(step_id).retry
            except KeyError:
                override = None
            if override is not None:
                return override
        return self.retry


# ------------------------------------------------------------------- sources


class VerifySpec(_Model):
    type: Literal["none", "hmac_sha256", "github", "stripe"] = "none"
    secret_env: str | None = Field(
        default=None, description="Name of the environment variable holding the signing secret"
    )
    header: str = "X-Signature"
    prefix: str = ""
    tolerance_seconds: int = 300

    @model_validator(mode="after")
    def _secret_required(self) -> VerifySpec:
        if self.type != "none" and not self.secret_env:
            raise ValueError(f"verify type {self.type!r} requires secret_env")
        return self


class ExtractSpec(_Model):
    """Where to read a value from: a request header or a dotted JSON path."""

    header: str | None = None
    path: str | None = None
    default: str | None = None


class RedisStreamSpec(_Model):
    """Consume this source from a Redis Stream (consumer group, at-least-once)."""

    stream: str
    group: str = "eta"
    event_type_field: str = "event_type"
    payload_field: str = "payload"
    idempotency_field: str = "idempotency_key"
    max_deliveries: int = Field(default=5, ge=1)
    claim_idle: Duration = 60.0
    batch_size: int = Field(default=50, ge=1, le=1000)


class SourceSpec(_Model):
    name: str = Field(pattern=IDENT)
    enabled: bool = True
    redis_stream: RedisStreamSpec | None = None
    verify: VerifySpec = Field(default_factory=VerifySpec)
    event_type: ExtractSpec = Field(default_factory=lambda: ExtractSpec(default="event"))
    idempotency_key: ExtractSpec | None = Field(
        default=None, description="Falls back to a SHA-256 of the raw body when unset/missing"
    )


@functools.lru_cache(maxsize=1024)
def _parse_workflow_json(canonical: str) -> WorkflowSpec:
    return WorkflowSpec.model_validate(json.loads(canonical))


def workflow_from_snapshot(spec: dict[str, Any]) -> WorkflowSpec:
    """Parse a stored (pinned) workflow snapshot, memoised by content."""
    return _parse_workflow_json(json.dumps(spec, sort_keys=True, separators=(",", ":")))
