"""Built-in actions."""

from __future__ import annotations

import asyncio
import email.utils
import json
import logging
import time
from typing import Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from eta.actions.base import ActionContext, ActionRegistry, default_registry
from eta.errors import PermanentError, RetryableError

MAX_BODY_CHARS = 65_536
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


class _Params(BaseModel):
    model_config = ConfigDict(extra="forbid")


# ---------------------------------------------------------------- http


class HttpParams(_Params):
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"] = "POST"
    url: HttpUrl
    headers: dict[str, str] = Field(default_factory=dict)
    params: dict[str, str | int | float | bool] = Field(default_factory=dict)
    json_body: Any = Field(default=None, alias="json")
    body: str | None = None
    timeout: float = Field(default=10.0, gt=0, le=300)
    expected_status: list[int] | None = None
    idempotency_header: str | None = "Idempotency-Key"


def parse_retry_after(value: str | None) -> float | None:
    """``Retry-After`` is either delta-seconds or an HTTP date."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        parsed = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(parsed.timestamp() - time.time(), 0.0)


def _decode_body(response: httpx.Response) -> Any:
    if response.headers.get("content-type", "").startswith("application/json"):
        try:
            return response.json()
        except json.JSONDecodeError:
            pass
    text = response.text
    return text if len(text) <= MAX_BODY_CHARS else text[:MAX_BODY_CHARS] + "...[truncated]"


async def http_request(p: HttpParams, ctx: ActionContext) -> dict[str, Any]:
    """Call an HTTP endpoint; 5xx/408/429/network errors are retried, other 4xx are not."""
    headers = dict(p.headers)
    if p.idempotency_header and p.method != "GET":
        headers.setdefault(p.idempotency_header, ctx.idempotency_key)
    headers.setdefault("User-Agent", "event-triggered-automation-agent/1.0")
    try:
        response = await ctx.http.request(
            p.method,
            str(p.url),
            headers=headers,
            params=p.params,
            json=p.json_body if p.body is None else None,
            content=p.body,
            timeout=p.timeout,
        )
    except httpx.TimeoutException as exc:
        raise RetryableError(f"timeout calling {p.url}: {exc!r}") from exc
    except httpx.TransportError as exc:
        raise RetryableError(f"transport error calling {p.url}: {exc!r}") from exc

    ok = response.status_code in p.expected_status if p.expected_status else response.is_success
    body = _decode_body(response)
    if ok:
        return {"status": response.status_code, "body": body}
    snippet = str(body)[:500]
    message = f"{p.method} {p.url} -> HTTP {response.status_code}: {snippet}"
    if response.status_code in RETRYABLE_STATUS or response.status_code >= 500:
        raise RetryableError(
            message, retry_after=parse_retry_after(response.headers.get("retry-after"))
        )
    raise PermanentError(message)


# ---------------------------------------------------------- utilities


class TransformParams(_Params):
    output: Any


async def transform(p: TransformParams, ctx: ActionContext) -> Any:
    """Return ``output`` (already template-rendered) as the step output."""
    return p.output


class LogParams(_Params):
    message: str
    level: Literal["debug", "info", "warning", "error"] = "info"
    fields: dict[str, Any] = Field(default_factory=dict)


async def log_action(p: LogParams, ctx: ActionContext) -> dict[str, Any]:
    """Write a structured log line."""
    ctx.logger.log(getattr(logging, p.level.upper()), p.message, extra={"fields": p.fields})
    return {"message": p.message}


class DelayParams(_Params):
    seconds: float = Field(ge=0, le=300)


async def delay(p: DelayParams, ctx: ActionContext) -> dict[str, Any]:
    """Sleep (bounded by the step timeout). For short, in-process waits only."""
    await asyncio.sleep(p.seconds)
    return {"slept": p.seconds}


class FailParams(_Params):
    mode: Literal["retryable", "permanent"] = "retryable"
    message: str = "injected failure"
    succeed_on_attempt: int | None = Field(default=None, ge=1)


async def fail(p: FailParams, ctx: ActionContext) -> dict[str, Any]:
    """Fault injection for testing retry/DLQ behaviour."""
    if p.succeed_on_attempt is not None and ctx.attempt >= p.succeed_on_attempt:
        return {"succeeded_on_attempt": ctx.attempt}
    if p.mode == "permanent":
        raise PermanentError(p.message)
    raise RetryableError(p.message)


class EmitParams(_Params):
    event_type: str = Field(min_length=1)
    payload: Any = None
    source: str = "internal"


async def emit_event(p: EmitParams, ctx: ActionContext) -> dict[str, Any]:
    """Publish a new event, triggering other workflows (idempotent across retries)."""
    result = await ctx.emit(p.source, p.event_type, p.payload, ctx.idempotency_key)
    return {"event_id": result.event_id, "duplicate": result.duplicate, "run_ids": result.run_ids}


def register_builtins(registry: ActionRegistry) -> None:
    registry.register("http_request", http_request, params=HttpParams)
    registry.register("transform", transform, params=TransformParams)
    registry.register("log", log_action, params=LogParams)
    registry.register("delay", delay, params=DelayParams)
    registry.register("fail", fail, params=FailParams)
    registry.register("emit_event", emit_event, params=EmitParams)


register_builtins(default_registry)
