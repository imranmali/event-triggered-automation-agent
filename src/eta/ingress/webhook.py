"""HTTP webhook ingress.

Contract with senders:

* ``202 Accepted`` — the event and its runs are durably committed.
* ``200 OK`` with ``"duplicate": true`` — already received; nothing new created.
* ``4xx`` — the request is wrong and retrying will not help (bad signature,
  unknown source, malformed body, too large).
* ``503`` — transient storage problem; the sender should retry (and will be
  deduplicated if the first attempt actually committed).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from collections.abc import Mapping
from typing import Any
from urllib.parse import parse_qsl

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from eta import metrics
from eta.errors import SignatureError
from eta.ingress.service import IngestService
from eta.ingress.signatures import verify
from eta.workflows.paths import MISSING, get_path
from eta.workflows.spec import ExtractSpec, SourceSpec

log = logging.getLogger(__name__)

# Never persisted with the event: credentials and signatures.
SENSITIVE_HEADERS = frozenset(
    {
        "authorization",
        "cookie",
        "proxy-authorization",
        "x-hub-signature",
        "x-hub-signature-256",
        "stripe-signature",
        "x-signature",
    }
)


def extract(spec: ExtractSpec | None, headers: Mapping[str, str], payload: Any) -> str | None:
    if spec is None:
        return None
    if spec.header:
        value = {k.lower(): v for k, v in headers.items()}.get(spec.header.lower())
        if value:
            return value
    if spec.path:
        value = get_path(payload, spec.path)
        if value is not MISSING and value is not None and value != "":
            return str(value)
    return spec.default


def parse_body(body: bytes, content_type: str) -> Any:
    if not body:
        return None
    if content_type.startswith("application/x-www-form-urlencoded"):
        form = dict(parse_qsl(body.decode("utf-8", "replace"), keep_blank_values=True))
        # GitHub (and others) can send JSON wrapped in a "payload" form field.
        if set(form) == {"payload"}:
            try:
                return json.loads(form["payload"])
            except json.JSONDecodeError:
                pass
        return form
    try:
        return json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        if content_type.startswith("text/"):
            return body.decode("utf-8", "replace")
        raise ValueError(f"body is not valid JSON: {exc}") from exc


async def _read_body(request: Request, limit: int) -> bytes:
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > limit:
        raise HTTPException(413, "payload too large")
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            raise HTTPException(413, "payload too large")
        chunks.append(chunk)
    return b"".join(chunks)


def build_router(
    sources: Mapping[str, SourceSpec],
    ingest: IngestService,
    *,
    max_body_bytes: int,
    environ: Mapping[str, str] | None = None,
) -> APIRouter:
    env = os.environ if environ is None else environ
    router = APIRouter(tags=["webhooks"])

    @router.post("/webhooks/{source_name}", status_code=202)
    async def receive(source_name: str, request: Request) -> JSONResponse:
        source = sources.get(source_name)
        # Queue-backed sources are not reachable over HTTP.
        if source is None or not source.enabled or source.redis_stream is not None:
            metrics.EVENTS_RECEIVED.labels("unknown", "webhook", "rejected").inc()
            raise HTTPException(404, "unknown source")

        body = await _read_body(request, max_body_bytes)
        headers = dict(request.headers)

        secret = env.get(source.verify.secret_env) if source.verify.secret_env else None
        try:
            verify(source.verify, body, headers, secret)
        except SignatureError as exc:
            metrics.EVENTS_RECEIVED.labels(source_name, "webhook", "rejected").inc()
            log.warning(
                "webhook signature rejected", extra={"source": source_name, "reason": str(exc)}
            )
            raise HTTPException(401, "invalid signature") from None

        try:
            payload = parse_body(body, request.headers.get("content-type", ""))
        except ValueError as exc:
            metrics.EVENTS_RECEIVED.labels(source_name, "webhook", "rejected").inc()
            raise HTTPException(400, str(exc)) from None

        event_type = extract(source.event_type, headers, payload)
        if not event_type:
            metrics.EVENTS_RECEIVED.labels(source_name, "webhook", "rejected").inc()
            raise HTTPException(400, "cannot determine event type")
        key = extract(source.idempotency_key, headers, payload) or (
            "sha256:" + hashlib.sha256(body).hexdigest()
        )
        stored_headers = {k: v for k, v in headers.items() if k.lower() not in SENSITIVE_HEADERS}

        try:
            result = await ingest.ingest(
                source=source_name,
                event_type=event_type,
                payload=payload,
                idempotency_key=key,
                headers=stored_headers,
                ingress="webhook",
            )
        except Exception:
            log.exception("failed to persist webhook", extra={"source": source_name})
            raise HTTPException(503, "temporarily unable to accept event; retry") from None

        return JSONResponse(
            status_code=200 if result.duplicate else 202,
            content={
                "event_id": result.event_id,
                "duplicate": result.duplicate,
                "run_ids": result.run_ids,
            },
        )

    return router
