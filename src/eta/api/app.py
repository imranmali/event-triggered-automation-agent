"""FastAPI application: webhook ingress, admin API, health and metrics."""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from typing import Annotated, Any

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.encoders import jsonable_encoder
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from eta import __version__, metrics
from eta.models import DeadLetterStatus, ReplayMode, RunStatus
from eta.runtime import Runtime
from eta.workflows.registry import to_ref


class PublishEvent(BaseModel):
    source: str = Field(min_length=1, max_length=100)
    event_type: str = Field(min_length=1, max_length=200)
    payload: Any = None
    idempotency_key: str | None = Field(default=None, max_length=300)


class ReplayRequest(BaseModel):
    mode: ReplayMode = ReplayMode.RESUME
    use_latest_workflow: bool = False


class BulkReplayRequest(ReplayRequest):
    workflow: str | None = None
    limit: int = Field(default=100, ge=1, le=1000)


def _encode(obj: Any) -> Any:
    return jsonable_encoder(asdict(obj) if hasattr(obj, "__dataclass_fields__") else obj)


def build_admin_router(rt: Runtime) -> APIRouter:
    async def require_admin(authorization: Annotated[str | None, Header()] = None) -> None:
        token = rt.settings.admin_token
        if not token:
            return
        expected = f"Bearer {token}"
        if not authorization or not hmac.compare_digest(authorization, expected):
            raise HTTPException(401, "admin token required", headers={"WWW-Authenticate": "Bearer"})

    r = APIRouter(prefix="/api", tags=["admin"], dependencies=[Depends(require_admin)])
    store = rt.store

    @r.get("/stats")
    async def stats() -> Any:
        return {**await store.stats(), "worker_inflight": rt.worker.inflight}

    @r.get("/workflows")
    async def list_workflows() -> Any:
        return [
            {
                "name": wf.name,
                "version": wf.version,
                "enabled": wf.enabled,
                "description": wf.description,
                "triggers": [t.model_dump(mode="json") for t in wf.triggers],
                "steps": [{"id": s.id, "action": s.action} for s in wf.steps],
            }
            for wf in rt.config.workflows
        ]

    @r.get("/actions")
    async def list_actions() -> Any:
        return [
            {"name": n, "description": rt.actions.get(n).description} for n in rt.actions.names()
        ]

    @r.post("/events", status_code=202)
    async def publish(body: PublishEvent, response: Response) -> Any:
        key = (
            body.idempotency_key
            or "sha256:"
            + hashlib.sha256(
                json.dumps(
                    {"t": body.event_type, "p": body.payload}, sort_keys=True, default=str
                ).encode()
            ).hexdigest()
        )
        result = await rt.ingest.ingest(
            source=body.source,
            event_type=body.event_type,
            payload=body.payload,
            idempotency_key=key,
            ingress="api",
        )
        if result.duplicate:
            response.status_code = 200
        return _encode(result)

    @r.get("/events")
    async def list_events(
        source: str | None = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> Any:
        return [
            _encode(e) for e in await store.list_events(source=source, limit=limit, offset=offset)
        ]

    @r.get("/events/{event_id}")
    async def get_event(event_id: str) -> Any:
        ev = await store.get_event(event_id)
        if ev is None:
            raise HTTPException(404, "event not found")
        runs = await store.list_runs(event_id=event_id, limit=500)
        return {**_encode(ev), "runs": [_encode(run) for run in runs]}

    @r.get("/runs")
    async def list_runs(
        status: RunStatus | None = None,
        workflow: str | None = None,
        event_id: str | None = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> Any:
        runs = await store.list_runs(
            status=status, workflow=workflow, event_id=event_id, limit=limit, offset=offset
        )
        return [_encode(run) for run in runs]

    @r.get("/runs/{run_id}")
    async def get_run(run_id: str) -> Any:
        run = await store.get_run(run_id)
        if run is None:
            raise HTTPException(404, "run not found")
        return {
            **_encode(run),
            "steps": [_encode(s) for s in await store.get_steps(run_id)],
            "history": [_encode(h) for h in await store.get_history(run_id)],
        }

    @r.post("/runs/{run_id}/cancel")
    async def cancel_run(run_id: str) -> Any:
        if not await store.cancel_run(run_id):
            raise HTTPException(409, "only queued runs can be cancelled")
        return {"run_id": run_id, "status": RunStatus.CANCELLED}

    @r.get("/dlq")
    async def list_dlq(
        status: DeadLetterStatus | None = DeadLetterStatus.OPEN,
        workflow: str | None = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> Any:
        items = await store.list_dead_letters(
            status=status, workflow=workflow, limit=limit, offset=offset
        )
        return [_encode(d) for d in items]

    @r.get("/dlq/{dlq_id}")
    async def get_dlq(dlq_id: str) -> Any:
        dl = await store.get_dead_letter(dlq_id)
        if dl is None:
            raise HTTPException(404, "dead letter not found")
        out = _encode(dl)
        if dl.run_id:
            run = await store.get_run(dl.run_id)
            out["run"] = _encode(run)
            out["steps"] = [_encode(s) for s in await store.get_steps(dl.run_id)]
            if run is not None:
                out["event"] = _encode(await store.get_event(run.event_id))
        return out

    async def _replay(dlq_id: str, body: ReplayRequest) -> str:
        dl = await store.get_dead_letter(dlq_id)
        if dl is None:
            raise HTTPException(404, "dead letter not found")
        workflow = None
        if body.use_latest_workflow and dl.workflow_name:
            spec = rt.config.workflows.get(dl.workflow_name)
            if spec is None:
                raise HTTPException(409, f"workflow {dl.workflow_name!r} no longer exists")
            workflow = to_ref(spec)
        try:
            run_id = await store.replay_dead_letter(dlq_id, mode=body.mode, workflow=workflow)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from None
        rt.worker.notify()
        return run_id

    @r.post("/dlq/{dlq_id}/replay")
    async def replay(dlq_id: str, body: ReplayRequest | None = None) -> Any:
        run_id = await _replay(dlq_id, body or ReplayRequest())
        return {"dlq_id": dlq_id, "run_id": run_id, "status": RunStatus.QUEUED}

    @r.post("/dlq/replay")
    async def bulk_replay(body: BulkReplayRequest) -> Any:
        items = await store.list_dead_letters(
            status=DeadLetterStatus.OPEN, workflow=body.workflow, limit=body.limit
        )
        replayed, skipped = [], []
        for dl in items:
            if dl.run_id is None:
                skipped.append(dl.id)
                continue
            try:
                replayed.append({"dlq_id": dl.id, "run_id": await _replay(dl.id, body)})
            except HTTPException:
                skipped.append(dl.id)
        return {"replayed": replayed, "skipped": skipped}

    @r.post("/dlq/{dlq_id}/discard")
    async def discard(dlq_id: str) -> Any:
        if not await store.discard_dead_letter(dlq_id):
            raise HTTPException(409, "dead letter is not open (or does not exist)")
        return {"dlq_id": dlq_id, "status": DeadLetterStatus.DISCARDED}

    return r


async def refresh_gauges(rt: Runtime) -> None:
    s = await rt.store.stats()
    metrics.QUEUE_READY.set(s["ready"])
    metrics.QUEUE_LAG.set(s["queue_lag_seconds"])
    metrics.DLQ_OPEN.set(s["dead_letters_open"])
    for status, count in s["runs_by_status"].items():
        metrics.RUNS_BY_STATUS.labels(status).set(count)


def create_app(
    rt: Runtime,
    *,
    run_worker: bool = False,
    run_consumers: bool = False,
    manage_lifecycle: bool = True,
) -> FastAPI:
    from eta.ingress.webhook import build_router

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        if run_worker:
            rt.start_worker()
        if run_consumers:
            rt.start_consumers()
        try:
            yield
        finally:
            if manage_lifecycle:
                await rt.shutdown()

    app = FastAPI(
        title="Event-Triggered Automation Agent",
        version=__version__,
        lifespan=lifespan,
    )
    app.include_router(
        build_router(
            rt.config.sources,
            rt.ingest,
            max_body_bytes=rt.settings.max_body_bytes,
            environ=rt.environ,
        )
    )
    app.include_router(build_admin_router(rt))

    @app.get("/healthz", tags=["ops"])
    async def healthz() -> Any:
        return {"status": "ok", "version": __version__}

    @app.get("/readyz", tags=["ops"])
    async def readyz(response: Response) -> Any:
        try:
            await rt.store.ping()
        except Exception as exc:
            response.status_code = 503
            return {"status": "unavailable", "error": type(exc).__name__}
        return {"status": "ready"}

    @app.get("/metrics", tags=["ops"])
    async def prometheus() -> Response:
        # Scraping must keep working (process metrics) even if the DB hiccups.
        with contextlib.suppress(Exception):
            await refresh_gauges(rt)
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app
