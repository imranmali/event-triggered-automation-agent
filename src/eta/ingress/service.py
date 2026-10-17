"""Ingest service: the single entry point every ingress adapter goes through."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import Any

from eta import metrics
from eta.db.store import Store
from eta.models import IngestResult, NewEvent
from eta.workflows.registry import WorkflowRegistry

log = logging.getLogger(__name__)


class IngestService:
    def __init__(
        self,
        store: Store,
        workflows: WorkflowRegistry,
        *,
        on_runs_created: Callable[[], None] | None = None,
    ) -> None:
        self.store = store
        self.workflows = workflows
        self.on_runs_created = on_runs_created

    async def ingest(
        self,
        *,
        source: str,
        event_type: str,
        payload: Any,
        idempotency_key: str,
        headers: Mapping[str, str] | None = None,
        ingress: str = "api",
    ) -> IngestResult:
        headers = dict(headers or {})
        refs = self.workflows.match(source, event_type, payload, headers)
        result = await self.store.ingest_event(
            NewEvent(
                source=source,
                event_type=event_type,
                payload=payload,
                idempotency_key=idempotency_key,
                headers=headers,
                ingress=ingress,
            ),
            refs,
        )
        if result.duplicate:
            outcome = "duplicate"
        elif not result.run_ids:
            outcome = "no_match"
        else:
            outcome = "accepted"
        metrics.EVENTS_RECEIVED.labels(source, ingress, outcome).inc()
        log.info(
            "event ingested",
            extra={
                "event_id": result.event_id,
                "source": source,
                "event_type": event_type,
                "outcome": outcome,
                "runs": len(result.run_ids),
            },
        )
        if result.run_ids and not result.duplicate and self.on_runs_created:
            self.on_runs_created()
        return result

    async def emit(self, source: str, event_type: str, payload: Any, key: str) -> IngestResult:
        """Used by the ``emit_event`` action to chain workflows."""
        return await self.ingest(
            source=source,
            event_type=event_type,
            payload=payload,
            idempotency_key=key,
            ingress="internal",
        )
