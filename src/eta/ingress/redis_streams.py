"""Redis Streams queue ingress (consumer groups, at-least-once).

* Messages are acknowledged (``XACK``) only *after* the event is committed to
  the database, so a crash between read and commit causes a redelivery — which
  the ingest idempotency key then deduplicates.
* Messages left pending by a dead consumer are taken over with ``XAUTOCLAIM``
  once they have been idle for ``claim_idle`` seconds.
* Poison messages (undecodable, or delivered more than ``max_deliveries``
  times) are moved to the dead-letter queue and acknowledged so they stop
  blocking the stream.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError

from eta import metrics
from eta.db.store import Store
from eta.ingress.service import IngestService
from eta.workflows.spec import SourceSpec

log = logging.getLogger(__name__)


class PoisonMessage(Exception):
    pass


def _decode(value: Any) -> str:
    return value.decode("utf-8", "replace") if isinstance(value, bytes) else str(value)


class RedisStreamConsumer:
    def __init__(
        self,
        redis: Redis,
        source: SourceSpec,
        ingest: IngestService,
        store: Store,
        *,
        consumer_name: str,
        block_ms: int = 1000,
    ) -> None:
        if source.redis_stream is None:
            raise ValueError(f"source {source.name!r} has no redis_stream configuration")
        self.redis = redis
        self.source = source
        self.cfg = source.redis_stream
        self.ingest = ingest
        self.store = store
        self.consumer = consumer_name
        self.block_ms = block_ms

    async def ensure_group(self) -> None:
        try:
            await self.redis.xgroup_create(self.cfg.stream, self.cfg.group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def run(self, stop: asyncio.Event) -> None:
        log.info(
            "redis stream consumer started",
            extra={"stream": self.cfg.stream, "group": self.cfg.group, "consumer": self.consumer},
        )
        await self.ensure_group()
        backoff = 1.0
        last_claim = 0.0
        loop = asyncio.get_running_loop()
        while not stop.is_set():
            try:
                if loop.time() - last_claim >= max(self.cfg.claim_idle / 2, 1.0):
                    await self.reclaim_stale()
                    last_claim = loop.time()
                await self.poll_once()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except (RedisError, OSError, Exception):
                log.exception("redis consumer error; backing off", extra={"backoff": backoff})
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=backoff)
                backoff = min(backoff * 2, 30.0)
        log.info("redis stream consumer stopped", extra={"stream": self.cfg.stream})

    async def poll_once(self) -> int:
        response: Any = await self.redis.xreadgroup(
            self.cfg.group,
            self.consumer,
            {self.cfg.stream: ">"},
            count=self.cfg.batch_size,
            block=self.block_ms,
        )
        handled = 0
        for _stream, messages in response or []:
            for msg_id, fields in messages:
                await self.handle(_decode(msg_id), fields, deliveries=1)
                handled += 1
        return handled

    async def reclaim_stale(self) -> int:
        """Take over messages a dead consumer read but never acknowledged."""
        idle_ms = int(self.cfg.claim_idle * 1000)
        start = "0-0"
        reclaimed = 0
        while True:
            result = await self.redis.xautoclaim(
                self.cfg.stream, self.cfg.group, self.consumer, idle_ms, start_id=start, count=100
            )
            next_start, messages = _decode(result[0]), result[1]
            for msg_id, fields in messages:
                if fields is None:  # entry deleted from the stream while pending
                    await self.redis.xack(self.cfg.stream, self.cfg.group, msg_id)
                    continue
                mid = _decode(msg_id)
                pending = await self.redis.xpending_range(
                    self.cfg.stream, self.cfg.group, min=mid, max=mid, count=1
                )
                deliveries = int(pending[0]["times_delivered"]) if pending else 1
                await self.handle(mid, fields, deliveries=deliveries)
                reclaimed += 1
            if next_start in ("0-0", "0"):
                return reclaimed
            start = next_start

    async def handle(self, msg_id: str, raw_fields: dict[Any, Any], *, deliveries: int) -> None:
        fields = {_decode(k): _decode(v) for k, v in raw_fields.items()}
        stream = self.cfg.stream
        try:
            if deliveries > self.cfg.max_deliveries:
                raise PoisonMessage(f"delivered {deliveries} times (max {self.cfg.max_deliveries})")
            event_type, payload, key = self._parse(msg_id, fields)
        except PoisonMessage as exc:
            try:
                await self.store.record_ingress_dead_letter(
                    source=self.source.name,
                    reason="poison_message",
                    error=str(exc),
                    payload={"stream": stream, "message_id": msg_id, "fields": fields},
                )
            except Exception:
                metrics.QUEUE_MESSAGES.labels(stream, "error").inc()
                log.exception("failed to dead-letter poison message; will be redelivered")
                return
            await self.redis.xack(stream, self.cfg.group, msg_id)
            metrics.QUEUE_MESSAGES.labels(stream, "poison").inc()
            metrics.DEAD_LETTERED.labels("ingress", "poison_message").inc()
            log.error(
                "poison message dead-lettered", extra={"message_id": msg_id, "error": str(exc)}
            )
            return

        try:
            result = await self.ingest.ingest(
                source=self.source.name,
                event_type=event_type,
                payload=payload,
                idempotency_key=key,
                headers={"x-stream-message-id": msg_id},
                ingress="redis_stream",
            )
        except Exception:
            # Not acknowledged: stays pending and is retried via XAUTOCLAIM.
            metrics.QUEUE_MESSAGES.labels(stream, "error").inc()
            log.exception(
                "failed to ingest stream message; will be redelivered", extra={"message_id": msg_id}
            )
            return
        await self.redis.xack(stream, self.cfg.group, msg_id)
        metrics.QUEUE_MESSAGES.labels(stream, "duplicate" if result.duplicate else "ingested").inc()

    def _parse(self, msg_id: str, fields: dict[str, str]) -> tuple[str, Any, str]:
        event_type = fields.get(self.cfg.event_type_field)
        if not event_type:
            raise PoisonMessage(f"missing field {self.cfg.event_type_field!r}")
        raw_payload = fields.get(self.cfg.payload_field)
        if raw_payload is None:
            payload: Any = {
                k: v
                for k, v in fields.items()
                if k not in {self.cfg.event_type_field, self.cfg.idempotency_field}
            }
        else:
            try:
                payload = json.loads(raw_payload)
            except json.JSONDecodeError as exc:
                raise PoisonMessage(f"payload is not valid JSON: {exc}") from exc
        key = fields.get(self.cfg.idempotency_field) or f"{self.cfg.stream}:{msg_id}"
        return event_type, payload, key
