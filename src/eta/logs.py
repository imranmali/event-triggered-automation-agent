"""Structured (JSON) logging with context propagation via contextvars."""

from __future__ import annotations

import contextvars
import json
import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

_log_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "eta_log_context", default={}
)

_RESERVED = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


@contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    """Attach fields (run_id, event_id, ...) to every log line in this scope."""
    token = _log_context.set({**_log_context.get(), **fields})
    try:
        yield
    finally:
        _log_context.reset(token)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            **_log_context.get(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class ContextTextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extras = {**_log_context.get()}
        extras.update(
            {k: v for k, v in record.__dict__.items() if k not in _RESERVED and not k.startswith("_")}
        )
        if extras:
            base += " " + " ".join(f"{k}={v}" for k, v in extras.items())
        return base


def configure_logging(level: str = "INFO", json_output: bool = True) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        JsonFormatter()
        if json_output
        else ContextTextFormatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    for noisy in ("httpx", "httpcore", "aiosqlite", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
