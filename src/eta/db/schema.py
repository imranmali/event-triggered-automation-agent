"""Relational schema. The ``runs`` table doubles as the durable work queue."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    Dialect,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import TypeDecorator


class UTCDateTime(TypeDecorator[datetime]):
    """Timezone-aware UTC datetimes on every backend (SQLite has no tz support)."""

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("naive datetimes are not allowed")
        value = value.astimezone(UTC)
        return value.replace(tzinfo=None) if dialect.name == "sqlite" else value

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


JSONType = JSON().with_variant(JSONB(), "postgresql")

metadata = MetaData(
    naming_convention={
        "ix": "ix_%(table_name)s_%(column_0_N_name)s",
        "uq": "uq_%(table_name)s_%(column_0_N_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }
)

ID = String(32)

events = Table(
    "events",
    metadata,
    Column("id", ID, primary_key=True),
    Column("source", String(100), nullable=False),
    Column("event_type", String(200), nullable=False),
    Column("idempotency_key", String(300), nullable=False),
    Column("payload", JSONType, nullable=True),
    Column("headers", JSONType, nullable=False),
    Column("ingress", String(30), nullable=False),
    Column("received_at", UTCDateTime(), nullable=False),
    UniqueConstraint("source", "idempotency_key"),
    Index(None, "received_at"),
)

workflow_versions = Table(
    "workflow_versions",
    metadata,
    Column("name", String(200), nullable=False),
    Column("version", String(64), nullable=False),
    Column("spec", JSONType, nullable=False),
    Column("created_at", UTCDateTime(), nullable=False),
    PrimaryKeyConstraint("name", "version"),
)

runs = Table(
    "runs",
    metadata,
    Column("id", ID, primary_key=True),
    Column("event_id", ID, ForeignKey("events.id"), nullable=False),
    Column("workflow_name", String(200), nullable=False),
    Column("workflow_version", String(64), nullable=False),
    Column("status", String(20), nullable=False),
    Column("attempts", Integer, nullable=False, default=0),
    Column("generation", Integer, nullable=False, default=0),
    Column("next_attempt_at", UTCDateTime(), nullable=True),
    Column("lease_owner", String(200), nullable=True),
    Column("lease_token", String(32), nullable=True),
    Column("lease_expires_at", UTCDateTime(), nullable=True),
    Column("current_step", String(200), nullable=True),
    Column("last_error", Text, nullable=True),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("updated_at", UTCDateTime(), nullable=False),
    Column("finished_at", UTCDateTime(), nullable=True),
    # Idempotent run creation: an event triggers a given workflow at most once.
    UniqueConstraint("event_id", "workflow_name"),
    Index(None, "status", "next_attempt_at"),
    Index(None, "status", "lease_expires_at"),
    Index(None, "workflow_name", "status"),
)

step_results = Table(
    "step_results",
    metadata,
    Column("run_id", ID, ForeignKey("runs.id", ondelete="CASCADE"), nullable=False),
    Column("step_id", String(200), nullable=False),
    Column("status", String(20), nullable=False),
    Column("attempts", Integer, nullable=False, default=0),
    Column("output", JSONType, nullable=True),
    Column("error", Text, nullable=True),
    Column("started_at", UTCDateTime(), nullable=True),
    Column("finished_at", UTCDateTime(), nullable=True),
    PrimaryKeyConstraint("run_id", "step_id"),
)

run_history = Table(
    "run_history",
    metadata,
    Column(
        "id",
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    ),
    Column("run_id", ID, ForeignKey("runs.id", ondelete="CASCADE"), nullable=False),
    Column("ts", UTCDateTime(), nullable=False),
    Column("kind", String(40), nullable=False),
    Column("detail", JSONType, nullable=False),
    Index(None, "run_id", "id"),
)

dead_letters = Table(
    "dead_letters",
    metadata,
    Column("id", ID, primary_key=True),
    Column("kind", String(20), nullable=False),
    Column("status", String(20), nullable=False),
    Column("run_id", ID, ForeignKey("runs.id", ondelete="CASCADE"), nullable=True),
    Column("workflow_name", String(200), nullable=True),
    Column("source", String(100), nullable=True),
    Column("reason", String(200), nullable=False),
    Column("error", Text, nullable=True),
    Column("payload", JSONType, nullable=True),
    Column("replay_count", Integer, nullable=False, default=0),
    Column("created_at", UTCDateTime(), nullable=False),
    Column("resolved_at", UTCDateTime(), nullable=True),
    Index(None, "status", "created_at"),
    Index(None, "run_id"),
)


def row_to_dict(row: Any) -> dict[str, Any]:
    return dict(row._mapping)
