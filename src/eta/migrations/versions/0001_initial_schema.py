"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-10-17
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")
TS = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        "events",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("source", sa.String(length=100), nullable=False),
        sa.Column("event_type", sa.String(length=200), nullable=False),
        sa.Column("idempotency_key", sa.String(length=300), nullable=False),
        sa.Column("payload", JSON, nullable=True),
        sa.Column("headers", JSON, nullable=False),
        sa.Column("ingress", sa.String(length=30), nullable=False),
        sa.Column("received_at", TS, nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_events")),
        sa.UniqueConstraint(
            "source", "idempotency_key", name=op.f("uq_events_source_idempotency_key")
        ),
    )
    with op.batch_alter_table("events", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_events_received_at"), ["received_at"], unique=False)

    op.create_table(
        "workflow_versions",
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("version", sa.String(length=64), nullable=False),
        sa.Column("spec", JSON, nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.PrimaryKeyConstraint("name", "version", name=op.f("pk_workflow_versions")),
    )
    op.create_table(
        "runs",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("event_id", sa.String(length=32), nullable=False),
        sa.Column("workflow_name", sa.String(length=200), nullable=False),
        sa.Column("workflow_version", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", TS, nullable=True),
        sa.Column("lease_owner", sa.String(length=200), nullable=True),
        sa.Column("lease_token", sa.String(length=32), nullable=True),
        sa.Column("lease_expires_at", TS, nullable=True),
        sa.Column("current_step", sa.String(length=200), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("updated_at", TS, nullable=False),
        sa.Column("finished_at", TS, nullable=True),
        sa.ForeignKeyConstraint(["event_id"], ["events.id"], name=op.f("fk_runs_event_id_events")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_runs")),
        sa.UniqueConstraint(
            "event_id", "workflow_name", name=op.f("uq_runs_event_id_workflow_name")
        ),
    )
    with op.batch_alter_table("runs", schema=None) as batch_op:
        batch_op.create_index(
            batch_op.f("ix_runs_status_lease_expires_at"),
            ["status", "lease_expires_at"],
            unique=False,
        )
        batch_op.create_index(
            batch_op.f("ix_runs_status_next_attempt_at"),
            ["status", "next_attempt_at"],
            unique=False,
        )
        batch_op.create_index(
            batch_op.f("ix_runs_workflow_name_status"), ["workflow_name", "status"], unique=False
        )

    op.create_table(
        "dead_letters",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("kind", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("run_id", sa.String(length=32), nullable=True),
        sa.Column("workflow_name", sa.String(length=200), nullable=True),
        sa.Column("source", sa.String(length=100), nullable=True),
        sa.Column("reason", sa.String(length=200), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("payload", JSON, nullable=True),
        sa.Column("replay_count", sa.Integer(), nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("resolved_at", TS, nullable=True),
        sa.ForeignKeyConstraint(
            ["run_id"], ["runs.id"], name=op.f("fk_dead_letters_run_id_runs"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_dead_letters")),
    )
    with op.batch_alter_table("dead_letters", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_dead_letters_run_id"), ["run_id"], unique=False)
        batch_op.create_index(
            batch_op.f("ix_dead_letters_status_created_at"), ["status", "created_at"], unique=False
        )

    op.create_table(
        "run_history",
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column("run_id", sa.String(length=32), nullable=False),
        sa.Column("ts", TS, nullable=False),
        sa.Column("kind", sa.String(length=40), nullable=False),
        sa.Column("detail", JSON, nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id"], ["runs.id"], name=op.f("fk_run_history_run_id_runs"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_run_history")),
    )
    with op.batch_alter_table("run_history", schema=None) as batch_op:
        batch_op.create_index(
            batch_op.f("ix_run_history_run_id_id"), ["run_id", "id"], unique=False
        )

    op.create_table(
        "step_results",
        sa.Column("run_id", sa.String(length=32), nullable=False),
        sa.Column("step_id", sa.String(length=200), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("output", JSON, nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("started_at", TS, nullable=True),
        sa.Column("finished_at", TS, nullable=True),
        sa.ForeignKeyConstraint(
            ["run_id"], ["runs.id"], name=op.f("fk_step_results_run_id_runs"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("run_id", "step_id", name=op.f("pk_step_results")),
    )


def downgrade() -> None:
    op.drop_table("step_results")
    with op.batch_alter_table("run_history", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_run_history_run_id_id"))

    op.drop_table("run_history")
    with op.batch_alter_table("dead_letters", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_dead_letters_status_created_at"))
        batch_op.drop_index(batch_op.f("ix_dead_letters_run_id"))

    op.drop_table("dead_letters")
    with op.batch_alter_table("runs", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_runs_workflow_name_status"))
        batch_op.drop_index(batch_op.f("ix_runs_status_next_attempt_at"))
        batch_op.drop_index(batch_op.f("ix_runs_status_lease_expires_at"))

    op.drop_table("runs")
    op.drop_table("workflow_versions")
    with op.batch_alter_table("events", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_events_received_at"))

    op.drop_table("events")
