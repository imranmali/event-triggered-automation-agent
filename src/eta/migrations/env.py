"""Alembic environment (async). Invoked via ``eta db upgrade``."""

from __future__ import annotations

import asyncio

from alembic import context
from sqlalchemy.engine import Connection

from eta.db.schema import metadata
from eta.db.store import create_engine

config = context.config
target_metadata = metadata


def _run(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        render_as_batch=connection.dialect.name == "sqlite",
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def _run_async() -> None:
    engine = create_engine(config.get_main_option("sqlalchemy.url") or "")
    async with engine.connect() as conn:
        await conn.run_sync(_run)
        await conn.commit()
    await engine.dispose()


if context.is_offline_mode():
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
    )
    with context.begin_transaction():
        context.run_migrations()
else:
    asyncio.run(_run_async())
