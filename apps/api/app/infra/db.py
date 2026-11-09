"""Async SQLAlchemy 2.0 setup with RLS-aware session factory.

The single source of truth for tenant scoping is the Postgres GUC
``app.current_org``. ``session_for_request`` opens a transaction and calls
``set_config('app.current_org', :org, true)`` so the value is automatically
rolled back when the transaction commits or aborts. Keep these settings
transaction-local so pooled connections don't retain tenant context.

App-layer queries should still include ``WHERE org_id = :org`` predicates as
defense in depth, but the RLS policies on every tenant-scoped table are the
primary control. See ADR-0004.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Protocol
from weakref import WeakSet

from sqlalchemy import event, text
from sqlalchemy.engine import AdaptedConnection, Engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from app.core.config import Settings, get_settings
from app.core.logging import get_logger
from app.infra.shutdown_budget import remaining_seconds

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator
    from uuid import UUID

    from sqlalchemy.pool import ConnectionPoolEntry


logger = get_logger(__name__)


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


_engine: AsyncEngine | None = None
_sessionmaker: async_sessionmaker[AsyncSession] | None = None


class _AsyncpgTransport(Protocol):
    def is_closed(self) -> bool: ...
    def terminate(self) -> None: ...


class WorkerDatabaseConnections:
    """Track only asyncpg connections checked out by this worker and its tasks."""

    def __init__(self) -> None:
        self.active: WeakSet[_AsyncpgTransport] = WeakSet()
        self.aborted = False

    def add(self, connection: _AsyncpgTransport) -> None:
        if self.aborted:
            connection.terminate()
            raise RuntimeError("Worker database connections are closing")
        self.active.add(connection)

    def abort_active(self) -> int:
        # Closing a worker's own client transport does not commit the server
        # transaction. PostgreSQL rolls it back when it processes the disconnect.
        active = [connection for connection in self.active if not connection.is_closed()]
        if active:
            self.aborted = True
            for connection in active:
                connection.terminate()
        return len(active)


_worker_connections: ContextVar[WorkerDatabaseConnections | None] = ContextVar(
    "worker_db_connections", default=None
)


@contextmanager
def track_worker_database_connections() -> Iterator[WorkerDatabaseConnections]:
    connections = WorkerDatabaseConnections()
    token = _worker_connections.set(connections)
    try:
        yield connections
    finally:
        _worker_connections.reset(token)


def _track_worker_checkout(
    connection: AdaptedConnection, _record: ConnectionPoolEntry, _proxy: object
) -> None:
    connections = _worker_connections.get()
    if connections is not None:
        connections.add(connection.driver_connection)


def _forget_worker_checkin(
    connection: AdaptedConnection | None, _record: ConnectionPoolEntry
) -> None:
    connections = _worker_connections.get()
    if connections is not None and connection is not None:
        connections.active.discard(connection.driver_connection)


async def _verify_runtime_role(connection: Any) -> None:
    # Check the login role, including memberships, because a login can reset
    # SET ROLE or switch to a reachable role. PostgreSQL ignores FORCE RLS
    # for superusers and roles with BYPASSRLS.
    try:
        can_bypass = await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_roles AS r
                WHERE (r.rolsuper OR r.rolbypassrls)
                  AND pg_catalog.pg_has_role(session_user, r.oid, 'MEMBER')
            )
            """
        )
        if can_bypass is not False:
            raise RuntimeError(
                "The database runtime login must not have SUPERUSER or BYPASSRLS access."
            )
    except BaseException:
        # A rejected connect listener runs before SQLAlchemy checks out the
        # connection. Close the physical connection here rather than relying
        # on normal pool return, including when the probe is cancelled.
        connection.terminate()
        raise


def _check_runtime_role(connection: AdaptedConnection, _record: ConnectionPoolEntry) -> None:
    # The role check runs before SQLAlchemy emits checkout.
    connections = _worker_connections.get()
    if connections is not None:
        connections.add(connection.driver_connection)
    connection.run_async(_verify_runtime_role)


def _build_engine(settings: Settings) -> AsyncEngine:
    engine = create_async_engine(
        settings.database.url.get_secret_value(),
        pool_size=settings.database.pool_size,
        max_overflow=settings.database.max_overflow,
        pool_timeout=settings.database.pool_timeout_seconds,
        pool_recycle=settings.database.pool_recycle_seconds,
        echo=settings.database.echo,
        connect_args={
            "server_settings": {
                "application_name": "eaip-api",
                "statement_timeout": str(settings.database.statement_timeout_ms),
            }
        },
        future=True,
    )

    event.listen(engine.sync_engine, "connect", _check_runtime_role)
    event.listen(engine.sync_engine, "checkout", _track_worker_checkout)
    event.listen(engine.sync_engine, "checkin", _forget_worker_checkin)

    return engine


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = _build_engine(settings)
        # Lazy import to avoid a circular dependency between tracing and infra.
        from app.core.tracing import instrument_sqlalchemy_engine

        instrument_sqlalchemy_engine(_engine)
        logger.info("db.engine.initialized", pool_size=settings.database.pool_size)
    return _engine


def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    global _sessionmaker
    if _sessionmaker is None:
        _sessionmaker = async_sessionmaker(
            bind=get_engine(),
            expire_on_commit=False,
            autoflush=False,
        )
    return _sessionmaker


async def dispose_engine() -> None:
    """Dispose async engine. Best-effort during loop teardown."""
    global _engine, _sessionmaker
    engine = _engine
    if engine is None:
        return
    worker = _worker_connections.get() is not None
    try:
        if worker:
            timeout = get_settings().kafka.shutdown_cleanup_seconds
            async with asyncio.timeout(remaining_seconds(timeout)):
                await engine.dispose()
        else:
            await engine.dispose()
        logger.info("db.engine.disposed")
    except Exception as exc:
        logger.debug("db.engine.dispose_suppressed", error_type=type(exc).__name__)
        if worker:
            raise
    finally:
        if _engine is engine:
            _engine = _sessionmaker = None


async def session_for_request(*, org_id: UUID | None) -> AsyncIterator[AsyncSession]:
    """Yield a session inside a transaction with ``app.current_org`` set.

    Use ``set_config(name, value, is_local := true)`` instead of
    ``SET LOCAL`` so we can pass ``org_id`` as a bound parameter without
    string interpolation. ``is_local := true`` scopes the change to the
    current transaction so it rolls back on commit or rollback automatically.

    If ``org_id`` is None (e.g. /healthz, login, or RLS-bypass admin paths),
    we leave the GUC empty — RLS policies use ``current_setting('app.current_org', true)``
    which returns NULL when unset, and the policies fail closed (see migration).
    """
    sm = get_sessionmaker()
    async with sm() as session, session.begin():
        if org_id is not None:
            await session.execute(
                text("SELECT set_config('app.current_org', :org, true)"),
                {"org": str(org_id)},
            )
        yield session


async def session_unscoped() -> AsyncIterator[AsyncSession]:
    """Yield a session that bypasses RLS via ``app.bypass_rls = 'on'``.

    Use ONLY for:
      * Unauthenticated paths that must look up identity data before the org
        is known (login, refresh, API-key resolution).
      * The internal outbox publisher, which scans the queue across tenants.
      * Migration tools and the seed script.

    Application request handlers must use ``session_for_request`` instead.
    The bypass is scoped to the transaction and rolls back on commit/abort.
    """
    sm = get_sessionmaker()
    async with sm() as session, session.begin():
        await session.execute(text("SELECT set_config('app.bypass_rls', 'on', true)"))
        yield session


# --- ping helpers used by /readyz ----------------------------------------


async def ping() -> None:
    sm = get_sessionmaker()
    async with sm() as s:
        await s.execute(text("SELECT 1"))


__all__ = [
    "Base",
    "Engine",
    "dispose_engine",
    "get_engine",
    "get_sessionmaker",
    "ping",
    "session_for_request",
    "session_unscoped",
]
