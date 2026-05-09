"""Test fixtures.

Integration tests rely on three testcontainers — Postgres, Redis, Redpanda.
The session-scoped fixture brings them up, runs Alembic upgrade, and tears
them down at the end of the test session. Per-test cleanup is done with
TRUNCATE between tests rather than container restart to keep the loop fast.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Environment, get_settings
from app.modules.identity.security import reset_jwt_keys_for_tests


# --- containers ---------------------------------------------------------


def _start_containers() -> dict[str, str]:
    """Start containers; return env vars to set."""
    from testcontainers.kafka import RedpandaContainer
    from testcontainers.postgres import PostgresContainer
    from testcontainers.redis import RedisContainer

    pg = PostgresContainer(
        "postgres:16-alpine", username="eaip", password="eaip", dbname="eaip"
    )
    rd = RedisContainer("redis:7-alpine")
    kf = RedpandaContainer("redpandadata/redpanda:latest")

    pg.start()
    rd.start()
    kf.start()

    pg_host = pg.get_container_host_ip()
    pg_port = pg.get_exposed_port(5432)
    rd_host = rd.get_container_host_ip()
    rd_port = rd.get_exposed_port(6379)

    env = {
        "EAIP_DATABASE_URL": f"postgresql+asyncpg://eaip:eaip@{pg_host}:{pg_port}/eaip",
        "EAIP_REDIS_URL": f"redis://{rd_host}:{rd_port}/0",
        "EAIP_KAFKA_BOOTSTRAP_SERVERS": kf.get_bootstrap_server(),
        "EAIP_ENVIRONMENT": "test",
        "EAIP_LOG_LEVEL": "WARNING",
        "EAIP_RATELIMIT_ENABLED": "false",
        "EAIP_OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:14317",  # nothing listens; spans dropped quietly
    }
    pytest._eaip_containers = (pg, rd, kf)  # type: ignore[attr-defined]
    return env


@pytest.fixture(scope="session", autouse=True)
def _containers() -> Iterator[None]:
    """Spin containers up once per test session."""
    if os.environ.get("EAIP_SKIP_CONTAINERS") == "1":
        # CI sometimes runs against an already-provisioned stack.
        yield
        return
    env = _start_containers()
    os.environ.update(env)
    get_settings.cache_clear()
    yield
    # Tear down.
    for c in pytest._eaip_containers:  # type: ignore[attr-defined]
        try:
            c.stop()
        except Exception:
            pass


@pytest.fixture(scope="session", autouse=True)
def _migrate(_containers: None) -> None:
    """Run Alembic upgrade head once the containers are up."""
    from alembic import command
    from alembic.config import Config

    # Locate alembic.ini relative to the apps/api root regardless of cwd.
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cfg = Config(os.path.join(here, "alembic.ini"))
    cfg.set_main_option("script_location", os.path.join(here, "alembic"))
    command.upgrade(cfg, "head")


# --- per-test cleanup ---------------------------------------------------


@pytest.fixture(autouse=True)
def _truncate_tables() -> Iterator[None]:
    """Reset rows between tests using a SYNC psycopg2 connection.

    We deliberately use a sync driver here, NOT asyncpg/SQLAlchemy-async.
    The async driver ties connections to the event loop active when the
    connection was opened. Test teardown happens AFTER that loop closes
    (pytest-asyncio finalizers run on a different loop), so any async DB
    call here triggers "Event loop is closed" or "Future attached to a
    different loop". A sync driver has no loop affinity.
    """
    yield
    import re
    import psycopg2

    from app.core.config import get_settings

    url = get_settings().database.url.get_secret_value()
    # convert SQLAlchemy URL (postgresql+asyncpg://...) to libpq form for psycopg2
    sync_url = re.sub(r'^postgresql\+asyncpg://', 'postgresql://', url)
    conn = psycopg2.connect(sync_url)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "TRUNCATE TABLE refresh_tokens, api_keys, memberships, outbox, "
                    "audit_log, organizations, users RESTART IDENTITY CASCADE"
                )
    finally:
        conn.close()


# --- DB session for tests ----------------------------------------------


@pytest_asyncio.fixture
async def db_session() -> AsyncIterator[AsyncSession]:
    from app.infra.db import get_sessionmaker

    sm = get_sessionmaker()
    async with sm() as session:
        async with session.begin():
            yield session


@pytest_asyncio.fixture
async def db_for_org() -> object:
    """Returns a callable that yields a session bound to the given org_id."""
    from app.infra.db import get_sessionmaker

    async def _factory(org_id: object) -> AsyncIterator[AsyncSession]:
        sm = get_sessionmaker()
        async with sm() as session:
            async with session.begin():
                if org_id is not None:
                    await session.execute(
                        text("SELECT set_config('app.current_org', :o, true)"),
                        {"o": str(org_id)},
                    )
                yield session

    return _factory




# --- Reset module-level singletons between tests ---
# Module-level singletons (engine, sessionmaker, redis pool, kafka producer)
# get bound to the event loop active at first use. With per-test loops, the
# next test reuses objects bound to a dead loop -> InterfaceError storms and
# "Connection._cancel was never awaited" warnings. We reset them per test.
@pytest_asyncio.fixture(autouse=True)
async def _reset_infra_singletons() -> AsyncIterator[None]:
    from app.infra import db as _db, redis as _redis, kafka as _kafka

    # Pre-test: clear any leftover singletons.
    _db._engine = None
    _db._sessionmaker = None
    _redis._pool = None
    _redis._client = None
    _kafka._producer = None

    yield

    # Post-test: best-effort dispose.
    try:
        if _db._engine is not None:
            await _db._engine.dispose()
    except Exception:
        pass
    try:
        if _kafka._producer is not None:
            await _kafka._producer.stop()
    except Exception:
        pass
    try:
        if _redis._client is not None:
            await _redis._client.aclose()
    except Exception:
        pass
    _db._engine = None
    _db._sessionmaker = None
    _redis._pool = None
    _redis._client = None
    _kafka._producer = None

# --- HTTP client --------------------------------------------------------


@pytest_asyncio.fixture
async def client() -> AsyncIterator[AsyncClient]:
    """Async HTTP client wired to the ASGI app."""
    reset_jwt_keys_for_tests()
    from app.main import create_app

    settings = get_settings()
    assert settings.environment == Environment.TEST, "tests must run with EAIP_ENVIRONMENT=test"

    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as c:
        # Trigger lifespan startup explicitly so background tasks are running.
        async with app.router.lifespan_context(app):
            yield c
