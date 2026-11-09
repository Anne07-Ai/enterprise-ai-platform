"""Integration test fixtures.

Integration tests rely on three testcontainers — Postgres, Redis, Redpanda.
The session-scoped fixture brings them up, runs Alembic upgrade, and tears
them down at the end of the test session. Per-test cleanup is done with
TRUNCATE between tests rather than container restart to keep the loop fast.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio
from app.core.config import Environment, get_settings
from app.modules.identity.security import reset_jwt_keys_for_tests
from httpx import ASGITransport, AsyncClient
from sqlalchemy import make_url, text
from sqlalchemy.exc import ArgumentError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from sqlalchemy.ext.asyncio import AsyncSession

# --- containers ---------------------------------------------------------


@contextmanager
def _start_containers() -> Iterator[dict[str, str]]:
    """Yield isolated service URLs, closing earlier containers if startup fails."""
    from testcontainers.kafka import RedpandaContainer
    from testcontainers.postgres import PostgresContainer
    from testcontainers.redis import RedisContainer

    with (
        PostgresContainer(
            "pgvector/pgvector:pg16",
            username="eaip_test_admin",
            password="eaip_test_admin",
            dbname="eaip_test",
        ) as pg,
        RedisContainer("redis:7-alpine") as rd,
        RedpandaContainer("redpandadata/redpanda:latest") as kf,
    ):
        host = pg.get_container_host_ip()
        port = pg.get_exposed_port(5432)
        yield {
            "EAIP_TEST_ADMIN_DATABASE_URL": (
                f"postgresql+asyncpg://eaip_test_admin:eaip_test_admin@{host}:{port}/eaip_test"
            ),
            "EAIP_DATABASE_URL": (
                f"postgresql+asyncpg://eaip_test_runtime:eaip_test_runtime@{host}:{port}/eaip_test"
            ),
            "EAIP_REDIS_URL": f"redis://{rd.get_container_host_ip()}:{rd.get_exposed_port(6379)}/0",
            "EAIP_KAFKA_BOOTSTRAP_SERVERS": kf.get_bootstrap_server(),
            "EAIP_ENVIRONMENT": "test",
            "EAIP_LOG_LEVEL": "WARNING",
            "EAIP_RATELIMIT_ENABLED": "false",
            "EAIP_OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:14317",
        }


@pytest.fixture(scope="session", autouse=True)
def _containers() -> Iterator[None]:
    """Use disposable containers unless a dedicated test stack is configured."""
    if os.environ.get("EAIP_SKIP_CONTAINERS") == "1":
        yield
        return
    with _start_containers() as env, pytest.MonkeyPatch.context() as patch:
        for key, value in env.items():
            patch.setenv(key, value)
        get_settings.cache_clear()
        try:
            yield
        finally:
            get_settings.cache_clear()


@pytest.fixture(scope="session")
def admin_database_url(_containers: None) -> str:
    """Require explicit, separate migration/cleanup and runtime test identities."""
    admin = os.environ.get("EAIP_TEST_ADMIN_DATABASE_URL")
    runtime = os.environ.get("EAIP_DATABASE_URL")
    if not admin or not runtime or os.environ.get("EAIP_ENVIRONMENT") != "test":
        pytest.fail("Integration tests require test mode and explicit admin and runtime URLs.")
    try:
        admin_url, runtime_url = make_url(admin), make_url(runtime)
    except (ArgumentError, ValueError):
        pytest.fail("Invalid integration database URL.", pytrace=False)
    if (
        admin_url.drivername != "postgresql+asyncpg"
        or runtime_url.drivername != "postgresql+asyncpg"
        or not admin_url.database
        or not admin_url.database.startswith("eaip_test")
        or (admin_url.host, admin_url.port, admin_url.database)
        != (runtime_url.host, runtime_url.port, runtime_url.database)
        or not admin_url.username
        or not runtime_url.username
        or admin_url.username == runtime_url.username
        or admin_url.query
        or runtime_url.query
    ):
        pytest.fail(
            "Admin and runtime URLs must use distinct logins on the same eaip_test database."
        )
    return admin


def _sync_url(url: str) -> str:
    return make_url(url).set(drivername="postgresql").render_as_string(hide_password=False)


@pytest.fixture(scope="session", autouse=True)
def _migrate(admin_database_url: str) -> None:
    """Run Alembic with the schema owner, then provision the test runtime login."""
    import psycopg2
    from alembic import command
    from alembic.config import Config
    from psycopg2 import sql

    api_root = Path(__file__).resolve().parents[2]
    cfg = Config(str(api_root / "alembic.ini"))
    cfg.set_main_option("script_location", str(api_root / "alembic"))
    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setenv("EAIP_DATABASE_URL", admin_database_url)
            get_settings.cache_clear()
            command.upgrade(cfg, "head")
    finally:
        get_settings.cache_clear()

    if os.environ.get("EAIP_SKIP_CONTAINERS") == "1":
        return  # External test stacks must provision runtime grants in advance.

    runtime = make_url(os.environ["EAIP_DATABASE_URL"])
    assert runtime.username is not None
    assert runtime.password is not None
    role = sql.Identifier(runtime.username)
    grants = (api_root.parents[1] / "scripts/postgres-runtime-grants.sql").read_text()
    conn = psycopg2.connect(_sync_url(admin_database_url))
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD %s").format(role),
                (runtime.password,),
            )
            cur.execute(grants.replace(':"runtime_role"', role.as_string(conn)))
    finally:
        conn.close()


# --- per-test cleanup ---------------------------------------------------


@pytest.fixture(autouse=True)
def _truncate_tables(_migrate: None, admin_database_url: str) -> Iterator[None]:
    """Reset disposable test rows using the schema owner and a loop-free driver."""
    yield
    import psycopg2

    conn = psycopg2.connect(_sync_url(admin_database_url))
    try:
        with conn, conn.cursor() as cur:
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
    async with sm() as session, session.begin():
        yield session


@pytest_asyncio.fixture
async def db_for_org() -> object:
    """Returns a callable that yields a session bound to the given org_id."""
    from app.infra.db import get_sessionmaker

    async def _factory(org_id: object) -> AsyncIterator[AsyncSession]:
        sm = get_sessionmaker()
        async with sm() as session, session.begin():
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
    from app.infra import db as _db
    from app.infra import kafka as _kafka
    from app.infra import redis as _redis

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
    # Trigger lifespan startup explicitly so background tasks are running.
    async with (
        AsyncClient(transport=transport, base_url="http://testserver") as c,
        app.router.lifespan_context(app),
    ):
        yield c
