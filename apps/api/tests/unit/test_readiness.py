"""Readiness and startup lifecycle with deterministic, network-free dependencies."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio
from app import main
from app.api.v1 import healthz
from app.infra import db, kafka, outbox
from app.infra import redis as redis_infra
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = pytest.mark.unit


@pytest.fixture
def probes(monkeypatch: pytest.MonkeyPatch) -> dict[str, AsyncMock]:
    backends = {"postgres": db, "redis": redis_infra, "kafka": kafka}
    checks = {name: AsyncMock() for name in backends}
    for name, backend in backends.items():
        monkeypatch.setattr(backend, "ping", checks[name])
    return checks


@pytest.mark.asyncio
async def test_readyz_success_checks_all_dependencies(probes: dict[str, AsyncMock]) -> None:
    async with AsyncClient(
        transport=ASGITransport(app=main.create_app()), base_url="http://testserver"
    ) as client:
        response = await client.get("/readyz")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "checks": {"postgres": "ok", "redis": "ok", "kafka": "ok"},
    }
    for probe in probes.values():
        probe.assert_awaited_once()


@pytest.mark.asyncio
async def test_readyz_503_lists_only_failed_dependency_names(
    monkeypatch: pytest.MonkeyPatch, probes: dict[str, AsyncMock]
) -> None:
    probes["postgres"].side_effect = RuntimeError("fake private PostgreSQL connection details")
    probes["kafka"].side_effect = RuntimeError("fake private Kafka connection details")
    logger = Mock()
    monkeypatch.setattr(healthz, "logger", logger)

    async with AsyncClient(
        transport=ASGITransport(app=main.create_app()), base_url="http://testserver"
    ) as client:
        response = await client.get("/readyz")

    assert response.status_code == 503
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json()["detail"] == "Unavailable dependencies: postgres, kafka."
    assert "private" not in response.text
    assert "private" not in str(logger.mock_calls)
    probes["redis"].assert_awaited_once()


@pytest.mark.asyncio
async def test_readiness_timeout_cancels_probe(
    monkeypatch: pytest.MonkeyPatch, probes: dict[str, AsyncMock]
) -> None:
    cancelled = asyncio.Event()

    async def hanging_ping() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    probes["redis"].side_effect = hanging_ping
    monkeypatch.setattr(healthz, "_PROBE_TIMEOUT_SECONDS", 0.05)
    async with AsyncClient(
        transport=ASGITransport(app=main.create_app()), base_url="http://testserver"
    ) as client:
        response = await client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["detail"] == "Unavailable dependencies: redis."
    assert cancelled.is_set()


@pytest.fixture
def kafka_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(kafka, "_producer", None)
    monkeypatch.setattr(kafka, "_producer_lock", None)
    monkeypatch.setattr(kafka, "_producer_loop", None)


@pytest_asyncio.fixture
async def started_client(
    monkeypatch: pytest.MonkeyPatch, kafka_state: None
) -> AsyncIterator[AsyncClient]:
    # Use the configured loop scopes, like the integration client fixture.
    startup_loop = asyncio.get_running_loop()

    async def on_startup_loop(*_args: object) -> None:
        assert asyncio.get_running_loop() is startup_loop

    execute = AsyncMock(side_effect=on_startup_loop)
    session = AsyncMock()
    session.__aenter__.return_value = SimpleNamespace(execute=execute)
    redis_ping = AsyncMock(side_effect=on_startup_loop)
    fetch_metadata = AsyncMock(side_effect=on_startup_loop)
    producer = SimpleNamespace(
        start=AsyncMock(side_effect=on_startup_loop),
        stop=AsyncMock(side_effect=on_startup_loop),
        client=SimpleNamespace(fetch_all_metadata=fetch_metadata),
    )
    monkeypatch.setattr(main, "setup_tracing", Mock())
    monkeypatch.setattr(main, "get_engine", Mock())
    monkeypatch.setattr(main, "dispose_engine", AsyncMock(side_effect=on_startup_loop))
    monkeypatch.setattr(db, "get_sessionmaker", Mock(return_value=Mock(return_value=session)))
    monkeypatch.setattr(
        redis_infra, "get_redis", Mock(return_value=SimpleNamespace(ping=redis_ping))
    )
    monkeypatch.setattr(redis_infra, "dispose_redis", AsyncMock(side_effect=on_startup_loop))
    monkeypatch.setattr(kafka, "ShutdownProducer", Mock(return_value=producer))
    monkeypatch.setattr(outbox, "start_publisher", AsyncMock(side_effect=on_startup_loop))
    monkeypatch.setattr(outbox, "stop_publisher", AsyncMock(side_effect=on_startup_loop))

    app = main.create_app()
    async with (
        AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client,
        app.router.lifespan_context(app),
    ):
        yield client


@pytest.mark.asyncio
async def test_readiness_request_shares_lifespan_loop(started_client: AsyncClient) -> None:
    response = await started_client.get("/readyz")

    assert response.status_code == 200, response.text
    assert response.json()["checks"] == {"postgres": "ok", "redis": "ok", "kafka": "ok"}


@pytest.mark.asyncio
async def test_kafka_start_failure_closes_candidate_and_allows_retry(
    monkeypatch: pytest.MonkeyPatch, kafka_state: None
) -> None:
    failed = SimpleNamespace(
        start=AsyncMock(side_effect=RuntimeError("start failed")),
        stop=AsyncMock(),
    )
    healthy = SimpleNamespace(start=AsyncMock(), stop=AsyncMock())
    constructor = Mock(side_effect=[failed, healthy])
    monkeypatch.setattr(kafka, "ShutdownProducer", constructor)

    with pytest.raises(RuntimeError, match="start failed"):
        await kafka.get_producer()
    failed.stop.assert_awaited_once()
    assert kafka._producer is None
    try:
        assert await kafka.get_producer() is healthy
        healthy.start.assert_awaited_once()
    finally:
        await kafka.dispose_producer()
    healthy.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_kafka_callers_wait_for_one_started_producer(
    monkeypatch: pytest.MonkeyPatch, kafka_state: None
) -> None:
    starting, complete, second_entered = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def start() -> None:
        starting.set()
        await complete.wait()

    async def get_second() -> object:
        second_entered.set()
        return await kafka.get_producer()

    producer = SimpleNamespace(start=AsyncMock(side_effect=start), stop=AsyncMock())
    constructor = Mock(return_value=producer)
    monkeypatch.setattr(kafka, "ShutdownProducer", constructor)
    first = asyncio.create_task(kafka.get_producer())
    await starting.wait()
    second = asyncio.create_task(get_second())
    try:
        await second_entered.wait()
        assert not second.done()
    finally:
        complete.set()
        first_result, second_result = await asyncio.gather(first, second)
        await kafka.dispose_producer()

    assert first_result is producer
    assert second_result is producer
    constructor.assert_called_once()
    producer.start.assert_awaited_once()


@pytest.mark.asyncio
async def test_kafka_cancelled_start_releases_candidate(
    monkeypatch: pytest.MonkeyPatch, kafka_state: None
) -> None:
    starting = asyncio.Event()

    async def start() -> None:
        starting.set()
        await asyncio.Event().wait()

    producer = SimpleNamespace(start=AsyncMock(side_effect=start), stop=AsyncMock())
    monkeypatch.setattr(kafka, "ShutdownProducer", Mock(return_value=producer))
    task = asyncio.create_task(kafka.get_producer())
    await starting.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    producer.stop.assert_awaited_once()
    assert kafka._producer is None


@pytest.mark.asyncio
async def test_lifespan_unwinds_resources_after_partial_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def close(name: str) -> None:
        calls.append(name)

    monkeypatch.setattr(main, "setup_tracing", Mock())
    monkeypatch.setattr(main, "get_engine", Mock())
    monkeypatch.setattr(redis_infra, "get_redis", Mock())
    monkeypatch.setattr(kafka, "get_producer", AsyncMock(side_effect=RuntimeError("start failed")))
    monkeypatch.setattr(kafka, "dispose_producer", lambda: close("kafka"))
    monkeypatch.setattr(redis_infra, "dispose_redis", lambda: close("redis"))
    monkeypatch.setattr(main, "dispose_engine", lambda: close("postgres"))
    start_publisher = AsyncMock()
    monkeypatch.setattr(outbox, "start_publisher", start_publisher)

    with pytest.raises(RuntimeError, match="start failed"):
        async with main.lifespan(FastAPI()):
            pytest.fail("Application started after dependency failure")

    assert calls == ["kafka", "redis", "postgres"]
    start_publisher.assert_not_awaited()
