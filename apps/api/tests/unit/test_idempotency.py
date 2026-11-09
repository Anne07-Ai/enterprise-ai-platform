"""Replay and lock-boundary regressions using an in-process ASGI client."""

import asyncio
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from app.core.deps import CurrentPrincipal
from app.middleware.idempotency import IdempotencyMiddleware
from httpx import ASGITransport, AsyncClient
from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route


class Cache:
    def __init__(self) -> None:
        self.records: dict[str, str] = {}
        self.contended = asyncio.Event()

    async def get(self, key: str) -> str | None:
        return self.records.get(key)

    async def set(self, key: str, value: str, *, nx: bool = False, ex: int) -> bool:
        if nx and key in self.records:
            self.contended.set()
            return False
        self.records[key] = value
        return True

    async def eval(self, script: str, numkeys: int, key: str, owner: str) -> None:
        if self.records.get(key) == owner:
            del self.records[key]


@pytest.mark.asyncio
async def test_mounted_credentials_and_anonymous_responses_are_never_cached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = Cache()
    monkeypatch.setattr("app.middleware.idempotency.get_redis", lambda: cache)

    async def credential(request: Request) -> JSONResponse:
        return JSONResponse({"plaintext": "synthetic value"}, status_code=201)

    async def stream(request: Request) -> StreamingResponse:
        async def body() -> AsyncIterator[str]:
            yield "data: synthetic\n\n"

        return StreamingResponse(body(), media_type="text/event-stream")

    app = Starlette(
        routes=[
            Route("/v1/api-keys", credential, methods=["POST"]),
            Route("/v1/auth/refresh", credential, methods=["POST"]),
            Route("/v1/chat/stream", stream, methods=["POST"]),
        ]
    )
    app.add_middleware(IdempotencyMiddleware)
    principal = CurrentPrincipal(user_id=uuid4(), org_id=uuid4(), role="owner")

    async def authenticate(request: Request, call_next: RequestResponseEndpoint) -> Response:
        if not request.url.path.endswith("/refresh"):
            request.state.principal = principal
        return await call_next(request)

    app.add_middleware(BaseHTTPMiddleware, dispatch=authenticate)
    async with AsyncClient(
        transport=ASGITransport(app=app, root_path="/gateway"), base_url="http://test"
    ) as client:
        headers = {"Idempotency-Key": "test-key"}
        refresh = await client.post("/gateway/v1/auth/refresh", headers=headers)
        assert refresh.status_code == 201
        sse = await client.post("/gateway/v1/chat/stream", headers=headers)
        assert sse.status_code == 200
        assert sse.headers["content-type"].startswith("text/event-stream")
        assert not cache.records

        issued = await client.post("/gateway/v1/api-keys", headers=headers)
        repeated = await client.post("/gateway/v1/api-keys", headers=headers)
        assert issued.status_code == 201
        assert repeated.status_code == 422
        assert len(cache.records) == 1
        assert "synthetic value" not in next(iter(cache.records.values()))


@pytest.mark.asyncio
async def test_contending_request_checks_fingerprint_before_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = Cache()
    monkeypatch.setattr("app.middleware.idempotency.get_redis", lambda: cache)
    running, finish = asyncio.Event(), asyncio.Event()

    async def operation(request: Request) -> JSONResponse:
        running.set()
        await finish.wait()
        return JSONResponse(await request.json())

    app = Starlette(routes=[Route("/operation", operation, methods=["POST"])])
    app.add_middleware(IdempotencyMiddleware)
    principal = CurrentPrincipal(user_id=uuid4(), org_id=uuid4(), role="owner")

    async def authenticate(request: Request, call_next: RequestResponseEndpoint) -> Response:
        request.state.principal = principal
        return await call_next(request)

    app.add_middleware(BaseHTTPMiddleware, dispatch=authenticate)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        headers = {"Idempotency-Key": "test-key"}
        async with asyncio.timeout(2):
            first = asyncio.create_task(client.post("/operation", headers=headers, json={"id": 1}))
            await running.wait()
            second = asyncio.create_task(client.post("/operation", headers=headers, json={"id": 2}))
            await cache.contended.wait()
            finish.set()
            issued, conflict = await asyncio.gather(first, second)
        assert issued.status_code == 200
        assert conflict.status_code == 422
        assert "different request body" in conflict.json()["detail"]
