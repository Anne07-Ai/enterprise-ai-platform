"""Idempotency middleware (per RFC draft-ietf-httpapi-idempotency-key-header).

Applies only to mutating methods (POST/PUT/PATCH/DELETE). Behavior:

  1. Client supplies ``Idempotency-Key: <uuid>``.
  2. We scope the Redis cache slot to the authenticated actor and permissions.
  3. First request executes normally; the response is cached for TTL seconds.
  4. Subsequent requests with the SAME key + same body return the cached response.
     Successful API key issuance stores only a completion marker.
  5. Same key with a DIFFERENT body returns 422 (idempotency conflict).
  6. Expired keys re-execute fresh.

In-flight detection (a second request arrives while the first is processing)
uses a SET NX lock — the second waits briefly, then returns 422 if not done.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from typing import TYPE_CHECKING, Any, Final, Protocol, cast
from uuid import uuid4

from starlette._utils import get_route_path
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from app.core.config import get_settings
from app.core.errors import IdempotencyConflictError, domain_error_handler
from app.infra.redis import get_redis

if TYPE_CHECKING:
    from collections.abc import AsyncIterable

    from starlette.requests import Request
    from starlette.types import Message

    from app.core.deps import CurrentPrincipal


class _StreamingBody(Protocol):
    # Starlette's call_next returns a streaming response typed as Response.
    body_iterator: AsyncIterable[bytes | memoryview | str | Message]


class _RedisScript(Protocol):
    async def eval(self, script: str, numkeys: int, *keys_and_args: str) -> object: ...


HEADER: Final[str] = "Idempotency-Key"
MUTATING: Final[frozenset[str]] = frozenset({"POST", "PUT", "PATCH", "DELETE"})
RELEASE_LOCK: Final[str] = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


def _fingerprint(method: str, path: str, body: bytes) -> str:
    h = hashlib.sha256()
    h.update(method.encode())
    h.update(b"|")
    h.update(path.encode())
    h.update(b"|")
    h.update(body)
    return h.hexdigest()


def _principal_scope(principal: CurrentPrincipal) -> str:
    actor = [
        str(principal.org_id),
        principal.kind,
        str(principal.api_key_id or principal.user_id),
        principal.role,
        sorted(set(principal.scopes)),
    ]
    return hashlib.sha256(json.dumps(actor).encode()).hexdigest()


async def _replay(request: Request, record: dict[str, Any], fingerprint: str) -> Response:
    if record.get("fingerprint") != fingerprint:
        return await domain_error_handler(
            request,
            IdempotencyConflictError("Idempotency-Key reused with a different request body."),
        )
    if record.get("completed"):
        return await domain_error_handler(
            request,
            IdempotencyConflictError("API key already issued. Its plaintext is shown once."),
        )
    body = record["body"]
    return Response(
        content=body.encode() if isinstance(body, str) else body,
        status_code=record["status"],
        headers={**record.get("headers", {}), "Idempotent-Replay": "true"},
        media_type=record.get("media_type", "application/json"),
    )


class IdempotencyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        settings = get_settings().idempotency
        if not settings.enabled or request.method not in MUTATING:
            return await call_next(request)

        key = request.headers.get(HEADER)
        principal: CurrentPrincipal | None = getattr(request.state, "principal", None)
        route_path = get_route_path(request.scope).rstrip("/")
        if not key or principal is None or route_path == "/v1/chat/stream":
            return await call_next(request)

        body = await request.body()
        # Starlette caches the bytes from body() for the downstream handler.

        fingerprint = _fingerprint(
            request.method, request.scope["path"] + "?" + request.url.query, body
        )
        key_digest = hashlib.sha256(key.encode()).hexdigest()
        cache_key = f"{settings.redis_key_prefix}:v2:{_principal_scope(principal)}:{key_digest}"
        credential_issuance = request.method == "POST" and route_path == "/v1/api-keys"
        redis = get_redis()

        async def cached_response() -> Response | None:
            cached = await redis.get(cache_key)
            if cached is None:
                return None
            try:
                record = json.loads(cached)
            except (UnicodeDecodeError, json.JSONDecodeError):
                return None
            if isinstance(record, dict):
                return await _replay(request, record, fingerprint)
            return None

        replay = await cached_response()
        if replay is not None:
            return replay

        # In-flight detection — short lived lock, the second caller waits up to ~2s.
        lock_key = f"{cache_key}:lock"
        lock_owner = uuid4().hex
        if not await redis.set(lock_key, lock_owner, nx=True, ex=10):
            for _ in range(20):
                await asyncio.sleep(0.1)
                replay = await cached_response()
                if replay is not None:
                    return replay
            return await domain_error_handler(
                request,
                IdempotencyConflictError("Idempotent request still in flight."),
            )

        try:
            replay = await cached_response()
            if replay is not None:
                return replay
            response = await call_next(request)
            chunks: list[bytes] = []
            async for chunk in cast("_StreamingBody", response).body_iterator:
                if isinstance(chunk, (bytes, memoryview)):
                    chunks.append(bytes(chunk))
                elif isinstance(chunk, str):
                    chunks.append(chunk.encode())
                else:
                    raise TypeError("Idempotent responses require an inline body.")
            response_body = b"".join(chunks)

            if credential_issuance:
                if 200 <= response.status_code < 300:
                    marker = {"fingerprint": fingerprint, "completed": True}
                    await redis.set(cache_key, json.dumps(marker), ex=settings.ttl_seconds)
            elif 200 <= response.status_code < 500:
                payload = {
                    "fingerprint": fingerprint,
                    "status": response.status_code,
                    "body": response_body.decode("utf-8", errors="replace"),
                    "headers": {
                        k: v
                        for k, v in response.headers.items()
                        if k.lower() not in {"content-length"}
                    },
                    "media_type": response.media_type,
                }
                await redis.set(cache_key, json.dumps(payload), ex=settings.ttl_seconds)

            return Response(
                content=response_body,
                status_code=response.status_code,
                headers=dict(response.headers),
                media_type=response.media_type,
            )
        finally:
            # redis-py's eval() awaits the command on the async client.
            await cast("_RedisScript", redis).eval(RELEASE_LOCK, 1, lock_key, lock_owner)
