"""Idempotency middleware (per RFC draft-ietf-httpapi-idempotency-key-header).

Applies only to mutating methods (POST/PUT/PATCH/DELETE). Behavior:

  1. Client supplies ``Idempotency-Key: <uuid>``.
  2. We hash (method, path, body) and the key into a Redis cache slot.
  3. First request executes normally; the response is cached for TTL seconds.
  4. Subsequent requests with the SAME key + same body return the cached response.
  5. Same key with a DIFFERENT body returns 422 (idempotency conflict).
  6. Expired keys re-execute fresh.

In-flight detection (a second request arrives while the first is processing)
uses a SET NX lock — the second waits briefly, then returns 422 if not done.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import suppress
from typing import TYPE_CHECKING, Final, Protocol, cast

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from app.core.config import get_settings
from app.core.errors import IdempotencyConflictError, domain_error_handler
from app.infra.redis import get_redis

if TYPE_CHECKING:
    from collections.abc import AsyncIterable

    from starlette.requests import Request
    from starlette.types import Message


class _StreamingBody(Protocol):
    # Starlette's call_next returns a streaming response typed as Response.
    body_iterator: AsyncIterable[bytes | memoryview | str | Message]


HEADER: Final[str] = "Idempotency-Key"
MUTATING: Final[frozenset[str]] = frozenset({"POST", "PUT", "PATCH", "DELETE"})


def _fingerprint(method: str, path: str, body: bytes) -> str:
    h = hashlib.sha256()
    h.update(method.encode())
    h.update(b"|")
    h.update(path.encode())
    h.update(b"|")
    h.update(body)
    return h.hexdigest()


class IdempotencyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        settings = get_settings().idempotency
        if not settings.enabled or request.method not in MUTATING:
            return await call_next(request)

        key = request.headers.get(HEADER)
        if not key:
            return await call_next(request)

        body = await request.body()
        # Starlette caches the bytes from body() for the downstream handler.

        fingerprint = _fingerprint(request.method, request.url.path, body)
        org_id = getattr(request.state, "org_id", "anon")
        cache_key = f"{settings.redis_key_prefix}:{org_id}:{key}"
        redis = get_redis()

        cached = await redis.get(cache_key)
        if cached is not None:
            try:
                blob = json.loads(cached)
            except Exception:
                blob = None
            if blob and blob.get("fingerprint") == fingerprint:
                return Response(
                    content=blob["body"].encode()
                    if isinstance(blob["body"], str)
                    else blob["body"],
                    status_code=blob["status"],
                    headers={**blob.get("headers", {}), "Idempotent-Replay": "true"},
                    media_type=blob.get("media_type", "application/json"),
                )
            # Same key, different body — that's a conflict.
            with suppress(Exception):
                return await domain_error_handler(
                    request,
                    IdempotencyConflictError(
                        "Idempotency-Key reused with a different request body."
                    ),
                )

        # In-flight detection — short lived lock, the second caller waits up to ~2s.
        lock_key = f"{cache_key}:lock"
        if not await redis.set(lock_key, b"1", nx=True, ex=10):
            for _ in range(20):
                await asyncio.sleep(0.1)
                blob = await redis.get(cache_key)
                if blob is not None:
                    parsed = json.loads(blob)
                    return Response(
                        content=parsed["body"].encode()
                        if isinstance(parsed["body"], str)
                        else parsed["body"],
                        status_code=parsed["status"],
                        headers={**parsed.get("headers", {}), "Idempotent-Replay": "true"},
                        media_type=parsed.get("media_type", "application/json"),
                    )
            return await domain_error_handler(
                request,
                IdempotencyConflictError("Idempotent request still in flight."),
            )

        try:
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

            if 200 <= response.status_code < 500:
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
            await redis.delete(lock_key)
