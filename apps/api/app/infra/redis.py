"""Async Redis client wrapper, single global pool."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, cast

from redis.asyncio import ConnectionPool, Redis

from app.core.config import get_settings
from app.core.logging import get_logger

if TYPE_CHECKING:
    from redis.asyncio.connection import Connection


class _AsyncClose(Protocol):
    async def aclose(self) -> None: ...


logger = get_logger(__name__)

_pool: ConnectionPool[Connection] | None = None
_client: Redis[bytes] | None = None


def get_redis() -> Redis[bytes]:
    global _pool, _client
    if _client is None:
        settings = get_settings()
        _pool = ConnectionPool.from_url(
            settings.redis.url.get_secret_value(),
            max_connections=settings.redis.pool_max_connections,
            socket_timeout=settings.redis.socket_timeout_seconds,
            decode_responses=False,
        )
        _client = Redis(connection_pool=_pool)
        logger.info("redis.client.initialized")
    return _client


async def dispose_redis() -> None:
    """Dispose redis client and pool. Best-effort: errors during loop
    teardown (e.g. pytest teardown firing on a closed loop) are swallowed."""
    global _pool, _client
    if _client is not None:
        try:
            # redis>=5.2 supports aclose; the dev stubs predate that API.
            await cast("_AsyncClose", _client).aclose()
            logger.info("redis.client.disposed")
        except (RuntimeError, Exception) as e:
            logger.debug("redis.client.dispose_suppressed", error=str(e))
    if _pool is not None:
        try:
            await _pool.disconnect(inuse_connections=True)
        except (RuntimeError, Exception) as e:
            logger.debug("redis.pool.dispose_suppressed", error=str(e))
    _client = None
    _pool = None


async def ping() -> None:
    await get_redis().ping()
