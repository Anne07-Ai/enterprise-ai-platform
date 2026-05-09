"""Async Redis client wrapper, single global pool."""

from __future__ import annotations

from redis.asyncio import ConnectionPool, Redis

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_pool: ConnectionPool | None = None
_client: Redis | None = None


def get_redis() -> Redis:
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
            await _client.aclose()
            logger.info("redis.client.disposed")
        except (RuntimeError, Exception) as e:  # noqa: BLE001
            logger.debug("redis.client.dispose_suppressed", error=str(e))
    if _pool is not None:
        try:
            await _pool.disconnect(inuse_connections=True)
        except (RuntimeError, Exception) as e:  # noqa: BLE001
            logger.debug("redis.pool.dispose_suppressed", error=str(e))
    _client = None
    _pool = None


async def ping() -> None:
    await get_redis().ping()
