"""Token-bucket rate limiting backed by Redis.

Two buckets per request: per-org and per-IP. The limit returns 429 with a
``Retry-After`` header when either bucket is empty.

Implementation uses a Lua script for atomicity. Each call updates the bucket
state (tokens, last refill timestamp) and returns whether the request is
allowed plus the seconds until the next token would be available.
"""

from __future__ import annotations

import time

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from app.core.config import get_settings
from app.core.errors import RateLimitedError, domain_error_handler
from app.infra.redis import get_redis
from app.middleware.auth import _is_public, PUBLIC_PATHS

# KEYS[1] = bucket key
# ARGV[1] = capacity, ARGV[2] = refill_rate_per_s, ARGV[3] = now_ms, ARGV[4] = ttl
# Returns: {allowed (0/1), retry_after_ms}
_LUA_TOKEN_BUCKET = """
local tokens_key = KEYS[1] .. ":t"
local ts_key     = KEYS[1] .. ":s"
local capacity   = tonumber(ARGV[1])
local rate       = tonumber(ARGV[2])
local now_ms     = tonumber(ARGV[3])
local ttl        = tonumber(ARGV[4])

local tokens = tonumber(redis.call('GET', tokens_key))
local last   = tonumber(redis.call('GET', ts_key))
if tokens == nil then tokens = capacity end
if last == nil then last = now_ms end

local elapsed = math.max(0, now_ms - last)
tokens = math.min(capacity, tokens + (elapsed / 1000.0) * rate)

local allowed = 0
local retry_ms = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry_ms = math.ceil((1 - tokens) * 1000.0 / rate)
end

redis.call('SET', tokens_key, tokens, 'PX', ttl)
redis.call('SET', ts_key, now_ms, 'PX', ttl)

return {allowed, retry_ms}
"""


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: object) -> None:  # noqa: D401
        super().__init__(app)
        self._script_sha: str | None = None

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        settings = get_settings().rate_limit
        if not settings.enabled or _is_public(request.url.path, PUBLIC_PATHS):
            return await call_next(request)

        try:
            await self._enforce(request, settings)
        except RateLimitedError as exc:
            return await domain_error_handler(request, exc)
        return await call_next(request)

    async def _enforce(self, request: Request, s: object) -> None:  # type: ignore[no-untyped-def]
        # Bypass if request hasn't been authenticated yet (e.g. /v1/auth/login).
        org_id = getattr(request.state, "org_id", None)
        ip = request.client.host if request.client else "unknown"

        redis = get_redis()
        if self._script_sha is None:
            self._script_sha = await redis.script_load(_LUA_TOKEN_BUCKET)

        now_ms = int(time.time() * 1000)
        ttl = max(60_000, int(s.per_org_burst / max(s.per_org_rps, 1) * 2_000))  # type: ignore[attr-defined]

        # Per-IP bucket (always).
        result = await redis.evalsha(
            self._script_sha,
            1,
            f"{s.redis_key_prefix}:ip:{ip}",  # type: ignore[attr-defined]
            s.per_ip_burst,                   # type: ignore[attr-defined]
            s.per_ip_rps,                     # type: ignore[attr-defined]
            now_ms,
            ttl,
        )
        if result[0] == 0:
            raise RateLimitedError(
                "IP rate limit exceeded.",
                retry_after=max(1, int(result[1] / 1000)),
            )

        # Per-org bucket if we know the tenant.
        if org_id is not None:
            result = await redis.evalsha(
                self._script_sha,
                1,
                f"{s.redis_key_prefix}:org:{org_id}",  # type: ignore[attr-defined]
                s.per_org_burst,                       # type: ignore[attr-defined]
                s.per_org_rps,                         # type: ignore[attr-defined]
                now_ms,
                ttl,
            )
            if result[0] == 0:
                raise RateLimitedError(
                    "Organization rate limit exceeded.",
                    retry_after=max(1, int(result[1] / 1000)),
                )
