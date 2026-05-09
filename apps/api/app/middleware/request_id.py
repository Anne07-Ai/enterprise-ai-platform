"""Request id middleware.

Reads ``X-Request-Id`` if the client supplied one (e.g. propagated from a
gateway). Otherwise generates a new ULID-shaped value. Sets request.state.request_id
and emits the same value on the response so clients can correlate.
"""

from __future__ import annotations

from uuid import uuid4

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from app.core.logging import bind_request_context, clear_request_context

HEADER = "X-Request-Id"


class RequestIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        rid = request.headers.get(HEADER) or uuid4().hex
        request.state.request_id = rid
        bind_request_context(request_id=rid)
        try:
            response = await call_next(request)
        finally:
            clear_request_context()
        response.headers[HEADER] = rid
        return response
