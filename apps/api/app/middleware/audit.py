"""Request-level audit middleware.

Lightweight: every authenticated request emits an ``http.request`` audit event
*after* the response is composed, with status code and duration. This rides
the same outbox publisher as domain events — no Kafka calls in the request
hot path.

Important: the request handler's own DB session has already committed by the
time this middleware runs in BaseHTTPMiddleware (it executes around the inner
ASGI call). We therefore open a short-lived session of our own and write the
event there. The transactional outbox on the request itself still owns the
events emitted by the handler — this middleware only writes the http-level
envelope.
"""

from __future__ import annotations

import time
from uuid import UUID

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from app.core.logging import get_logger
from app.infra.db import session_for_request
from app.middleware.auth import _is_public, PUBLIC_PATHS
from app.modules.identity.events import emit_audit

logger = get_logger(__name__)


class AuditMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if _is_public(request.url.path, PUBLIC_PATHS):
            return await call_next(request)

        start = time.monotonic()
        response = await call_next(request)
        elapsed_ms = int((time.monotonic() - start) * 1000)

        principal = getattr(request.state, "principal", None)
        if principal is None:
            return response

        org_id: UUID = principal.org_id
        try:
            async for session in session_for_request(org_id=org_id):
                await emit_audit(
                    session,
                    org_id=org_id,
                    actor_user_id=principal.user_id,
                    actor_kind=principal.kind,
                    action="http.request",
                    target_type="http",
                    target_id=f"{request.method} {request.url.path}",
                    request_id=getattr(request.state, "request_id", None),
                    attributes={
                        "method": request.method,
                        "path": request.url.path,
                        "status": response.status_code,
                        "duration_ms": elapsed_ms,
                    },
                )
        except Exception as exc:  # pragma: no cover — never fail the response on an audit hiccup
            logger.warning("audit.middleware.failed", error=str(exc))

        return response
