"""Tenant middleware.

Auth middleware sets ``request.state.org_id`` from the JWT/API-key.
This middleware is the place to validate that the requested resource path
belongs to the active org, and to refresh the membership/role from the DB
if the access token is older than a few seconds (defense in depth against
revoked memberships).

For Phase 2 we keep it minimal — the JWT carries the role, and per-request
membership re-validation is deferred to Phase 3 once we have observability
to measure the cost.
"""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from app.middleware.auth import _is_public, PUBLIC_PATHS


class TenantMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if _is_public(request.url.path, PUBLIC_PATHS):
            return await call_next(request)
        # The DB session created via get_db() reads request.state.org_id
        # and runs SET LOCAL inside the transaction. Nothing else to do here
        # for Phase 2 — but this layer is the right place for future
        # cross-tenant resource-path validation (e.g. /v1/orgs/{id}/...).
        return await call_next(request)
