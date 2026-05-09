"""Authentication middleware.

Resolves the principal from either:
  * Bearer JWT (access tokens issued by /v1/auth/login)
  * X-Api-Key header (or Authorization: ApiKey <plaintext>)

Public paths (login, refresh, healthz, readyz, openapi.json, docs) are skipped.
The middleware sets ``request.state.principal`` and ``request.state.org_id``;
downstream code obtains them via ``get_current_principal`` / ``get_db``.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Final
from uuid import UUID

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from app.core.deps import CurrentPrincipal
from app.core.errors import AuthenticationError, domain_error_handler
from app.core.logging import bind_request_context
from app.infra.db import session_unscoped
from app.modules.identity.security import verify_access_token

PUBLIC_PATHS: Final[tuple[str, ...]] = (
    "/healthz",
    "/readyz",
    "/openapi.json",
    "/docs",
    "/redoc",
    "/v1/auth/login",
    "/v1/auth/refresh",
)


def _is_public(path: str, public: Iterable[str]) -> bool:
    return any(path == p or path.startswith(p + "/") for p in public)


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path
        if _is_public(path, PUBLIC_PATHS):
            return await call_next(request)

        try:
            principal = await self._resolve_principal(request)
        except AuthenticationError as exc:
            return await domain_error_handler(request, exc)

        request.state.principal = principal
        request.state.org_id = principal.org_id
        bind_request_context(
            org_id=str(principal.org_id),
            user_id=str(principal.user_id) if principal.user_id else None,
        )
        return await call_next(request)

    async def _resolve_principal(self, request: Request) -> CurrentPrincipal:
        # Prefer Bearer JWT.
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            payload = verify_access_token(auth[7:])
            return CurrentPrincipal(
                user_id=UUID(payload["sub"]),
                org_id=UUID(payload["org"]),
                role=payload["role"],
                scopes=tuple(payload.get("scopes", [])),
                kind="user",
                jti=payload.get("jti"),
            )

        # Then X-Api-Key.
        plaintext = request.headers.get("X-Api-Key")
        if plaintext is None and auth.startswith("ApiKey "):
            plaintext = auth[7:]
        if plaintext:
            from app.modules.identity import service as identity_service

            async for session in session_unscoped():
                key, _org = await identity_service.resolve_api_key(session, plaintext=plaintext)
                return CurrentPrincipal(
                    user_id=key.created_by,
                    org_id=key.org_id,
                    role="member",  # API keys default to member-level role; scopes drive checks.
                    scopes=tuple(key.scopes) if key.scopes else (),
                    kind="api_key",
                )

        raise AuthenticationError("Authentication required.")
