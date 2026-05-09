"""FastAPI dependency providers.

Imports here are kept minimal so route modules can ``from app.core.deps import ...``
without dragging the world along.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated
from uuid import UUID

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings, get_settings
from app.core.errors import AuthenticationError, AuthorizationError
from app.infra.db import session_for_request, session_unscoped


# --- principals ----------------------------------------------------------


class CurrentPrincipal:
    """Carries the authenticated principal for a request (user or API key)."""

    __slots__ = ("user_id", "org_id", "role", "scopes", "kind", "jti")

    def __init__(
        self,
        *,
        user_id: UUID | None,
        org_id: UUID,
        role: str,
        scopes: tuple[str, ...] = (),
        kind: str = "user",
        jti: str | None = None,
    ) -> None:
        self.user_id = user_id
        self.org_id = org_id
        self.role = role
        self.scopes = scopes
        self.kind = kind
        self.jti = jti


def get_current_principal(request: Request) -> CurrentPrincipal:
    """Return the authenticated principal — set by the auth middleware."""
    principal: CurrentPrincipal | None = getattr(request.state, "principal", None)
    if principal is None:
        raise AuthenticationError("Authentication required.")
    return principal


CurrentPrincipalDep = Annotated[CurrentPrincipal, Depends(get_current_principal)]


def require_role(*allowed: str) -> object:
    """Dependency factory: 403 unless the principal's role is in ``allowed``."""

    async def _checker(principal: CurrentPrincipalDep) -> CurrentPrincipal:
        if principal.role not in allowed:
            raise AuthorizationError(
                f"Role '{principal.role}' is not permitted for this operation."
            )
        return principal

    return Depends(_checker)


def require_scope(*required: str) -> object:
    async def _checker(principal: CurrentPrincipalDep) -> CurrentPrincipal:
        missing = [s for s in required if s not in principal.scopes and "*" not in principal.scopes]
        if missing:
            raise AuthorizationError(f"Missing required scopes: {', '.join(missing)}")
        return principal

    return Depends(_checker)


# --- DB session ----------------------------------------------------------


async def get_db(request: Request) -> AsyncIterator[AsyncSession]:
    """Yield a per-request DB session with ``app.current_org`` already set.

    The session factory enforces RLS by setting ``app.current_org`` via
    ``set_config(..., true)`` inside the session's transaction. This dep is
    the single place application code obtains a session.
    """
    org_id: UUID | None = getattr(request.state, "org_id", None)
    async for session in session_for_request(org_id=org_id):
        yield session


async def get_unscoped_db() -> AsyncIterator[AsyncSession]:
    """Yield a DB session that bypasses RLS — for login/refresh endpoints only.

    Mounting this dep in a route is an explicit, audited choice. Code review
    should question every new use; the default for tenant-scoped paths is
    ``get_db`` (which sets ``app.current_org``).
    """
    async for session in session_unscoped():
        yield session


DBSession = Annotated[AsyncSession, Depends(get_db)]
UnscopedDBSession = Annotated[AsyncSession, Depends(get_unscoped_db)]
SettingsDep = Annotated[Settings, Depends(get_settings)]
