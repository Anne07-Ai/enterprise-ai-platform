"""Role/permission primitives.

Roles are coarse buckets that map to permission sets. Use ``has_permission``
or the FastAPI dep ``require_permission`` for fine-grained checks at handlers.
"""

from __future__ import annotations

from enum import StrEnum

from fastapi import Depends

from app.core.deps import CurrentPrincipalDep
from app.core.errors import AuthorizationError


class OrgRole(StrEnum):
    OWNER = "owner"
    ADMIN = "admin"
    MEMBER = "member"
    VIEWER = "viewer"


# Permission strings live in the format "<resource>:<action>".
# Roles map to permission sets. ROLE_RANK is used for "role >= X" comparisons.

ROLE_RANK: dict[str, int] = {
    OrgRole.VIEWER: 0,
    OrgRole.MEMBER: 1,
    OrgRole.ADMIN: 2,
    OrgRole.OWNER: 3,
}

ROLE_PERMISSIONS: dict[str, frozenset[str]] = {
    OrgRole.OWNER: frozenset(
        {
            "org:read", "org:update", "org:delete",
            "members:read", "members:invite", "members:remove", "members:change_role",
            "api_keys:read", "api_keys:create", "api_keys:revoke",
        }
    ),
    OrgRole.ADMIN: frozenset(
        {
            "org:read", "org:update",
            "members:read", "members:invite", "members:remove", "members:change_role",
            "api_keys:read", "api_keys:create", "api_keys:revoke",
        }
    ),
    OrgRole.MEMBER: frozenset(
        {
            "org:read",
            "members:read",
            "api_keys:read",
        }
    ),
    OrgRole.VIEWER: frozenset(
        {
            "org:read",
            "members:read",
        }
    ),
}


def has_permission(role: str, permission: str) -> bool:
    return permission in ROLE_PERMISSIONS.get(role, frozenset())


def role_at_least(role: str, threshold: str) -> bool:
    return ROLE_RANK.get(role, -1) >= ROLE_RANK.get(threshold, 99)


def require_permission(permission: str) -> object:
    """FastAPI dep factory: 403 unless the principal's role grants ``permission``.

    API keys also flow through here — a key's ``scopes`` list (e.g. ``["org:read"]``)
    is treated equivalently to a role-derived permission. ``"*"`` in scopes grants all.
    """

    async def _checker(principal: CurrentPrincipalDep) -> None:
        if "*" in principal.scopes or permission in principal.scopes:
            return
        if has_permission(principal.role, permission):
            return
        raise AuthorizationError(
            f"Missing permission '{permission}' for role '{principal.role}'."
        )

    return Depends(_checker)
