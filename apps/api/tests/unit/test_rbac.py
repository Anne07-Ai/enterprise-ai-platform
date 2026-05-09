"""Unit tests for the role/permission matrix."""

from __future__ import annotations

import pytest

from app.modules.identity.rbac import (
    OrgRole,
    has_permission,
    role_at_least,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("role", "permission", "expected"),
    [
        (OrgRole.OWNER, "org:delete", True),
        (OrgRole.ADMIN, "org:delete", False),
        (OrgRole.ADMIN, "members:invite", True),
        (OrgRole.MEMBER, "members:invite", False),
        (OrgRole.MEMBER, "org:read", True),
        (OrgRole.VIEWER, "api_keys:read", False),
    ],
)
def test_permission_matrix(role: str, permission: str, expected: bool) -> None:
    assert has_permission(role, permission) is expected


@pytest.mark.unit
def test_role_ranking() -> None:
    assert role_at_least(OrgRole.OWNER, OrgRole.ADMIN)
    assert role_at_least(OrgRole.ADMIN, OrgRole.ADMIN)
    assert not role_at_least(OrgRole.MEMBER, OrgRole.ADMIN)
    assert not role_at_least(OrgRole.VIEWER, OrgRole.MEMBER)
