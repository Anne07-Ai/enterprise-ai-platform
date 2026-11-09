"""Organization bootstrap requires a user principal."""

from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from app.core.deps import CurrentPrincipal
from app.core.errors import AuthorizationError
from app.modules.identity import api, service
from app.modules.identity.schemas import OrganizationCreate
from sqlalchemy.ext.asyncio import AsyncSession


async def test_attributed_api_key_cannot_create_organization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # API-key authentication carries created_by as user_id for attribution.
    principal = CurrentPrincipal(
        kind="api_key", user_id=uuid4(), org_id=uuid4(), role="member", scopes=("*",)
    )
    bootstrap = AsyncMock(side_effect=AssertionError("API key reached organization bootstrap"))
    monkeypatch.setattr(service, "create_org", bootstrap)

    with pytest.raises(AuthorizationError):
        await api.create_org(
            OrganizationCreate(slug="forbidden-org", name="Forbidden Org"),
            Mock(spec=AsyncSession),
            principal,
        )

    bootstrap.assert_not_awaited()
