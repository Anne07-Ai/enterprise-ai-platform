"""PostgreSQL DELETE RETURNING preserves identity repository semantics."""

from contextlib import asynccontextmanager

import pytest
from app.core.errors import NotFoundError
from app.infra.db import session_for_request
from app.modules.identity import repository

from tests.integration._helpers import make_user_and_org

pytestmark = pytest.mark.integration


async def test_delete_distinguishes_removed_missing_and_rls_hidden_rows() -> None:
    owner, org = await make_user_and_org()
    _, other_org = await make_user_and_org(email="other@example.com", org_slug="other")
    scoped = asynccontextmanager(session_for_request)

    async with scoped(org_id=org.id) as session:
        membership = await repository.get_membership(session, org_id=org.id, user_id=owner.id)
        assert membership is not None
        membership_id = membership.id

    async with scoped(org_id=other_org.id) as session:
        with pytest.raises(NotFoundError):
            await repository.delete_membership(session, membership_id=membership_id)

    async with scoped(org_id=org.id) as session:
        await repository.delete_membership(session, membership_id=membership_id)
        with pytest.raises(NotFoundError):
            await repository.delete_membership(session, membership_id=membership_id)
        await repository.delete_org(session, org_id=org.id)
        with pytest.raises(NotFoundError):
            await repository.delete_org(session, org_id=org.id)
