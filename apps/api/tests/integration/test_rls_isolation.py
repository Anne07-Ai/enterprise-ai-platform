"""**LOAD-BEARING SECURITY TEST.**

Two organizations exist with their own memberships and API keys.
We bind a session to org A and assert that:

  * Queries WITHOUT a tenant predicate at the app layer return only A's rows.
  * Direct INSERTs targeting B's org_id are rejected by the RLS WITH CHECK clause.
  * A session bound to no tenant (current_org GUC unset) sees zero tenant rows.

This test is the sentinel for the entire platform's tenancy model. If it ever
fails, every release is blocked until the policy is fixed.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from app.core.config import get_settings
from app.infra import db, outbox
from app.infra.db import get_sessionmaker
from app.modules.identity.models import ApiKey, Membership, Organization, RefreshToken, User
from app.modules.identity.security import generate_api_key, hash_password
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration._helpers import make_user_and_org

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_cross_tenant_rows_are_invisible_under_rls() -> None:
    sm = get_sessionmaker()

    # Assert the premise of this test before interpreting any visible rows.
    async with sm() as session:
        role = await session.execute(
            text(
                """
                SELECT r.rolsuper, r.rolbypassrls, c.relrowsecurity, c.relforcerowsecurity,
                       pg_has_role(session_user, c.relowner, 'MEMBER')
                FROM pg_roles AS r, pg_class AS c
                WHERE r.rolname = session_user AND c.oid = 'public.memberships'::regclass
                """
            )
        )
        assert tuple(role.one()) == (False, False, True, True, False)
        assert await session.scalar(
            text(
                """
                SELECT NOT EXISTS (
                    SELECT 1 FROM pg_inherits
                    WHERE inhparent = 'public.audit_log'::regclass
                      AND has_table_privilege(session_user, inhrelid, 'SELECT')
                )
                """
            )
        )

    # 1. Create the universe (no tenant binding while inserting orgs/users —
    #    organizations and users are not RLS-restricted, only their per-tenant
    #    relations are).
    async with sm() as session, session.begin():
        org_a = Organization(slug="org-a", name="A", is_test=True)
        org_b = Organization(slug="org-b", name="B", is_test=True)
        user_a = User(
            email="a@example.com", display_name="A", password_hash=hash_password("xxxxxxxx")
        )
        user_b = User(
            email="b@example.com", display_name="B", password_hash=hash_password("yyyyyyyy")
        )
        session.add_all([org_a, org_b, user_a, user_b])
        await session.flush()
        org_a_id, org_b_id = org_a.id, org_b.id
        user_a_id, user_b_id = user_a.id, user_b.id

    # 2. Insert tenant-scoped rows for each org with the GUC bound to that tenant.
    async def _populate(org_id: object, user_id: object) -> None:
        async with sm() as session, session.begin():
            await session.execute(
                text("SELECT set_config('app.current_org', :o, true)"),
                {"o": str(org_id)},
            )
            session.add(Membership(org_id=org_id, user_id=user_id, role="owner"))
            _, prefix, h = generate_api_key(is_test=True)
            session.add(
                ApiKey(
                    org_id=org_id,
                    created_by=user_id,
                    name="seed",
                    prefix=prefix,
                    hash=h,
                    scopes=[],
                )
            )

    await _populate(org_a_id, user_a_id)
    await _populate(org_b_id, user_b_id)

    # 3. Bound to A: queries with NO app-layer tenant predicate must return ONLY A's rows.
    async with sm() as session, session.begin():
        await session.execute(
            text("SELECT set_config('app.current_org', :o, true)"),
            {"o": str(org_a_id)},
        )
        memberships = (await session.execute(select(Membership))).scalars().all()
        api_keys = (await session.execute(select(ApiKey))).scalars().all()

        assert len(memberships) == 1, "RLS leaked B's membership row to A"
        assert memberships[0].org_id == org_a_id
        assert len(api_keys) == 1, "RLS leaked B's API key row to A"
        assert api_keys[0].org_id == org_a_id

    # 4. Bound to A: WITH CHECK should reject an insert targeting B.
    with pytest.raises(DBAPIError) as rejected:
        async with sm() as session, session.begin():
            await session.execute(
                text("SELECT set_config('app.current_org', :o, true)"),
                {"o": str(org_a_id)},
            )
            session.add(Membership(org_id=org_b_id, user_id=user_a_id, role="member"))
            await session.flush()
    assert getattr(rejected.value.orig, "sqlstate", None) == "42501"

    # 5. With NO GUC set: tenant tables must return zero rows (fail-closed).
    async with sm() as session, session.begin():
        memberships = (await session.execute(select(Membership))).scalars().all()
        api_keys = (await session.execute(select(ApiKey))).scalars().all()
        tokens = (await session.execute(select(RefreshToken))).scalars().all()
        assert memberships == []
        assert api_keys == []
        assert tokens == []


@pytest.mark.parametrize("rollback", [False, True], ids=["commit", "rollback"])
async def test_pool_resets_bypass_and_tenant_scope(
    monkeypatch: pytest.MonkeyPatch, rollback: bool
) -> None:
    settings = get_settings()
    engine = db._build_engine(
        settings.model_copy(
            update={
                "database": settings.database.model_copy(update={"pool_size": 1, "max_overflow": 0})
            }
        )
    )
    factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    monkeypatch.setattr(db, "_sessionmaker", factory)

    async def state(session: AsyncSession) -> tuple[int, str | None, bool]:
        row = (
            await session.execute(
                text(
                    """
                    SELECT pg_backend_pid(),
                           NULLIF(current_setting('app.current_org', true), ''),
                           COALESCE(current_setting('app.bypass_rls', true), '') = 'on'
                    """
                )
            )
        ).one()
        return int(row[0]), row[1], bool(row[2])

    try:
        _, org_a = await make_user_and_org(email="a@example.com", org_slug="pool-a")
        _, org_b = await make_user_and_org(email="b@example.com", org_slug="pool-b")
        scoped = asynccontextmanager(db.session_for_request)
        unscoped = asynccontextmanager(db.session_unscoped)

        async with scoped(org_id=None) as session:
            pid, tenant, bypass = await state(session)
            assert tenant is None
            assert bypass is False

        async def exercise_internal_bypass() -> None:
            async with unscoped() as session:
                assert await state(session) == (pid, None, True)
                assert set(await session.scalars(select(Membership.org_id))) == {
                    org_a.id,
                    org_b.id,
                }
                await session.execute(
                    text("SELECT set_config('app.current_org', :org, true)"),
                    {"org": str(org_a.id)},
                )
                session.add(Organization(slug="pool-probe", name="Probe", is_test=True))
                await session.flush()
                if rollback:
                    raise RuntimeError("transaction rollback probe")

        if rollback:
            with pytest.raises(RuntimeError, match="transaction rollback probe"):
                await exercise_internal_bypass()
        else:
            await exercise_internal_bypass()

        async with scoped(org_id=None) as session:
            assert await state(session) == (pid, None, False)
            assert (await session.scalars(select(Membership.org_id))).all() == []
            probe = await session.scalar(
                select(Organization.id).where(Organization.slug == "pool-probe")
            )
            assert (probe is None) is rollback

        for org in (org_a, org_b):
            async with scoped(org_id=org.id) as session:
                assert await state(session) == (pid, str(org.id), False)
                assert (await session.scalars(select(Membership.org_id))).all() == [org.id]
    finally:
        await engine.dispose()


async def test_internal_outbox_publisher_retains_scoped_cross_tenant_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, org_a = await make_user_and_org(email="a@example.com", org_slug="outbox-a")
    _, org_b = await make_user_and_org(email="b@example.com", org_slug="outbox-b")
    scoped = asynccontextmanager(db.session_for_request)
    for org in (org_a, org_b):
        async with scoped(org_id=org.id) as session:
            await outbox.enqueue(session, topic="test.events", payload={}, org_id=org.id)

    async with scoped(org_id=None) as session:
        assert await session.scalar(text("SELECT count(*) FROM outbox")) == 0

    send = AsyncMock()
    monkeypatch.setattr(outbox.kafka_infra, "send", send)
    assert await outbox._publish_batch() == 2
    assert send.await_count == 2

    async with scoped(org_id=None) as session:
        assert await session.scalar(text("SELECT count(*) FROM outbox")) == 0
    for org in (org_a, org_b):
        async with scoped(org_id=org.id) as session:
            result = await session.execute(text("SELECT org_id, published_at FROM outbox"))
            row = result.one()
            assert row.org_id == org.id
            assert row.published_at is not None
