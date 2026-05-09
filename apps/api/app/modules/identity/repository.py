"""Data access for identity. Service-layer code calls these methods, not the
ORM directly, so query patterns are auditable and tenant-filter discipline is
centralized.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import and_, delete, select, update
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ConflictError, NotFoundError
from app.modules.identity.models import (
    ApiKey,
    Membership,
    Organization,
    RefreshToken,
    User,
)


# --- organizations -------------------------------------------------------


async def get_org_by_id(session: AsyncSession, org_id: UUID) -> Organization:
    org = await session.get(Organization, org_id)
    if org is None:
        raise NotFoundError(f"Organization {org_id} not found.")
    return org


async def get_org_by_slug(session: AsyncSession, slug: str) -> Organization | None:
    return (await session.execute(select(Organization).where(Organization.slug == slug))).scalar_one_or_none()


async def create_org(session: AsyncSession, *, slug: str, name: str, is_test: bool) -> Organization:
    if await get_org_by_slug(session, slug) is not None:
        raise ConflictError(f"Organization slug '{slug}' is already taken.")
    org = Organization(slug=slug, name=name, is_test=is_test)
    session.add(org)
    await session.flush()
    return org


async def update_org(session: AsyncSession, *, org_id: UUID, name: str) -> Organization:
    org = await get_org_by_id(session, org_id)
    org.name = name
    await session.flush()
    return org


async def delete_org(session: AsyncSession, *, org_id: UUID) -> None:
    result = await session.execute(delete(Organization).where(Organization.id == org_id))
    if result.rowcount == 0:
        raise NotFoundError(f"Organization {org_id} not found.")


# --- users ---------------------------------------------------------------


async def get_user_by_id(session: AsyncSession, user_id: UUID) -> User:
    user = await session.get(User, user_id)
    if user is None:
        raise NotFoundError(f"User {user_id} not found.")
    return user


async def get_user_by_email(session: AsyncSession, email: str) -> User | None:
    return (
        await session.execute(select(User).where(User.email == email))
    ).scalar_one_or_none()


async def create_user(
    session: AsyncSession,
    *,
    email: str,
    display_name: str,
    password_hash: str | None,
) -> User:
    if await get_user_by_email(session, email) is not None:
        raise ConflictError(f"User with email '{email}' already exists.")
    user = User(email=email, display_name=display_name, password_hash=password_hash)
    session.add(user)
    await session.flush()
    return user


# --- memberships ---------------------------------------------------------


async def list_memberships_for_user(session: AsyncSession, user_id: UUID) -> list[Membership]:
    result = await session.execute(select(Membership).where(Membership.user_id == user_id))
    return list(result.scalars().all())


async def list_memberships_for_org(session: AsyncSession, org_id: UUID) -> list[Membership]:
    result = await session.execute(select(Membership).where(Membership.org_id == org_id))
    return list(result.scalars().all())


async def get_membership(
    session: AsyncSession, *, org_id: UUID, user_id: UUID
) -> Membership | None:
    return (
        await session.execute(
            select(Membership).where(
                and_(Membership.org_id == org_id, Membership.user_id == user_id)
            )
        )
    ).scalar_one_or_none()


async def create_membership(
    session: AsyncSession,
    *,
    org_id: UUID,
    user_id: UUID,
    role: str,
    invited_by: UUID | None = None,
) -> Membership:
    if await get_membership(session, org_id=org_id, user_id=user_id) is not None:
        raise ConflictError("User is already a member of this organization.")
    m = Membership(org_id=org_id, user_id=user_id, role=role, invited_by=invited_by)
    session.add(m)
    await session.flush()
    return m


async def update_membership_role(
    session: AsyncSession, *, membership_id: UUID, role: str
) -> Membership:
    membership = await session.get(Membership, membership_id)
    if membership is None:
        raise NotFoundError(f"Membership {membership_id} not found.")
    membership.role = role
    await session.flush()
    return membership


async def delete_membership(session: AsyncSession, *, membership_id: UUID) -> None:
    result = await session.execute(delete(Membership).where(Membership.id == membership_id))
    if result.rowcount == 0:
        raise NotFoundError(f"Membership {membership_id} not found.")


# --- API keys ------------------------------------------------------------


async def create_api_key(
    session: AsyncSession,
    *,
    org_id: UUID,
    created_by: UUID,
    name: str,
    prefix: str,
    hash_: str,
    scopes: list[str],
) -> ApiKey:
    key = ApiKey(
        org_id=org_id,
        created_by=created_by,
        name=name,
        prefix=prefix,
        hash=hash_,
        scopes=scopes,
    )
    session.add(key)
    await session.flush()
    return key


async def list_api_keys(session: AsyncSession, *, org_id: UUID) -> list[ApiKey]:
    result = await session.execute(
        select(ApiKey).where(ApiKey.org_id == org_id).order_by(ApiKey.created_at.desc())
    )
    return list(result.scalars().all())


async def get_api_key_by_prefix(session: AsyncSession, prefix: str) -> ApiKey | None:
    return (
        await session.execute(select(ApiKey).where(ApiKey.prefix == prefix))
    ).scalar_one_or_none()


async def revoke_api_key(session: AsyncSession, *, key_id: UUID) -> ApiKey:
    key = await session.get(ApiKey, key_id)
    if key is None:
        raise NotFoundError(f"API key {key_id} not found.")
    if key.status == "revoked":
        raise ConflictError("API key is already revoked.")
    key.status = "revoked"
    key.revoked_at = datetime.now(UTC)
    await session.flush()
    return key


async def touch_api_key(session: AsyncSession, *, key_id: UUID) -> None:
    await session.execute(
        update(ApiKey).where(ApiKey.id == key_id).values(last_used_at=datetime.now(UTC))
    )


# --- refresh tokens ------------------------------------------------------


async def store_refresh_token(
    session: AsyncSession,
    *,
    org_id: UUID,
    user_id: UUID,
    token_hash: str,
    expires_at: datetime,
    user_agent: str | None,
    ip: str | None,
) -> RefreshToken:
    rt = RefreshToken(
        org_id=org_id,
        user_id=user_id,
        token_hash=token_hash,
        expires_at=expires_at,
        user_agent=user_agent,
        ip=ip,
    )
    session.add(rt)
    await session.flush()
    return rt


async def get_active_refresh_token(
    session: AsyncSession, *, token_hash: str
) -> RefreshToken | None:
    return (
        await session.execute(
            select(RefreshToken).where(
                and_(
                    RefreshToken.token_hash == token_hash,
                    RefreshToken.revoked_at.is_(None),
                    RefreshToken.expires_at > datetime.now(UTC),
                )
            )
        )
    ).scalar_one_or_none()


async def revoke_refresh_token(
    session: AsyncSession, *, token_id: UUID, rotated_to: UUID | None = None
) -> None:
    try:
        rt = (
            await session.execute(select(RefreshToken).where(RefreshToken.id == token_id))
        ).scalar_one()
    except NoResultFound as exc:
        raise NotFoundError("Refresh token not found.") from exc
    rt.revoked_at = datetime.now(UTC)
    rt.rotated_to = rotated_to
    await session.flush()
