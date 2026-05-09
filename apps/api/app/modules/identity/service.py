"""Identity module service layer.

Service functions are the public surface of this module. Cross-module callers
import this file; they MUST NOT touch ``repository`` or ``models`` directly.

Every function takes an explicit ``AsyncSession`` so transactions stay under
the caller's control. The session arrives with ``app.current_org`` already
set by the request middleware (RLS), or unset for unauthenticated paths
like login.
"""

from __future__ import annotations

import secrets
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import (
    AuthenticationError,
    AuthorizationError,
    ConflictError,
)
from app.core.tracing import traced
from app.modules.identity import events, repository as repo
from app.modules.identity.models import (
    ApiKey,
    Membership,
    Organization,
    RefreshToken,
    User,
)
from app.modules.identity.rbac import OrgRole, role_at_least
from app.modules.identity.security import (
    generate_api_key,
    hash_opaque_token,
    hash_password,
    mint_access_token,
    mint_refresh_token,
    verify_password,
)


# --- orgs ----------------------------------------------------------------


@traced("identity.create_org")
async def create_org(
    session: AsyncSession,
    *,
    slug: str,
    name: str,
    creator_user_id: UUID,
    is_test: bool = False,
) -> Organization:
    org = await repo.create_org(session, slug=slug, name=name, is_test=is_test)
    await repo.create_membership(
        session, org_id=org.id, user_id=creator_user_id, role=OrgRole.OWNER.value
    )
    await events.emit_audit(
        session,
        org_id=org.id,
        actor_user_id=creator_user_id,
        actor_kind="user",
        action="org.created",
        target_type="org",
        target_id=str(org.id),
        attributes={"slug": slug, "name": name},
    )
    return org


@traced("identity.get_org")
async def get_org(session: AsyncSession, *, org_id: UUID) -> Organization:
    return await repo.get_org_by_id(session, org_id)


@traced("identity.update_org")
async def update_org(
    session: AsyncSession, *, org_id: UUID, name: str, actor_user_id: UUID
) -> Organization:
    org = await repo.update_org(session, org_id=org_id, name=name)
    await events.emit_audit(
        session,
        org_id=org.id,
        actor_user_id=actor_user_id,
        actor_kind="user",
        action="org.updated",
        target_type="org",
        target_id=str(org.id),
        attributes={"name": name},
    )
    return org


@traced("identity.delete_org")
async def delete_org(session: AsyncSession, *, org_id: UUID, actor_user_id: UUID) -> None:
    await repo.delete_org(session, org_id=org_id)
    await events.emit_audit(
        session,
        org_id=org_id,
        actor_user_id=actor_user_id,
        actor_kind="user",
        action="org.deleted",
        target_type="org",
        target_id=str(org_id),
    )


# --- memberships ---------------------------------------------------------


@traced("identity.invite_member")
async def invite_member(
    session: AsyncSession,
    *,
    org_id: UUID,
    inviter_user_id: UUID,
    email: str,
    role: str,
    display_name: str | None,
) -> Membership:
    user = await repo.get_user_by_email(session, email)
    if user is None:
        # Inviting a brand-new user — they pick a password on first login via
        # an out-of-band invitation flow (Phase 4). For Phase 2 we create the
        # user with no password set; SSO would also land here.
        user = await repo.create_user(
            session,
            email=email,
            display_name=display_name or email.split("@")[0],
            password_hash=None,
        )
    membership = await repo.create_membership(
        session, org_id=org_id, user_id=user.id, role=role, invited_by=inviter_user_id
    )
    await events.emit_audit(
        session,
        org_id=org_id,
        actor_user_id=inviter_user_id,
        actor_kind="user",
        action="member.invited",
        target_type="user",
        target_id=str(user.id),
        attributes={"email": email, "role": role},
    )
    return membership


@traced("identity.change_member_role")
async def change_member_role(
    session: AsyncSession,
    *,
    membership_id: UUID,
    new_role: str,
    actor_user_id: UUID,
    actor_role: str,
) -> Membership:
    if not role_at_least(actor_role, OrgRole.ADMIN.value):
        raise AuthorizationError("Only owners or admins can change member roles.")
    membership = await repo.update_membership_role(
        session, membership_id=membership_id, role=new_role
    )
    await events.emit_audit(
        session,
        org_id=membership.org_id,
        actor_user_id=actor_user_id,
        actor_kind="user",
        action="member.role_changed",
        target_type="membership",
        target_id=str(membership.id),
        attributes={"role": new_role},
    )
    return membership


@traced("identity.remove_member")
async def remove_member(
    session: AsyncSession, *, membership_id: UUID, actor_user_id: UUID
) -> None:
    membership = await session.get(Membership, membership_id)
    if membership is None:
        from app.core.errors import NotFoundError

        raise NotFoundError(f"Membership {membership_id} not found.")
    org_id = membership.org_id
    await repo.delete_membership(session, membership_id=membership_id)
    await events.emit_audit(
        session,
        org_id=org_id,
        actor_user_id=actor_user_id,
        actor_kind="user",
        action="member.removed",
        target_type="membership",
        target_id=str(membership_id),
    )


@traced("identity.list_memberships")
async def list_memberships(session: AsyncSession, *, org_id: UUID) -> list[Membership]:
    return await repo.list_memberships_for_org(session, org_id=org_id)


@traced("identity.memberships_for_user")
async def memberships_for_user(session: AsyncSession, *, user_id: UUID) -> list[Membership]:
    return await repo.list_memberships_for_user(session, user_id)


# --- auth ----------------------------------------------------------------


@traced("identity.authenticate")
async def authenticate(
    session: AsyncSession,
    *,
    email: str,
    password: str,
    org_slug: str | None,
    user_agent: str | None = None,
    ip: str | None = None,
) -> tuple[User, Organization, str, str, int]:
    """Verify credentials and mint a token pair.

    Returns (user, active_org, access_token, refresh_token, access_ttl_seconds).
    """
    from app.core.config import get_settings

    user = await repo.get_user_by_email(session, email)
    # Always run a verify against a fixed dummy hash on missing-user paths so
    # the response time doesn't reveal account existence (timing-channel mitigation).
    dummy = "$argon2id$v=19$m=65536,t=3,p=2$ZHVtbXkAAAAAAAAA$" + ("0" * 32)
    if user is None or user.password_hash is None:
        verify_password(dummy, password)
        raise AuthenticationError("Invalid credentials.")
    if not verify_password(user.password_hash, password):
        raise AuthenticationError("Invalid credentials.")
    if not user.is_active:
        raise AuthenticationError("Account disabled.")

    memberships = await repo.list_memberships_for_user(session, user.id)
    if not memberships:
        raise AuthorizationError("This user has no organization memberships.")

    if org_slug:
        # Filter to the requested org slug.
        active = None
        for m in memberships:
            org = await repo.get_org_by_id(session, m.org_id)
            if org.slug == org_slug:
                active = (m, org)
                break
        if active is None:
            raise AuthorizationError("User is not a member of the requested organization.")
        membership, org = active
    elif len(memberships) == 1:
        membership = memberships[0]
        org = await repo.get_org_by_id(session, membership.org_id)
    else:
        raise ConflictError(
            "User belongs to multiple organizations. Pass `org_slug` to disambiguate."
        )

    access_token, _, exp = mint_access_token(
        user_id=user.id, org_id=org.id, role=membership.role
    )
    refresh_token, refresh_hash, refresh_exp = mint_refresh_token(
        user_id=user.id, org_id=org.id
    )
    await repo.store_refresh_token(
        session,
        org_id=org.id,
        user_id=user.id,
        token_hash=refresh_hash,
        expires_at=refresh_exp,
        user_agent=user_agent,
        ip=ip,
    )
    await events.emit_audit(
        session,
        org_id=org.id,
        actor_user_id=user.id,
        actor_kind="user",
        action="auth.login",
        attributes={"ip": ip, "user_agent": user_agent},
    )
    settings = get_settings()
    return user, org, access_token, refresh_token, settings.auth.access_token_ttl_seconds


@traced("identity.refresh_tokens")
async def refresh_tokens(
    session: AsyncSession,
    *,
    refresh_token: str,
    user_agent: str | None = None,
    ip: str | None = None,
) -> tuple[str, str, int]:
    """Rotate the refresh token, issue a new access token. Old refresh becomes invalid."""
    from app.core.config import get_settings

    rt = await repo.get_active_refresh_token(session, token_hash=hash_opaque_token(refresh_token))
    if rt is None:
        raise AuthenticationError("Invalid or expired refresh token.")

    membership = await repo.get_membership(session, org_id=rt.org_id, user_id=rt.user_id)
    if membership is None:
        raise AuthorizationError("Membership has been revoked.")

    new_access, _, _ = mint_access_token(
        user_id=rt.user_id, org_id=rt.org_id, role=membership.role
    )
    new_refresh, new_hash, new_exp = mint_refresh_token(user_id=rt.user_id, org_id=rt.org_id)
    new_record = await repo.store_refresh_token(
        session,
        org_id=rt.org_id,
        user_id=rt.user_id,
        token_hash=new_hash,
        expires_at=new_exp,
        user_agent=user_agent,
        ip=ip,
    )
    await repo.revoke_refresh_token(session, token_id=rt.id, rotated_to=new_record.id)
    await events.emit_audit(
        session,
        org_id=rt.org_id,
        actor_user_id=rt.user_id,
        actor_kind="user",
        action="auth.refresh_rotated",
    )
    settings = get_settings()
    return new_access, new_refresh, settings.auth.access_token_ttl_seconds


@traced("identity.logout")
async def logout(
    session: AsyncSession, *, refresh_token: str, actor_user_id: UUID, org_id: UUID
) -> None:
    rt = await repo.get_active_refresh_token(
        session, token_hash=hash_opaque_token(refresh_token)
    )
    if rt is None:
        # Idempotent — logout on an already-revoked token is fine.
        return
    await repo.revoke_refresh_token(session, token_id=rt.id)
    await events.emit_audit(
        session,
        org_id=org_id,
        actor_user_id=actor_user_id,
        actor_kind="user",
        action="auth.logout",
    )


# --- API keys ------------------------------------------------------------


@traced("identity.create_api_key")
async def create_api_key_for_org(
    session: AsyncSession,
    *,
    org_id: UUID,
    creator_user_id: UUID,
    is_test: bool,
    name: str,
    scopes: list[str],
) -> tuple[ApiKey, str]:
    """Returns (model, plaintext). Plaintext shown to caller exactly once."""
    plaintext, prefix, hash_ = generate_api_key(is_test=is_test)
    key = await repo.create_api_key(
        session,
        org_id=org_id,
        created_by=creator_user_id,
        name=name,
        prefix=prefix,
        hash_=hash_,
        scopes=scopes,
    )
    await events.emit_audit(
        session,
        org_id=org_id,
        actor_user_id=creator_user_id,
        actor_kind="user",
        action="api_key.created",
        target_type="api_key",
        target_id=str(key.id),
        attributes={"name": name, "scopes": scopes},
    )
    return key, plaintext


@traced("identity.list_api_keys")
async def list_api_keys(session: AsyncSession, *, org_id: UUID) -> list[ApiKey]:
    return await repo.list_api_keys(session, org_id=org_id)


@traced("identity.revoke_api_key")
async def revoke_api_key(
    session: AsyncSession, *, org_id: UUID, key_id: UUID, actor_user_id: UUID
) -> ApiKey:
    key = await repo.revoke_api_key(session, key_id=key_id)
    await events.emit_audit(
        session,
        org_id=org_id,
        actor_user_id=actor_user_id,
        actor_kind="user",
        action="api_key.revoked",
        target_type="api_key",
        target_id=str(key.id),
    )
    return key


@traced("identity.resolve_api_key")
async def resolve_api_key(
    session: AsyncSession, *, plaintext: str
) -> tuple[ApiKey, Organization]:
    """Look up an API key by its plaintext; raise on unknown or revoked keys.

    Used by the auth middleware. We extract the prefix portion (the visible
    prefix is `<label><body[:8]>`), look up the key by prefix to avoid
    scanning, then verify the full hash.
    """
    settings_label_lengths = (
        len("eaip_live_") + 8,
        len("eaip_test_") + 8,
    )
    prefix: str | None = None
    for n in settings_label_lengths:
        if len(plaintext) >= n:
            prefix = plaintext[:n]
            break
    if prefix is None:
        raise AuthenticationError("Malformed API key.")

    key = await repo.get_api_key_by_prefix(session, prefix)
    if key is None or key.status != "active":
        raise AuthenticationError("Invalid API key.")
    if not verify_password(key.hash, plaintext):
        raise AuthenticationError("Invalid API key.")

    org = await repo.get_org_by_id(session, key.org_id)
    await repo.touch_api_key(session, key_id=key.id)
    return key, org


# --- helpers used by deps ------------------------------------------------


@traced("identity.user_for_principal")
async def user_for_principal(session: AsyncSession, *, user_id: UUID) -> User:
    return await repo.get_user_by_id(session, user_id)


# Re-exported for cross-module imports.
__all__ = [
    "create_org",
    "get_org",
    "update_org",
    "delete_org",
    "invite_member",
    "change_member_role",
    "remove_member",
    "list_memberships",
    "memberships_for_user",
    "authenticate",
    "refresh_tokens",
    "logout",
    "create_api_key_for_org",
    "list_api_keys",
    "revoke_api_key",
    "resolve_api_key",
    "user_for_principal",
]
