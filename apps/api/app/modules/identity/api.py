"""HTTP surface of the identity module."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Header, Request, status

from app.core.deps import CurrentPrincipalDep, DBSession, UnscopedDBSession
from app.core.errors import AuthenticationError, AuthorizationError
from app.modules.identity import service
from app.modules.identity.rbac import OrgRole, require_permission
from app.modules.identity.schemas import (
    ApiKeyCreate,
    ApiKeyCreateOut,
    ApiKeyOut,
    ChangeRoleRequest,
    InviteRequest,
    LoginRequest,
    MeOut,
    MembershipOut,
    OrganizationCreate,
    OrganizationOut,
    OrganizationUpdate,
    RefreshRequest,
    TokenPair,
)

# --- routers --------------------------------------------------------------
# Three sub-routers so OpenAPI tags group cleanly.

auth_router = APIRouter(prefix="/v1/auth", tags=["auth"])
me_router = APIRouter(prefix="/v1", tags=["users"])
orgs_router = APIRouter(prefix="/v1/orgs", tags=["orgs"])
api_keys_router = APIRouter(prefix="/v1/api-keys", tags=["api-keys"])


# --- /v1/auth -------------------------------------------------------------


@auth_router.post(
    "/login",
    response_model=TokenPair,
    summary="Exchange email + password for a token pair",
    responses={401: {"description": "Invalid credentials"}, 409: {"description": "Multiple orgs — pass org_slug"}},
)
async def login(payload: LoginRequest, request: Request, db: UnscopedDBSession) -> TokenPair:
    user_agent = request.headers.get("user-agent")
    ip = request.client.host if request.client else None
    _, _, access, refresh, ttl = await service.authenticate(
        db,
        email=payload.email,
        password=payload.password,
        org_slug=payload.org_slug,
        user_agent=user_agent,
        ip=ip,
    )
    return TokenPair(access_token=access, refresh_token=refresh, expires_in=ttl)


@auth_router.post(
    "/refresh",
    response_model=TokenPair,
    summary="Rotate the refresh token, issue a new access token",
    responses={401: {"description": "Invalid or expired refresh token"}},
)
async def refresh(payload: RefreshRequest, request: Request, db: UnscopedDBSession) -> TokenPair:
    user_agent = request.headers.get("user-agent")
    ip = request.client.host if request.client else None
    access, new_refresh, ttl = await service.refresh_tokens(
        db,
        refresh_token=payload.refresh_token,
        user_agent=user_agent,
        ip=ip,
    )
    return TokenPair(access_token=access, refresh_token=new_refresh, expires_in=ttl)


@auth_router.post(
    "/logout",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Revoke the supplied refresh token",
)
async def logout(
    payload: RefreshRequest,
    db: DBSession,
    principal: CurrentPrincipalDep,
) -> None:
    if principal.user_id is None:
        raise AuthenticationError("Logout requires a user-bound principal.")
    await service.logout(
        db,
        refresh_token=payload.refresh_token,
        actor_user_id=principal.user_id,
        org_id=principal.org_id,
    )


# --- /v1/me ---------------------------------------------------------------


@me_router.get("/me", response_model=MeOut, summary="The authenticated user and their memberships")
async def me(principal: CurrentPrincipalDep, db: DBSession) -> MeOut:
    if principal.user_id is None:
        raise AuthenticationError("API keys cannot resolve /me; use /v1/orgs/{id} instead.")
    user = await service.user_for_principal(db, user_id=principal.user_id)
    org = await service.get_org(db, org_id=principal.org_id)
    memberships = await service.memberships_for_user(db, user_id=principal.user_id)
    return MeOut(
        user=user,
        current_org=org,
        role=OrgRole(principal.role),
        memberships=[MembershipOut.model_validate(m) for m in memberships],
    )


# --- /v1/orgs -------------------------------------------------------------


@orgs_router.post(
    "",
    response_model=OrganizationOut,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new organization. Caller becomes its owner.",
)
async def create_org(
    payload: OrganizationCreate, db: DBSession, principal: CurrentPrincipalDep
) -> OrganizationOut:
    if principal.user_id is None:
        raise AuthorizationError("API keys cannot create organizations.")
    org = await service.create_org(
        db,
        slug=payload.slug,
        name=payload.name,
        creator_user_id=principal.user_id,
        is_test=payload.is_test,
    )
    return OrganizationOut.model_validate(org)


@orgs_router.get(
    "/{org_id}",
    response_model=OrganizationOut,
    dependencies=[require_permission("org:read")],
)
async def get_org(org_id: UUID, db: DBSession, principal: CurrentPrincipalDep) -> OrganizationOut:
    if org_id != principal.org_id:
        raise AuthorizationError("Cannot read organizations outside the active context.")
    return OrganizationOut.model_validate(await service.get_org(db, org_id=org_id))


@orgs_router.patch(
    "/{org_id}",
    response_model=OrganizationOut,
    dependencies=[require_permission("org:update")],
)
async def update_org(
    org_id: UUID,
    payload: OrganizationUpdate,
    db: DBSession,
    principal: CurrentPrincipalDep,
) -> OrganizationOut:
    if org_id != principal.org_id or principal.user_id is None:
        raise AuthorizationError("Cannot update organizations outside the active context.")
    org = await service.update_org(
        db, org_id=org_id, name=payload.name or "", actor_user_id=principal.user_id
    )
    return OrganizationOut.model_validate(org)


@orgs_router.delete(
    "/{org_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[require_permission("org:delete")],
)
async def delete_org(
    org_id: UUID, db: DBSession, principal: CurrentPrincipalDep
) -> None:
    if org_id != principal.org_id or principal.user_id is None:
        raise AuthorizationError("Cannot delete organizations outside the active context.")
    await service.delete_org(db, org_id=org_id, actor_user_id=principal.user_id)


@orgs_router.get(
    "/{org_id}/memberships",
    response_model=list[MembershipOut],
    dependencies=[require_permission("members:read")],
)
async def list_memberships(
    org_id: UUID, db: DBSession, principal: CurrentPrincipalDep
) -> list[MembershipOut]:
    if org_id != principal.org_id:
        raise AuthorizationError("Cannot list memberships outside the active context.")
    return [MembershipOut.model_validate(m) for m in await service.list_memberships(db, org_id=org_id)]


@orgs_router.post(
    "/{org_id}/memberships",
    response_model=MembershipOut,
    status_code=status.HTTP_201_CREATED,
    dependencies=[require_permission("members:invite")],
)
async def invite_member(
    org_id: UUID,
    payload: InviteRequest,
    db: DBSession,
    principal: CurrentPrincipalDep,
) -> MembershipOut:
    if org_id != principal.org_id or principal.user_id is None:
        raise AuthorizationError("Cannot invite members outside the active context.")
    m = await service.invite_member(
        db,
        org_id=org_id,
        inviter_user_id=principal.user_id,
        email=payload.email,
        role=payload.role.value,
        display_name=payload.display_name,
    )
    return MembershipOut.model_validate(m)


@orgs_router.patch(
    "/{org_id}/memberships/{membership_id}",
    response_model=MembershipOut,
    dependencies=[require_permission("members:change_role")],
)
async def change_role(
    org_id: UUID,
    membership_id: UUID,
    payload: ChangeRoleRequest,
    db: DBSession,
    principal: CurrentPrincipalDep,
) -> MembershipOut:
    if org_id != principal.org_id or principal.user_id is None:
        raise AuthorizationError("Cannot change roles outside the active context.")
    m = await service.change_member_role(
        db,
        membership_id=membership_id,
        new_role=payload.role.value,
        actor_user_id=principal.user_id,
        actor_role=principal.role,
    )
    return MembershipOut.model_validate(m)


@orgs_router.delete(
    "/{org_id}/memberships/{membership_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[require_permission("members:remove")],
)
async def remove_member(
    org_id: UUID,
    membership_id: UUID,
    db: DBSession,
    principal: CurrentPrincipalDep,
) -> None:
    if org_id != principal.org_id or principal.user_id is None:
        raise AuthorizationError("Cannot remove members outside the active context.")
    await service.remove_member(
        db, membership_id=membership_id, actor_user_id=principal.user_id
    )


# --- /v1/api-keys ---------------------------------------------------------


@api_keys_router.post(
    "",
    response_model=ApiKeyCreateOut,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new API key. The plaintext key is shown ONCE.",
    dependencies=[require_permission("api_keys:create")],
)
async def create_api_key(
    payload: ApiKeyCreate,
    db: DBSession,
    principal: CurrentPrincipalDep,
    x_test_key: str | None = Header(default=None, alias="X-Test-Key"),
) -> ApiKeyCreateOut:
    if principal.user_id is None:
        raise AuthorizationError("API keys cannot create more API keys.")
    org = await service.get_org(db, org_id=principal.org_id)
    key, plaintext = await service.create_api_key_for_org(
        db,
        org_id=principal.org_id,
        creator_user_id=principal.user_id,
        is_test=org.is_test or x_test_key == "1",
        name=payload.name,
        scopes=payload.scopes,
    )
    base = ApiKeyOut.model_validate(key).model_dump()
    base["plaintext"] = plaintext
    return ApiKeyCreateOut.model_validate(base)


@api_keys_router.get(
    "",
    response_model=list[ApiKeyOut],
    dependencies=[require_permission("api_keys:read")],
)
async def list_api_keys(db: DBSession, principal: CurrentPrincipalDep) -> list[ApiKeyOut]:
    return [ApiKeyOut.model_validate(k) for k in await service.list_api_keys(db, org_id=principal.org_id)]


@api_keys_router.delete(
    "/{key_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[require_permission("api_keys:revoke")],
)
async def revoke_api_key(
    key_id: UUID, db: DBSession, principal: CurrentPrincipalDep
) -> None:
    if principal.user_id is None:
        raise AuthorizationError("API keys cannot revoke other API keys.")
    await service.revoke_api_key(
        db, org_id=principal.org_id, key_id=key_id, actor_user_id=principal.user_id
    )
