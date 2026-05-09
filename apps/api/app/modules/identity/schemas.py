"""Pydantic IO schemas for the identity module."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field, StringConstraints

from app.modules.identity.rbac import OrgRole

Slug = Annotated[
    str,
    StringConstraints(
        min_length=2,
        max_length=64,
        pattern=r"^[a-z][a-z0-9-]{1,63}$",
    ),
]


# --- shared --------------------------------------------------------------


class _ORM(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# --- organizations -------------------------------------------------------


class OrganizationCreate(BaseModel):
    slug: Slug
    name: str = Field(min_length=1, max_length=255)
    is_test: bool = False


class OrganizationUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)


class OrganizationOut(_ORM):
    id: UUID
    slug: str
    name: str
    is_test: bool
    created_at: datetime
    updated_at: datetime


# --- users / me ----------------------------------------------------------


class UserOut(_ORM):
    id: UUID
    email: EmailStr
    display_name: str
    is_active: bool
    created_at: datetime


class MembershipOut(_ORM):
    id: UUID
    org_id: UUID
    user_id: UUID
    role: OrgRole
    created_at: datetime


class MeOut(BaseModel):
    user: UserOut
    current_org: OrganizationOut
    role: OrgRole
    memberships: list[MembershipOut]


# --- auth ----------------------------------------------------------------


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=256)
    org_slug: str | None = Field(
        default=None,
        description="Required when the user has memberships in more than one org.",
    )


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "Bearer"
    expires_in: int


class RefreshRequest(BaseModel):
    refresh_token: str


# --- memberships ---------------------------------------------------------


class InviteRequest(BaseModel):
    email: EmailStr
    role: OrgRole = OrgRole.MEMBER
    display_name: str | None = Field(default=None, max_length=255)


class ChangeRoleRequest(BaseModel):
    role: OrgRole


# --- API keys ------------------------------------------------------------


class ApiKeyCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    scopes: list[str] = Field(default_factory=list)


class ApiKeyOut(_ORM):
    id: UUID
    name: str
    prefix: str
    scopes: list[str]
    status: str
    last_used_at: datetime | None = None
    revoked_at: datetime | None = None
    created_at: datetime


class ApiKeyCreateOut(ApiKeyOut):
    plaintext: str = Field(
        description="Shown ONCE on creation. Store it securely; you cannot retrieve it again.",
    )
