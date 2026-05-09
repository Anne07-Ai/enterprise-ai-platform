"""Composes the v1 router from each module's surface."""

from fastapi import APIRouter

from app.api.v1.healthz import router as healthz_router
from app.modules.identity.api import (
    api_keys_router,
    auth_router,
    me_router,
    orgs_router,
)

api_router = APIRouter()
api_router.include_router(healthz_router)
api_router.include_router(auth_router)
api_router.include_router(me_router)
api_router.include_router(orgs_router)
api_router.include_router(api_keys_router)
