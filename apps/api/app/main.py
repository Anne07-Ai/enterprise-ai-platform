"""FastAPI application factory.

Lifespan handles:
  * Logging + tracing setup (idempotent).
  * Engine / Redis / Kafka producer initialization.
  * Outbox publisher background task.
  * Graceful shutdown of all of the above.

Middleware order (outermost → innermost):
  1. RequestIdMiddleware       — assigns X-Request-Id, binds log context
  2. CORSMiddleware            — handle preflight before any auth
  3. AuthMiddleware            — resolves the principal
  4. TenantMiddleware          — guards tenant boundary (Phase 3+)
  5. RateLimitMiddleware       — per-org/per-IP token bucket
  6. IdempotencyMiddleware     — Idempotency-Key handling
  7. AuditMiddleware           — emits http.request audit event

ASGI applies middleware in reverse-add order, so we add them bottom-up.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app import __version__
from app.api.v1.router import api_router
from app.core.config import get_settings
from app.core.errors import install_exception_handlers
from app.core.logging import get_logger, setup_logging
from app.core.tracing import instrument_fastapi, setup_tracing
from app.infra import kafka, outbox, redis as redis_infra
from app.infra.db import dispose_engine, get_engine
from app.middleware.audit import AuditMiddleware
from app.middleware.auth import AuthMiddleware
from app.middleware.idempotency import IdempotencyMiddleware
from app.middleware.rate_limit import RateLimitMiddleware
from app.middleware.request_id import RequestIdMiddleware
from app.middleware.tenant import TenantMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    setup_logging(settings)
    setup_tracing(settings)
    logger = get_logger("app.main")

    # Eagerly initialize backends so startup-time misconfiguration is loud.
    get_engine()
    redis_infra.get_redis()
    await kafka.get_producer()
    await outbox.start_publisher()
    logger.info("api.startup.complete", version=__version__, environment=settings.environment.value)

    try:
        yield
    finally:
        logger.info("api.shutdown.begin")
        await outbox.stop_publisher()
        await kafka.dispose_producer()
        await redis_infra.dispose_redis()
        await dispose_engine()
        logger.info("api.shutdown.complete")


def create_app() -> FastAPI:
    settings = get_settings()
    setup_logging(settings)

    app = FastAPI(
        title="Enterprise AI Workflow Platform — API",
        version=__version__,
        description=(
            "Multi-tenant AI workflow platform. See ARCHITECTURE.md for context "
            "and ADRs 0001–0005 for load-bearing decisions."
        ),
        openapi_url="/openapi.json",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # Order matters — these are added in the reverse of dispatch order.
    app.add_middleware(AuditMiddleware)
    app.add_middleware(IdempotencyMiddleware)
    app.add_middleware(RateLimitMiddleware)
    app.add_middleware(TenantMiddleware)
    app.add_middleware(AuthMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Request-Id", "Idempotent-Replay"],
    )
    app.add_middleware(RequestIdMiddleware)

    install_exception_handlers(app)
    app.include_router(api_router)

    # FastAPI auto-instrumentation only after middleware/routers are wired.
    instrument_fastapi(app)

    # Customize OpenAPI security schemes so the docs page advertises Bearer + ApiKey.
    _patch_openapi(app)
    return app


def _patch_openapi(app: FastAPI) -> None:
    from fastapi.openapi.utils import get_openapi

    def _custom() -> dict[str, object]:
        if app.openapi_schema:
            return app.openapi_schema
        schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
        )
        schema["openapi"] = "3.1.0"
        schema.setdefault("components", {})["securitySchemes"] = {
            "BearerJWT": {
                "type": "http",
                "scheme": "bearer",
                "bearerFormat": "JWT",
                "description": "Issued by /v1/auth/login as RS256-signed JWT.",
            },
            "ApiKey": {
                "type": "apiKey",
                "in": "header",
                "name": "X-Api-Key",
                "description": "Org-scoped API key, prefix eaip_live_/eaip_test_.",
            },
        }
        schema["security"] = [{"BearerJWT": []}, {"ApiKey": []}]
        app.openapi_schema = schema
        return schema

    app.openapi = _custom  # type: ignore[method-assign]


app = create_app()
