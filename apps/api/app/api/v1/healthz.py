"""Liveness and readiness probes.

* /healthz — process is up. No external IO. Cheap. Used by k8s liveness.
* /readyz  — exercises Postgres, Redis, and the Kafka producer. Used by readiness.
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, status
from pydantic import BaseModel

from app import __version__
from app.infra import db, kafka, redis as redis_infra

router = APIRouter(tags=["system"])


class HealthOut(BaseModel):
    status: str
    version: str


class ReadyOut(BaseModel):
    status: str
    checks: dict[str, str]


@router.get("/healthz", response_model=HealthOut, summary="Liveness")
async def healthz() -> HealthOut:
    return HealthOut(status="ok", version=__version__)


@router.get(
    "/readyz",
    response_model=ReadyOut,
    summary="Readiness — exercises Postgres, Redis, and Kafka",
    responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"description": "One or more dependencies unhealthy"}},
)
async def readyz() -> ReadyOut:
    async def _check(name: str, coro: object) -> tuple[str, str]:
        try:
            await asyncio.wait_for(coro, timeout=2.0)  # type: ignore[arg-type]
            return name, "ok"
        except Exception as exc:
            return name, f"fail: {type(exc).__name__}"

    results = await asyncio.gather(
        _check("postgres", db.ping()),
        _check("redis", redis_infra.ping()),
        _check("kafka", kafka.ping()),
    )
    checks = dict(results)
    overall = "ok" if all(v == "ok" for v in checks.values()) else "degraded"
    if overall != "ok":
        # Surface the failure as 503 so orchestrators stop sending traffic.
        from fastapi import HTTPException

        raise HTTPException(status_code=503, detail=checks)
    return ReadyOut(status=overall, checks=checks)
