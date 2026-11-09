"""Liveness and readiness probes.

* /healthz — process is up. No external IO. Cheap. Used by k8s liveness.
* /readyz  — exercises Postgres, Redis, and the Kafka producer. Used by readiness.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from app import __version__
from app.core.logging import get_logger
from app.infra import db, kafka
from app.infra import redis as redis_infra

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

router = APIRouter(tags=["system"])
logger = get_logger(__name__)
_PROBE_TIMEOUT_SECONDS = 2.0


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
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {"description": "One or more dependencies unhealthy"}
    },
)
async def readyz() -> ReadyOut:
    async def _check(name: str, ping: Callable[[], Awaitable[None]]) -> tuple[str, str]:
        try:
            await asyncio.wait_for(ping(), timeout=_PROBE_TIMEOUT_SECONDS)
            return name, "ok"
        except Exception as exc:
            reason = "timeout" if isinstance(exc, TimeoutError) else "unavailable"
            logger.warning("readiness.check.failed", dependency=name, reason=reason)
            return name, "fail"

    results = await asyncio.gather(
        _check("postgres", db.ping),
        _check("redis", redis_infra.ping),
        _check("kafka", kafka.ping),
    )
    failed = [name for name, result in results if result == "fail"]
    if failed:
        # The problem-details handler only preserves string details. Use the
        # fixed dependency names and never include upstream exception text.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Unavailable dependencies: {', '.join(failed)}.",
        )
    return ReadyOut(status="ok", checks=dict(results))
