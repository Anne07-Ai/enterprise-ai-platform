"""Unit tests for the ProblemDetails envelope shape."""

from __future__ import annotations

import pytest

from app.core.errors import (
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    NotFoundError,
    PROBLEM_BASE,
    ProblemDetails,
    RateLimitedError,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("exc_class", "expected_status"),
    [
        (NotFoundError, 404),
        (ConflictError, 409),
        (AuthenticationError, 401),
        (AuthorizationError, 403),
        (RateLimitedError, 429),
    ],
)
def test_status_codes(exc_class: type, expected_status: int) -> None:
    err = exc_class("oops")
    assert err.status_code == expected_status


@pytest.mark.unit
def test_problem_details_serializes() -> None:
    pd = ProblemDetails(
        type=f"{PROBLEM_BASE}/conflict",
        title="X",
        status=409,
        detail="dup",
        instance="/v1/orgs",
    )
    body = pd.model_dump(exclude_none=True)
    assert body["type"].endswith("/conflict")
    assert body["status"] == 409
    assert "tenant_id" not in body  # exclude_none
