"""Structured logging via structlog.

Every log record is enriched with:
  * timestamp (ISO 8601, UTC)
  * level
  * logger / event
  * trace_id, span_id (auto-injected when an OTel span is active)
  * request_id, org_id, user_id (auto-injected from contextvars)

We avoid the stdlib ``logging`` config dance and route everything through
``structlog`` — the third-party libraries that use stdlib logging still flow
through here because we install a stdlib formatter that defers to structlog's
processor chain.
"""

from __future__ import annotations

import logging
import sys
from contextvars import ContextVar
from typing import Any

import structlog
from opentelemetry import trace as otel_trace
from structlog.types import EventDict, Processor

from app.core.config import Settings

# --- contextvars ---------------------------------------------------------
# Middleware writes to these, processors read from them on every log call.

_request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)
_org_id_var: ContextVar[str | None] = ContextVar("org_id", default=None)
_user_id_var: ContextVar[str | None] = ContextVar("user_id", default=None)


def bind_request_context(
    *,
    request_id: str | None = None,
    org_id: str | None = None,
    user_id: str | None = None,
) -> None:
    """Bind request-scoped values for structured logging."""
    if request_id is not None:
        _request_id_var.set(request_id)
    if org_id is not None:
        _org_id_var.set(org_id)
    if user_id is not None:
        _user_id_var.set(user_id)


def clear_request_context() -> None:
    _request_id_var.set(None)
    _org_id_var.set(None)
    _user_id_var.set(None)


# --- processors ----------------------------------------------------------


def _add_request_context(_logger: Any, _name: str, event_dict: EventDict) -> EventDict:
    if (rid := _request_id_var.get()) is not None:
        event_dict.setdefault("request_id", rid)
    if (oid := _org_id_var.get()) is not None:
        event_dict.setdefault("org_id", oid)
    if (uid := _user_id_var.get()) is not None:
        event_dict.setdefault("user_id", uid)
    return event_dict


def _add_trace_context(_logger: Any, _name: str, event_dict: EventDict) -> EventDict:
    span = otel_trace.get_current_span()
    ctx = span.get_span_context()
    if ctx and ctx.is_valid:
        event_dict.setdefault("trace_id", format(ctx.trace_id, "032x"))
        event_dict.setdefault("span_id", format(ctx.span_id, "016x"))
    return event_dict


# --- setup ---------------------------------------------------------------


def setup_logging(settings: Settings) -> None:
    """Configure structlog and route stdlib logging through it."""
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)

    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        _add_request_context,
        _add_trace_context,
        timestamper,
    ]

    if settings.log_json:
        renderer: Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping()[settings.log_level.upper()]
        ),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Route stdlib logging (used by FastAPI/uvicorn/sqlalchemy) through structlog
    # so the JSON format is uniform regardless of where the log line originated.
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=shared_processors,
            processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, renderer],
        )
    )
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(settings.log_level.upper())

    # Quiet libraries that are too chatty at INFO without losing important context.
    for noisy in ("uvicorn.access", "kafka.conn", "aiokafka.conn", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)  # type: ignore[no-any-return]
