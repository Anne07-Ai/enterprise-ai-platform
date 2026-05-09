"""OpenTelemetry setup and the @traced decorator for service-method spans."""

from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, TypeVar, cast

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

from app.core.config import Settings

P = ParamSpec("P")
R = TypeVar("R")


def setup_tracing(settings: Settings) -> None:
    """Configure the global tracer provider and auto-instrument libraries."""
    resource = Resource.create(
        {
            "service.name": settings.observability.service_name,
            "service.version": settings.observability.service_version,
            "deployment.environment": settings.environment.value,
        }
    )

    provider = TracerProvider(
        resource=resource,
        sampler=ParentBased(TraceIdRatioBased(settings.observability.sample_ratio)),
    )
    exporter = OTLPSpanExporter(
        endpoint=settings.observability.exporter_otlp_endpoint,
        insecure=settings.observability.exporter_otlp_insecure,
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    # Auto-instrumentation. FastAPI is instrumented after app construction
    # in main.py because it needs the app instance.
    HTTPXClientInstrumentor().instrument()
    RedisInstrumentor().instrument()
    AsyncPGInstrumentor().instrument()
    # SQLAlchemy is instrumented when the engine is created in app.infra.db.


def instrument_fastapi(app: Any) -> None:
    FastAPIInstrumentor.instrument_app(app, excluded_urls="healthz,readyz,metrics")


def instrument_sqlalchemy_engine(engine: Any) -> None:
    SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine, enable_commenter=True)


def get_tracer(name: str) -> trace.Tracer:
    return trace.get_tracer(name)


def traced(
    name: str | None = None,
    *,
    attributes: dict[str, str] | None = None,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Decorate an async service method to wrap it in an OTel span.

    Usage::

        @traced("identity.create_org")
        async def create_org(self, ...): ...

    The span is named with the supplied name (or ``module.qualname`` if omitted)
    and gets ``code.function`` / ``code.namespace`` attributes plus any user
    attributes from ``attributes``. Exceptions are recorded on the span.
    """

    def decorator(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        span_name = name or f"{fn.__module__}.{fn.__qualname__}"
        tracer = trace.get_tracer(fn.__module__)

        @functools.wraps(fn)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            with tracer.start_as_current_span(span_name) as span:
                span.set_attribute("code.function", fn.__qualname__)
                span.set_attribute("code.namespace", fn.__module__)
                if attributes:
                    for k, v in attributes.items():
                        span.set_attribute(k, v)
                try:
                    return await fn(*args, **kwargs)
                except Exception as exc:
                    span.record_exception(exc)
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(exc)))
                    raise

        return cast(Callable[P, Awaitable[R]], wrapper)

    return decorator
