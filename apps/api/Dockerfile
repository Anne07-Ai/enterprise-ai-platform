# syntax=docker/dockerfile:1.7
# ---------------------------------------------------------------------------
# Multi-stage build for the eaip-api service.
# Builder uses uv to produce a frozen .venv; final stage copies only the venv
# and the app source, runs as a non-root user, exposes 8000.
# ---------------------------------------------------------------------------

ARG PYTHON_VERSION=3.12-slim-bookworm

# ============================ builder ====================================
FROM python:${PYTHON_VERSION} AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv

# Build tools needed for any wheels that don't ship binaries (asyncpg sometimes does).
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install uv via the official installer for the builder stage only.
COPY --from=ghcr.io/astral-sh/uv:0.5.18 /uv /usr/local/bin/uv

WORKDIR /build

# Copy lock + manifest first so dependency layer is cached when only app code changes.
COPY pyproject.toml uv.lock* ./

# Sync dependencies (no project install yet so app code changes don't bust this layer).
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project || \
    uv sync --no-dev --no-install-project

# Now copy app sources and install the project itself.
COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev || uv sync --no-dev

# ============================ runtime =====================================
FROM python:${PYTHON_VERSION} AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH=/app

# Minimal runtime deps. curl is included only for the HEALTHCHECK.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user with a fixed uid for reproducibility.
RUN groupadd --system --gid 10001 eaip \
 && useradd  --system --uid 10001 --gid 10001 --home /home/eaip --create-home eaip

# Copy the virtualenv from the builder.
COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY --from=builder /build/app /app/app
COPY --from=builder /build/alembic /app/alembic
COPY --from=builder /build/alembic.ini /app/alembic.ini

USER eaip

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:8000/healthz || exit 1

# Single worker per replica; horizontal scaling lives at the orchestrator layer.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips", "*"]
