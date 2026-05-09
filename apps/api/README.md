# eaip-api

FastAPI service for the Enterprise AI Workflow Platform — multi-tenant identity, RBAC, and request plumbing (auth, tenancy, idempotency, rate limiting, audit, observability).

See `../../ARCHITECTURE.md` for the full architecture and `../../docs/adr/` for decision records.

## Quickstart

```bash
uv sync
uv run alembic upgrade head
EAIP_SEED_DEMO=1 uv run python -m app.scripts.seed
uv run pytest -v
uv run uvicorn app.main:app --reload --port 8000
```

## Layout

- `app/core/` — settings, logging, tracing, errors, deps
- `app/infra/` — db (RLS-aware), redis, kafka, outbox
- `app/middleware/` — request_id, auth, tenant, rate_limit, idempotency, audit
- `app/modules/identity/` — orgs, users, memberships, API keys
- `app/api/v1/` — versioned router, healthz/readyz
- `tests/` — pytest, testcontainers (postgres + redis + redpanda)

## Environment

All settings prefixed `EAIP_`. See `../../.env.example`.
