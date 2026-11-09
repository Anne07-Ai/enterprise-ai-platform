# eaip-api

FastAPI service for the Enterprise AI Workflow Platform — multi-tenant identity, RBAC, and request plumbing (auth, tenancy, idempotency, rate limiting, audit, observability).

See `../../ARCHITECTURE.md` for the full architecture and `../../docs/adr/` for decision records.

## Quickstart

Install the locked dependencies from `apps/api`.

```bash
uv sync --frozen
EAIP_ENVIRONMENT=test uv run pytest tests/unit --cov-fail-under=0
```

Start the local infrastructure and provision separate migration and runtime
logins before running the API. See the
[repository quickstart](../../README.md#running-locally) and
[database role setup](../../docs/database-roles.md).

The integration fixture provisions disposable PostgreSQL, Redis and Redpanda
services by default. The full suite also needs a disposable S3-compatible
endpoint with an `eaip-documents` bucket for the document roundtrip. Configure
`EAIP_MINIO_ENDPOINT_URL`, `EAIP_MINIO_ACCESS_KEY` and `EAIP_MINIO_SECRET_KEY`
with test-only values, then run `uv run pytest tests/integration` with Docker
available.

For an external test stack, follow the
[disposable database contract](../../docs/database-roles.md#integration-tests).

## Layout

- `app/core/` — settings, logging, tracing, errors, deps
- `app/infra/` — db (RLS-aware), redis, kafka, outbox
- `app/middleware/` — request_id, auth, tenant, rate_limit, idempotency, audit
- `app/modules/identity/` — orgs, users, memberships, API keys
- `app/api/v1/` — versioned router, healthz/readyz
- `tests/` — pytest, testcontainers (postgres + redis + redpanda)

## Environment

All settings prefixed `EAIP_`. See `../../.env.example`.
