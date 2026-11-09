# Known issues and verification

## Quality gates

| Check | Local evidence |
| --- | --- |
| Unit tests | 37 passed without dependency URLs or provider credentials. |
| Targeted integration tests | 13 passed against disposable PostgreSQL, Redis, and Redpanda. |
| Ruff format | All 86 files passed. CI treats formatting as a blocking check. |
| Ruff lint | 103 existing findings. The CI step remains advisory. |
| mypy | 96 existing errors across 14 files. The CI step remains advisory. |

Hosted CI verification is pending. The full integration suite also requires a
disposable S3-compatible endpoint for the document roundtrip.

## Tenant isolation and organization creation

The application checks runtime login privileges on new physical connections.
Use distinct schema-owner and restricted runtime logins, as described in
[database roles](docs/database-roles.md).

The targeted database tests cover representative tenant reads and writes,
pooled commit and rollback, tenant and bypass reset, organization creation
and rollback, and internal outbox database access. This evidence covers the
tested policies and depends on the effective privileges of the deployed login.
The outbox test substitutes broker send. End-to-end delivery remains a
separate check.

## Readiness

`/readyz` passed its PostgreSQL, Redis, and Kafka checks against disposable
services. The Kafka probe checks metadata reachability. Controlled tests cover
dependency failure reporting, timeout cancellation, producer startup and
retry, and partial startup cleanup.

## Identity follow-ups

- Multi-organization membership visibility, API-key permission scope, and
  principal isolation for idempotent response replay remain follow-up work.
- The demo seed address fails login schema validation. Use a valid email
  address for test users exercising login.
