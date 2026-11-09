# Known issues and verification

## Quality gates

| Check | Local evidence |
| --- | --- |
| Unit tests | 64 passed without dependency URLs or provider credentials. |
| Integration tests | 37 passed against disposable PostgreSQL, Redis, Redpanda, and S3Mock. |
| Ruff format | All 98 files passed. |
| Ruff lint | Zero findings. |
| Strict mypy | Zero errors in 65 source files. |

Phase 3 hosted CI verification is pending. The workflow requires lint,
strict type checking, unit tests, formatting, and the Kafka integration
suite. The document roundtrip requires disposable S3-compatible storage.
The local run used S3Mock. MinIO-specific compatibility remains unverified.

## Tenant isolation and organization creation

The application checks runtime login privileges on new physical connections.
Use distinct schema-owner and restricted runtime logins, as described in
[database roles](docs/database-roles.md).

The targeted database tests cover representative tenant reads and writes,
pooled commit and rollback, tenant and bypass reset, organization creation
and rollback, and internal outbox database access. This evidence covers the
tested policies and depends on the effective privileges of the deployed login.
The RLS-specific outbox test substitutes broker send. The Kafka suite
below verifies actual delivery.

## Kafka delivery

The outbox can deliver the same event again if Kafka acknowledges it before
the publisher's database transaction commits. Workers commit the next
offset after successful processing. On an unhandled processing or commit
failure they stop consuming and begin cleanup. Kafka shutdown has configurable
stage deadlines, with cancellation joined before database resources close.
See [Kafka worker operations](docs/runbooks/kafka-workers.md) for supervision
and recovery.

Tests cover outbox-to-broker publication, retry after a refused connection,
duplicates after publisher transaction rollback, and worker offset replay.
A disposable broker restart also verified recovery of an in-flight publish.
A disposable outage drill also verified clean SIGTERM during a stalled
send with the broker offline, and replay after restart. Exactly-once business
effects, poison-message handling, and bounds during simultaneous dependency
stalls remain open.

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
