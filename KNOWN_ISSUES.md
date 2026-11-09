# Known issues and verification

## Quality gates

| Check | Local evidence |
| --- | --- |
| Unit tests | 76 passed without dependency URLs or provider credentials. |
| Integration tests | 47 passed against disposable PostgreSQL, Redis, Redpanda, and S3Mock. |
| Ruff format | All 105 files passed. |
| Ruff lint | Zero findings. |
| Strict mypy | Zero errors in 68 source files. |

Phase 4 hosted CI verification is pending. The workflow requires lint,
strict type checking, unit tests, formatting, and Kafka delivery, offset
and inbox integration tests. The document roundtrip requires disposable
S3-compatible storage. The local run used S3Mock. MinIO-specific
compatibility remains blocked by image availability.

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
failure they stop consuming and begin cleanup. Worker cleanup shares a
configurable monotonic shutdown deadline.
See [Kafka worker operations](docs/runbooks/kafka-workers.md) for supervision
and recovery.

Tests cover outbox-to-broker publication, retry after a refused connection,
duplicates after publisher transaction rollback, and worker offset replay.
A disposable fault drill verified bounded SIGTERM and replay while Kafka
was offline, PostgreSQL was paused, and a loopback provider did not respond.
That result applies to the tested dependencies and failure combination.

Inbox receipts protect committed PostgreSQL effects for repeat delivery
of the same tenant, consumer and wire event ID. External provider calls
can repeat after rollback. A receipt cannot deduplicate a different event
ID or a historical event with no retained receipt. Poison-message handling,
distinct-event document races and a hard process bound with non-cooperative
dependencies remain open.

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
