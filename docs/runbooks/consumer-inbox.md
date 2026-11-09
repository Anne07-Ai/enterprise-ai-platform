# Consumer inbox rollout and recovery

Revision `0003` adds receipts keyed by organization, consumer and wire
event UUID. The receipt commits with the handler's PostgreSQL writes and
follow-on outbox rows. Kafka offsets are acknowledged after that transaction.
A replay of the same receipt key skips committed database effects. Provider
calls made before a database rollback can repeat.

## Forward rollout

1. Confirm the deployment's migration owner, restricted runtime login,
   current Alembic revision, PostgreSQL version and required extensions.
   Verify the runtime login is separate from the owner and has no
   superuser, `BYPASSRLS` or reachable elevated role. Keep migration
   credentials in the migration job's secret scope. See
   [database role setup](../database-roles.md).
2. Audit each producing path and retained backlog for valid, stable UUIDs in
   the wire `event_id`. Preserve the same UUID on retries and explicit
   replay. Keep both consumer identities and Kafka group IDs stable.
   Resolve missing or malformed IDs before enabling the new workers.
3. Rehearse the upgrade against a disposable database populated at
   `0002`. Under a restricted login, verify tenant isolation and
   organization bootstrap before and after the upgrade. Verify first claim,
   duplicate claim, same UUID in distinct tenants, rollback and replay, and
   runtime denial of cross-tenant reads and writes. Reapply the upgrade and
   verify that receipts survive. Verify the downgrade guard with offline
   SQL generation.
4. Record a verified backup and the recovery checkpoint described below.
   Pause ingestion and embedding consumption, allow active transactions
   to resolve, and verify the prior worker instances have stopped. Leave
   group IDs and offsets intact.
5. Use the approved migration job to inspect the current revision and
   apply exactly `0003` from `0002`. Use the owner login to apply the
   version-matched runtime grants in one transaction, with psql
   `ON_ERROR_STOP` enabled. The runtime grant on the inbox is
   `SELECT, INSERT`. Use the restricted login to check forced RLS,
   tenant-less invisibility and absence of `UPDATE`, `DELETE` and
   `TRUNCATE` privileges on the table.
6. Start the new worker version with its existing consumer identities.
   Verify expected group assignments, offset advancement after database
   commits, receipt creation, duplicate skips and normal outbox progress.
   Resume the rollout only after the canary checks pass. Receipt protection
   applies once all workers processing that stream run the new version.

If the canary fails, pause affected groups and retain revision `0003` and
its receipts. The downgrade guard blocks removing the inbox. An older worker
binary cannot honor receipts, so keep its consumers paused until the owner
approves the risk of replay without inbox protection. Resume with the fixed
binary and existing group offsets after verification.

## Retention and archival

Keep all receipts online during the initial rollout. Measure the inbox
table and index size and growth. Retention must cover the oldest event that
can return along any permitted replay path, including time spent pending
in the outbox, Kafka topic retention, authorized offset resets, dead-letter
redrive, restored backups and disaster-recovery replay. Consider delays
that combine along one path. Kafka topic retention alone is insufficient.

Before changing retention, record the approved replay horizons, estimated
receipt growth, backup recovery window and expected behavior outside that
window. An archive protects replay only when receipts can be restored to the
active inbox before replay is allowed. Require a separate reviewed plan,
a verified restore and an approved change for any archive-and-delete
operation. Preserve tenant access controls and encrypt the backup or archive.
Keep archive access in the backup plane.

Organization deletion cascades to its receipts. Restoring an organization
under its previous UUID requires restoring its matching receipts and
business state together. A restored tenant without those receipts can
repeat effects from older events.

## Backup and recovery

1. Use the approved backup plane to capture the database schema, business
   rows, outbox and inbox together. Verify that the backup can read all
   rows of forced-RLS tables. A dump using the restricted application
   login cannot prove receipt completeness. Retain migration revision,
   ownership and grants in restore evidence. Coordinate object storage
   backup with any restored document references.
2. Establish a recovery checkpoint during a controlled pause. Stop new
   consumption and allow in-flight transactions and Kafka commits to settle.
   Record the broker's next committed offset for each group and partition,
   topic retention boundaries and the matching database backup or recovery
   point. A timestamp alone cannot establish that the database includes
   the effects behind a Kafka offset.
3. Restore into a disposable environment first. Verify revision `0003`,
   inbox primary key, foreign key and forced RLS, grants, representative
   tenant receipt counts and duplicate rejection under the runtime login.
   Compare outbox and business state to the chosen checkpoint. Verify
   document object availability when recovering document data.
4. Keep consumers paused while reconciling broker and database state.
   A database restored to an earlier point may lack effects represented
   by newer Kafka offsets. Resume from a verified next offset whose preceding records are reflected
   in the restored database, or use an approved conservative replay point.
   If required records are outside broker retention, stop recovery at that
   gate and reconcile from a verified archive before acknowledging further
   work. Consider follow-on outbox events that may already have reached
   the broker and may be published again after restore.
5. Resume workers in stages and verify receipts, downstream effects and
   offset advancement. Record the restored database checkpoint, broker
   offsets, coverage gaps and operator approvals. See
   [Kafka worker operations](kafka-workers.md) for outage triage.
