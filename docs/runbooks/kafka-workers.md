# Kafka worker operations

## Deployment and supervision

Run ingestion and embedding as separate supervised processes. The entry
points are `python -m app.workers.ingestion` and
`python -m app.workers.embedding` from the API environment. Keep their
consumer groups, `ingestion-worker` and `embedding-worker`, stable across
restarts so the broker retains the committed offsets. Use the locked API
environment. The Kafka shutdown adapter requires the locked `aiokafka`
version. Use the restricted [database runtime login](../database-roles.md)
for the API and both workers.

Configure the supervisor to restart failed workers with backoff and report
repeated failures. An unhandled record-processing or offset-commit error
stops consumption and starts cleanup. Restarting the same group allows it to
resume from Kafka's last committed offset. Investigate a persistent failure
at one offset before altering offsets or group identity.

Send SIGTERM to the Python process for a planned stop. Use a launch command
that passes signals through or replaces its wrapper process with the worker.
Avoid force termination as routine shutdown. Give the application time to
finish its configured shutdown stages, then use the supervisor deadline as
the backstop for a process that does not exit.

## Shutdown settings

All settings accept positive finite seconds.

| Setting | Default | Applies to |
| --- | --- | --- |
| `EAIP_KAFKA_WORKER_SHUTDOWN_TIMEOUT_SECONDS` | 10 seconds | Shared monotonic shutdown budget for ingestion and embedding. |
| `EAIP_KAFKA_SHUTDOWN_GRACE_SECONDS` | 5 seconds | Cap on active work, outbox drain and producer flush. |
| `EAIP_KAFKA_SHUTDOWN_CLEANUP_SECONDS` | 2 seconds | Cap on individual cleanup stages. |

On SIGTERM or SIGINT, a worker starts its shared budget and stops taking new
records. Its initial drain is limited to the lesser of the grace setting
and half the total budget. Subsequent stages use their cap or the remaining
budget, whichever is shorter. Worker failure also starts bounded cleanup.
The API lifespan still uses the Kafka stage caps independently.

When the initial drain expires, the worker cancels active work and the
outbox publisher so their database transactions can unwind. If rollback
stalls, the worker aborts its own active PostgreSQL connections. A database
transport abort or an exhausted shutdown budget yields exit status 1.
Unacknowledged work remains eligible for replay when its transaction rolls
back.

Consumer cleanup gates new offset commits and cancels and joins outstanding
offset requests before normal Kafka client close. If that join times out,
cleanup takes the client-abort path. A cancelled request may already have
reached the broker. On restart, use the broker's committed offset to
determine where processing resumes.

Give the supervisor more time than the application budget and measure
actual process exit in a disposable outage drill. The application budget
depends on the event loop getting scheduled and on dependency code
responding to cancellation or transport closure.

### Kubernetes supervision profile

Start target-environment rehearsal with the default 10-second worker
budget and a 30-second pod `terminationGracePeriodSeconds`. Leave
`preStop` unset for workers. If the deployment needs a lifecycle hook
or changes the application budget, allow time for that hook, the worker
budget, and an observed margin for signal delivery and process exit.
Validate the resulting deadline under load before rollout.

Set the container's `command` to `["/opt/venv/bin/python"]` and
its `args` to `["-m", "app.workers.embedding"]`. Launch ingestion
separately with `app.workers.ingestion` as its module. Setting both fields
replaces the API image's Uvicorn command and arguments and makes Python the
container process that receives SIGTERM. Verify the command and environment
in the built worker image. Let the workload controller replace terminated
pods and use container restart with backoff for worker failures. Keep
consumer groups stable so replacement workers use the broker's stored offsets.

In a disposable rehearsal, measure cooperative SIGTERM and supervisor
escalation separately. For the backstop case, verify that an in-flight
operation remains stalled after SIGTERM, then record the supervisor's
forced termination reason and actual process exit. A forced kill skips
application cleanup. After dependencies recover, inspect the broker's
committed offset, confirm PostgreSQL released any transaction locks,
and verify pending outbox delivery and replay with the same event ID.
Allow for rollback to finish after PostgreSQL can process the lost
connection. A signal-related exit code alone does not distinguish
deadline escalation from an out-of-memory kill.

Repeat both cases on the intended Kubernetes runner with the built
image, configured grace period, and target dependency topology.
Local subprocess drills establish the tested process and replay
behavior. The target supervisor remains a separate release gate.

## Delivery and replay

The application writes business state and its outbox row in the same
PostgreSQL transaction. The publisher marks an outbox row after Kafka
acknowledgement. A failed send leaves the row eligible for another attempt.
If Kafka accepted the record and the database transaction rolls back, the
same event can be published again. A replay of the same RAG outbox row
retains its payload `event_id`. The Kafka message key is used for
partitioning and is not a deduplication key.

A consumer commits the next offset for a partition after its handler's
database transaction succeeds. Failure before that commit leaves the event
eligible for replay. If the offset request's wire outcome is ambiguous,
a restart resumes from the broker's stored value.

Ingestion and embedding claim events for existing organizations in
`consumer_inbox`, scoped by organization, consumer and event UUID. The
receipt commits in the same PostgreSQL transaction as the handler's
database effects and any follow-on outbox rows. A duplicate of a committed receipt skips the handler's
business effects. A rollback also discards the receipt, allowing replay.
Events for deleted organizations are skipped. The receipt protects
committed database effects. An external
call, such as an embedding request, may already have run before a rollback
and can run again on replay.

### Inbox rollout

Apply Alembic revision `0003` with the schema-owner login before starting
the updated workers. Reapply the repository's runtime grants after the
migration so the restricted login receives `SELECT` and `INSERT` on
`consumer_inbox`. Follow the [database role instructions](../database-roles.md).
The inbox uses forced tenant RLS. Finish the worker rollout before relying
on inbox protection across the group. Dropping the table or deleting
receipts removes replay protection for those events.

Producers must supply a valid, stable UUID `event_id` in the Kafka payload.
Preserve that UUID when retrying or replaying the same event and assign a
new UUID to each distinct event. Missing or malformed IDs fail processing,
so check legacy events before rollout. An offset reset can replay events
from before receipts existed.

## Broker outage triage

1. Compare API liveness with dependency readiness. `GET /healthz` reports
   process liveness without external calls. `GET /readyz` probes PostgreSQL,
   Redis and Kafka. Its Kafka check exercises metadata reachability. A
   running API can return 503 during a Kafka outage, and a startup failure
   can prevent the HTTP listener from serving either probe.
2. Inspect broker health and advertised listeners from the worker network.
   Check both consumer groups' lag, outbox progress, and supervisor restarts.
   Worker processes have no HTTP readiness endpoint. The API probe cannot
   establish that a worker is consuming or that publication is progressing.
3. Restore broker connectivity, then confirm supervised workers rejoin
   their original groups. Look for new committed outbox batches and
   declining consumer-group lag. If an authorized backlog monitor is
   available, confirm the oldest pending row gets younger. In a disposable
   environment, confirm a test event reaches its expected downstream state.
4. If the same record fails after recovery, retain the failing partition and
   offset in restricted incident notes. Check handler dependencies and event
   schema before restarting again. Avoid manual offset advancement, group
   recreation, or marking pending outbox rows as published as routine recovery.

## Monitoring

Compare publisher activity with consumer-group lag. The publisher logs
`outbox.publish.selected` before sending a nonempty batch. Its `count`
and `oldest_selected_age_seconds` describe the unlocked pending rows
selected for that batch. Rows locked by another publisher and rows
outside the batch are outside this sample.

The publisher logs `outbox.publish.batch` after the database commits.
Its `count` is the number of rows marked published. Its `oldest_published_age_seconds` is
the greatest age among those rows at commit. During an outage, there may
be no successful samples. Use an authorized backlog monitor if you need
the age of the oldest row still pending. Consumer-group lag measures how
far a consumer is behind records already on Kafka.

Count `outbox.publish.failed` events as failed send attempts.
`outbox.publisher.error` records a batch-loop failure. Both use an
`error_type` field. A failed send followed by a later batch can indicate
recovery without identifying which attempt published a particular row.
Worker `ingestion.message.failed` and `embedding.message.failed` events
include the processing or offset-commit stage and exception type.
Count `kafka.consumer.duplicate` by its fixed `consumer` label to track
receipt conflicts that skipped handler work. This is a count of
observed duplicates, including repeated deliveries of the same event.

`kafka.worker.shutdown_finished` reports `duration_seconds`,
`deadline_exhausted`, and `database_abort` when worker cleanup returns.
Use supervisor exit timing and restart counts if no finish event arrives.

Track worker exit and restart counts using the supervisor. Track committed
offsets and group lag using broker-side tooling. Use aggregate labels such
as environment, component, and failure category. Keep event IDs, document
IDs, partition and offset details, connection details, and payloads out of
metric labels and shared incident reports. Restrict access to diagnostic logs.
Alert on repeated worker exits or deadline events. Correlate the stage
with the broker outage window.

| Stage | Deadline events |
| --- | --- |
| Worker drain and total budget | `kafka.worker.shutdown_deadline_exceeded`, `kafka.worker.shutdown_budget_exhausted` |
| PostgreSQL rollback recovery | `kafka.worker.database_shutdown_aborted` |
| Consumer close | `kafka.consumer.shutdown_deadline_exceeded`, `kafka.consumer.cleanup_failed` |
| Outbox drain | `outbox.publisher.shutdown_deadline_exceeded` |
| Producer close | `kafka.producer.shutdown_deadline_exceeded`, `kafka.producer.stop_failed` |
| Kafka client transport close | `kafka.client.cleanup_deadline_exceeded` |

Log counts indicate observed attempts and failures. They do not provide a
deduplicated lifetime retry count or a count of duplicate business effects.
Trace headers can correlate operations when a trace is available. Confirm
delivery using broker records and committed database state.

## Local verification

From `apps/api`, run the targeted checks against a disposable Docker engine.
The fixture provisions the service URLs, applies migrations as a separate
schema owner, and truncates test data between cases.

```bash
EAIP_ENVIRONMENT=test EAIP_SKIP_CONTAINERS=0 EAIP_OTEL_SAMPLE_RATIO=0 \
  uv run --frozen pytest --no-cov -v \
  tests/integration/test_outbox_kafka.py tests/integration/test_consumer_inbox.py \
  tests/integration/test_healthz.py
```

For a broker-outage drill, use a dedicated test broker and a committed
synthetic outbox event. Observe pending state while the broker is unavailable.
Restore connectivity, then verify receipt and the outbox marker. For a
shutdown drill, send SIGTERM during a stalled send or handler. Record the
application's exit time, exit status, deadline events and whether replay
completes after the dependency recovers. For a combined outage drill,
hold a worker-owned database operation and interrupt broker connectivity
before sending SIGTERM. Verify the interrupted transaction and offset
outcome after recovery. The integration suite covers connection refusal, acknowledgement followed
by database rollback, and replay from a failed consumer offset.
