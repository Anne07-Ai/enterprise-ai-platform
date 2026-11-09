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
stops consumption and starts cleanup. Restarting the same group allows it to resume from Kafka's last committed
offset. A persistent failure at one offset needs investigation before
operators alter offsets or group identity.

Send SIGTERM to the Python process for a planned stop. Use a launch command
that passes signals through or replaces its wrapper process with the worker.
Avoid force termination as routine shutdown. Give the application time to
finish its configured shutdown stages, then use the supervisor deadline as
the backstop for a process that does not exit.

## Shutdown settings

Configure the following on the API and workers. The worker drain applies
to ingestion and embedding. The API uses the publisher and producer stages
during lifespan shutdown. Both settings accept positive finite seconds.

| Setting | Default | Applies to |
| --- | --- | --- |
| `EAIP_KAFKA_SHUTDOWN_GRACE_SECONDS` | 5 seconds | The worker's active startup or processing, the outbox publisher's active batch, and the producer's graceful flush. Each stage gets its own budget. |
| `EAIP_KAFKA_SHUTDOWN_CLEANUP_SECONDS` | 2 seconds | Local Kafka cleanup stages. Consumer group leave and fallback cleanup get separate budgets. |

On SIGTERM or SIGINT, the worker stops taking new records. An in-flight
operation can finish during its grace window. On expiry the worker cancels
and joins the operation, giving its database transaction time to unwind.
Publisher cancellation similarly waits for rollback before producer disposal.
A cancelled or unacknowledged outbox batch remains eligible for replay when
its database transaction rolls back. Kafka can still have accepted a record
when the sender did not receive its acknowledgement.

The timeouts are per stage. The configured default Kafka stages can total
25 seconds before database rollback, resource disposal and event-loop
scheduling. Set the supervisor termination timeout with margin beyond
that sum and verify actual process exit with a disposable outage drill.
Initial Kafka metadata cancellation also depends on the locked SDK's
startup cleanup. A deadline log
indicates that a stage exceeded its budget and entered its cancellation or
fallback path.

## Delivery and replay

The application writes business state and its outbox row in the same
PostgreSQL transaction. The publisher marks an outbox row after Kafka
acknowledgement. A failed send leaves the row eligible for another attempt.
If Kafka accepted the record and the database transaction rolls back, the
same event can be published again. A replay of the same RAG outbox row
retains its payload `event_id`. The Kafka message key is used for
partitioning and is not a deduplication key.

A consumer commits the next offset for a partition after its handler's
database transaction succeeds. An unhandled failure leaves its offset
eligible for replay. A successful transaction followed by a failed offset commit can
also replay the event. The business handlers have status and row guards, but
a replay can repeat work such as an embedding provider call. There is no
general transactional inbox or exactly-once business guarantee.

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
`outbox.publish.batch` after the database commits. Its `count` is the
number of rows marked published. Its `oldest_published_age_seconds` is
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

Track worker exit and restart counts using the supervisor. Track committed
offsets and group lag using broker-side tooling. Use aggregate labels such
as environment, component, and failure category. Keep event IDs, document
IDs, partition and offset details, connection details, and payloads out of
metric labels and shared incident reports. Restrict access to diagnostic logs.
Alert on repeated worker exits or deadline events. Correlate the stage
with the broker outage window.

| Stage | Deadline events |
| --- | --- |
| Worker drain | `kafka.worker.shutdown_deadline_exceeded` |
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
  tests/integration/test_outbox_kafka.py tests/integration/test_healthz.py
```

For a broker-outage drill, use a dedicated test broker and a committed
synthetic outbox event. Observe pending state while the broker is unavailable.
Restore connectivity, then verify receipt and the outbox marker. For a
shutdown drill, send SIGTERM during a stalled send or handler. Record the
application's exit time and whether replay completes after the dependency
recovers. Record which process and shutdown stage the drill exercised.
The integration suite covers connection refusal, acknowledgement followed
by database rollback, and replay from a failed consumer offset.
