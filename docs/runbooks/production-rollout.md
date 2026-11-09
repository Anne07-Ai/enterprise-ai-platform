# Production rollout and incident response

This runbook defines a staging acceptance procedure. Infrastructure ownership, deployment manifests, and release approval belong to the target environment.

## Preflight

1. Release engineering builds the exact reviewed commit using the lockfile. Pin the runtime image and dependencies by digest or approved package source, record the SBOM and scan results, and require frozen-only dependency resolution. The existing Dockerfile has a non-frozen fallback and mutable base tags.
2. Platform engineering reviews the target workload definitions. Use separate API, embedding and ingestion workloads with direct process signal delivery. Keep admin migration credentials in a dedicated one-shot job and restricted runtime credentials in the workloads. Set explicit resource requests and limits from load and shutdown drills.
3. Security verifies the network boundary, proxy trust, credentials and key mounts. Limit forwarded-header trust to the approved proxy, and restrict direct pod access. Require durable shared JWT keys across API replicas and reject missing mounts at admission or preflight. The application can generate ephemeral keys when files are absent. Require broker authentication and transport controls proven against the current client, which supplies neither SASL nor TLS configuration. Gate the release if the target cannot meet that requirement.
4. Require secure dependency endpoints and approved non-default credentials. Restrict object-storage access to required buckets or prefixes. Disable demo seeding. Protect telemetry and operator endpoints with network policy and authentication, and review attributes and logs for user data.
5. Budget the worst-case PostgreSQL connections across all API and worker replicas, including rolling surge and pool overflow. The default pool can use up to 30 connections per process. Size worker replicas against partition count, database headroom and provider rate limits.

## Probes and supervision

| Component | Initial staging contract |
| --- | --- |
| API startup | Probe `/healthz` on the private workload endpoint. Start with a 60-second total startup allowance and measure cold start with dependent services unavailable and recovering. |
| API liveness | Probe `/healthz` independently of shared dependencies, for example every 10 seconds with a 2-second timeout and three failures before restart. |
| API readiness | Probe `/readyz` every 10 seconds with a timeout above its 2-second dependency probe budget, for example 3 seconds. PostgreSQL, Redis and Kafka are checked. Validate traffic withdrawal and return on recovery. Object storage, provider health and schema compatibility need separate rollout checks. |
| Kafka workers | Use process supervision, stable consumer groups and workload-specific health evidence. Confirm initialization, assignment, progress and restart reason using supervisor and broker signals. The workers expose no HTTP health endpoints. |
| Termination | Apply the Kafka worker runbook to worker processes. Verify the API separately with in-flight requests and its embedded outbox publisher. Withdraw ingress, measure drain plus shutdown, and configure separate budgets based on the result. |

## Monitoring and staging drills

The local Collector, Prometheus, Tempo, Loki and Grafana configuration is a starting point. Install and exercise the target collectors and alert rules before release. Aggregate by environment, service and consumer group. Keep tenant and event identifiers out of metric labels.

| Signal | Source and initial staging drill |
| --- | --- |
| Consumer lag | Broker exporter using committed group offsets and partition end offsets. Create a disposable backlog. Verify a page when lag rises and progress stops for 10 minutes, then confirm it clears after catch-up. Calibrate against the processing SLO. |
| Oldest unpublished outbox age | Export age and row count through a scoped, reviewed monitor. Keep cross-tenant monitor privileges separate from application roles. Stop the broker and verify a page when the oldest age exceeds 5 minutes, then restore and confirm drainage. |
| Degraded shutdown and repeated restart | Supervisor exit reason and structured worker shutdown logs. Verify that forced kills and degraded exits remain distinguishable. Page on a forced kill during rollout or repeated degraded exits within 15 minutes. |
| Database and dependency health | Database exporter and probe outcomes. Alert on sustained readiness loss, connection saturation and lock waits. Check that liveness remains independent and pool state recovers. |
| Provider and object storage | Observe aggregate provider retry and error rates, plus a separate authenticated object-store canary with disposable keys and data. Exercise expiry, revocation, timeout and permission-denial behavior. |

Thresholds are staging starting points. Record the dashboard, paging route, named on-call owner, alert fire and resolution timestamps, and rollout decision.

## Rollout and rollback

1. Rehearse with synthetic data in the target topology. Verify fresh startup, dependency outage recovery, worker SIGTERM, supervisor hard kill and restart, object storage access controls, migration against a populated copy, and backup restore. Record image digests and exact configuration revisions.
2. Obtain migration approval and follow the consumer inbox runbook. Apply and verify schema and grants before starting the candidate workers. Keep the last compatible image and workload configuration available.
3. Start a canary API and bounded worker capacity. Keep a single intended consumer group per workload. Avoid concurrently running old workers that skip inbox receipts on the same processing scope. Verify pending outbox age, consumer lag, receipt creation, storage checks and authentication across replicas before increasing traffic.
4. Halt expansion for tenant or credential boundary violations, unexpected repeat effects, failed readiness, rising backlog or unplanned restarts. Capture revision, scope, offsets, outbox age and sanitized error evidence. Preserve receipts and pending rows. Reduce exposure and page the owning team.
5. Prefer rolling back application images or configuration after verifying schema and event compatibility. Pause workers if the prior image cannot honor receipts. Keep the additive inbox schema and receipts. Resume only after replay safety is verified. For data repair or restore, reconcile business data, inbox, outbox and broker offsets under the incident lead with explicit approval.

Probe and shutdown design references are the [Kubernetes probe documentation](https://kubernetes.io/docs/concepts/workloads/pods/probes/) and the [Pod termination flow](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination-flow).
