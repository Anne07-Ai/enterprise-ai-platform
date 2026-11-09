# Provider calls and object storage

## Retry boundaries

| Operation | Current behavior and remaining risk |
| --- | --- |
| Anthropic Messages create and stream open | The locked SDK, Anthropic 0.102.0, defaults to two retries after the initial attempt. It retries connection and timeout errors and eligible HTTP failures, including 408, 409, 429 and 5xx. Both chat entry points inherit this behavior. The application setting for Anthropic retries is unused. An accepted request with a lost response can incur additional inference, usage or billing. After an SSE token is delivered, a failed stream terminates without transparently replaying the turn. |
| OpenAI Embeddings | Search, the chat retrieval tool and the embedding worker issue a POST. The default is one initial attempt plus three retries for HTTPX request failures, 429 and 5xx. A read timeout can follow successful inference. Repeating a computation can incur duplicate usage or billing. Worker replay can repeat inference when the inbox receipt and database changes rolled back after the provider call. |
| S3 GET | Ingestion downloads stored bytes. A repeated read does not mutate the object. It can repeat request or transfer costs, and it can return different data if another writer replaces the object. |
| S3 PUT | The adapter delegates request retries to the SDK. Retrying the same bytes at the same key can converge on one visible object in an unversioned bucket. With versioning, multiple versions can remain. A retried HTTP upload can allocate a different document ID and key. The S3 write precedes database and outbox commit, so a lost response or database rollback can leave an orphan. |
| S3 DELETE | The adapter delegates request retries to the SDK. An unversioned bucket can converge to absence, subject to permissions and concurrent writes. A versioned bucket can retain objects or delete markers. The API calls S3 before database commit and treats storage failure as best effort. Storage failure can leave bytes after a 204, and database failure after successful S3 deletion can leave a live row pointing to missing bytes. There is no verified orphan reconciliation job. |
| Kafka outbox publication | The producer waits for broker acknowledgement. Database rollback or a lost outcome around acknowledgement can cause repeat publication. The transactional inbox protects committed database effects under its tenant, consumer and event identity. It does not fence inference or other calls made outside PostgreSQL. |

Local fault injection with HTTPX MockTransport raised a read timeout after
each synthetic request reached the transport. It observed four OpenAI POSTs
and three Anthropic POSTs for each of Messages create and stream open.
All observed requests lacked an idempotency key header. This exercises the
locked application and SDK retry paths with synthetic input and no network.
The inspected SDK did not emit a provider idempotency header. A vendor-side
deduplication contract, behavior under a real outage and billing effects
remain unverified. Passing an arbitrary idempotency header would not
establish those guarantees.

Treat timeouts after dispatch as ambiguous. Keep retry budgets finite, track
attempt counts and sanitized request IDs, and account for worker replay on
top of client retries. Keep redacted request metadata only. Avoid logging
prompts, documents, credentials or upstream response bodies. Changes to
provider attempt budgets, classification of ambiguous outcomes and any
durable operation ledger require a separately verified retry policy before
production. Such a ledger can prevent duplicate local application of a
stored result. It cannot resolve a lost provider outcome without a
provider-supported correlation or deduplication contract.

## HTTP replay and rollout

The Redis replay cache is scoped to tenant, authenticated actor and
permissions. API key actors have their own database IDs. Anonymous requests
such as login and refresh, and streaming chat requests, skip the cache.
Successful API key issuance stores a completion marker after database
commit. The first response returns plaintext. A replay with the same
idempotency key within the cache window returns 422 and issues no new key.
On an ambiguous response, use the authorized key listing to confirm creation,
then revoke and reissue through the normal control if necessary.

Redis replay remains an expiring best-effort facility. The default cache
TTL is 24 hours and the in-flight lease is 10 seconds. Lease expiration,
Redis failure or a crash after database commit but before cache write can
permit duplicate execution. Do not use this cache as a durable issuance or
provider-call ledger. The new lease release uses an owner comparison in a
single Redis script. Verify the deployment Redis role permits the required
script and commands.

The v2 namespace does not read old entries. Old replicas can continue
serving v1 entries during a mixed rollout. Stop routing traffic to old
replicas before admitting the new version for the affected endpoints.
Legacy Redis entries, backups and snapshots may contain API-key plaintext
or authentication responses. Restrict access, account for configured TTL and
backup retention, and use the incident process to assess credential
rotation and approved targeted remediation. This change performs no cache
purge. Treat retries spanning the cutover as potentially new requests.

## Object storage compatibility gate

Local disposable S3Mock 5.2.3 verified path-style PUT, exact byte GET,
HEAD content type and length, DELETE and missing-object behavior. It also
accepted mismatched credentials. The document API integration tests check
upload and database-backed metadata, list and deletion responses. They do
not independently verify object bytes or authentication. The observed
S3Mock behavior is insufficient for a production authentication gate.

Target MinIO verification is **BLOCKED**. The pinned release manifest
lookup was denied or unauthorized. The development Compose tags are mutable
and do not identify an approved production target. S3-compatible API
syntax and an organization prefix do not establish storage isolation.
The current adapter uses a shared bucket, trusts the caller-supplied key
and has no per-tenant credential enforcement.

Use a disposable instance of the approved target release pinned by
digest. Match target TLS, endpoint addressing, signature, versioning and
permissions. Generate synthetic content and credentials. Record server
identity, selected configuration and sanitized outcomes.

1. Check path-style SigV4 PUT, exact byte GET, HEAD content type and length,
   DELETE and verified absence under the target versioning configuration.
2. Reject bad and unsigned credentials. With two separately scoped test
   principals, reject GET, HEAD, PUT and DELETE against the other prefix
   and verify list visibility follows policy. Separately verify application
   tenant isolation with two authorized tenants and no direct object access.
3. Exercise a refused or stalled connection, denied PUT/DELETE and
   PostgreSQL rollback after successful PUT or DELETE. Record the resulting
   database and object state. Verify the deployment's reconciliation
   controls with those same fault cases before enabling production writes.

Use synthetic keys and data throughout. Keep customer buckets and production
credentials out of the drill.
