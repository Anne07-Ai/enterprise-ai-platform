# Roadmap

**Implemented** means code or configuration exists. **Verified** is reserved
for the checks reported in [Known Issues](KNOWN_ISSUES.md).
**Partial** identifies work with remaining implementation or integration gaps.

| Phase | Status | Current scope and remaining work |
| --- | --- | --- |
| Foundation | Implemented | Local Compose stack, healthcheck and topic scripts, architecture and ADRs, and API CI. |
| Identity and tenancy | Partial | Organization roles, password login, JWTs, API keys, migrations and RLS are implemented. Live role and isolation verification remains a release gate. OIDC, workspaces and tenant SQL lint are planned. |
| Document ingestion and retrieval | Partial | Upload, outbox, ingestion, OpenAI embeddings and pgvector search are implemented. Provider-backed end-to-end verification, hybrid retrieval and retrieval evaluation remain open. |
| Streaming chat | Partial | Synchronous and SSE endpoints, in-process tool calls and citations are implemented. Persistent conversations and a frontend are planned. |
| Agent runtime | Partial | The chat tool loop runs in the API. A general tool registry, separate runtime worker and bidirectional run UI are planned. |
| Production readiness | Planned | Deployment manifests, managed infrastructure, staged release automation, SLOs, alerting and runbooks. |

Billing, model training and federated search remain out of scope.
