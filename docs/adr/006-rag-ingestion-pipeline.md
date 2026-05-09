# ADR-006: RAG ingestion pipeline

**Date:** 2026-05-09
**Status:** Accepted
**Phase:** 3

## Context

The platform needs to let tenants upload documents (PDF, plain text, markdown) and ask questions against them. Standard "retrieval-augmented generation" — chunk the document, embed each chunk, store the vectors, and at query time embed the question and find the most similar chunks to feed into an LLM.

Several decisions had to be made:

1. Where do uploaded files live?
2. How is ingestion triggered — inline in the request, or async via workers?
3. How are documents chunked?
4. Which embedding provider?
5. Where are vectors stored — pgvector, Qdrant, Pinecone, Weaviate?
6. How do we handle failure mid-ingestion without losing tenant data or producing stuck states?

## Decision

**1. Storage: MinIO (S3-compatible) under `documents/<org_id>/<doc_id>/<filename>`.**

Already in the stack from Phase 1. Tenant isolation by path prefix; the API enforces org boundary. Production swap to S3 / Azure Blob / GCS is one line in the storage adapter.

**2. Async via workers, not inline.**

PDF extraction can take seconds; embedding hundreds of chunks can take 30+ seconds. Doing it inline blocks the upload request and burns a request slot. Pattern:

```
POST /v1/documents
  -> store file in MinIO
  -> insert documents row with status='pending'
  -> emit document.uploaded event (Kafka, via outbox)
  -> return 201 immediately

ingestion-worker (Kafka consumer)
  -> document.uploaded -> fetch from MinIO -> extract text -> chunk
  -> insert document_chunks rows (embedding=NULL)
  -> update status='embedding'
  -> emit document.chunked event for each chunk

embedding-worker (Kafka consumer)
  -> document.chunked -> call OpenAI embeddings API -> UPDATE chunk.embedding
  -> when all chunks for a document are embedded, update status='ready'
```

Workers use the `aiokafka` consumer with a unique consumer group per worker class. At-least-once delivery; idempotency comes from the unique `(document_id, chunk_index)` constraint on `document_chunks`.

**3. Chunking: paragraph-aware, ~500 tokens, 50-token overlap.**

Split on double-newline (paragraphs) first. Greedily pack paragraphs into a chunk until adding the next paragraph would exceed 500 tokens; emit chunk and start a new one with a 50-token overlap from the tail of the previous chunk for context continuity. Token counting via `tiktoken` (OpenAI's tokenizer for GPT-4 / embedding models). Edge cases: paragraphs longer than 500 tokens get split on sentence boundaries; documents with no paragraph breaks get split on sentences.

This isn't the most sophisticated strategy. Smarter options exist — semantic chunking, hierarchical chunking, late chunking. We'll add them as needed. Paragraph-aware is the proven baseline.

**4. Embeddings: OpenAI text-embedding-3-small (1536 dimensions).**

Industry standard, cheap ($0.02 per million tokens), good quality at this size. The dimension count is hardcoded into the schema (`vector(1536)`), so changing providers means a new migration.

The codebase has an `EmbeddingProvider` protocol so the OpenAI implementation is swappable. The provider is selected via `EAIP_OPENAI_API_KEY` being set; absent that, requests fail loudly. No silent fallback to a different provider.

**5. Vector store: pgvector, not a dedicated vector DB.**

Reasoning:

- **Operational simplicity.** Postgres is already in the stack with backups, monitoring, replication, RLS. Adding Qdrant or Pinecone means another system to operate.
- **Tenant isolation comes for free.** RLS on `document_chunks(org_id, ...)` means cross-tenant leakage is impossible at the DB layer. With an external vector DB we'd have to enforce this at the application layer — every query a potential bug.
- **Joins.** RAG queries often want to enrich vector results with metadata (document name, upload date, ACLs). With pgvector that's a single SQL query; with an external vector DB it's two round trips and reconciliation logic.
- **Scale ceiling is fine for MVP.** pgvector with HNSW index handles tens of millions of vectors per node with sub-100ms queries. Beyond that we'd reconsider, but we're nowhere close.

Trade: dedicated vector DBs have better tooling (rerankers, hybrid search built in) and can scale further. We'll revisit if and when needed.

**6. Failure handling.**

- **Mid-ingestion crashes** are recoverable: the worker is idempotent because `(document_id, chunk_index)` is unique. Re-processing a document re-inserts the same chunks (rejected by uniqueness) or no-ops.
- **Stuck documents** (status='pending' / 'extracting' / 'embedding' for too long) are surfaced by a periodic reconciliation query that uses the partial index `documents_status_idx WHERE status != 'ready'`.
- **Permanent failures** set `status='failed'` with `error_message`. The user sees a clear error in the API; we don't quietly black-hole anything.
- **Outbox pattern** for the initial `document.uploaded` event: the row insert and the Kafka message are committed in one transaction (already implemented in Phase 2's outbox). No "DB updated but Kafka never heard" gap.

## Consequences

**Positive**

- New documents available for retrieval seconds after upload — async pipeline doesn't block the user.
- pgvector keeps tenant isolation enforced at the database level, not the app.
- Workers can be scaled independently if ingestion or embedding becomes the bottleneck.
- Re-processing documents is safe (idempotent).

**Negative / accepted trade-offs**

- More moving pieces than inline processing. Two new worker services, additional Kafka topics, status-reconciliation logic.
- pgvector requires `vector(1536)` to be hardcoded in the schema; switching embedding dimensions requires a migration. Acceptable — we don't expect to switch providers casually.
- Chunking is naive (paragraph-based). Smarter chunking can come later without changing the schema.

**Open questions for future phases**

- HNSW index parameters (`m`, `ef_construction`). Will benchmark on realistic corpus before adding the index.
- Hybrid search (keyword + vector). Likely needed for technical documentation. Add when a real corpus shows ranking gaps.
- Reranking. The current top-k retrieval is unreranked. Add a Voyage or Cohere reranker if relevance is poor.
- Document deletion / re-ingestion semantics. CASCADE handles the storage side; need to define UX (soft delete? immediate?).

## References

- pgvector: https://github.com/pgvector/pgvector
- OpenAI embeddings docs: https://platform.openai.com/docs/guides/embeddings
- ADR-002 (pgvector for vector storage) — this ADR extends and ratifies that choice.
- ADR-005 (LLM provider abstraction) — same shape applied here for embeddings.
