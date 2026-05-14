"""Tools exposed to the chat agent.

Each tool is a plain async function — no LangChain decorators needed.
The agent layer in app/modules/chat/agent.py wraps these as LangGraph
tools at call time. Keeping the tools themselves framework-free means
they're unit-testable with just a session + storage + the tool args.

Why two tools instead of one:
    * search_documents — semantic search, returns chunks with scores.
      Used when the user asks a question whose answer is in their docs.
    * get_document — full metadata for a known document_id. Used when
      the agent wants to confirm a citation source or list properties.

Tenant scoping:
    Every tool takes the caller's org_id and binds it to the session's
    RLS GUC. The agent gets the org_id from the chat request, never
    from the LLM. The LLM cannot escalate scope by lying in its
    function-call arguments.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import AsyncSession

from app.modules.rag import service as rag_service
from app.modules.rag.embeddings import build_default_provider

logger = logging.getLogger(__name__)


# --- tool result shapes ---------------------------------------------------


@dataclass(frozen=True)
class SearchHit:
    """One result from search_documents — shape the LLM sees."""

    document_id: str
    document_name: str
    chunk_index: int
    text: str
    score: float


@dataclass(frozen=True)
class DocumentInfo:
    """Document metadata returned by get_document."""

    document_id: str
    name: str
    mime_type: str
    status: str
    chunk_count: int | None
    byte_size: int


# --- search_documents -----------------------------------------------------


SEARCH_DOCUMENTS_SCHEMA: dict[str, Any] = {
    "name": "search_documents",
    "description": (
        "Semantic search over the user's uploaded documents. "
        "Returns the most relevant chunks for a natural-language query, "
        "with similarity scores in [0, 1]. Use this when the user asks "
        "a question whose answer might be in their documents."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "The natural-language search query.",
            },
            "limit": {
                "type": "integer",
                "description": "Max hits to return. 3-5 is typical. Default 5.",
                "default": 5,
            },
        },
        "required": ["query"],
    },
}


async def search_documents(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    query: str,
    limit: int = 5,
) -> list[SearchHit]:
    """Run a semantic search scoped to the caller's tenant."""
    if not query or not query.strip():
        return []
    limit = max(1, min(limit, 20))

    await session.execute(
        sql_text("SELECT set_config('app.current_org', :org, true)"),
        {"org": str(org_id)},
    )

    embedder = build_default_provider()
    try:
        hits = await rag_service.search_chunks(
            session, embedder, query=query, limit=limit
        )
    finally:
        await embedder.aclose()

    logger.info(
        "tool.search_documents.ran",
        extra={"org_id": str(org_id), "query_len": len(query), "hit_count": len(hits)},
    )

    return [
        SearchHit(
            document_id=str(h.chunk.document_id),
            document_name=h.document_name,
            chunk_index=h.chunk.chunk_index,
            text=h.chunk.text,
            score=float(h.score),
        )
        for h in hits
    ]


# --- get_document ---------------------------------------------------------


GET_DOCUMENT_SCHEMA: dict[str, Any] = {
    "name": "get_document",
    "description": (
        "Fetch metadata for a single document by ID. Use this when you "
        "already know the document_id (for example, from a previous "
        "search_documents result) and need its name, mime type, status, "
        "or chunk count."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "document_id": {
                "type": "string",
                "description": "UUID of the document.",
            },
        },
        "required": ["document_id"],
    },
}


async def get_document(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    document_id: str,
) -> DocumentInfo | None:
    """Fetch document metadata. Returns None if missing or RLS-filtered."""
    try:
        doc_uuid = uuid.UUID(document_id)
    except ValueError:
        logger.warning("tool.get_document.bad_uuid", extra={"document_id": document_id})
        return None

    await session.execute(
        sql_text("SELECT set_config('app.current_org', :org, true)"),
        {"org": str(org_id)},
    )

    doc = await rag_service.get_document(session, document_id=doc_uuid)
    if doc is None:
        logger.info(
            "tool.get_document.miss",
            extra={"org_id": str(org_id), "document_id": document_id},
        )
        return None

    return DocumentInfo(
        document_id=str(doc.id),
        name=doc.name,
        mime_type=doc.mime_type,
        status=doc.status,
        chunk_count=doc.chunk_count,
        byte_size=doc.byte_size,
    )


# --- registry -------------------------------------------------------------


TOOL_SCHEMAS = [SEARCH_DOCUMENTS_SCHEMA, GET_DOCUMENT_SCHEMA]
"""All tool schemas in one list — handed to the Anthropic SDK as the
``tools=`` parameter of the messages.create call. The agent loop in
agent.py uses these to know what's available; the dispatcher uses
the ``name`` field to route invocations to the right Python function.
"""