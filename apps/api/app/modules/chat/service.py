"""Chat service — orchestrates DB session + agent invocation.

The HTTP handler hands this module the validated request + the
authenticated principal. We open one DB session with the caller's
org_id bound to the RLS GUC, run the agent inside that session's
transaction context, and translate the agent's plain dataclass
result into the wire-format ChatResponse.

Why a service layer rather than calling agent.run() directly from
api.py:
    * Keeps the HTTP handler thin — easy to read, easy to test.
    * Lets us swap orchestration details (different session policy,
      adding tracing, batching, etc.) without touching the route.
    * Mirrors the pattern used in modules/rag/service.py.

Session policy:
    The agent makes multiple LLM round-trips and multiple tool calls.
    Each tool call uses the supplied session. We hold one transaction
    open for the whole turn so all reads see a consistent snapshot.
    On error we roll back. On success we commit.
"""
from __future__ import annotations

import logging
import uuid

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.modules.chat.agent import run as agent_run
from app.modules.chat.schemas import ChatCitation, ChatRequest, ChatResponse

logger = logging.getLogger(__name__)


async def handle_chat(
    session_maker: async_sessionmaker,
    *,
    org_id: uuid.UUID,
    request: ChatRequest,
) -> ChatResponse:
    """Run one chat turn for the given tenant and return the response.

    Opens one session with a single transaction. The agent receives this
    session and uses it for every tool call. RLS is bound inside each
    tool via set_config('app.current_org', ...) so even if the session
    is reused (or in future, pooled), every tool call re-establishes
    its scope. That's defense in depth.

    Errors propagate to the HTTP handler. The handler maps them to
    appropriate status codes (5xx for upstream LLM failures, 5xx for
    DB failures, etc.).
    """
    history = [
        {"role": m.role, "content": m.content} for m in request.history
    ]

    async with session_maker() as session:
        async with session.begin():
            result = await agent_run(
                session,
                org_id=org_id,
                user_message=request.message,
                history=history,
            )

    logger.info(
        "chat.completed",
        extra={
            "org_id": str(org_id),
            "iterations": result.iterations,
            "truncated": result.truncated,
            "citation_count": len(result.citations),
            "answer_length": len(result.answer),
        },
    )

    return ChatResponse(
        answer=result.answer,
        citations=[
            ChatCitation(
                document_id=c.document_id,
                document_name=c.document_name,
                chunk_index=c.chunk_index,
                score=c.score,
            )
            for c in result.citations
        ],
        iterations=result.iterations,
        truncated=result.truncated,
    )