"""POST /v1/chat — tool-using agent over RAG.

Thin HTTP handler. Auth + permission gate via `require_permission`,
dependency injection via the central `DBSession` / `CurrentPrincipalDep`
aliases (same pattern as identity and rag modules), and orchestration
delegated to `service.handle_chat`.

Status codes:
    200 — answer returned, possibly truncated (see `truncated`)
    401 — no/invalid JWT (handled by CurrentPrincipalDep)
    403 — principal lacks chat:create (handled by require_permission)
    422 — malformed body (FastAPI default for Pydantic validation)
    502 — upstream Anthropic API error (rate limit, auth, transient)
    500 — anything else (DB, internal bug)
"""

import logging
from collections.abc import AsyncIterator

import anthropic
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import StreamingResponse

from app.core.deps import CurrentPrincipalDep
from app.infra.db import get_sessionmaker
from app.modules.chat.schemas import ChatRequest, ChatResponse
from app.modules.chat.service import handle_chat, handle_chat_stream
from app.modules.chat.sse import DoneEvent, ErrorEvent, format_event
from app.modules.identity.rbac import require_permission

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/chat", tags=["chat"])


@router.post(
    "",
    response_model=ChatResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("chat:create")],
)
async def post_chat(
    payload: ChatRequest,
    principal: CurrentPrincipalDep,
) -> ChatResponse:
    """Run one chat turn for the caller's organization.

    The agent calls back into the RAG layer via tools to ground its
    answer in the caller's documents. Conversation state is supplied
    by the client in `history`; the server does not persist it.

    The handler exposes a single 200 response. Any partial / fallback
    answer (e.g. agent hit max iterations) still returns 200 with
    `truncated: true` and whatever text the model produced. Hard
    failures (network, upstream auth) come back as 5xx.
    """
    if principal.user_id is None:
        # Defense in depth: chat is for human users, not service-to-service
        # API keys. We could add api_keys:chat in the future if needed.
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="API keys cannot use chat",
        )

    sm = get_sessionmaker()

    try:
        return await handle_chat(sm, org_id=principal.org_id, request=payload)
    except anthropic.APIStatusError as e:
        # Anthropic returned a non-2xx (401, 429, 500, etc.) — surface
        # it to the client as a 502 since the failure is upstream.
        logger.warning(
            "chat.anthropic.status_error",
            extra={
                "status": e.status_code,
                "request_id": getattr(e, "request_id", None),
            },
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"upstream LLM error (status {e.status_code})",
        ) from e
    except anthropic.APIConnectionError as e:
        logger.warning("chat.anthropic.connection_error", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="upstream LLM unreachable",
        ) from e
    except anthropic.APIError as e:
        # Catch-all for other Anthropic SDK errors.
        logger.warning("chat.anthropic.api_error", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="upstream LLM error",
        ) from e


# ---------------------------------------------------------------------------
# Streaming variant
# ---------------------------------------------------------------------------


@router.post(
    "/stream",
    status_code=status.HTTP_200_OK,
    dependencies=[require_permission("chat:create")],
    responses={
        200: {
            "content": {"text/event-stream": {}},
            "description": "SSE stream of agent events.",
        }
    },
)
async def post_chat_stream(
    payload: ChatRequest,
    principal: CurrentPrincipalDep,
) -> StreamingResponse:
    """Stream the agent's progress as server-sent events.

    Returns text/event-stream. Events emitted (named):
        token        — incremental text from the model
        tool_use     — agent decided to call a tool
        tool_result  — tool finished (summary only)
        citations    — full citation list (sent once near the end)
        done         — terminal marker with iteration count
        error        — non-recoverable failure (terminal)

    Client should treat `done` or `error` as end-of-stream. Errors that
    happen BEFORE we open the stream (auth, validation) come back as
    normal HTTP responses (401/403/422).
    """
    if principal.user_id is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="API keys cannot use chat",
        )

    sm = get_sessionmaker()

    async def _gen() -> AsyncIterator[str]:
        # We deliberately do NOT wrap LLM/network errors here. Service
        # layer yields ErrorEvent + DoneEvent on failure so the client
        # sees a clean terminal event rather than a torn connection.
        try:
            async for event in handle_chat_stream(sm, org_id=principal.org_id, request=payload):
                yield format_event(event)
        except Exception as exc:
            # Defense in depth: if service somehow raises instead of
            # yielding ErrorEvent, emit one ourselves so the client's
            # EventSource sees a graceful close.
            logger.exception("chat.stream.handler_failed")
            yield format_event(ErrorEvent(message=str(exc), code="internal"))
            yield format_event(DoneEvent(iterations=0, truncated=False))

    return StreamingResponse(
        _gen(),
        media_type="text/event-stream",
        headers={
            # Disable buffering by intermediaries so events flush immediately.
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
