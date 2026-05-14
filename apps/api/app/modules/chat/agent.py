"""Tool-using chat agent.

Implements a tight ReAct-style loop directly against the Anthropic
messages.create API. No LangGraph — the loop is small enough that an
explicit implementation is easier to reason about and debug than a
graph framework. ADR-008 documents this trade-off.

Control flow:
    1. Caller hands in (messages, tools, org_id, session).
    2. agent.run() calls Anthropic with the messages + tool schemas.
    3. If the response is `stop_reason="end_turn"` -> we're done,
       return the text.
    4. If the response is `stop_reason="tool_use"` -> for each tool_use
       block, dispatch to the matching Python function, build a
       tool_result block, append both to messages, loop.
    5. Loop bounded by max_iterations (default 6). Beyond that we stop
       and return what we have plus a warning — prevents runaway costs
       from a confused model.

What this is NOT:
    * Not streaming. Phase 4c will add SSE on top of this.
    * Not persistent. State lives in the messages list during one call.
    * Not parallel tool execution. We run tools sequentially. Anthropic
      sometimes emits multiple tool_use blocks per turn; we process them
      in order.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

from anthropic import AsyncAnthropic
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.modules.chat.tools import (
    TOOL_SCHEMAS,
    get_document,
    search_documents,
)

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """\
You are a helpful assistant for a multi-tenant document platform.

You can answer questions using two tools:

* search_documents(query, limit) — semantic search over the user's
  uploaded documents. Returns chunks ranked by relevance with a
  similarity score in [0, 1].
* get_document(document_id) — fetch metadata for a specific document.

Use search_documents whenever the user asks a question that might
be answered by their documents. After tool results come back, write
a clear, concise answer using only the content of the chunks you saw.
Cite each fact by document name and chunk index in the format
[document_name #chunk_index]. If the chunks don't contain the answer,
say so plainly — do not invent information.

If the user is making small-talk or asking a question that's clearly
not about their documents, answer directly without tool use.
"""

MAX_ITERATIONS = 6
"""Hard ceiling on the agent loop. The agent should usually finish in
2-3 iterations: (1) decide tool, (2) get tool result, (3) write final
answer. We allow a few extra in case it decides to chain searches.
Beyond MAX_ITERATIONS we abort with a partial answer."""


@dataclass
class Citation:
    """One source the answer drew from."""

    document_id: str
    document_name: str
    chunk_index: int
    score: float | None = None


@dataclass
class AgentResult:
    """What the chat endpoint hands back to the caller."""

    answer: str
    citations: list[Citation] = field(default_factory=list)
    iterations: int = 0
    truncated: bool = False
    """True if we hit MAX_ITERATIONS before the model emitted end_turn."""


async def run(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    user_message: str,
    history: list[dict[str, Any]] | None = None,
) -> AgentResult:
    """Run one turn of the agent.

    ``history`` is prior turns in Anthropic's message format
    (``{"role": "user"|"assistant", "content": ...}``). Pass an empty
    list (or None) for a fresh conversation.

    Returns the final answer text plus extracted citations.
    """
    if not user_message or not user_message.strip():
        return AgentResult(answer="I need a question to answer.")

    settings = get_settings()
    client = AsyncAnthropic(
        api_key=settings.anthropic.api_key.get_secret_value(),
        timeout=settings.anthropic.request_timeout_seconds,
    )

    messages: list[dict[str, Any]] = list(history or [])
    messages.append({"role": "user", "content": user_message})

    citations_by_key: dict[tuple[str, int], Citation] = {}
    iterations = 0
    truncated = False
    final_text = ""

    try:
        while iterations < MAX_ITERATIONS:
            iterations += 1
            resp = await client.messages.create(
                model=settings.anthropic.model,
                max_tokens=settings.anthropic.max_tokens,
                system=SYSTEM_PROMPT,
                tools=TOOL_SCHEMAS,
                messages=messages,
            )

            # Append the assistant's reply to history exactly as returned —
            # Anthropic's API requires the full tool_use block to be present
            # in the message history when we later add the tool_result.
            messages.append({"role": "assistant", "content": resp.content})

            if resp.stop_reason == "end_turn":
                final_text = _extract_text(resp.content)
                break

            if resp.stop_reason != "tool_use":
                # Unusual stop_reason (max_tokens, refusal, etc.) —
                # take what text we have and stop.
                final_text = _extract_text(resp.content) or (
                    f"(agent stopped: {resp.stop_reason})"
                )
                break

            # Process every tool_use block in this turn, in order.
            tool_results: list[dict[str, Any]] = []
            for block in resp.content:
                if block.type != "tool_use":
                    continue
                result_text = await _dispatch_tool(
                    session,
                    org_id=org_id,
                    tool_name=block.name,
                    tool_input=block.input,
                    citations_sink=citations_by_key,
                )
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result_text,
                })

            messages.append({"role": "user", "content": tool_results})

        else:
            truncated = True
            final_text = (
                final_text
                or "I ran out of reasoning steps before reaching a final answer."
            )
    finally:
        # AsyncAnthropic uses an underlying httpx client — close it.
        try:
            await client.close()
        except Exception:  # noqa: BLE001
            pass

    return AgentResult(
        answer=final_text.strip(),
        citations=list(citations_by_key.values()),
        iterations=iterations,
        truncated=truncated,
    )


# --- helpers --------------------------------------------------------------


def _extract_text(content: Any) -> str:
    """Pull text out of an Anthropic content list (list of blocks)."""
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts = []
    for block in content:
        # Two shapes: SDK objects (block.type, block.text) or dicts.
        btype = getattr(block, "type", None) or (
            block.get("type") if isinstance(block, dict) else None
        )
        if btype != "text":
            continue
        btext = getattr(block, "text", None) or (
            block.get("text") if isinstance(block, dict) else None
        )
        if btext:
            parts.append(btext)
    return "\n".join(parts)


async def _dispatch_tool(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    tool_name: str,
    tool_input: dict[str, Any],
    citations_sink: dict[tuple[str, int], Citation],
) -> str:
    """Run one tool call. Returns the string to put in tool_result.content.

    Side effect: appends Citation objects to citations_sink as
    documents are encountered via search results.
    """
    logger.info(
        "agent.tool_call",
        extra={"tool": tool_name, "input_keys": list(tool_input.keys())},
    )

    if tool_name == "search_documents":
        hits = await search_documents(
            session,
            org_id=org_id,
            query=tool_input.get("query", ""),
            limit=int(tool_input.get("limit", 5)),
        )
        for h in hits:
            key = (h.document_id, h.chunk_index)
            if key not in citations_sink:
                citations_sink[key] = Citation(
                    document_id=h.document_id,
                    document_name=h.document_name,
                    chunk_index=h.chunk_index,
                    score=h.score,
                )
        if not hits:
            return "No matching chunks found."
        lines = []
        for h in hits:
            lines.append(
                f"[{h.document_name} #{h.chunk_index}] (score={h.score:.3f})\n{h.text}"
            )
        return "\n\n---\n\n".join(lines)

    if tool_name == "get_document":
        info = await get_document(
            session,
            org_id=org_id,
            document_id=tool_input.get("document_id", ""),
        )
        if info is None:
            return "Document not found."
        return (
            f"document_id={info.document_id}\n"
            f"name={info.name}\n"
            f"mime_type={info.mime_type}\n"
            f"status={info.status}\n"
            f"chunk_count={info.chunk_count}\n"
            f"byte_size={info.byte_size}"
        )

    logger.warning("agent.unknown_tool", extra={"tool": tool_name})
    return f"Unknown tool: {tool_name}"