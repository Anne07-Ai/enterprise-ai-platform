"""Pydantic schemas for the chat endpoint.

Wire format only — no business logic. Maps the HTTP body to/from
the agent module's plain dataclasses.

Keeping schemas separate from agent.AgentResult means:
    * agent.py stays framework-free (no Pydantic dependency).
    * If we add a different transport later (gRPC, websocket),
      we add a new schemas module, not touch the agent.
    * Validation errors are caught at the API boundary, not inside
      the agent.
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ChatMessageIn(BaseModel):
    """One past turn supplied by the client to give the agent context.

    The agent prepends these to the new user message before calling
    Anthropic. The client is responsible for tracking the conversation;
    the server is stateless across requests in this phase.
    """

    model_config = ConfigDict(extra="forbid")

    role: Literal["user", "assistant"]
    content: str = Field(min_length=1, max_length=20_000)


class ChatRequest(BaseModel):
    """POST /v1/chat request body."""

    model_config = ConfigDict(extra="forbid")

    message: str = Field(
        min_length=1,
        max_length=10_000,
        description="The user's new question or instruction.",
    )
    history: list[ChatMessageIn] = Field(
        default_factory=list,
        max_length=40,
        description=(
            "Prior conversation turns, oldest first. Client-supplied; "
            "the server does not persist conversation state in this phase."
        ),
    )


class ChatCitation(BaseModel):
    """One source the agent used to ground its answer."""

    model_config = ConfigDict(extra="forbid")

    document_id: str
    document_name: str
    chunk_index: int
    score: float | None = None


class ChatResponse(BaseModel):
    """POST /v1/chat 200 response body."""

    model_config = ConfigDict(extra="forbid")

    answer: str
    citations: list[ChatCitation]
    iterations: int = Field(
        description="How many model calls the agent made for this turn.",
    )
    truncated: bool = Field(
        description=(
            "True if the agent hit MAX_ITERATIONS before finishing. "
            "The answer may be partial."
        ),
    )