"""Server-sent events (SSE) framing for the streaming chat endpoint.

The SSE spec (https://html.spec.whatwg.org/multipage/server-sent-events.html)
defines a simple text-based wire format:

    event: <event-name>
    data: <one-line JSON>
    <blank line>

Multi-line data is allowed but we always JSON-encode our payloads as a
single line for simplicity. Every event has a name; the client uses
EventSource.addEventListener(name, handler) to route.

This module exposes:
    * a typed Event union of what we emit
    * format_event(event) -> str: serialize to wire bytes
    * format_heartbeat() -> str: a comment line for keepalive

Why a separate module:
    * Keeps the streaming agent code free of wire-format clutter.
    * Easier to unit test (just compare strings).
    * If we add a different transport later (e.g. WebSocket frames),
      we swap the formatter without touching the event semantics.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Union

# --- event payloads -------------------------------------------------------


@dataclass(frozen=True)
class TokenEvent:
    """One incremental text chunk from the model."""

    text: str


@dataclass(frozen=True)
class ToolUseEvent:
    """The model decided to call a tool. Sent at tool_use_start."""

    tool: str
    input: dict[str, Any]


@dataclass(frozen=True)
class ToolResultEvent:
    """A tool finished executing. Summary only, never the raw payload —
    that would defeat the point of streaming."""

    tool: str
    hit_count: int | None = None
    ok: bool = True


@dataclass(frozen=True)
class CitationsEvent:
    """The full citation list, sent once after the final answer."""

    citations: list[dict[str, Any]]


@dataclass(frozen=True)
class DoneEvent:
    """End-of-stream marker with summary metadata."""

    iterations: int
    truncated: bool


@dataclass(frozen=True)
class ErrorEvent:
    """A non-recoverable error during streaming. Client should treat
    this as terminal."""

    message: str
    code: str = "internal"


Event = Union[
    TokenEvent,
    ToolUseEvent,
    ToolResultEvent,
    CitationsEvent,
    DoneEvent,
    ErrorEvent,
]


# --- wire format ----------------------------------------------------------


_EVENT_NAMES: dict[type, str] = {
    TokenEvent: "token",
    ToolUseEvent: "tool_use",
    ToolResultEvent: "tool_result",
    CitationsEvent: "citations",
    DoneEvent: "done",
    ErrorEvent: "error",
}


def _payload(event: Event) -> dict[str, Any]:
    """Extract the JSON-serializable payload from an event dataclass."""
    if isinstance(event, TokenEvent):
        return {"text": event.text}
    if isinstance(event, ToolUseEvent):
        return {"tool": event.tool, "input": event.input}
    if isinstance(event, ToolResultEvent):
        return {"tool": event.tool, "hit_count": event.hit_count, "ok": event.ok}
    if isinstance(event, CitationsEvent):
        return {"citations": event.citations}
    if isinstance(event, DoneEvent):
        return {"iterations": event.iterations, "truncated": event.truncated}
    if isinstance(event, ErrorEvent):
        return {"message": event.message, "code": event.code}
    raise TypeError(f"unknown event type: {type(event).__name__}")


def format_event(event: Event) -> str:
    """Serialize one Event to its SSE wire form."""
    name = _EVENT_NAMES[type(event)]
    data = json.dumps(_payload(event), ensure_ascii=False, separators=(",", ":"))
    return f"event: {name}\ndata: {data}\n\n"


def format_heartbeat() -> str:
    """A comment line. Sent periodically to keep proxies from
    timing out a quiet connection. Clients ignore comment lines."""
    return ": keepalive\n\n"
