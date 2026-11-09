"""Exercise Anthropic message and event boundaries without a provider call."""

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import httpx
import pytest
from anthropic import AsyncAnthropic
from anthropic.types import Message, RawContentBlockDeltaEvent, TextBlock, TextDelta, Usage
from app.modules.chat import agent
from app.modules.chat.sse import CitationsEvent, DoneEvent, TokenEvent
from sqlalchemy.ext.asyncio import AsyncSession


async def test_tool_turn_replays_sdk_blocks_in_the_next_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[dict[str, object]] = []
    tool_call = {
        "type": "tool_use",
        "id": "toolu_test",
        "name": "get_document",
        "input": {"document_id": str(uuid4())},
    }

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        first_turn = len(requests) == 1
        return httpx.Response(
            200,
            json={
                "type": "message",
                "id": "msg_test",
                "model": "test-model",
                "role": "assistant",
                "content": [tool_call] if first_turn else [{"type": "text", "text": "Finished"}],
                "stop_reason": "tool_use" if first_turn else "end_turn",
                "stop_sequence": None,
                "usage": {"input_tokens": 1, "output_tokens": 1},
            },
        )

    client = AsyncAnthropic(
        api_key="test-key",
        base_url="https://anthropic.invalid",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(respond)),
    )
    monkeypatch.setattr(agent, "AsyncAnthropic", lambda **kwargs: client)
    get_document = AsyncMock(return_value=None)
    monkeypatch.setattr(agent, "get_document", get_document)

    result = await agent.run(
        Mock(spec=AsyncSession),
        org_id=uuid4(),
        user_message="Find this document",
        history=[{"role": "assistant", "content": "How can I help?"}],
    )

    assert result.answer == "Finished"
    assert result.iterations == 2
    get_document.assert_awaited_once()
    assert requests[1]["messages"] == [
        {"role": "assistant", "content": "How can I help?"},
        {"role": "user", "content": "Find this document"},
        {"role": "assistant", "content": [tool_call]},
        {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": "toolu_test",
                    "content": "Document not found.",
                }
            ],
        },
    ]


async def test_stream_emits_sdk_text_delta_and_finishes(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeStream:
        async def __aiter__(self) -> AsyncIterator[RawContentBlockDeltaEvent]:
            yield RawContentBlockDeltaEvent(
                type="content_block_delta", index=0, delta=TextDelta(type="text_delta", text="Hi")
            )

        async def get_final_message(self) -> Message:
            return Message(
                type="message",
                id="msg_test",
                role="assistant",
                model="test-model",
                content=[TextBlock(type="text", text="Hi")],
                stop_reason="end_turn",
                usage=Usage(input_tokens=1, output_tokens=1),
            )

    @asynccontextmanager
    async def stream(**kwargs: object) -> AsyncIterator[FakeStream]:
        yield FakeStream()

    close = AsyncMock()
    monkeypatch.setattr(
        agent,
        "AsyncAnthropic",
        lambda **kwargs: SimpleNamespace(messages=SimpleNamespace(stream=stream), close=close),
    )

    events = [
        event
        async for event in agent.run_streaming(
            Mock(spec=AsyncSession), org_id=uuid4(), user_message="Hello"
        )
    ]

    assert events == [
        TokenEvent(text="Hi"),
        CitationsEvent(citations=[]),
        DoneEvent(iterations=1, truncated=False),
    ]
    close.assert_awaited_once()
