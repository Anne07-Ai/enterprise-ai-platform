"""Offline checks for embedding client lifecycle."""

from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest
from app.modules.rag.embeddings import OpenAIEmbeddingProvider
from pydantic import SecretStr

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_close_failure_logs_without_details_and_clears_client(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    failed = AsyncMock(spec=httpx.AsyncClient)
    failed.aclose.side_effect = RuntimeError("sensitive close details")
    failed.post.return_value = httpx.Response(200, json={"data": [{"embedding": [1.0]}]})
    replacement = AsyncMock(spec=httpx.AsyncClient)
    replacement.post.return_value = httpx.Response(200, json={"data": [{"embedding": [2.0]}]})
    clients = iter([failed, replacement])
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: next(clients))
    provider = OpenAIEmbeddingProvider(api_key=SecretStr("fixture-key"), dimensions=1)

    assert await provider.embed(["first"]) == [[1.0]]
    await provider.aclose()
    assert await provider.embed(["second"]) == [[2.0]]
    await provider.aclose()

    failed.aclose.assert_awaited_once()
    replacement.aclose.assert_awaited_once()
    assert "openai.embeddings.client_close_failed" in caplog.messages
    assert "sensitive close details" not in caplog.text
