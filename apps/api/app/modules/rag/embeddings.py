"""Embedding providers for the RAG pipeline.

The platform's chat layer is Anthropic-first (ADR-005). Embeddings are
a different decision: we use OpenAI text-embedding-3-small (ADR-006)
because it's the cheapest credible option with broad recognition.

This module exposes a Protocol so the rest of the codebase doesn't
hard-couple to OpenAI. Tests substitute a fake provider; future work
can add Voyage, Cohere, or a local model with no churn elsewhere.

Performance notes:
* OpenAI accepts up to 2048 inputs per request. We batch at 100 to
  keep per-request payloads reasonable and limit blast radius if a
  request fails (re-do 100 chunks, not 2048).
* Each request is retried up to ``max_retries`` times with exponential
  backoff. 429 (rate limit) and 5xx are retryable; 4xx is not.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Protocol, Sequence

import httpx
from pydantic import SecretStr

logger = logging.getLogger(__name__)


class EmbeddingError(Exception):
    """Raised when an embedding call fails after all retries."""


class EmbeddingProvider(Protocol):
    """Embed text into vectors.

    Implementations MUST:
    * Return one vector per input, in the same order.
    * Return vectors of consistent dimension across all calls in the
      lifetime of an instance.
    * Raise ``EmbeddingError`` for non-recoverable failures.
    """

    @property
    def dimensions(self) -> int: ...

    @property
    def model_name(self) -> str: ...

    async def embed(self, texts: Sequence[str]) -> list[list[float]]: ...


class OpenAIEmbeddingProvider:
    """OpenAI implementation of ``EmbeddingProvider``.

    Constructed once per worker process. Reuses an ``httpx.AsyncClient``
    for connection pooling. The client is created lazily so import-time
    code doesn't touch the network.
    """

    BASE_URL = "https://api.openai.com/v1"
    BATCH_SIZE = 100

    def __init__(
        self,
        *,
        api_key: SecretStr,
        model: str = "text-embedding-3-small",
        dimensions: int = 1536,
        timeout_seconds: float = 30.0,
        max_retries: int = 3,
    ) -> None:
        if not api_key.get_secret_value():
            raise ValueError(
                "OpenAIEmbeddingProvider requires a non-empty api_key. "
                "Set EAIP_OPENAI_API_KEY in the environment."
            )
        self._api_key = api_key
        self._model = model
        self._dimensions = dimensions
        self._timeout = timeout_seconds
        self._max_retries = max_retries
        self._client: httpx.AsyncClient | None = None

    @property
    def dimensions(self) -> int:
        return self._dimensions

    @property
    def model_name(self) -> str:
        return self._model

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.BASE_URL,
                timeout=self._timeout,
                headers={
                    "Authorization": f"Bearer {self._api_key.get_secret_value()}",
                    "Content-Type": "application/json",
                },
            )
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:  # noqa: BLE001
                pass
            self._client = None

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        if not texts:
            return []

        results: list[list[float]] = []
        for start in range(0, len(texts), self.BATCH_SIZE):
            batch = list(texts[start : start + self.BATCH_SIZE])
            vectors = await self._embed_batch(batch)
            if len(vectors) != len(batch):
                raise EmbeddingError(
                    f"OpenAI returned {len(vectors)} vectors for {len(batch)} inputs"
                )
            results.extend(vectors)
        return results

    async def _embed_batch(self, batch: list[str]) -> list[list[float]]:
        client = await self._get_client()
        payload = {"model": self._model, "input": batch}

        last_exc: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                resp = await client.post("/embeddings", json=payload)
            except httpx.RequestError as e:
                last_exc = e
                logger.warning(
                    "openai.embeddings.network_error",
                    extra={"attempt": attempt, "error": str(e)},
                )
            else:
                if resp.status_code == 200:
                    body = resp.json()
                    return [item["embedding"] for item in body["data"]]

                # Retryable: rate limit or server error.
                if resp.status_code == 429 or resp.status_code >= 500:
                    last_exc = EmbeddingError(
                        f"OpenAI {resp.status_code}: {resp.text[:200]}"
                    )
                    logger.warning(
                        "openai.embeddings.retryable_error",
                        extra={
                            "attempt": attempt,
                            "status_code": resp.status_code,
                        },
                    )
                else:
                    # Non-retryable client error (bad model, bad key, etc.)
                    raise EmbeddingError(
                        f"OpenAI {resp.status_code}: {resp.text[:500]}"
                    )

            if attempt < self._max_retries:
                # Exponential backoff: 0.5s, 1s, 2s.
                await asyncio.sleep(0.5 * (2**attempt))

        raise EmbeddingError(
            f"OpenAI embeddings failed after {self._max_retries + 1} attempts"
        ) from last_exc


def build_default_provider() -> OpenAIEmbeddingProvider:
    """Construct the provider from settings. Convenience helper for
    workers and tests that want the real OpenAI client.
    """
    from app.core.config import get_settings

    settings = get_settings()
    return OpenAIEmbeddingProvider(
        api_key=settings.openai.api_key,
        model=settings.openai.embedding_model,
        dimensions=settings.openai.embedding_dimensions,
        timeout_seconds=settings.openai.request_timeout_seconds,
        max_retries=settings.openai.max_retries,
    )