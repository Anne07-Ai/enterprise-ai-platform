"""S3-compatible storage adapter for the RAG pipeline.

MinIO in the dev stack speaks the S3 API, so this same code works
against AWS S3, Azure Blob (via the S3 gateway), GCS (via interop),
or Tigris in production — just point ``endpoint_url`` elsewhere.

Storage layout (see ADR-006):
    documents/<org_id>/<document_id>/<original_filename>

Tenant isolation is enforced by the API layer (we never construct a
path containing another tenant's org_id) and by IAM in production.
This module trusts the caller.

The client is created lazily and reused. Tests pass an explicit
client to avoid coupling to global state.
"""
from __future__ import annotations

import logging
from typing import AsyncIterator
from uuid import UUID

import aioboto3
from botocore.config import Config

from app.core.config import get_settings

logger = logging.getLogger(__name__)


class StorageError(Exception):
    """Raised when an object operation fails."""


class DocumentStorage:
    """Async S3-compatible object store wrapper.

    Methods are intentionally minimal: put, get, delete, build_uri.
    No buckets-listing, no presigned URLs yet — add when needed.
    """

    DOCUMENTS_BUCKET = "eaip-documents"

    def __init__(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
    ) -> None:
        self._endpoint = endpoint_url
        self._access_key = access_key
        self._secret_key = secret_key
        self._region = region
        # aioboto3 sessions are cheap; one per instance is fine.
        self._session = aioboto3.Session()
        # path-style addressing — required for MinIO, harmless for S3.
        self._config = Config(s3={"addressing_style": "path"}, signature_version="s3v4")

    def _client_ctx(self):
        return self._session.client(
            "s3",
            endpoint_url=self._endpoint,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name=self._region,
            config=self._config,
        )

    @staticmethod
    def build_key(*, org_id: UUID, document_id: UUID, filename: str) -> str:
        """Canonical object key for a tenant's document."""
        # Strip directory components from filename — defensive.
        safe = filename.replace("/", "_").replace("\\", "_")
        return f"{org_id}/{document_id}/{safe}"

    def build_uri(self, key: str) -> str:
        """Return the s3:// URI for storage in the database."""
        return f"s3://{self.DOCUMENTS_BUCKET}/{key}"

    async def put(
        self, *, key: str, data: bytes, content_type: str
    ) -> None:
        async with self._client_ctx() as client:
            try:
                await client.put_object(
                    Bucket=self.DOCUMENTS_BUCKET,
                    Key=key,
                    Body=data,
                    ContentType=content_type,
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "storage.put_failed",
                    extra={"key": key, "error": str(exc)},
                )
                raise StorageError(f"failed to store object {key!r}") from exc

    async def get(self, *, key: str) -> bytes:
        async with self._client_ctx() as client:
            try:
                resp = await client.get_object(
                    Bucket=self.DOCUMENTS_BUCKET, Key=key
                )
                async with resp["Body"] as stream:
                    return await stream.read()
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "storage.get_failed",
                    extra={"key": key, "error": str(exc)},
                )
                raise StorageError(f"failed to read object {key!r}") from exc

    async def delete(self, *, key: str) -> None:
        async with self._client_ctx() as client:
            try:
                await client.delete_object(
                    Bucket=self.DOCUMENTS_BUCKET, Key=key
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "storage.delete_failed",
                    extra={"key": key, "error": str(exc)},
                )
                raise StorageError(f"failed to delete object {key!r}") from exc


_storage: DocumentStorage | None = None


def get_storage() -> DocumentStorage:
    """Process-wide storage singleton.

    Reads endpoint and credentials from settings. Reset via
    ``reset_storage_for_tests`` when switching test contexts.
    """
    global _storage
    if _storage is None:
        settings = get_settings()
        _storage = DocumentStorage(
            endpoint_url=settings.minio.endpoint_url,
            access_key=settings.minio.access_key.get_secret_value(),
            secret_key=settings.minio.secret_key.get_secret_value(),
            region=settings.minio.region,
        )
    return _storage


def reset_storage_for_tests() -> None:
    global _storage
    _storage = None