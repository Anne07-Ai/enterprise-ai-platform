"""Text extraction by MIME type.

Plain-text and markdown are returned as-is (UTF-8 decoded).
PDFs go through pypdf's page-by-page text extraction with paragraphs
preserved by double-newline separation.

Future formats (docx, html, csv) plug in here without touching the
worker entrypoint.
"""
from __future__ import annotations

import io
import logging

from pypdf import PdfReader

logger = logging.getLogger(__name__)


class ExtractionError(Exception):
    """Raised when the bytes can't be turned into text."""


SUPPORTED_MIME = frozenset({"text/plain", "text/markdown", "application/pdf"})


def extract_text(*, mime_type: str, data: bytes) -> str:
    """Convert raw bytes to plain text suitable for chunking.

    Raises ``ExtractionError`` if the mime type is unsupported or
    extraction fails. The API layer already validates mime against
    ALLOWED_MIME, so reaching the worker with an unsupported type is
    a programming error.
    """
    mime = mime_type.lower()
    if mime not in SUPPORTED_MIME:
        raise ExtractionError(f"unsupported mime_type: {mime!r}")

    if mime in ("text/plain", "text/markdown"):
        return _extract_text_plain(data)
    if mime == "application/pdf":
        return _extract_text_pdf(data)
    raise ExtractionError(f"no extractor for mime_type {mime!r}")


def _extract_text_plain(data: bytes) -> str:
    """Decode UTF-8, falling back to latin-1 if the bytes are not UTF-8.

    Real-world text files come with mixed encodings. We try UTF-8
    first (most common, most permissive about ASCII) and fall back to
    latin-1 (always succeeds, may produce mojibake but never crashes).
    """
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        logger.warning("extractor.utf8_failed_fallback_to_latin1")
        return data.decode("latin-1", errors="replace")


def _extract_text_pdf(data: bytes) -> str:
    """Extract text from each PDF page, separated by double-newline.

    pypdf returns one page's text per call. We concatenate pages with
    '\\n\\n' so the chunker's paragraph splitter treats page boundaries
    as paragraph boundaries.
    """
    try:
        reader = PdfReader(io.BytesIO(data))
    except Exception as exc:  # noqa: BLE001
        raise ExtractionError(f"pdf parse failed: {exc!s}") from exc

    pages: list[str] = []
    for page in reader.pages:
        try:
            text = page.extract_text() or ""
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "extractor.pdf_page_failed",
                extra={"error": str(exc)},
            )
            continue
        text = text.strip()
        if text:
            pages.append(text)

    if not pages:
        raise ExtractionError("pdf had no extractable text")
    return "\n\n".join(pages)