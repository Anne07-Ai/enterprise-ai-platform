"""Paragraph-aware text chunker for RAG ingestion.

Strategy (ADR-006):
1. Split on double-newline (paragraph boundaries).
2. Greedily pack paragraphs into a chunk until adding the next paragraph
   would exceed ``target_tokens``.
3. Emit each chunk with ``overlap_tokens`` of context from the tail of
   the previous chunk prepended.
4. Edge cases:
   * A paragraph longer than ``target_tokens`` is split on sentence
     boundaries (period + whitespace).
   * A sentence longer than ``target_tokens`` is split on token
     boundaries — last resort, avoids losing data.
5. Empty input -> empty list.

Token counts use the GPT-4 / text-embedding-3 tokenizer (cl100k_base).
That's a close-enough approximation for any modern model; off by a few
percent at worst.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

import tiktoken

# cl100k_base is the tokenizer for GPT-4, GPT-3.5-turbo, and the
# text-embedding-3 family. We cache the encoder at module level since
# constructing it is non-trivial.
_ENCODER = tiktoken.get_encoding("cl100k_base")


@dataclass(frozen=True)
class Chunk:
    """A single chunk ready to be embedded."""

    index: int
    text: str
    token_count: int


def chunk_text(
    text: str,
    *,
    target_tokens: int = 500,
    overlap_tokens: int = 50,
) -> list[Chunk]:
    """Split ``text`` into overlapping, paragraph-aware chunks.

    ``target_tokens`` is a soft ceiling: chunks won't exceed it except
    when a single paragraph is larger (then we sentence-split). The
    overlap is taken from the *end* of the previous chunk's tokens so
    that meaning carries across boundaries.
    """
    if not text or not text.strip():
        return []
    if target_tokens <= 0:
        raise ValueError("target_tokens must be positive")
    if overlap_tokens < 0 or overlap_tokens >= target_tokens:
        raise ValueError("overlap_tokens must be in [0, target_tokens)")

    paragraphs = _split_paragraphs(text)
    pieces: list[str] = []
    for para in paragraphs:
        token_count = _count_tokens(para)
        if token_count <= target_tokens:
            pieces.append(para)
        else:
            pieces.extend(_split_oversize_paragraph(para, target_tokens))

    # Greedy pack pieces into chunks under target_tokens.
    chunks: list[Chunk] = []
    buffer: list[str] = []
    buffer_tokens = 0
    overlap_tail: list[int] = []  # token ids from previous chunk tail

    def flush() -> None:
        nonlocal buffer, buffer_tokens, overlap_tail
        if not buffer:
            return
        body_text = "\n\n".join(buffer)
        if overlap_tail:
            prefix = _ENCODER.decode(overlap_tail).strip()
            full = f"{prefix}\n\n{body_text}" if prefix else body_text
        else:
            full = body_text
        token_ids = _ENCODER.encode(full)
        chunks.append(
            Chunk(index=len(chunks), text=full, token_count=len(token_ids))
        )
        # Compute overlap tail for the NEXT chunk from this chunk's tokens.
        if overlap_tokens > 0 and len(token_ids) > overlap_tokens:
            overlap_tail = token_ids[-overlap_tokens:]
        else:
            overlap_tail = []
        buffer = []
        buffer_tokens = 0

    for piece in pieces:
        piece_tokens = _count_tokens(piece)
        # Account for the overlap prefix we'll add when we flush.
        prospective = buffer_tokens + piece_tokens + (overlap_tokens if not buffer else 0)
        if buffer and prospective > target_tokens:
            flush()
        buffer.append(piece)
        buffer_tokens += piece_tokens

    flush()
    return chunks


# --- helpers --------------------------------------------------------------


_PARAGRAPH_SPLIT_RE = re.compile(r"\n\s*\n")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def _split_paragraphs(text: str) -> list[str]:
    return [p.strip() for p in _PARAGRAPH_SPLIT_RE.split(text) if p.strip()]


def _count_tokens(s: str) -> int:
    return len(_ENCODER.encode(s))


def _split_oversize_paragraph(para: str, target_tokens: int) -> list[str]:
    """Split a too-large paragraph by sentences, falling back to token
    chunks if a single sentence is still too large.
    """
    sentences = [s.strip() for s in _SENTENCE_SPLIT_RE.split(para) if s.strip()]
    out: list[str] = []
    for sent in sentences:
        if _count_tokens(sent) <= target_tokens:
            out.append(sent)
        else:
            # Brutal token-split as last resort. Rare with sensible inputs.
            token_ids = _ENCODER.encode(sent)
            for start in range(0, len(token_ids), target_tokens):
                slice_ids = token_ids[start : start + target_tokens]
                out.append(_ENCODER.decode(slice_ids))
    return out