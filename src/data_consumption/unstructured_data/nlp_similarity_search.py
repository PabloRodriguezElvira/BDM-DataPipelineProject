"""
Data Consumption — NLP Similarity Search (Text + Audio)
========================================================

Queries the Milvus vector database for documents semantically similar to a
given text query, then fetches the matching raw files from the Exploitation Zone
MinIO bucket to deliver full content to the caller.

This task directly consumes two assets produced by the Exploitation Zone:
  1. Milvus collections  ("text_embeddings", "audio_embeddings")
     — used for fast approximate nearest-neighbour search.
  2. MinIO exploitation-zone bucket (unstructured/text/ and unstructured/audio/)
     — used to retrieve the actual file bytes for the top-K matches.

How it works
------------
1. Encode the user query with the same lightweight hashing-trick vectorizer
   used during ingestion (128-dim for text, same hash function).
2. Search Milvus for the K nearest vectors using cosine similarity.
3. Fetch each matched file from MinIO and return its content.

Since the project focuses on the pipeline rather than retrieval quality, this
implementation is intentionally minimal: the hashing vectorizer is deterministic
and dependency-free, so results are reproducible without any ML model downloads.

Usage
-----
    # Run interactively (default query, K=5):
    python -m src.data_consumption.unstructured_data.nlp_similarity_search

    # Custom query and K:
    python -m src.data_consumption.unstructured_data.nlp_similarity_search \
        --query "car accident brooklyn highway" --top_k 3

    # Audio search (embeds query as if it described audio content):
    python -m src.data_consumption.unstructured_data.nlp_similarity_search \
        --query "emergency siren police" --modality audio --top_k 5
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys

import numpy as np

import src.common.global_variables as config
from src.common.minio_manager import read_object_bytes
from src.common.milvus_client import (
    connect as milvus_connect,
    get_text_collection,
    get_audio_collection,
)


# ── Hashing-trick vectorizer (must match unstructured_exploitation_zone.py) ───

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_TEXT_DIM  = 128   # must match config.MILVUS_EMBEDDING_DIM
_AUDIO_DIM = 64    # must match config.MILVUS_AUDIO_DIM


def _stable_hash(s: str) -> int:
    return int.from_bytes(hashlib.md5(s.encode("utf-8")).digest()[:8], "big")


def _embed_query(query: str, dim: int) -> list[float]:
    """
    Encode an arbitrary text query into a normalized float vector using the
    same hashing trick applied during the Exploitation Zone ingestion.
    Compatible with both text (dim=128) and audio (dim=64) Milvus collections.
    """
    vec = np.zeros(dim, dtype=np.float32)
    tokens = _TOKEN_RE.findall(query.lower())

    for tok in tokens:
        h = _stable_hash(tok)
        idx = h % dim
        sign = 1.0 if (h >> 32) & 1 else -1.0
        vec[idx] += sign

    for a, b in zip(tokens, tokens[1:]):
        h = _stable_hash(a + "_" + b)
        idx = h % dim
        sign = 1.0 if (h >> 32) & 1 else -1.0
        vec[idx] += 0.5 * sign

    norm = float(np.linalg.norm(vec))
    if norm > 0.0:
        vec /= norm
    return vec.tolist()


# ── Milvus search ─────────────────────────────────────────────────────────────

def _search_text(query_vec: list[float], top_k: int) -> list[dict]:
    """Search the text_embeddings Milvus collection and return result dicts."""
    collection = get_text_collection()
    collection.load()

    results = collection.search(
        data=[query_vec],
        anns_field="embedding",
        param={"metric_type": "COSINE", "params": {"nprobe": 16}},
        limit=top_k,
        output_fields=["filename", "headline", "category", "authors", "pub_date", "content_preview"],
    )

    hits = []
    for hit in results[0]:
        hits.append({
            "filename":        hit.entity.get("filename", ""),
            "headline":        hit.entity.get("headline", ""),
            "category":        hit.entity.get("category", ""),
            "authors":         hit.entity.get("authors", ""),
            "pub_date":        hit.entity.get("pub_date", ""),
            "content_preview": hit.entity.get("content_preview", ""),
            "score":           float(hit.score),
        })
    return hits


def _search_audio(query_vec: list[float], top_k: int) -> list[dict]:
    """Search the audio_embeddings Milvus collection and return result dicts."""
    collection = get_audio_collection()
    collection.load()

    results = collection.search(
        data=[query_vec],
        anns_field="embedding",
        param={"metric_type": "COSINE", "params": {"nprobe": 16}},
        limit=top_k,
        output_fields=["filename", "duration_s", "sample_rate", "num_channels"],
    )

    hits = []
    for hit in results[0]:
        hits.append({
            "filename":    hit.entity.get("filename", ""),
            "duration_s":  hit.entity.get("duration_s", 0.0),
            "sample_rate": hit.entity.get("sample_rate", 0),
            "channels":    hit.entity.get("num_channels", 1),
            "score":       float(hit.score),
        })
    return hits


# ── MinIO fetch ───────────────────────────────────────────────────────────────

def _fetch_text_file(filename: str) -> str | None:
    """Fetch a cleaned text file from the Exploitation Zone MinIO bucket."""
    key = config.EXPLOIT_TEXT_PREFIX + filename
    raw = read_object_bytes(config.EXPLOITATION_BUCKET, key)
    if raw is None:
        return None
    return raw.decode("utf-8", errors="replace")


def _fetch_audio_info(filename: str) -> bytes | None:
    """
    Fetch audio file bytes from the Exploitation Zone MinIO bucket.
    Returns the raw WAV bytes (the caller can inspect the header or save the file).
    """
    key = config.EXPLOIT_AUDIO_PREFIX + filename
    return read_object_bytes(config.EXPLOITATION_BUCKET, key)


# ── High-level search functions ───────────────────────────────────────────────

def search_texts(query: str, top_k: int = 5) -> list[dict]:
    """
    End-to-end text similarity search.

    Parameters
    ----------
    query   : free-text search string.
    top_k   : number of results to return.

    Returns a list of dicts with keys: filename, headline, category, authors,
    pub_date, score, file_content (first 500 chars of the matched file).
    """
    milvus_connect()
    vec  = _embed_query(query, _TEXT_DIM)
    hits = _search_text(vec, top_k)

    for hit in hits:
        content = _fetch_text_file(hit["filename"])
        hit["file_content"] = (content or "")[:500] if content else "[file not found in MinIO]"

    return hits


def search_audio(query: str, top_k: int = 5) -> list[dict]:
    """
    End-to-end audio similarity search using a text query.

    The query is embedded with the same hashing trick (dim=64 this time) and
    compared against the acoustic-statistics embeddings in Milvus. The WAV
    metadata (duration, sample rate) of the matched clips is returned along with
    the raw file size fetched from MinIO.
    """
    milvus_connect()
    vec  = _embed_query(query, _AUDIO_DIM)
    hits = _search_audio(vec, top_k)

    for hit in hits:
        wav_bytes = _fetch_audio_info(hit["filename"])
        hit["file_size_bytes"] = len(wav_bytes) if wav_bytes else 0
        hit["file_found"] = wav_bytes is not None

    return hits


# ── CLI entry point ───────────────────────────────────────────────────────────

def _print_text_results(results: list[dict], query: str) -> None:
    print(f"\n{'='*70}")
    print(f"  TEXT SIMILARITY SEARCH  —  query: \"{query}\"")
    print(f"{'='*70}")
    for i, r in enumerate(results, 1):
        print(f"\n[{i}] {r['headline']}  (score={r['score']:.4f})")
        print(f"    Category : {r['category']}")
        print(f"    Authors  : {r['authors']}")
        print(f"    Date     : {r['pub_date']}")
        print(f"    File     : {r['filename']}")
        preview = r.get("file_content", "")[:300].replace("\n", " ")
        print(f"    Preview  : {preview}…")
    print()


def _print_audio_results(results: list[dict], query: str) -> None:
    print(f"\n{'='*70}")
    print(f"  AUDIO SIMILARITY SEARCH  —  query: \"{query}\"")
    print(f"{'='*70}")
    for i, r in enumerate(results, 1):
        found = "✓" if r.get("file_found") else "✗"
        print(f"\n[{i}] {r['filename']}  (score={r['score']:.4f})")
        print(f"    Duration    : {r['duration_s']:.2f}s")
        print(f"    Sample rate : {r['sample_rate']} Hz")
        print(f"    Channels    : {r['channels']}")
        size_kb = r.get("file_size_bytes", 0) / 1024
        print(f"    MinIO fetch : {found}  ({size_kb:.1f} KB)")
    print()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="NLP similarity search over Milvus + MinIO")
    parser.add_argument("--query",    default="car accident brooklyn traffic", help="Search query")
    parser.add_argument("--top_k",   type=int, default=5, help="Number of results to return")
    parser.add_argument("--modality", choices=["text", "audio", "both"], default="both",
                        help="Which Milvus collection to search")
    args = parser.parse_args(argv)

    try:
        if args.modality in ("text", "both"):
            results = search_texts(args.query, args.top_k)
            _print_text_results(results, args.query)

        if args.modality in ("audio", "both"):
            results = search_audio(args.query, args.top_k)
            _print_audio_results(results, args.query)

    except Exception as exc:
        print(f"[ERROR] Similarity search failed: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
