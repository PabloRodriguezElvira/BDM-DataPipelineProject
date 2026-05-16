"""
Milvus client helpers.

Provides connection, collection creation, and upsert utilities used by the
Exploitation Zone pipeline for text and audio embeddings.
"""

from __future__ import annotations

import time
from typing import List

from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    MilvusClient,
    connections,
    utility,
)

import src.common.global_variables as config

MAX_CONNECTION_ATTEMPTS = 30
RETRY_DELAY_SECONDS = 2

# ── Index parameters ────────────────────────────────────────────────────────
_INDEX_PARAMS = {
    "metric_type": "COSINE",
    "index_type":  "IVF_FLAT",
    "params":      {"nlist": 128},
}


# ── Connection ───────────────────────────────────────────────────────────────

def wait_for_milvus() -> None:
    """Block until Milvus is reachable."""
    for attempt in range(1, MAX_CONNECTION_ATTEMPTS + 1):
        try:
            connections.connect(
                alias="default",
                host=config.MILVUS_HOST,
                port=config.MILVUS_PORT,
            )
            return
        except Exception as exc:
            if attempt == MAX_CONNECTION_ATTEMPTS:
                raise RuntimeError("Milvus not reachable after retries.") from exc
            print(f"[Milvus] Waiting ({attempt}/{MAX_CONNECTION_ATTEMPTS})...")
            time.sleep(RETRY_DELAY_SECONDS)


def connect() -> None:
    """Connect to Milvus, waiting for the server to be ready if necessary.
    Tolerates the ~30-60s startup time of a freshly-launched container."""
    wait_for_milvus()


# ── Schema builders ──────────────────────────────────────────────────────────

def _base_fields(dim: int) -> list[FieldSchema]:
    return [
        FieldSchema("id",        DataType.INT64,       is_primary=True, auto_id=True),
        FieldSchema("filename",  DataType.VARCHAR,      max_length=512),
        FieldSchema("embedding", DataType.FLOAT_VECTOR, dim=dim),
    ]


def _text_schema() -> CollectionSchema:
    fields = _base_fields(config.MILVUS_EMBEDDING_DIM) + [
        FieldSchema("headline",        DataType.VARCHAR, max_length=512),
        FieldSchema("category",        DataType.VARCHAR, max_length=128),
        FieldSchema("authors",         DataType.VARCHAR, max_length=256),
        FieldSchema("pub_date",        DataType.VARCHAR, max_length=64),
        FieldSchema("content_preview", DataType.VARCHAR, max_length=1024),
        FieldSchema("char_count",      DataType.INT64),
        FieldSchema("word_count",      DataType.INT64),
    ]
    return CollectionSchema(fields, description="Text article embeddings with category labels (Exploitation Zone)")


def _audio_schema() -> CollectionSchema:
    fields = _base_fields(config.MILVUS_AUDIO_DIM) + [
        FieldSchema("duration_s",   DataType.FLOAT),
        FieldSchema("sample_rate",  DataType.INT64),
        FieldSchema("num_channels", DataType.INT64),
    ]
    return CollectionSchema(fields, description="Audio file embeddings (Exploitation Zone)")


# ── Collection management ────────────────────────────────────────────────────

def _existing_dim(name: str) -> int | None:
    """Return the embedding-field dim of the existing collection, or None."""
    try:
        existing = Collection(name)
        for f in existing.schema.fields:
            if f.name == "embedding":
                return f.params.get("dim")
    except Exception:
        return None
    return None


def get_or_create_collection(
    name: str, schema: CollectionSchema, expected_dim: int
) -> Collection:
    """
    Return the named collection, creating it (+ index) if absent.
    If a collection with the same name exists but its embedding dimension
    doesn't match `expected_dim`, drop it and recreate (a vector field's
    dim cannot be altered in place). This makes the pipeline safe to run
    after the encoder has changed.
    """
    if utility.has_collection(name):
        current = _existing_dim(name)
        if current is not None and current != expected_dim:
            print(
                f"[Milvus] Collection '{name}' has dim={current}, "
                f"expected {expected_dim}. Dropping and recreating."
            )
            utility.drop_collection(name)

    if not utility.has_collection(name):
        col = Collection(name=name, schema=schema)
        col.create_index(field_name="embedding", index_params=_INDEX_PARAMS)
        print(f"[Milvus] Created collection '{name}'.")
    else:
        col = Collection(name)
    col.load()
    return col


def get_text_collection() -> Collection:
    return get_or_create_collection(
        config.MILVUS_TEXT_COLLECTION, _text_schema(), config.MILVUS_EMBEDDING_DIM
    )


def get_audio_collection() -> Collection:
    return get_or_create_collection(
        config.MILVUS_AUDIO_COLLECTION, _audio_schema(), config.MILVUS_AUDIO_DIM
    )


# ── Upsert helpers ───────────────────────────────────────────────────────────

def _truncate(values: List[str], max_bytes: int) -> List[str]:
    """
    Clip every string so its UTF-8 encoding fits in `max_bytes` bytes.
    Milvus VARCHAR `max_length` is enforced in BYTES (UTF-8), not characters,
    so multi-byte characters (accents, em-dashes, emoji) can overflow the
    limit even when the character count looks small. `errors="ignore"` on
    the decode drops any partial multi-byte char left at the boundary.
    The vector and the rest of the metadata still get persisted — we just
    lose the tail of one string field. The full original is always
    available in MinIO.
    """
    out: List[str] = []
    for s in values:
        s = s or ""
        encoded = s.encode("utf-8")
        if len(encoded) <= max_bytes:
            out.append(s)
        else:
            out.append(encoded[:max_bytes].decode("utf-8", errors="ignore"))
    return out


def upsert_text_embeddings(
    collection:  Collection,
    filenames:   List[str],
    embeddings:  List[List[float]],
    headlines:   List[str],
    categories:  List[str],
    authors:     List[str],
    pub_dates:   List[str],
    previews:    List[str],
    char_counts: List[int],
    word_counts: List[int],
) -> None:
    """Insert a batch of text embedding records into Milvus.
    Variable-length strings are truncated to fit the schema's max_length."""
    data = [
        _truncate(filenames,   512),
        embeddings,
        _truncate(headlines,   512),
        _truncate(categories,  128),
        _truncate(authors,     256),
        _truncate(pub_dates,    64),
        _truncate(previews,   1024),
        char_counts,
        word_counts,
    ]
    collection.insert(data)
    collection.flush()


def upsert_audio_embeddings(
    collection: Collection,
    filenames:    List[str],
    embeddings:   List[List[float]],
    durations:    List[float],
    sample_rates: List[int],
    channels:     List[int],
) -> None:
    """Insert a batch of audio embedding records into Milvus."""
    data = [
        _truncate(filenames, 512),
        embeddings,
        durations,
        sample_rates,
        channels,
    ]
    collection.insert(data)
    collection.flush()
