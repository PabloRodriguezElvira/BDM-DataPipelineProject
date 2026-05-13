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
    """Connect to Milvus (idempotent)."""
    try:
        connections.connect(
            alias="default",
            host=config.MILVUS_HOST,
            port=config.MILVUS_PORT,
        )
    except Exception as exc:
        raise RuntimeError(f"Cannot connect to Milvus: {exc}") from exc


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

def get_or_create_collection(name: str, schema: CollectionSchema) -> Collection:
    """Return the named collection, creating it (+ index) if absent."""
    if not utility.has_collection(name):
        col = Collection(name=name, schema=schema)
        col.create_index(field_name="embedding", index_params=_INDEX_PARAMS)
        print(f"[Milvus] Created collection '{name}'.")
    else:
        col = Collection(name)
    col.load()
    return col


def get_text_collection() -> Collection:
    return get_or_create_collection(config.MILVUS_TEXT_COLLECTION, _text_schema())


def get_audio_collection() -> Collection:
    return get_or_create_collection(config.MILVUS_AUDIO_COLLECTION, _audio_schema())


# ── Upsert helpers ───────────────────────────────────────────────────────────

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
    """Insert a batch of text embedding records into Milvus."""
    data = [
        filenames,
        embeddings,
        headlines,
        categories,
        authors,
        pub_dates,
        previews,
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
        filenames,
        embeddings,
        durations,
        sample_rates,
        channels,
    ]
    collection.insert(data)
    collection.flush()
