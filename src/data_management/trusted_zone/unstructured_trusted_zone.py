"""
Trusted Zone pipeline for unstructured data (audio and text).

Reads audio (.wav) and text (.txt) files from the Landing Zone persistent storage
in MinIO, applies information-preserving cleaning transformations using Spark,
and writes the results to the Trusted Zone bucket in MinIO.

Transformations applied
-----------------------
Text files:
  - Detect and skip corrupted / unreadable files
  - Standardize encoding to UTF-8
  - Strip leading/trailing whitespace per line
  - Collapse multiple consecutive blank lines into one
  - Normalize internal whitespace (tabs -> spaces, multiple spaces -> single space)
  - Convert content to lowercase

Audio files (.wav):
  - Detect and skip corrupted files (malformed RIFF header)
  - Skip clips shorter than TRUSTED_AUDIO_MIN_DURATION_SECONDS
  - Convert to 16-bit PCM if needed
  - Mix down to mono if multi-channel
  - Resample to TRUSTED_AUDIO_TARGET_SAMPLE_RATE if needed
  - Re-encode as standard WAV

Accepted files are written to trusted-zone/unstructured/{audio|text}/data/.
Skipped files are kept in .../skipped/ for traceability.

"""

import io
import audioop
import re
import wave

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, udf
from pyspark.sql.types import (
    BinaryType, StringType, StructField, StructType,
)

import json
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
from deltalake.writer import write_deltalake

import src.common.global_variables as config
from src.common.minio_manager import list_objects, read_object_bytes, write_object_bytes
from src.data_management.landing_zone.process_metadata_to_delta import (
    _flatten_metadata_payload,
    _cast_null_columns,
)


# ── Metadata helpers (inline) ─────────────────────────────────────────────────

def _delta_storage_opts() -> dict:
    return {
        "AWS_ACCESS_KEY_ID":          config.MINIO_ROOT_USER,
        "AWS_SECRET_ACCESS_KEY":      config.MINIO_ROOT_PASSWORD,
        "AWS_ENDPOINT_URL":           config.MINIO_ENDPOINT_URL,
        "AWS_REGION":                 "us-east-1",
        "AWS_ALLOW_HTTP":             "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def _append_to_delta(uri: str, flat_row: dict) -> None:
    """Append a single flattened metadata row to a Delta Lake table."""
    df = pd.DataFrame([flat_row]).convert_dtypes()
    arrow_table = _cast_null_columns(pa.Table.from_pandas(df, preserve_index=False))
    try:
        write_deltalake(
            uri, arrow_table, mode="append", schema_mode="merge",
            engine="rust", storage_options=_delta_storage_opts(),
        )
    except TypeError:
        write_deltalake(uri, arrow_table, mode="append", storage_options=_delta_storage_opts())


def _write_trusted_metadata(
    filename: str,
    data_type: str,
    was_accepted: bool,
    skip_reason: str | None,
    transformations: list[str],
) -> None:
    """
    Read the per-file landing metadata JSON from MinIO, enrich it with
    Trusted Zone processing information, then:
      1. Write the enriched JSON back to the Trusted Zone MinIO bucket.
      2. Append a flattened row to the Trusted Zone Delta table.

    Paths used:
      Landing JSON  : LANDING_BUCKET / LANDING_{TEXT|AUDIO}_METADATA_PREFIX / metadata_{stem}.json
      Trusted JSON  : TRUSTED_BUCKET / TRUSTED_{TEXT|AUDIO}_METADATA_PREFIX / metadata_{stem}.json
      Trusted Delta : TRUSTED_{TEXT|AUDIO}_DELTA_URI
    """
    stem = filename.rsplit(".", 1)[0]
    meta_filename = f"metadata_{stem}.json"

    if data_type == "text":
        landing_meta_prefix  = config.LANDING_TEXT_METADATA_PREFIX
        trusted_meta_prefix  = config.TRUSTED_TEXT_METADATA_PREFIX
        trusted_delta_uri    = config.TRUSTED_TEXT_DELTA_URI
        trusted_data_prefix  = config.TRUSTED_TEXT_PREFIX
    else:
        landing_meta_prefix  = config.LANDING_AUDIO_METADATA_PREFIX
        trusted_meta_prefix  = config.TRUSTED_AUDIO_METADATA_PREFIX
        trusted_delta_uri    = config.TRUSTED_AUDIO_DELTA_URI
        trusted_data_prefix  = config.TRUSTED_AUDIO_PREFIX

    # Read landing metadata JSON (best-effort; empty dict if not found)
    raw_meta = read_object_bytes(config.LANDING_BUCKET, landing_meta_prefix + meta_filename)
    landing_meta: dict = json.loads(raw_meta.decode("utf-8")) if raw_meta else {}

    # Enrich with Trusted Zone processing info
    landing_meta["trusted_zone"] = {
        "processed_at_utc":      datetime.now(timezone.utc).isoformat(),
        "was_accepted":          was_accepted,
        "skip_reason":           skip_reason,
        "transformations_applied": transformations,
        "trusted_data_path":     f"{trusted_data_prefix}{filename}" if was_accepted else None,
        "trusted_metadata_path": f"{trusted_meta_prefix}{meta_filename}",
    }

    # 1) Write enriched JSON to Trusted Zone MinIO
    meta_bytes = json.dumps(landing_meta, indent=2, ensure_ascii=False).encode("utf-8")
    write_object_bytes(
        config.TRUSTED_BUCKET,
        trusted_meta_prefix + meta_filename,
        meta_bytes,
        "application/json",
    )

    # 2) Append to Trusted Zone Delta table
    _append_to_delta(trusted_delta_uri, _flatten_metadata_payload(landing_meta))


def clean_text(raw: bytes, filename: str) -> tuple[bytes | None, str | None]:
    """
    Apply information-preserving cleaning to a raw text file.
    Returns (cleaned_bytes, skip_reason); skip_reason is None when accepted.
    """
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = raw.decode("latin-1")
        except Exception:
            return None, "unreadable_encoding"

    if not text.strip():
        return None, "empty_file"

    text = text.lower()
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    lines = [line.strip() for line in text.split("\n")]

    cleaned_lines = []
    prev_blank = False
    for line in lines:
        is_blank = (line == "")
        if is_blank and prev_blank:
            continue
        cleaned_lines.append(line)
        prev_blank = is_blank

    final_lines = [re.sub(r"[ \t]+", " ", ln) for ln in cleaned_lines]
    result = "\n".join(final_lines).strip() + "\n"
    return result.encode("utf-8"), None


def clean_audio(raw: bytes, filename: str) -> tuple[bytes | None, str | None]:
    """
    Apply information-preserving cleaning to a raw WAV file.
    Returns (cleaned_bytes, skip_reason); skip_reason is None when accepted.
    """
    try:
        with wave.open(io.BytesIO(raw)) as wf:
            params = wf.getparams()
            frames = wf.readframes(wf.getnframes())
    except Exception:
        return None, "corrupted_wav"

    nchannels = params.nchannels
    sampwidth = params.sampwidth
    framerate = params.framerate
    nframes   = params.nframes

    if framerate > 0 and (nframes / framerate) < config.TRUSTED_AUDIO_MIN_DURATION_SECONDS:
        return None, f"too_short_{nframes / framerate:.3f}s"

    if sampwidth != config.TRUSTED_AUDIO_PCM_SAMPLE_WIDTH:
        frames = audioop.lin2lin(frames, sampwidth, config.TRUSTED_AUDIO_PCM_SAMPLE_WIDTH)
        sampwidth = config.TRUSTED_AUDIO_PCM_SAMPLE_WIDTH

    if nchannels > 1:
        frames = audioop.tomono(frames, sampwidth, 0.5, 0.5)
        nchannels = 1

    if framerate != config.TRUSTED_AUDIO_TARGET_SAMPLE_RATE:
        frames, _ = audioop.ratecv(
            frames, sampwidth, nchannels,
            framerate, config.TRUSTED_AUDIO_TARGET_SAMPLE_RATE, None,
        )
        framerate = config.TRUSTED_AUDIO_TARGET_SAMPLE_RATE

    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(nchannels)
        wf.setsampwidth(sampwidth)
        wf.setframerate(framerate)
        wf.writeframes(frames)
    return buf.getvalue(), None


def process_text_files(spark: SparkSession):
    print("[TEXT] Listing Landing Zone objects...")
    keys = list_objects(config.LANDING_BUCKET, config.TRUSTED_LANDING_TEXT_PREFIX)
    if not keys:
        print("[TEXT] No text files found.")
        return

    print(f"[TEXT] Found {len(keys)} file(s).")
    rows = [
        Row(key=key, filename=key.split("/")[-1], raw=raw)
        for key in keys
        if (raw := read_object_bytes(config.LANDING_BUCKET, key)) is not None
    ]

    schema = StructType([
        StructField("key",      StringType(), False),
        StructField("filename", StringType(), False),
        StructField("raw",      BinaryType(), False),
    ])
    result_schema = StructType([
        StructField("cleaned",     BinaryType(), True),
        StructField("skip_reason", StringType(), True),
    ])

    @udf(result_schema)
    def clean_text_udf(raw, filename):
        cleaned, reason = clean_text(raw, filename)
        return Row(cleaned=cleaned, skip_reason=reason)

    df = (
        spark.createDataFrame(rows, schema=schema)
        .withColumn("result",      clean_text_udf(col("raw"), col("filename")))
        .withColumn("cleaned",     col("result.cleaned"))
        .withColumn("skip_reason", col("result.skip_reason"))
        .drop("result")
    )

    accepted = df.filter(col("skip_reason").isNull()).collect()
    skipped  = df.filter(col("skip_reason").isNotNull()).collect()
    print(f"[TEXT] Accepted: {len(accepted)} | Skipped: {len(skipped)}")

    for row in accepted:
        write_object_bytes(
            config.TRUSTED_BUCKET,
            config.TRUSTED_TEXT_PREFIX + row["filename"],
            row["cleaned"],
            "text/plain; charset=utf-8",
        )
    for row in skipped:
        write_object_bytes(
            config.TRUSTED_BUCKET,
            config.TRUSTED_TEXT_SKIPPED_PREFIX + row["filename"],
            row["raw"],
            "text/plain",
        )
        print(f"[TEXT][SKIP] {row['filename']} — {row['skip_reason']}")

    # ── Write enriched metadata for each file to Trusted Zone (JSON + Delta) ─
    _TEXT_TRANSFORMATIONS = [
        "encoding_standardization_utf8",
        "lowercase_normalization",
        "whitespace_normalization",
        "consecutive_blank_line_collapse",
        "empty_file_removal",
    ]
    for row in accepted:
        _write_trusted_metadata(
            filename=row["filename"], data_type="text",
            was_accepted=True, skip_reason=None,
            transformations=_TEXT_TRANSFORMATIONS,
        )
    for row in skipped:
        _write_trusted_metadata(
            filename=row["filename"], data_type="text",
            was_accepted=False, skip_reason=row["skip_reason"],
            transformations=[],
        )
    print(f"[TEXT] Metadata written for {len(accepted) + len(skipped)} file(s).")
    print("[TEXT] Done.")


def process_audio_files(spark: SparkSession):
    print("[AUDIO] Listing Landing Zone objects...")
    keys = list_objects(config.LANDING_BUCKET, config.TRUSTED_LANDING_AUDIO_PREFIX)
    if not keys:
        print("[AUDIO] No audio files found.")
        return

    print(f"[AUDIO] Found {len(keys)} file(s).")
    rows = [
        Row(key=key, filename=key.split("/")[-1], raw=raw)
        for key in keys
        if (raw := read_object_bytes(config.LANDING_BUCKET, key)) is not None
    ]

    schema = StructType([
        StructField("key",      StringType(), False),
        StructField("filename", StringType(), False),
        StructField("raw",      BinaryType(), False),
    ])
    result_schema = StructType([
        StructField("cleaned",     BinaryType(), True),
        StructField("skip_reason", StringType(), True),
    ])

    @udf(result_schema)
    def clean_audio_udf(raw, filename):
        cleaned, reason = clean_audio(raw, filename)
        return Row(cleaned=cleaned, skip_reason=reason)

    df = (
        spark.createDataFrame(rows, schema=schema)
        .withColumn("result",      clean_audio_udf(col("raw"), col("filename")))
        .withColumn("cleaned",     col("result.cleaned"))
        .withColumn("skip_reason", col("result.skip_reason"))
        .drop("result")
    )

    accepted = df.filter(col("skip_reason").isNull()).collect()
    skipped  = df.filter(col("skip_reason").isNotNull()).collect()
    print(f"[AUDIO] Accepted: {len(accepted)} | Skipped: {len(skipped)}")

    for row in accepted:
        write_object_bytes(
            config.TRUSTED_BUCKET,
            config.TRUSTED_AUDIO_PREFIX + row["filename"],
            row["cleaned"],
            "audio/wav",
        )
    for row in skipped:
        write_object_bytes(
            config.TRUSTED_BUCKET,
            config.TRUSTED_AUDIO_SKIPPED_PREFIX + row["filename"],
            row["raw"],
            "audio/wav",
        )
        print(f"[AUDIO][SKIP] {row['filename']} — {row['skip_reason']}")

    # ── Write enriched metadata for each file to Trusted Zone (JSON + Delta) ─
    _AUDIO_TRANSFORMATIONS = [
        "corrupted_file_removal",
        "mono_mixdown",
        f"resample_to_{config.TRUSTED_AUDIO_TARGET_SAMPLE_RATE}hz",
        "pcm_16bit_normalization",
        f"min_duration_{config.TRUSTED_AUDIO_MIN_DURATION_SECONDS}s_enforcement",
    ]
    for row in accepted:
        _write_trusted_metadata(
            filename=row["filename"], data_type="audio",
            was_accepted=True, skip_reason=None,
            transformations=_AUDIO_TRANSFORMATIONS,
        )
    for row in skipped:
        _write_trusted_metadata(
            filename=row["filename"], data_type="audio",
            was_accepted=False, skip_reason=row["skip_reason"],
            transformations=[],
        )
    print(f"[AUDIO] Metadata written for {len(accepted) + len(skipped)} file(s).")
    print("[AUDIO] Done.")


def main():
    spark = (
        SparkSession.builder
        .appName("TrustedZone-Unstructured")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    try:
        process_text_files(spark)
        process_audio_files(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
