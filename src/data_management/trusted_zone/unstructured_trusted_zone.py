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

import src.common.global_variables as config
from src.common.minio_manager import list_objects, read_object_bytes, write_object_bytes


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

    print("[AUDIO] Done.")


def main():
    spark = SparkSession.builder.appName("TrustedZone-Unstructured").master("local[*]").getOrCreate()
    try:
        process_text_files(spark)
        process_audio_files(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
