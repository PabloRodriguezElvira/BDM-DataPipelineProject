import hashlib
import io
import json
import wave
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Optional

from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error

from src.common.minio_client import get_minio_client
from src.common.progress_bar import ProgressBar
import src.common.global_variables as config

from src.data_management.landing_zone.structured_to_delta import process_csv_object


# Data class to represent an operation in landing zone
@dataclass
class ObjectMove:
    source: str
    destination: Optional[str]
    is_structured_csv: bool = False
    is_semi_structured_json: bool = False
    is_unstructured_text: bool = False
    is_unstructured_audio: bool = False
    metadata_destination: Optional[str] = None


def classify_destination(object_name: str) -> Optional[str]:
    """
    Return destination object path in persistent landing for every object in temporal landing folder.
    CSV files are excluded here because they are processed through Delta Lake.
    """
    relative_path = object_name.removeprefix(config.LANDING_TEMPORAL_PATH).strip("/")
    if not relative_path:
        return None

    normalized_relative_path = relative_path.replace("\\", "/")
    if "/" in normalized_relative_path:
        return None

    file_name = PurePosixPath(normalized_relative_path).name
    extension = PurePosixPath(normalized_relative_path).suffix.lower()

    if extension == ".json":
        # if the JSON starts with meta_unstructured is the metadata coming from streaming images
        if file_name.startswith("meta_unstructured_"):
            base_name = file_name.replace("meta_unstructured_report_", "")
            # We cut the last 12 characters in order to obtain the camera
            camera_id = base_name[:-12]
            # Send it to the corresponding camera metadata folder
            return f"{config.LANDING_PERSISTENT_PATH}unstructured/images/metadata/{camera_id}/{file_name}"
        # Other JSON goes to semi-structured
        else:
            return f"{config.LANDING_PERSISTENT_PATH}semi_structured/data/{file_name}"

    if extension == ".wav":
        return f"{config.LANDING_PERSISTENT_PATH}unstructured/audio/data/{file_name}"

    if extension == ".txt":
        return  f"{config.LANDING_PERSISTENT_PATH}unstructured/text/data/{file_name}"

    return None


def get_semi_structured_metadata_destination(object_name: str) -> str:
    """
    Return destination object path for semi-structured metadata JSON.
    """
    file_name = PurePosixPath(object_name).name
    return (
        f"{config.LANDING_PERSISTENT_PATH}semi_structured/metadata/"
        f"metadata_{file_name}"
    )


def get_unstructured_text_metadata_destination(object_name: str) -> str:
    """
    Return destination object path for unstructured text metadata JSON.
    """
    file_stem = PurePosixPath(object_name).stem
    return (
        f"{config.LANDING_PERSISTENT_PATH}unstructured/text/metadata/"
        f"metadata_{file_stem}.json"
    )


def get_unstructured_audio_metadata_destination(object_name: str) -> str:
    """
    Return destination object path for unstructured audio metadata JSON.
    """
    file_stem = PurePosixPath(object_name).stem
    return (
        f"{config.LANDING_PERSISTENT_PATH}unstructured/audio/metadata/"
        f"metadata_{file_stem}.json"
    )


def download_object_bytes(client: Minio, object_name: str) -> bytes:
    """
    Download an object from MinIO and return its content as bytes.
    """
    response = client.get_object(config.LANDING_BUCKET, object_name)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def upload_json_bytes(client: Minio, object_name: str, payload: bytes):
    """
    Upload raw JSON bytes to MinIO.
    """
    client.put_object(
        config.LANDING_BUCKET,
        object_name,
        io.BytesIO(payload),
        len(payload),
        content_type="application/json",
    )


def build_semi_structured_metadata(
    source_object: str,
    data_destination: str,
    metadata_destination: str,
    payload: dict,
    raw_bytes: bytes,
) -> dict:
    """
    Build technical and derived metadata for a semi-structured weather JSON.
    """
    source_metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    data_section = payload.get("data", {}) if isinstance(payload, dict) else {}
    properties = data_section.get("properties", {}) if isinstance(data_section, dict) else {}
    geometry = data_section.get("geometry", {}) if isinstance(data_section, dict) else {}
    periods = properties.get("periods", [])

    endpoint = source_metadata.get("endpoint", "")
    granularity = "hourly" if "hourly" in endpoint else "daily"

    valid_times = properties.get("validTimes")
    valid_from = None
    valid_duration = None
    if isinstance(valid_times, str) and "/" in valid_times:
        valid_from, valid_duration = valid_times.split("/", maxsplit=1)

    missing_fields = []
    for field_name, value in {
        "metadata": payload.get("metadata") if isinstance(payload, dict) else None,
        "data": payload.get("data") if isinstance(payload, dict) else None,
        "data.properties": properties if properties else None,
        "data.properties.periods": periods if isinstance(periods, list) else None,
    }.items():
        if value is None:
            missing_fields.append(field_name)

    return {
        "file_metadata": {
            "file_name": PurePosixPath(source_object).name,
            "source_object_path": source_object,
            "persistent_data_path": data_destination,
            "persistent_metadata_path": metadata_destination,
            "file_size_bytes": len(raw_bytes),
            "checksum_sha256": hashlib.sha256(raw_bytes).hexdigest(),
            "content_type": "application/json",
            "ingested_at_utc": datetime.now(UTC).isoformat(),
        },
        "source_metadata": source_metadata,
        "content_metadata": {
            "schema_type": "weather_forecast",
            "granularity": granularity,
            "generated_at": properties.get("generatedAt"),
            "update_time": properties.get("updateTime"),
            "valid_times": valid_times,
            "valid_from": valid_from,
            "valid_duration": valid_duration,
            "periods_count": len(periods) if isinstance(periods, list) else 0,
            "geometry_type": geometry.get("type") if isinstance(geometry, dict) else None,
            "units": properties.get("units"),
        },
        "quality_metadata": {
            "is_valid_json": True,
            "has_required_fields": not missing_fields,
            "missing_fields": missing_fields,
            "has_periods": isinstance(periods, list) and len(periods) > 0,
        },
    }


def build_unstructured_text_metadata(
    source_object: str,
    data_destination: str,
    metadata_destination: str,
    raw_bytes: bytes,
) -> dict:
    """
    Build technical and derived metadata for an unstructured text file.
    """
    try:
        decoded_text = raw_bytes.decode("utf-8")
        is_utf8_decodable = True
    except UnicodeDecodeError:
        decoded_text = raw_bytes.decode("utf-8", errors="replace")
        is_utf8_decodable = False

    all_lines = decoded_text.splitlines()
    non_empty_lines = [line.strip() for line in all_lines if line.strip()]

    headline = non_empty_lines[0] if len(non_empty_lines) > 0 else None
    category = non_empty_lines[1] if len(non_empty_lines) > 1 else None
    authors = non_empty_lines[2] if len(non_empty_lines) > 2 else None
    published_date = non_empty_lines[3] if len(non_empty_lines) > 3 else None
    short_description = non_empty_lines[4] if len(non_empty_lines) > 4 else None
    source_url = non_empty_lines[5] if len(non_empty_lines) > 5 else None

    words = decoded_text.split()
    missing_fields = []
    for field_name, value in {
        "headline": headline,
        "category": category,
        "authors": authors,
        "published_date": published_date,
        "short_description": short_description,
        "source_url": source_url,
    }.items():
        if not value:
            missing_fields.append(field_name)

    return {
        "file_metadata": {
            "file_name": PurePosixPath(source_object).name,
            "source_object_path": source_object,
            "persistent_data_path": data_destination,
            "persistent_metadata_path": metadata_destination,
            "file_size_bytes": len(raw_bytes),
            "checksum_sha256": hashlib.sha256(raw_bytes).hexdigest(),
            "content_type": "text/plain",
            "encoding": "utf-8",
            "ingested_at_utc": datetime.now(UTC).isoformat(),
        },
        "source_metadata": {
            "source_dataset": config.UNSTRUCTURED_TEXT_DATASET,
            "source_url": source_url,
            "record_id": PurePosixPath(source_object).stem,
        },
        "content_metadata": {
            "schema_type": "news_article_text",
            "headline": headline,
            "category": category,
            "authors": authors,
            "published_date": published_date,
            "line_count": len(all_lines),
            "non_empty_line_count": len(non_empty_lines),
            "word_count": len(words),
            "character_count": len(decoded_text),
            "headline_length_chars": len(headline) if headline else 0,
            "summary_length_chars": len(short_description) if short_description else 0,
            "has_source_url": bool(source_url),
        },
        "quality_metadata": {
            "is_utf8_decodable": is_utf8_decodable,
            "has_expected_article_fields": not missing_fields,
            "missing_fields": missing_fields,
            "has_minimum_expected_lines": len(non_empty_lines) >= 6,
            "is_empty_file": len(decoded_text.strip()) == 0,
        },
    }


def build_unstructured_audio_metadata(
    source_object: str,
    data_destination: str,
    metadata_destination: str,
    raw_bytes: bytes,
) -> dict:
    """
    Build simple technical metadata for an unstructured audio file.
    """
    sample_rate = None
    channels = None
    sample_width_bytes = None
    frame_count = None
    duration_seconds = None
    is_valid_wav = False

    try:
        with wave.open(io.BytesIO(raw_bytes), "rb") as wav_file:
            sample_rate = wav_file.getframerate()
            channels = wav_file.getnchannels()
            sample_width_bytes = wav_file.getsampwidth()
            frame_count = wav_file.getnframes()
            duration_seconds = round(frame_count / sample_rate, 3) if sample_rate else None
            is_valid_wav = True
    except wave.Error:
        pass

    return {
        "file_metadata": {
            "file_name": PurePosixPath(source_object).name,
            "source_object_path": source_object,
            "persistent_data_path": data_destination,
            "persistent_metadata_path": metadata_destination,
            "file_size_bytes": len(raw_bytes),
            "checksum_sha256": hashlib.sha256(raw_bytes).hexdigest(),
            "content_type": "audio/wav",
            "ingested_at_utc": datetime.now(UTC).isoformat(),
        },
        "source_metadata": {
            "source_dataset": config.UNSTRUCTURED_AUDIO_DATASET,
            "record_id": PurePosixPath(source_object).stem,
        },
        "content_metadata": {
            "schema_type": "audio_recording",
            "audio_format": "wav",
            "duration_seconds": duration_seconds,
            "sample_rate_hz": sample_rate,
            "channels": channels,
            "sample_width_bytes": sample_width_bytes,
            "frame_count": frame_count,
        },
        "quality_metadata": {
            "is_valid_wav": is_valid_wav,
            "is_empty_file": len(raw_bytes) == 0,
        },
    }


def process_semi_structured_object(client: Minio, move: ObjectMove):
    """
    Persist the original semi-structured JSON under data/ and generate a metadata JSON under metadata/.
    """
    if not move.destination or not move.metadata_destination:
        raise ValueError(f"Destination paths are missing for object: {move.source}")

    raw_bytes = download_object_bytes(client, move.source)
    payload = json.loads(raw_bytes.decode("utf-8"))

    metadata_payload = build_semi_structured_metadata(
        source_object=move.source,
        data_destination=move.destination,
        metadata_destination=move.metadata_destination,
        payload=payload,
        raw_bytes=raw_bytes,
    )

    uploaded_objects = []
    try:
        upload_json_bytes(client, move.destination, raw_bytes)
        uploaded_objects.append(move.destination)

        metadata_bytes = json.dumps(metadata_payload, indent=2).encode("utf-8")
        upload_json_bytes(client, move.metadata_destination, metadata_bytes)
        uploaded_objects.append(move.metadata_destination)

        client.remove_object(config.LANDING_BUCKET, move.source)
    except Exception:
        for uploaded_object in uploaded_objects:
            client.remove_object(config.LANDING_BUCKET, uploaded_object)
        raise


def process_unstructured_text_object(client: Minio, move: ObjectMove):
    """
    Persist the original text file under data/ and generate a metadata JSON under metadata/.
    """
    if not move.destination or not move.metadata_destination:
        raise ValueError(f"Destination paths are missing for object: {move.source}")

    raw_bytes = download_object_bytes(client, move.source)
    metadata_payload = build_unstructured_text_metadata(
        source_object=move.source,
        data_destination=move.destination,
        metadata_destination=move.metadata_destination,
        raw_bytes=raw_bytes,
    )

    uploaded_objects = []
    try:
        client.put_object(
            config.LANDING_BUCKET,
            move.destination,
            io.BytesIO(raw_bytes),
            len(raw_bytes),
            content_type="text/plain",
        )
        uploaded_objects.append(move.destination)

        metadata_bytes = json.dumps(metadata_payload, indent=2).encode("utf-8")
        upload_json_bytes(client, move.metadata_destination, metadata_bytes)
        uploaded_objects.append(move.metadata_destination)

        client.remove_object(config.LANDING_BUCKET, move.source)
    except Exception:
        for uploaded_object in uploaded_objects:
            client.remove_object(config.LANDING_BUCKET, uploaded_object)
        raise


def process_unstructured_audio_object(client: Minio, move: ObjectMove):
    """
    Persist the original audio file under data/ and generate a metadata JSON under metadata/.
    """
    if not move.destination or not move.metadata_destination:
        raise ValueError(f"Destination paths are missing for object: {move.source}")

    raw_bytes = download_object_bytes(client, move.source)
    metadata_payload = build_unstructured_audio_metadata(
        source_object=move.source,
        data_destination=move.destination,
        metadata_destination=move.metadata_destination,
        raw_bytes=raw_bytes,
    )

    uploaded_objects = []
    try:
        client.put_object(
            config.LANDING_BUCKET,
            move.destination,
            io.BytesIO(raw_bytes),
            len(raw_bytes),
            content_type="audio/wav",
        )
        uploaded_objects.append(move.destination)

        metadata_bytes = json.dumps(metadata_payload, indent=2).encode("utf-8")
        upload_json_bytes(client, move.metadata_destination, metadata_bytes)
        uploaded_objects.append(move.metadata_destination)

        client.remove_object(config.LANDING_BUCKET, move.source)
    except Exception:
        for uploaded_object in uploaded_objects:
            client.remove_object(config.LANDING_BUCKET, uploaded_object)
        raise


def iter_objects_in_temporal_bucket(client: Minio):
    """
    Iterate temporal objects and yield operations.
    CSV files are marked to be processed into Delta.
    Other supported files are moved to persistent landing.
    Semi-structured JSONs, unstructured text files and audio files also generate metadata JSON.
    """
    for obj in client.list_objects(
        config.LANDING_BUCKET,
        prefix=config.LANDING_TEMPORAL_PATH,
        recursive=True,
    ):
        name = obj.object_name

        if obj.is_dir or not name or name == config.LANDING_TEMPORAL_PATH:
            continue

        if name.lower().endswith(".csv"):
            yield ObjectMove(source=name, destination=None, is_structured_csv=True)
            continue

        destination = classify_destination(name)
        if not destination:
            print(f"[WARN] Unknown type, skipping: {name}")
            continue

        is_semi_structured_json = (
            name.lower().endswith(".json")
            and not PurePosixPath(name).name.startswith("meta_unstructured_")
        )
        is_unstructured_text = name.lower().endswith(".txt")
        is_unstructured_audio = name.lower().endswith(".wav")

        yield ObjectMove(
            source=name,
            destination=destination,
            is_structured_csv=False,
            is_semi_structured_json=is_semi_structured_json,
            is_unstructured_text=is_unstructured_text,
            is_unstructured_audio=is_unstructured_audio,
            metadata_destination=(
                get_semi_structured_metadata_destination(name)
                if is_semi_structured_json
                else get_unstructured_text_metadata_destination(name)
                if is_unstructured_text
                else get_unstructured_audio_metadata_destination(name)
                if is_unstructured_audio
                else None
            ),
        )


def move_object(client: Minio, move: ObjectMove):
    """
    Copy the object to persistent folder and remove the temporal one.
    """

    if not move.destination:
        raise ValueError(f"Destination path is missing for object: {move.source}")

    copy_source = CopySource(config.LANDING_BUCKET, move.source)
    client.copy_object(config.LANDING_BUCKET, move.destination, copy_source)
    client.remove_object(config.LANDING_BUCKET, move.source)


def main():
    """
    Main process:
    - structured CSVs -> process into Delta Lake
    - other supported files -> move to persistent landing
    """

    client = get_minio_client()
    operations = list(iter_objects_in_temporal_bucket(client))

    if not operations:
        print("[INFO] No objects found in temporal_landing to process.")
        return

    with ProgressBar(
        total=len(operations),
        description="Processing landing objects",
        unit="files",
        unit_scale=False,
        unit_divisor=1,
    ) as progress:
        processed = 0
        failed = 0

        for obj in operations:
            progress.set_description(f"Processing {obj.source}", refresh=True)

            try:
                if obj.is_structured_csv:
                    process_csv_object(client, obj.source)
                elif obj.is_semi_structured_json:
                    process_semi_structured_object(client, obj)
                elif obj.is_unstructured_text:
                    process_unstructured_text_object(client, obj)
                elif obj.is_unstructured_audio:
                    process_unstructured_audio_object(client, obj)
                else:
                    move_object(client, obj)

                progress.update(1)
                processed += 1
                progress.write(f"[OK] {obj.source}")

            except Exception as exc:
                failed += 1
                progress.write(f"[ERROR] Failed processing {obj.source}: {exc}")

        progress.write(
            f"[OK] Objects seen={len(operations)}, processed={processed}, failed={failed}"
        )


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print(f"MinIO error: {exc}")
