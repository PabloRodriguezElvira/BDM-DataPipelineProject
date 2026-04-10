from dataclasses import dataclass
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
            # Send it to the corresponding camera 
            return f"{config.LANDING_PERSISTENT_PATH}unstructured/images/{camera_id}/{file_name}"
        # Other JSON goes to semi-structured
        else:
            return f"{config.LANDING_PERSISTENT_PATH}semi_structured/{file_name}"

    if extension == ".wav":
        return f"{config.LANDING_PERSISTENT_PATH}unstructured/audio/{file_name}"

    if extension == ".txt":
        return  f"{config.LANDING_PERSISTENT_PATH}unstructured/text/{file_name}"

    return None


def iter_objects_in_temporal_bucket(client: Minio):
    """
    Iterate temporal objects and yield operations.
    CSV files are marked to be processed into Delta.
    Other supported files are moved to persistent landing.
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

        yield ObjectMove(source=name, destination=destination, is_structured_csv=False)


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
