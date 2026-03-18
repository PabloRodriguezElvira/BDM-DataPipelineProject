from dataclasses import dataclass
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
    Return destination object path in persistent landing, or None if invalid.
    CSV files are excluded here because they are processed through Delta Lake.
    """

    suffix = object_name.lower().rsplit(".", 1)
    extension = f".{suffix[-1]}" if len(suffix) == 2 else ""

    if extension == ".json":
        temporal_folder = "semi_structured/"
    else:
        temporal_folder = "unstructured/audio/"

    relative_path = object_name.removeprefix(config.LANDING_TEMPORAL_PATH)
    if not relative_path.startswith(temporal_folder):
        return None

    return f"{config.LANDING_PERSISTENT_PATH}{relative_path}"


def iter_objects_in_temporal_bucket(client: Minio):
    """
    Generator: iterate temporal objects and yield operations.
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
    objects_to_process = list(iter_objects_in_temporal_bucket(client))

    if not objects_to_process:
        print("[INFO] No objects found in temporal_landing to process.")
        return

    with ProgressBar(
        total=len(objects_to_process),
        description="Processing landing objects",
        unit="file",
        unit_scale=False,
        unit_divisor=1,
    ) as progress:
        for obj in objects_to_process:
            progress.set_description(f"Processing {obj.source}", refresh=True)

            try:
                if obj.is_structured_csv:
                    process_csv_object(client, obj.source)
                else:
                    move_object(client, obj)

                progress.update(1)

            except Exception as exc:
                progress.write(f"[ERROR] Failed processing {obj.source}: {exc}")

        progress.write(f"[OK] Objects processed: {len(objects_to_process)}")


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print(f"MinIO error: {exc}")