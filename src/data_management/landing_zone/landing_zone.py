from __future__ import annotations
from dataclasses import dataclass

from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error

from src.common.minio_client import get_minio_client
from src.common.progress_bar import ProgressBar
import src.common.global_variables as config


# Data class to represent a move operation
@dataclass
class ObjectMove:
    source: str
    destination: str


def classify_destination(object_name: str):
    """
    Return destination object path in persistent landing, or None if invalid.
    """

    suffix = object_name.lower().rsplit(".", 1)
    extension = f".{suffix[-1]}" if len(suffix) == 2 else ""
    
    if extension == ".csv":
        temporal_folder = "structured/"
    elif extension == ".json":
        temporal_folder = "semi_structured/"
    else:
        temporal_folder = "unstructured/audio/"

    if not temporal_folder:
        return None

    relative_path = object_name.removeprefix(config.LANDING_TEMPORAL_PATH)
    if not relative_path.startswith(temporal_folder):
        return None

    return f"{config.LANDING_PERSISTENT_PATH}{relative_path}"


def iter_objects_in_temporal_bucket(client: Minio):
    """
    Generator: iterate temporal objects and yield move operations
    """

    for obj in client.list_objects(config.LANDING_BUCKET, prefix=config.LANDING_TEMPORAL_PATH, recursive=True):
        name = obj.object_name

        # Skip invalid or directory entries
        if obj.is_dir or not name or name == config.LANDING_TEMPORAL_PATH:
            continue

        destination = classify_destination(name)
        if not destination:
            print(f"[WARN] Unknown type, skipping: {name}")
            continue
        yield ObjectMove(source=name, destination=destination)


def move_object(client: Minio, move: ObjectMove):
    """
    Copy the object to persistent folder and remove the temporal one
    """

    copy_source = CopySource(config.LANDING_BUCKET, move.source)
    client.copy_object(config.LANDING_BUCKET, move.destination, copy_source)
    client.remove_object(config.LANDING_BUCKET, move.source)


def main():
    """
    Main process: move all temporal objects to their persistent destinations
    """

    client = get_minio_client()
    objects_to_move = list(iter_objects_in_temporal_bucket(client))

    if not objects_to_move:
        print("[INFO] No objects found in temporal_landing to move.")
        return

    with ProgressBar(
        total=len(objects_to_move),
        description="Moving landing objects",
        unit="file",
        unit_scale=False,
        unit_divisor=1,
    ) as progress:
        for obj in objects_to_move:
            progress.set_description(f"Moving {obj.source}", refresh=True)
            move_object(client, obj)
            progress.update(1)

        progress.write(f"[OK] Objects moved: {len(objects_to_move)}")



if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print(f"MinIO error: {exc}")
