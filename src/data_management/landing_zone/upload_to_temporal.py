"""
Use of the program:
Uploads local datasets to temporal landing path in MinIO.

Examples of usage:

- Upload all supported datasets:
  python -m src.data_management.landing_zone.upload_to_temporal

- Upload only one dataset:
  python -m src.data_management.landing_zone.upload_to_temporal --only structured

- Force overwrite of existing objects:
  python -m src.data_management.landing_zone.upload_to_temporal --overwrite
"""

from __future__ import annotations

import argparse
from pathlib import Path

from minio import Minio
from minio.error import S3Error

import src.common.global_variables as config
from src.common.minio_client import get_minio_client
from src.common.progress_bar import ProgressBar


DATASET_CONFIG = {
    "structured": {
        "local_path": Path("downloaded_data/structured"),
        "temporal_prefix": "structured",
    },
    "semi_structured": {
        "local_path": Path("downloaded_data/semi_structured"),
        "temporal_prefix": "semi_structured",
    },
    "unstructured_audio": {
        "local_path": Path("downloaded_data/unstructured/audio"),
        "temporal_prefix": "unstructured/audio",
    },
}


def _ensure_bucket_exists(client: Minio, bucket: str):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def _iter_files(base_dir: Path):
    return sorted(path for path in base_dir.rglob("*") if path.is_file())


def _object_exists(client: Minio, bucket: str, object_name: str) -> bool:
    try:
        client.stat_object(bucket, object_name)
        return True
    except S3Error as exc:
        if exc.code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
            return False
        raise


def upload_dataset_to_temporal(
    client: Minio,
    dataset_name: str,
    temporal_prefix: str,
    local_base_dir: Path,
    overwrite: bool
):
    if not local_base_dir.exists():
        print(f"[WARN] Path not found, skipping: {local_base_dir}")
        return

    files = _iter_files(local_base_dir)
    if not files:
        print(f"[WARN] No files found in: {local_base_dir}")
        return

    uploaded = 0
    skipped = 0
    temporal_root = config.LANDING_TEMPORAL_PATH.rstrip("/")
    dataset_prefix = temporal_prefix.strip("/")

    with ProgressBar(
        total=len(files),
        description=f"Uploading {dataset_name}",
        unit="files",
        unit_scale=False,
    ) as progress:
        for file_path in files:
            relative_path = file_path.relative_to(local_base_dir).as_posix()
            object_name = f"{temporal_root}/{dataset_prefix}/{relative_path}"

            if not overwrite and _object_exists(client, config.LANDING_BUCKET, object_name):
                skipped += 1
                progress.update(1)
                progress.write(f"[SKIP] {object_name} already exists.")
                continue

            client.fput_object(config.LANDING_BUCKET, object_name, str(file_path))
            uploaded += 1
            progress.update(1)
            progress.write(f"[OK] {object_name}")

    print(f"[OK] {dataset_name}: uploaded={uploaded}, skipped={skipped}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Upload local datasets to MinIO temporal landing."
    )
    parser.add_argument(
        "--only",
        choices=["all", "structured", "semi_structured", "unstructured_audio"],
        default="all",
        help="Upload only one dataset type. Default uploads all.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="If set, existing objects in MinIO are replaced.",
    )
    return parser.parse_args()


def main(only: str, overwrite: bool):
    client = get_minio_client()
    _ensure_bucket_exists(client, config.LANDING_BUCKET)

    selected = (
        DATASET_CONFIG.items() if only == "all" else [(only, DATASET_CONFIG[only])]
    )

    for data_type, dataset_config in selected:
        upload_dataset_to_temporal(
            client=client,
            dataset_name=data_type,
            temporal_prefix=dataset_config["temporal_prefix"],
            local_base_dir=dataset_config["local_path"],
            overwrite=overwrite
        )


if __name__ == "__main__":
    cli_args = parse_args()
    main(
        only=cli_args.only,
        overwrite=cli_args.overwrite,
    )
