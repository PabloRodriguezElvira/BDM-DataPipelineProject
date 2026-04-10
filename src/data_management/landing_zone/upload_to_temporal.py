"""
Use of the program:
Uploads local datasets to temporal landing path in MinIO.

Examples of usage:

- Upload all supported datasets:
  python -m src.data_management.landing_zone.upload_to_temporal

- Upload only one dataset:
  python -m src.data_management.landing_zone.upload_to_temporal --only structured

- Upload at most 10 files per dataset:
  python -m src.data_management.landing_zone.upload_to_temporal --max-files 10

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
    "structured": Path("downloaded_data/structured"),
    "semi_structured": Path("downloaded_data/semi_structured"),
    "unstructured_audio": Path("downloaded_data/unstructured/audio"),
    "unstructured_text": Path("downloaded_data/unstructured/text")
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
    local_base_dir: Path,
    overwrite: bool,
    max_files: int | None,
):
    if not local_base_dir.exists():
        print(f"[WARN] Path not found, skipping: {local_base_dir}")
        return

    files = _iter_files(local_base_dir)
    if max_files is not None:
        files = files[:max_files]

    if not files:
        print(f"[WARN] No files found in: {local_base_dir}")
        return

    uploaded = 0
    skipped = 0
    temporal_root = config.LANDING_TEMPORAL_PATH.rstrip("/")

    with ProgressBar(
        total=len(files),
        description=f"Uploading {dataset_name}",
        unit="files",
        unit_scale=False,
    ) as progress:
        for file_path in files:
            object_name = f"{temporal_root}/{file_path.name}"

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
        choices=["all", "structured", "semi_structured", "unstructured_audio", "unstructured_text"],
        default="all",
        help="Upload only one dataset type. Default uploads all.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Maximum number of files to upload per dataset type in this run.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="If set, existing objects in MinIO are replaced.",
    )
    args = parser.parse_args()

    if args.max_files is not None and args.max_files <= 0:
        parser.error("--max-files must be a positive integer.")

    return args


def main(only: str, overwrite: bool, max_files: int | None):
    client = get_minio_client()
    _ensure_bucket_exists(client, config.LANDING_BUCKET)

    selected = (
        DATASET_CONFIG.items() if only == "all" else [(only, DATASET_CONFIG[only])]
    )

    for data_type, local_path in selected:
        upload_dataset_to_temporal(
            client=client,
            dataset_name=data_type,
            local_base_dir=local_path,
            overwrite=overwrite,
            max_files=max_files,
        )


if __name__ == "__main__":
    cli_args = parse_args()
    main(
        only=cli_args.only,
        overwrite=cli_args.overwrite,
        max_files=cli_args.max_files,
    )
