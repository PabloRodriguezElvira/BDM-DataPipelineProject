"""
Use of the program:
Downloads unstructured 911 audio files from KaggleHub and stores them locally.

Examples of usage:

- Download all files from the dataset:
  python -m src.data_management.data_ingestion.unstructured_data_audio

- Limit how many files are downloaded:
  python -m src.data_management.data_ingestion.unstructured_data_audio --max-files 50

- Overwrite existing local files:
  python -m src.data_management.data_ingestion.unstructured_data_audio --overwrite
"""

import argparse
import os
import shutil
from pathlib import Path
from typing import Optional

from src.common.load_env import load_env_file
from src.common.progress_bar import ProgressBar

DATASET = "louisteitelbaum/911-recordings-first-6-seconds"
OUT_DIR = Path("downloaded_data/unstructured_data/audio")
AUDIO_EXTENSIONS = {".wav", ".mp3", ".flac", ".ogg", ".m4a", ".aac"}


def _configure_kagglehub_token():
    load_env_file(".env")

    token = os.getenv("KAGGLE_API_TOKEN") or os.getenv("kaggle_api_token") or ""
    os.environ["KAGGLE_API_TOKEN"] = token


def _download_dataset_to_cache(overwrite: bool) -> Path:
    _configure_kagglehub_token()

    import kagglehub

    dataset_path = kagglehub.dataset_download(
        DATASET,
        force_download=overwrite,
    )

    return Path(dataset_path)


def download_audio_from_kaggle(
    max_files: Optional[int],
    overwrite: bool,
):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    dataset_path = _download_dataset_to_cache(overwrite=overwrite)

    all_files = sorted([f for f in dataset_path.rglob("*") if f.is_file()])
    audio_files = [f for f in all_files if f.suffix.lower() in AUDIO_EXTENSIONS]
    selected_files = audio_files[:max_files] if max_files is not None else audio_files

    total_files = len(selected_files)
    copied = 0
    with ProgressBar(
        total=total_files,
        description="Copying KaggleHub audio",
        unit="files",
        unit_scale=False,
    ) as progress:
        for source_path in selected_files:
            output_path = OUT_DIR / source_path.name
            output_path.parent.mkdir(parents=True, exist_ok=True)

            if output_path.exists() and not overwrite:
                progress.update(1)
                progress.write(f"[SKIP] {output_path} already exists.")
                continue

            shutil.copy2(source_path, output_path)
            copied += 1
            progress.write(f"[OK] {output_path}")
            progress.update(1)

    print(f"[OK] Copied {copied} audio files in {OUT_DIR}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download unstructured audio files from Kaggle."
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Maximum number of files to download in this run.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="If set, existing local files are replaced.",
    )
    args = parser.parse_args()

    if args.max_files is not None and args.max_files <= 0:
        parser.error("--max-files must be a positive integer.")

    return args


if __name__ == "__main__":
    cli_args = parse_args()
    download_audio_from_kaggle(
        max_files=cli_args.max_files,
        overwrite=cli_args.overwrite,
    )
