"""
Use of the program:
Downloads unstructured text data from KaggleHub and stores it locally.

Examples of usage:

- Download and expand the full dataset into .txt files:
  python -m src.data_management.data_ingestion.unstructured_data_text

- Limit how many articles are converted:
  python -m src.data_management.data_ingestion.unstructured_data_text --max-files 5

- Overwrite existing local .txt files:
  python -m src.data_management.data_ingestion.unstructured_data_text --overwrite
"""

import argparse
import json
import os
import re
import shutil
import unicodedata
from pathlib import Path
from typing import Optional

from src.common.load_env import load_env_file
from src.common.progress_bar import ProgressBar

DATASET = "rmisra/news-category-dataset"
OUT_DIR = Path("downloaded_data/unstructured/text")
JSON_EXTENSIONS = {".json", ".jsonl", ".ndjson"}
TEXT_EXTENSIONS = {".txt", ".csv", ".tsv"}
MAX_FILENAME_LENGTH = 80


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


def _format_article_text(article: dict) -> str:
    headline = str(article.get("headline", "") or "").strip()
    category = str(article.get("category", "") or "").strip()
    authors = str(article.get("authors", "") or "").strip()
    date = str(article.get("date", "") or "").strip()
    short_description = str(article.get("short_description", "") or "").strip()
    link = str(article.get("link", "") or "").strip()

    return (
        f"Headline: {headline}\n\n"
        f"Category: {category}\n"
        f"Authors: {authors}\n"
        f"Date: {date}\n\n"
        f"Short description:\n"
        f"{short_description}\n\n"
        f"Source:\n"
        f"{link}\n"
    )


def _write_article_txt(
    article: dict,
    article_index: int,
    overwrite: bool,
) -> bool:
    output_path = OUT_DIR / f"{article_index:06d}.txt"

    if output_path.exists() and not overwrite:
        return False

    output_path.write_text(_format_article_text(article), encoding="utf-8")
    return True


def _split_json_to_txt(
    source_path: Path,
    max_files: Optional[int],
    overwrite: bool,
) -> tuple[int, bool]:
    converted = 0
    processed = 0
    reached_limit = False

    with source_path.open("r", encoding="utf-8") as source_file, ProgressBar(
        total=max_files,
        description=f"Expanding {source_path.name}",
        unit="files",
        unit_scale=False,
    ) as progress:
        for line_number, raw_line in enumerate(source_file, start=1):
            line = raw_line.strip()
            if not line:
                continue

            if max_files is not None and processed >= max_files:
                reached_limit = True
                break

            try:
                article = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSON in {source_path} at line {line_number}: {exc}"
                ) from exc

            if _write_article_txt(
                article=article,
                article_index=line_number,
                overwrite=overwrite,
            ):
                converted += 1

            processed += 1
            progress.update(1)

    completed_full_file = not reached_limit
    return converted, completed_full_file


def _delete_json_if_needed(source_path: Path, completed_full_file: bool):
    if not completed_full_file:
        return

    if source_path.exists() and source_path.parent == OUT_DIR:
        source_path.unlink()

    local_copy = OUT_DIR / source_path.name
    if local_copy.exists() and local_copy != source_path:
        local_copy.unlink()


def _process_json_sources(
    json_files: list[Path],
    max_files: Optional[int],
    overwrite: bool,
) -> int:
    total_converted = 0

    for source_path in json_files:
        remaining = None if max_files is None else max_files - total_converted
        if remaining is not None and remaining <= 0:
            break

        converted, completed_full_file = _split_json_to_txt(
            source_path=source_path,
            max_files=remaining,
            overwrite=overwrite,
        )
        total_converted += converted
        _delete_json_if_needed(
            source_path=source_path,
            completed_full_file=completed_full_file,
        )

    return total_converted


def _copy_plain_text_files(
    text_files: list[Path],
    max_files: Optional[int],
    overwrite: bool,
) -> int:
    copied = 0

    with ProgressBar(
        total=max_files,
        description="Copying KaggleHub text",
        unit="files",
        unit_scale=False,
    ) as progress:
        for source_path in text_files:
            if max_files is not None and copied >= max_files:
                break

            output_path = OUT_DIR / source_path.name
            output_path.parent.mkdir(parents=True, exist_ok=True)

            if output_path.exists() and not overwrite:
                progress.update(1)
                continue

            shutil.copy2(source_path, output_path)
            copied += 1
            progress.update(1)

    return copied


def download_text_from_kaggle(
    max_files: Optional[int],
    overwrite: bool,
):
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    local_json_files = sorted(
        [f for f in OUT_DIR.iterdir() if f.is_file() and f.suffix.lower() in JSON_EXTENSIONS]
    )
    if local_json_files:
        converted = _process_json_sources(
            json_files=local_json_files,
            max_files=max_files,
            overwrite=overwrite,
        )
        print(f"[OK] Created {converted} text files in {OUT_DIR}")
        return

    dataset_path = _download_dataset_to_cache(overwrite=overwrite)
    all_files = sorted([f for f in dataset_path.rglob("*") if f.is_file()])
    json_files = [f for f in all_files if f.suffix.lower() in JSON_EXTENSIONS]
    text_files = [f for f in all_files if f.suffix.lower() in TEXT_EXTENSIONS]

    converted = 0
    if json_files:
        converted = _process_json_sources(
            json_files=json_files,
            max_files=max_files,
            overwrite=overwrite,
        )

    remaining = None if max_files is None else max_files - converted
    copied = 0
    if remaining is None or remaining > 0:
        copied = _copy_plain_text_files(
            text_files=text_files,
            max_files=remaining,
            overwrite=overwrite,
        )

    print(f"[OK] Created {converted} text files and copied {copied} text files in {OUT_DIR}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download unstructured text data from Kaggle and expand JSON entries into .txt files."
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="How many entries are converted to text from the JSON file.",
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
    download_text_from_kaggle(
        max_files=cli_args.max_files,
        overwrite=cli_args.overwrite,
    )
