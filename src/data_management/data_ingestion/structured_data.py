"""
Use of the program:
The full dataset is 2246028 rows, but the downloaded CSVs and the amount of rows of each one
can be limited with parameters as shown below.

--limit: the number of rows per CSV.
--max-csvs: how many CSV parts are going to be processed.

Examples of usage:

- Download all parts with default settings:
  python -m src.data_management.data_ingestion.structured_data

- Set rows per CSV:
  python -m src.data_management.data_ingestion.structured_data --limit 50000

- Limit how many CSV parts are processed in one run:
  python -m src.data_management.data_ingestion.structured_data --max-csvs 5

- Use both options:
  python -m src.data_management.data_ingestion.structured_data --limit 20000 --max-csvs 3
"""

import argparse
import csv
import io
from pathlib import Path
from typing import Optional

import requests

import src.common.global_variables as config
from src.common.progress_bar import ProgressBar


def download_full_dataset_in_parts(
    limit: int = config.STRUCTURED_DEFAULT_LIMIT,
    max_csvs: Optional[int] = None,
):
    config.STRUCTURED_OUT_DIR.mkdir(parents=True, exist_ok=True)

    offset = 0
    total = 0
    part_number = 1
    progress_total = max_csvs * limit if max_csvs is not None else None

    with ProgressBar(
        total=progress_total,
        description="Downloading NYC collisions",
        unit="rows",
        unit_scale=False,
    ) as progress:
        while True:
            if max_csvs is not None and part_number > max_csvs:
                progress.write(f"[STOP] max_csvs={max_csvs} reached.")
                break

            part_path = config.STRUCTURED_OUT_DIR / f"nyc_collisions_part_{part_number:04d}.csv"

            if part_path.exists():
                existing_rows = count_rows_in_csv(part_path)

                if existing_rows > 0:
                    total += existing_rows
                    progress.update(existing_rows)
                    progress.write(
                        f"[SKIP] {part_path} already exists -> {existing_rows} rows (accumulated: {total})"
                    )

                    if existing_rows < limit:
                        break

                    offset += limit
                    part_number += 1
                    continue

            params = {"$limit": limit, "$offset": offset}
            response = requests.get(
                config.STRUCTURED_API_URL,
                params=params,
                timeout=config.STRUCTURED_REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            reader = csv.DictReader(io.StringIO(response.text))
            rows = list(reader)
            batch_size = len(rows)

            if batch_size == 0:
                break

            with part_path.open("w", newline="", encoding="utf-8") as f_out:
                writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            total += batch_size
            progress.update(batch_size)
            progress.write(f"[OK] {part_path} -> {batch_size} rows (accumulated: {total})")

            if batch_size < limit:
                break

            offset += limit
            part_number += 1

    print(f"[OK]: {total} rows in {config.STRUCTURED_OUT_DIR}")


def count_rows_in_csv(file_path: Path) -> int:
    with file_path.open("r", encoding="utf-8", newline="") as f_in:
        reader = csv.reader(f_in)
        next(reader, None)
        return sum(1 for _ in reader)


def parse_args():
    parser = argparse.ArgumentParser(description="Download NYC collisions dataset in CSV parts.")
    parser.add_argument(
        "--limit",
        type=int,
        default=config.STRUCTURED_DEFAULT_LIMIT,
        help=f"Rows per CSV file (default: {config.STRUCTURED_DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--max-csvs",
        type=int,
        default=None,
        help="Maximum number of CSV parts to process in this run.",
    )
    args = parser.parse_args()

    if args.limit <= 0:
        parser.error("--limit must be a positive integer.")
    if args.max_csvs is not None and args.max_csvs <= 0:
        parser.error("--max-csvs must be a positive integer.")

    return args


if __name__ == "__main__":
    cli_args = parse_args()
    download_full_dataset_in_parts(limit=cli_args.limit, max_csvs=cli_args.max_csvs)