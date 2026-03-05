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
from src.common.progress_bar import ProgressBar

API_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.csv"
DEFAULT_LIMIT = 50_000
OUT_DIR = Path("downloaded_data/structured")


# Downloads the full dataset by paginating in batches and writing each batch to a separate CSV.
def download_full_dataset_in_parts(limit: int = DEFAULT_LIMIT, max_csvs: Optional[int] = None):
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    offset = 0
    total = 0
    part_number = 1
    progress_total = max_csvs * limit if max_csvs is not None else None

    with ProgressBar(total=progress_total, description="Downloading NYC collisions", unit="rows", unit_scale=False) as progress:
        while True:
            if max_csvs is not None and part_number > max_csvs:
                progress.write(f"[STOP] max_csvs={max_csvs} reached.")
                break

            part_path = OUT_DIR / f"nyc_collisions_part_{part_number:04d}.csv"

            # If the part exists, it is not downloaded.
            if part_path.exists():
                existing_rows = count_rows_in_csv(part_path)

                # If we don't have any row in the CSV it can mean that the download has failed or it is corrupted.
                # We should try to download again.
                if existing_rows > 0:
                    total += existing_rows
                    progress.update(existing_rows)
                    progress.write(f"[SKIP] {part_path} already exists -> {existing_rows} rows (accumulated: {total})")

                    # If there are fewer rows than the configured limit, it is the last part.
                    if existing_rows < limit:
                        break

                    offset += limit
                    part_number += 1
                    continue
            
            # Download the CSV part.
            params = {"$limit": limit, "$offset": offset}
            r = requests.get(API_URL, params=params, timeout=60)
            r.raise_for_status()

            reader = csv.DictReader(io.StringIO(r.text))
            rows = list(reader)
            batch_size = len(rows)

            # Create the CSV
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

    print(f"[OK]: {total} rows in {OUT_DIR}")


def count_rows_in_csv(file_path: Path) -> int:
    with file_path.open("r", encoding="utf-8", newline="") as f_in:
        reader = csv.reader(f_in)
        next(reader, None)  # Skip header
        return sum(1 for _ in reader)


def parse_args():
    parser = argparse.ArgumentParser(description="Download NYC collisions dataset in CSV parts.")
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Rows per CSV file (default: {DEFAULT_LIMIT}).",
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
