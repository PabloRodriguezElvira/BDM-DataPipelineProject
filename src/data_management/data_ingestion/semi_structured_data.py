"""
Use of the program:
Downloads semi-structured weather JSON files for New York City locations
from the NWS API (weather.gov) and stores them locally.

Examples of usage:

- Download forecast and hourly forecast for all default NYC locations:
  python -m src.data_management.data_ingestion.semi_structured_data

- Limit how many NYC locations are processed:
  python -m src.data_management.data_ingestion.semi_structured_data --max-locations 3

- Download only daily forecast (skip hourly):
  python -m src.data_management.data_ingestion.semi_structured_data --no-hourly
"""

import argparse
import json
from datetime import UTC, datetime
from typing import Optional

import requests

import src.common.global_variables as config
from src.common.progress_bar import ProgressBar


def fetch_gridpoint(lat: float, lon: float) -> tuple[str, int, int]:
    response = requests.get(
        config.POINTS_URL.format(lat=lat, lon=lon),
        headers=config.REQUEST_HEADERS,
        timeout=config.REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    properties = response.json()["properties"]
    return properties["gridId"], int(properties["gridX"]), int(properties["gridY"])


def download_weather_jsons(max_locations: Optional[int], include_hourly: bool):
    config.SEMI_STRUCTURED_OUT_DIR.mkdir(parents=True, exist_ok=True)

    selected_locations = (
        config.NYC_LOCATIONS[:max_locations]
        if max_locations is not None
        else config.NYC_LOCATIONS
    )

    terminations = ["forecast"]
    if include_hourly:
        terminations.append("forecast/hourly")

    total_files = len(selected_locations) * len(terminations)
    downloaded = 0

    with ProgressBar(
        total=total_files,
        description="Downloading NYC weather",
        unit="files",
        unit_scale=False,
    ) as progress:
        for location in selected_locations:
            office, grid_x, grid_y = fetch_gridpoint(location["lat"], location["lon"])

            for termination in terminations:
                label = termination.replace("/", "_")
                output_path = config.SEMI_STRUCTURED_OUT_DIR / f"nyc_weather_{location['name']}_{label}.json"

                if output_path.exists():
                    progress.update(1)
                    progress.write(f"[SKIP] {output_path} already exists.")
                    continue

                source_url = config.FORECAST_URL.format(
                    office=office,
                    grid_x=grid_x,
                    grid_y=grid_y,
                    endpoint=termination,
                )

                response = requests.get(
                    source_url,
                    headers=config.REQUEST_HEADERS,
                    timeout=config.REQUEST_TIMEOUT_SECONDS,
                )
                response.raise_for_status()

                payload = {
                    "metadata": {
                        "location": location,
                        "grid": {"office": office, "grid_x": grid_x, "grid_y": grid_y},
                        "endpoint": termination,
                        "requested_at_utc": datetime.now(UTC).isoformat(),
                        "source_url": source_url,
                    },
                    "data": response.json(),
                }

                with output_path.open("w", encoding="utf-8") as f_out:
                    json.dump(payload, f_out, indent=2)

                downloaded += 1
                progress.update(1)
                progress.write(f"[OK] {output_path}")

    print(f"[OK]: downloaded {downloaded} JSON files in {config.SEMI_STRUCTURED_OUT_DIR}")


def parse_args():
    parser = argparse.ArgumentParser(description="Download NYC weather JSON files from the NWS API.")
    parser.add_argument(
        "--max-locations",
        type=int,
        default=None,
        help="Maximum number of NYC locations to process in this run.",
    )
    parser.add_argument(
        "--no-hourly",
        action="store_true",
        help="If set, only daily forecast JSONs are downloaded.",
    )
    args = parser.parse_args()

    if args.max_locations is not None and args.max_locations <= 0:
        parser.error("--max-locations must be a positive integer.")

    return args


if __name__ == "__main__":
    cli_args = parse_args()
    download_weather_jsons(
        max_locations=cli_args.max_locations,
        include_hourly=not cli_args.no_hourly,
    )
