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

- Overwrite existing files:
  python -m src.data_management.data_ingestion.semi_structured_data --overwrite
"""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Optional

import requests

from src.common.progress_bar import ProgressBar

POINTS_URL = "https://api.weather.gov/points/{lat},{lon}"
FORECAST_URL = "https://api.weather.gov/gridpoints/{office}/{grid_x},{grid_y}/{endpoint}"
OUT_DIR = Path("downloaded_data/semi_structured")
REQUEST_TIMEOUT_SECONDS = 60

# NWS asks users to send a descriptive User-Agent.
REQUEST_HEADERS = {
    "Accept": "application/geo+json",
    "User-Agent": "BDM-LandingZoneProject",
}

NYC_LOCATIONS = [
    {"name": "manhattan", "lat": 40.7831, "lon": -73.9712},
    {"name": "lower_manhattan", "lat": 40.7128, "lon": -74.0060},
    {"name": "harlem", "lat": 40.8116, "lon": -73.9465},
    {"name": "upper_west_side", "lat": 40.7870, "lon": -73.9754},
    {"name": "upper_east_side", "lat": 40.7736, "lon": -73.9566},
    {"name": "brooklyn", "lat": 40.6782, "lon": -73.9442},
    {"name": "williamsburg", "lat": 40.7081, "lon": -73.9571},
    {"name": "park_slope", "lat": 40.6720, "lon": -73.9772},
    {"name": "coney_island", "lat": 40.5749, "lon": -73.9850},
    {"name": "queens", "lat": 40.7282, "lon": -73.7949},
    {"name": "long_island_city", "lat": 40.7447, "lon": -73.9485},
    {"name": "flushing", "lat": 40.7654, "lon": -73.8174},
    {"name": "jamaica", "lat": 40.7027, "lon": -73.7890},
    {"name": "rockaway", "lat": 40.5795, "lon": -73.8371},
    {"name": "bronx", "lat": 40.8448, "lon": -73.8648},
    {"name": "south_bronx", "lat": 40.8183, "lon": -73.9029},
    {"name": "riverdale", "lat": 40.9006, "lon": -73.9067},
    {"name": "staten_island", "lat": 40.5795, "lon": -74.1502},
    {"name": "st_george", "lat": 40.6446, "lon": -74.0721},
    {"name": "tottenville", "lat": 40.5120, "lon": -74.2518},
]

# API call to find parameters [office], [grid_x] and [grid_y] which are necessary to execute the forecast request.
def fetch_gridpoint(lat: float, lon: float) -> tuple[str, int, int]:
    response = requests.get(
        POINTS_URL.format(lat=lat, lon=lon),
        headers=REQUEST_HEADERS,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    properties = response.json()["properties"]
    return properties["gridId"], int(properties["gridX"]), int(properties["gridY"])


def download_weather_jsons(max_locations: Optional[int], include_hourly: bool, overwrite: bool):
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Limit locations
    selected_locations = NYC_LOCATIONS[:max_locations] if max_locations is not None else NYC_LOCATIONS
    terminations = ["forecast"]
    if include_hourly:
        terminations.append("forecast/hourly")

    total_files = len(selected_locations) * len(terminations)
    downloaded = 0

    with ProgressBar(total=total_files, description="Downloading NYC weather", unit="files", unit_scale=False) as progress:
        # Get daily and hourly forecast of the selected locations (hourly is optional)
        for location in selected_locations:
            # Find parameters [office], [grid_x] and [grid_y].
            office, grid_x, grid_y = fetch_gridpoint(location["lat"], location["lon"])

            # Terminations are "forecast" and "forecast/hourly". The second one is optional.
            for termination in terminations:
                label = termination.replace("/", "_")
                output_path = OUT_DIR / f"nyc_weather_{location['name']}_{label}.json"

                if output_path.exists() and not overwrite:
                    progress.update(1)
                    progress.write(f"[SKIP] {output_path} already exists.")
                    continue

                source_url = FORECAST_URL.format(
                    office=office,
                    grid_x=grid_x,
                    grid_y=grid_y,
                    endpoint=termination,
                )

                # API call to request the forecast.
                response = requests.get(
                    source_url,
                    headers=REQUEST_HEADERS,
                    timeout=REQUEST_TIMEOUT_SECONDS,
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

                # Create JSON file.
                with output_path.open("w", encoding="utf-8") as f_out:
                    json.dump(payload, f_out, indent=2)

                downloaded += 1
                progress.update(1)
                progress.write(f"[OK] {output_path}")

    print(f"[OK]: downloaded {downloaded} JSON files in {OUT_DIR}")


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
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="If set, existing JSON files are replaced.",
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
        overwrite=cli_args.overwrite,
    )
