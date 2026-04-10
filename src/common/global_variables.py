from datetime import datetime
import os
from pathlib import Path

# ------------- Bucket names -------------
LANDING_BUCKET = "landing-zone"

# ------------- Paths for each zone -------------
LANDING_TEMPORAL_PATH = "temporal_landing/"
LANDING_PERSISTENT_PATH = "persistent_landing/"

# ------------- MinIO connection -------------
_minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000").strip()
_minio_secure = os.getenv("MINIO_SECURE", "false").strip().lower() in {
    "1", "true", "yes", "on"
}
_minio_endpoint_no_scheme = _minio_endpoint.replace("http://", "").replace("https://", "")
_minio_scheme = "https" if _minio_secure else "http"

MINIO_ENDPOINT = _minio_endpoint_no_scheme
MINIO_SECURE = _minio_secure

MINIO_ENDPOINT_URL = os.getenv(
    "MINIO_ENDPOINT_URL",
    f"{_minio_scheme}://{_minio_endpoint_no_scheme}",
)

MINIO_ROOT_USER = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "admin"))
MINIO_ROOT_PASSWORD = os.getenv(
    "MINIO_SECRET_KEY",
    os.getenv("MINIO_ROOT_PASSWORD", "admin123"),
)

MINIO_ACCESS_KEY = MINIO_ROOT_USER
MINIO_SECRET_KEY = MINIO_ROOT_PASSWORD

# ------------- Kafka -------------
KAFKA_SERVER = "kafka:29092"


# ------------- Airflow / orchestration -------------
PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/app")
PYTHON_BIN = os.getenv("PYTHON_BIN", "python")

AIRFLOW_DEFAULT_OWNER = "bdm"
AIRFLOW_DEFAULT_DEPENDS_ON_PAST = False
AIRFLOW_DEFAULT_RETRIES = 2
AIRFLOW_DEFAULT_RETRY_DELAY_MINUTES = 5

AIRFLOW_BATCH_DAG_ID = "bdm_batch_pipeline"
AIRFLOW_BATCH_DESCRIPTION = "Batch orchestration for ingestion and landing zone"
AIRFLOW_BATCH_START_DATE = datetime(2026, 4, 10)
AIRFLOW_BATCH_SCHEDULE = "0 2 * * *"
AIRFLOW_BATCH_CATCHUP = False
AIRFLOW_BATCH_MAX_ACTIVE_RUNS = 1
AIRFLOW_BATCH_TAGS = ["bdm", "ingestion", "landing-zone"]

AIRFLOW_STREAMING_DAG_ID = "bdm_streaming_bootstrap"
AIRFLOW_STREAMING_DESCRIPTION = "Manual bootstrap for Kafka producer/consumer of the image stream"
AIRFLOW_STREAMING_START_DATE = datetime(2026, 4, 10)
AIRFLOW_STREAMING_SCHEDULE = None
AIRFLOW_STREAMING_CATCHUP = False
AIRFLOW_STREAMING_MAX_ACTIVE_RUNS = 1
AIRFLOW_STREAMING_TAGS = ["bdm", "streaming", "kafka"]

AIRFLOW_STRUCTURED_LIMIT = 50000
AIRFLOW_STRUCTURED_MAX_CSVS = 5
AIRFLOW_SEMI_STRUCTURED_MAX_LOCATIONS = 5
AIRFLOW_UNSTRUCTURED_TEXT_MAX_FILES = 100
AIRFLOW_UNSTRUCTURED_AUDIO_MAX_FILES = 50
AIRFLOW_STREAMING_TIMEOUT_SECONDS = 120

# ------------- Semi-structured ingestion -------------
POINTS_URL = "https://api.weather.gov/points/{lat},{lon}"
FORECAST_URL = "https://api.weather.gov/gridpoints/{office}/{grid_x},{grid_y}/{endpoint}"
SEMI_STRUCTURED_OUT_DIR = Path("downloaded_data/semi_structured")
REQUEST_TIMEOUT_SECONDS = 60

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

# ------------- Structured ingestion -------------
STRUCTURED_API_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.csv"
STRUCTURED_DEFAULT_LIMIT = 50_000
STRUCTURED_OUT_DIR = Path("downloaded_data/structured")

# ------------- Unstructured audio ingestion -------------
UNSTRUCTURED_AUDIO_DATASET = "louisteitelbaum/911-recordings-first-6-seconds"
UNSTRUCTURED_AUDIO_OUT_DIR = Path("downloaded_data/unstructured/audio")
UNSTRUCTURED_AUDIO_EXTENSIONS = {".wav", ".mp3", ".flac", ".ogg", ".m4a", ".aac"}

# ------------- Unstructured image consumer -------------
UNSTRUCTURED_IMAGE_BASE_DIR = "src/downloaded_data/unstructured/real_time_images"
UNSTRUCTURED_IMAGE_TOPIC_NAME = "traffic-images"
UNSTRUCTURED_IMAGE_WINDOW_SECONDS = 15
UNSTRUCTURED_IMAGE_LOG_INTERVAL = 15

# ------------- Unstructured image producer -------------
UNSTRUCTURED_IMAGE_SOURCE_DATA_PATH = Path("src/downloaded_data/unstructured/images")
UNSTRUCTURED_IMAGE_BATCH_SIZE = 5
UNSTRUCTURED_IMAGE_TRAFFIC_UPDATE_SECONDS = 10
UNSTRUCTURED_IMAGE_SLEEP_SECONDS = 0.01

# ------------- Unstructured text ingestion -------------
UNSTRUCTURED_TEXT_DATASET = "rmisra/news-category-dataset"
UNSTRUCTURED_TEXT_OUT_DIR = Path("downloaded_data/unstructured/text")
UNSTRUCTURED_TEXT_JSON_EXTENSIONS = {".json", ".jsonl", ".ndjson"}
UNSTRUCTURED_TEXT_FILE_EXTENSIONS = {".txt", ".csv", ".tsv"}
UNSTRUCTURED_TEXT_MAX_FILENAME_LENGTH = 80
