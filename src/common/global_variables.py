from datetime import datetime
import os
from pathlib import Path

# -------------------------- BUCKET NAMES --------------------------
LANDING_BUCKET = "landing-zone"
TRUSTED_BUCKET = "trusted-zone"
EXPLOITATION_BUCKET = "exploitation-zone"

# -------------------------- PATHS FOR EACH ZONE --------------------------
LANDING_TEMPORAL_PATH = "temporal_landing/"
LANDING_PERSISTENT_PATH = "persistent_landing/"

TRUSTED_STRUCTURED_PATH = "structured/"
TRUSTED_UNSTRUCTURED_PATH  = "unstructured/"

# -------------------------- MINIO CONNECTION --------------------------
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


# -------------------------- APACHE KAFKA --------------------------
KAFKA_SERVER = "kafka:29092"


# -------------------------- AIRFLOW (ORCHESTRATION) --------------------------
PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/app")
PYTHON_BIN = os.getenv("PYTHON_BIN", "python")

AIRFLOW_DEFAULT_OWNER = "bdm"
AIRFLOW_DEFAULT_DEPENDS_ON_PAST = False
AIRFLOW_DEFAULT_RETRIES = 2
AIRFLOW_DEFAULT_RETRY_DELAY_MINUTES = 5

# Data ingestion + Landing Zone
AIRFLOW_LZ_DAG_ID = "data_ingestion_landing_zone"
AIRFLOW_LZ_DESCRIPTION = "Batch orchestration for ingestion and landing zone"
AIRFLOW_LZ_START_DATE = datetime(2026, 4, 10)
AIRFLOW_LZ_SCHEDULE = None
AIRFLOW_LZ_CATCHUP = False
AIRFLOW_LZ_MAX_ACTIVE_RUNS = 1
AIRFLOW_LZ_TAGS = ["bdm", "ingestion", "landing-zone"]
# Trusted Zone
AIRFLOW_TZ_DAG_ID = "trusted_zone_pipeline"
AIRFLOW_TZ_DESCRIPTION = "Batch pipeline: Landing Zone -> Trusted Zone (cleaning)"
AIRFLOW_TZ_START_DATE = datetime(2026, 4, 10)
AIRFLOW_TZ_SCHEDULE = None
AIRFLOW_TZ_CATCHUP = False
AIRFLOW_TZ_MAX_ACTIVE_RUNS = 1
AIRFLOW_TZ_TAGS = ["bdm", "trusted-zone"]

# Kafka Streaming
AIRFLOW_STREAMING_DAG_ID = "apache_kafka_streaming"
AIRFLOW_STREAMING_DESCRIPTION = "Manual bootstrap for Kafka producer/consumer of the image stream"
AIRFLOW_STREAMING_START_DATE = datetime(2026, 4, 10)
AIRFLOW_STREAMING_SCHEDULE = None
AIRFLOW_STREAMING_CATCHUP = False
AIRFLOW_STREAMING_MAX_ACTIVE_RUNS = 1
AIRFLOW_STREAMING_TAGS = ["bdm", "streaming", "kafka"]


# -------------------------- STRUCTURED INGESTION --------------------------
STRUCTURED_API_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.csv"
STRUCTURED_DEFAULT_LIMIT = 50_000
STRUCTURED_OUT_DIR = Path("downloaded_data/structured")


# -------------------------- SEMI-STRUCTURED INGESTION --------------------------
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


# -------------------------- UNSTRUCTURED AUDIO INGESTION --------------------------
UNSTRUCTURED_AUDIO_DATASET = "louisteitelbaum/911-recordings-first-6-seconds"
UNSTRUCTURED_AUDIO_OUT_DIR = Path("downloaded_data/unstructured/audio")
UNSTRUCTURED_AUDIO_EXTENSIONS = {".wav", ".mp3", ".flac", ".ogg", ".m4a", ".aac"}


# -------------------------- UNSTRUCTURED IMAGE CONSUMER --------------------------
UNSTRUCTURED_IMAGE_BASE_DIR = "downloaded_data/unstructured/real_time_images"
UNSTRUCTURED_IMAGE_TOPIC_NAME = "traffic-images"
UNSTRUCTURED_IMAGE_WINDOW_SECONDS = 15
UNSTRUCTURED_IMAGE_LOG_INTERVAL = 15


# -------------------------- UNSTRUCTURED IMAGE PRODUCER --------------------------
UNSTRUCTURED_IMAGE_SOURCE_DATA_PATH = Path("downloaded_data/unstructured/images")
UNSTRUCTURED_IMAGE_BATCH_SIZE = 5
UNSTRUCTURED_IMAGE_TRAFFIC_UPDATE_SECONDS = 10
UNSTRUCTURED_IMAGE_SLEEP_SECONDS = 0.01


# -------------------------- UNSTRUCTURED TEXT INGESTION --------------------------
UNSTRUCTURED_TEXT_DATASET = "rmisra/news-category-dataset"
UNSTRUCTURED_TEXT_OUT_DIR = Path("downloaded_data/unstructured/text")
UNSTRUCTURED_TEXT_JSON_EXTENSIONS = {".json", ".jsonl", ".ndjson"}
UNSTRUCTURED_TEXT_FILE_EXTENSIONS = {".txt", ".csv", ".tsv"}
UNSTRUCTURED_TEXT_MAX_FILENAME_LENGTH = 80


# -------------------------- CLICKHOUSE --------------------------
CLICKHOUSE_HOST     = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER     = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
CLICKHOUSE_DB       = os.getenv("CLICKHOUSE_DB", "trusted_zone")

# -------------------------- TRUSTED ZONE: STRUCTURED --------------------------
TRUSTED_LANDING_STRUCTURED_URI = (
    f"s3://{LANDING_BUCKET}/{LANDING_PERSISTENT_PATH}structured/delta"
)
TRUSTED_STRUCTURED_TABLE = "nyc_collisions"
TRUSTED_SKIPPED_ROWS_PREFIX = f"{TRUSTED_STRUCTURED_PATH}skipped/"

# -------------------------- MONGO DB --------------------------
MONGO_HOST     = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT     = int(os.getenv("MONGO_PORT", "27017"))
MONGO_USER     = os.getenv("MONGO_USER", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "mongo_pass")
MONGO_DB       = os.getenv("MONGO_DB", "trusted_zone")

# -------------------------- TRUSTED ZONE: SEMI-STRUCTURED --------------------------
TRUSTED_LANDING_SEMISTRUCTURED_WEATHER_URI = f"s3a://{LANDING_BUCKET}/{LANDING_PERSISTENT_PATH}semi-structured/data"
TRUSTED_LANDING_CAMERAS_UNSTRUCTURED_URI = f"s3a://{LANDING_BUCKET}/{LANDING_PERSISTENT_PATH}unstructured/images/metadata"

TRUSTED_WEATHER_COLLECTION = "weather_data"
TRUSTED_CAMERA_COLLECTION  = "camera_aggregates"

# -------------------------- TRUSTED ZONE: UNSTRUCTURED --------------------------
# Landing Zone source prefixes (inside LANDING_BUCKET)
TRUSTED_LANDING_AUDIO_PREFIX = f"{LANDING_PERSISTENT_PATH}unstructured/audio/data/"
TRUSTED_LANDING_TEXT_PREFIX  = f"{LANDING_PERSISTENT_PATH}unstructured/text/data/"

# Trusted Zone destination prefixes (inside TRUSTED_BUCKET)
TRUSTED_AUDIO_PREFIX         = f"{TRUSTED_UNSTRUCTURED_PATH}audio/data/"
TRUSTED_TEXT_PREFIX          = f"{TRUSTED_UNSTRUCTURED_PATH}text/data/"
TRUSTED_AUDIO_SKIPPED_PREFIX = f"{TRUSTED_UNSTRUCTURED_PATH}audio/skipped/"
TRUSTED_TEXT_SKIPPED_PREFIX  = f"{TRUSTED_UNSTRUCTURED_PATH}text/skipped/"

# Audio cleaning parameters
TRUSTED_AUDIO_TARGET_SAMPLE_RATE      = 16_000  
TRUSTED_AUDIO_MIN_DURATION_SECONDS    = 0.5
TRUSTED_AUDIO_PCM_SAMPLE_WIDTH        = 2      

# -------------------------- MILVUS --------------------------
MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))

MILVUS_TEXT_COLLECTION  = "text_embeddings"
MILVUS_AUDIO_COLLECTION = "audio_embeddings"
MILVUS_EMBEDDING_DIM    = 384   # sentence-transformers/all-MiniLM-L6-v2
MILVUS_AUDIO_DIM        = 768   # wav2vec2-base

# -------------------------- EXPLOITATION ZONE: STRUCTURED --------------------------
EXPLOIT_STRUCTURED_DB    = "exploitation_zone"
EXPLOIT_STRUCTURED_TABLE = "collisions_weather"

# -------------------------- EXPLOITATION ZONE: UNSTRUCTURED --------------------------

# Source prefixes (inside TRUSTED_BUCKET)
EXPLOIT_TRUSTED_TEXT_PREFIX  = TRUSTED_TEXT_PREFIX   # reuse trusted paths
EXPLOIT_TRUSTED_AUDIO_PREFIX = TRUSTED_AUDIO_PREFIX

# Destination prefixes (inside EXPLOITATION_BUCKET)
EXPLOIT_TEXT_PREFIX  = "unstructured/text/data/"
EXPLOIT_AUDIO_PREFIX = "unstructured/audio/data/"

# Airflow DAG
AIRFLOW_EZ_DAG_ID          = "exploitation_zone_unstructured_pipeline"
AIRFLOW_EZ_DESCRIPTION     = "Trusted Zone -> Exploitation Zone: embeddings (Milvus) + curated files (MinIO)"
AIRFLOW_EZ_START_DATE      = datetime(2026, 4, 10)
AIRFLOW_EZ_SCHEDULE        = None
AIRFLOW_EZ_CATCHUP         = False
AIRFLOW_EZ_MAX_ACTIVE_RUNS = 1
AIRFLOW_EZ_TAGS            = ["bdm", "exploitation-zone", "embeddings", "milvus"]
