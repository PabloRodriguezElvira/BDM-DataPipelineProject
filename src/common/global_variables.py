import os

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

# MinIO client settings (single source of truth)
MINIO_ENDPOINT = _minio_endpoint_no_scheme
MINIO_SECURE = _minio_secure

# Endpoint format required by DeltaLake/Rust S3 client.
MINIO_ENDPOINT_URL = os.getenv(
    "MINIO_ENDPOINT_URL",
    f"{_minio_scheme}://{_minio_endpoint_no_scheme}",
)

# Reuse app credentials by default; fallback to root credentials if provided that way.
MINIO_ROOT_USER = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "admin"))
MINIO_ROOT_PASSWORD = os.getenv(
    "MINIO_SECRET_KEY",
    os.getenv("MINIO_ROOT_PASSWORD", "admin123"),
)

# Aliases aligned with env var names.
MINIO_ACCESS_KEY = MINIO_ROOT_USER
MINIO_SECRET_KEY = MINIO_ROOT_PASSWORD
