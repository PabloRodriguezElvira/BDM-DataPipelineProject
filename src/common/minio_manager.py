import time
from io import BytesIO
from typing import Optional

from minio import Minio
from minio.error import S3Error

import src.common.global_variables as config
from src.common.minio_client import get_minio_client

MAX_CONNECTION_ATTEMPTS = 30
RETRY_DELAY_SECONDS = 2


client = get_minio_client()


def wait_for_minio():
    """
    Wait until MinIO is reachable before trying to create buckets.
    """
    for attempt in range(1, MAX_CONNECTION_ATTEMPTS + 1):
        try:
            client.list_buckets()
            return
        except Exception as exc:
            if attempt == MAX_CONNECTION_ATTEMPTS:
                raise RuntimeError(
                    "MinIO was not reachable after multiple retries."
                ) from exc
            print(
                f"Waiting for MinIO ({attempt}/{MAX_CONNECTION_ATTEMPTS})..."
            )
            time.sleep(RETRY_DELAY_SECONDS)

def create_bucket(bucket: str):
    """
    Create the bucket if it does not exist.
    """

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def create_folder(bucket: str, folder: str):
    """
    Creates the folder inside the bucker
    """

    # Folders must end with "/"
    if not folder.endswith("/"):
        folder += "/"

    # The object we put inside the bucket is an empty folder
    client.put_object(
        bucket,
        folder,
        data=BytesIO(b""),
        length=0,
        content_type="application/octet-stream",
    )

def list_objects(bucket: str, prefix: str) -> list[str]:
    """Return all object names under prefix in bucket."""
    return [
        obj.object_name
        for obj in client.list_objects(bucket, prefix=prefix, recursive=True)
        if not obj.object_name.endswith("/")
    ]


def read_object_bytes(bucket: str, key: str) -> Optional[bytes]:
    """Download key from bucket and return its raw bytes, or None on error."""
    try:
        response = client.get_object(bucket, key)
        data = response.read()
        response.close()
        response.release_conn()
        return data
    except Exception as exc:
        print(f"[WARN] Could not read {key}: {exc}")
        return None


def write_object_bytes(bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream"):
    """Upload data to bucket under key."""
    client.put_object(
        bucket,
        key,
        data=BytesIO(data),
        length=len(data),
        content_type=content_type,
    )

def main():
    """
    Creates the buckets with all their sub buckets
    """
    wait_for_minio()

    # Create landing bucket
    create_bucket(config.LANDING_BUCKET)

    # Create trusted zone bucket
    create_bucket(config.TRUSTED_BUCKET)

    # Create exploitation zone bucket
    create_bucket(config.EXPLOITATION_BUCKET)



if __name__ == "__main__":
    """
    Entry point: runs main and handles MinIO errors
    """

    try:
        main()
    except S3Error as e:
        print(f"Error MinIO: {e}")