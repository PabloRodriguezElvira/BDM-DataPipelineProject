import time
from io import BytesIO

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


def main():
    """
    Creates the buckets with all their sub buckets
    """
    wait_for_minio()

    # Create landing bucket
    create_bucket(config.LANDING_BUCKET)



if __name__ == "__main__":
    """
    Entry point: runs main and handles MinIO errors
    """

    try:
        main()
    except S3Error as e:
        print(f"Error MinIO: {e}")
