from minio import Minio

import src.common.global_variables as config


def get_minio_client():
    """
    Connects to the MinIO client using the credentials specified in compose.yaml
    """
    return Minio(
        config.MINIO_ENDPOINT,
        access_key=config.MINIO_ACCESS_KEY,
        secret_key=config.MINIO_SECRET_KEY,
        secure=config.MINIO_SECURE
    )
