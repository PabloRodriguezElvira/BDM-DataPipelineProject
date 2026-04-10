import io

import pandas as pd
from deltalake.writer import write_deltalake
from minio import Minio
from minio.error import S3Error

from src.common.minio_client import get_minio_client
from src.common.progress_bar import ProgressBar
import src.common.global_variables as config




def iter_structured_csvs(client: Minio):
    """
    Iterate only CSV files inside temporal landing.
    """
    prefix = config.LANDING_TEMPORAL_PATH

    for obj in client.list_objects(config.LANDING_BUCKET, prefix=prefix, recursive=True):
        if obj.is_dir or not obj.object_name:
            continue

        if obj.object_name.lower().endswith(".csv"):
            yield obj.object_name


def download_csv_as_dataframe(client: Minio, object_name: str) -> pd.DataFrame:
    """
    Download a CSV object from MinIO and load it into a pandas DataFrame.
    """
    response = client.get_object(config.LANDING_BUCKET, object_name)
    try:
        content = response.read()
    finally:
        response.close()
        response.release_conn()

    return pd.read_csv(io.BytesIO(content))


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize columns and cast to string to reduce schema conflicts across batches.
    """
    df = df.copy()

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )

    df = df.astype("string")
    return df


def get_delta_table_uri() -> str:
    """
    Delta table location inside MinIO persistent landing.
    """
    return f"s3://{config.LANDING_BUCKET}/{config.LANDING_PERSISTENT_PATH}structured/{DELTA_TABLE_NAME}"


def get_storage_options() -> dict:
    """
    Storage options for Delta Lake to connect to MinIO.
    """
    return {
        "AWS_ACCESS_KEY_ID": config.MINIO_ROOT_USER,
        "AWS_SECRET_ACCESS_KEY": config.MINIO_ROOT_PASSWORD,
        "AWS_ENDPOINT_URL": config.MINIO_ENDPOINT_URL,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def write_dataframe_to_delta(df: pd.DataFrame):
    """
    Append DataFrame to Delta table in persistent landing.
    """
    write_deltalake(
        get_delta_table_uri(),
        df,
        mode="append",
        storage_options=get_storage_options(),
    )


def remove_temporal_object(client: Minio, object_name: str):
    """
    Remove processed CSV from temporal landing.
    """
    client.remove_object(config.LANDING_BUCKET, object_name)


def process_csv_object(client: Minio, object_name: str):
    """
    Full process for one CSV:
    temporal CSV -> DataFrame -> Delta table -> delete temporal CSV
    """
    df = download_csv_as_dataframe(client, object_name)
    df = normalize_dataframe(df)

    if df.empty:
        print(f"[WARN] Empty CSV, deleting temporal object: {object_name}")
        remove_temporal_object(client, object_name)
        return

    df["source_file"] = object_name

    write_dataframe_to_delta(df)
    remove_temporal_object(client, object_name)
    print(f"[OK] {object_name} appended to Delta table")
