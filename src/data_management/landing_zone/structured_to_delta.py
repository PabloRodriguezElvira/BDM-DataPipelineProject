import io

import pandas as pd
import pyarrow as pa
from minio import Minio

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


def dataframe_to_arrow_table(df: pd.DataFrame) -> pa.Table:
    """
    Convert a normalized DataFrame into an Arrow Table ready for Delta Lake.
    """
    return pa.Table.from_pandas(df, preserve_index=False)


def process_csv_object(client: Minio, object_name: str) -> pa.Table | None:
    """
    Convert one temporal CSV into an Arrow Table ready to be written as Delta.
    """
    df = download_csv_as_dataframe(client, object_name)
    df = normalize_dataframe(df)

    if df.empty:
        print(f"[WARN] Empty CSV, skipping Delta conversion: {object_name}")
        return None

    df["source_file"] = object_name

    print(f"[OK] {object_name} converted into Arrow table")
    return dataframe_to_arrow_table(df)
