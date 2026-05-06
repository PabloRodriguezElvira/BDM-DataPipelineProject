import json

import pandas as pd
import pyarrow as pa
from deltalake.writer import write_deltalake

import src.common.global_variables as config


def _get_storage_options() -> dict:
    return {
        "AWS_ACCESS_KEY_ID": config.MINIO_ROOT_USER,
        "AWS_SECRET_ACCESS_KEY": config.MINIO_ROOT_PASSWORD,
        "AWS_ENDPOINT_URL": config.MINIO_ENDPOINT_URL,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def _flatten_metadata_payload(payload: dict, prefix: str = "") -> dict:
    """
    Flatten nested metadata dictionaries into a Delta-friendly row.
    Lists are JSON-encoded to avoid object dtype issues during Arrow conversion.
    """
    flat_row = {}
    for key, value in payload.items():
        flat_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            flat_row.update(_flatten_metadata_payload(value, flat_key))
        elif isinstance(value, list):
            flat_row[flat_key] = json.dumps(value, ensure_ascii=False)
        else:
            flat_row[flat_key] = value
    return flat_row


def _write_delta(uri: str, table, storage_options: dict):
    """
    Write an Arrow table to a Delta Lake URI.
    Tries the rust engine with schema_mode=merge first; if the installed
    deltalake version does not support it, falls back to a plain append.
    """
    try:
        write_deltalake(
            uri,
            table,
            mode="append",
            schema_mode="merge",
            engine="rust",
            storage_options=storage_options,
        )
    except TypeError:
        write_deltalake(uri, table, mode="append", storage_options=storage_options)


def _cast_null_columns(table: pa.Table) -> pa.Table:
    """
    Cast any pa.null() columns to pa.string().
    Delta Lake does not accept the null type, which PyArrow infers when
    a single-row DataFrame has None for every value in a column.
    """
    new_fields = []
    for i, field in enumerate(table.schema):
        if pa.types.is_null(field.type):
            table = table.set_column(i, field.name, table.column(i).cast(pa.string()))
            new_fields.append(field.name)
    return table


def process_metadata_to_delta(metadata_payload: dict, delta_folder: str):
    """
    Convert a metadata dictionary (audio, text, or weather) into a row
    in the corresponding Delta table under persistent_landing/.
    """
    flat_row = _flatten_metadata_payload(metadata_payload)
    df = pd.DataFrame([flat_row]).convert_dtypes()
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
    arrow_table = _cast_null_columns(arrow_table)

    uri = f"s3://{config.LANDING_BUCKET}/{config.LANDING_PERSISTENT_PATH}{delta_folder}"
    _write_delta(uri, arrow_table, _get_storage_options())
    print(f"[DELTA SUCCESS] Metadata integrated into table at: {delta_folder}")
    
  
