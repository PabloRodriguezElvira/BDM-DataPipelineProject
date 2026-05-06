"""
Trusted Zone pipeline for structured data (NYC motor vehicle collisions).

Reads the Delta Lake table produced by the Landing Zone from MinIO,
applies cleaning and type-casting transformations, and inserts the
results into a ClickHouse table for analytical querying.

Transformations applied
-----------------------
- Drop exact duplicate rows
- Drop rows with null or empty crash_date (critical field)
- Parse crash_date from MM/DD/YYYY string to a proper date
- Cast latitude and longitude to Float64 (null on parse error)
- Cast all injury/death count columns from string to UInt16 (0 on parse error)
- Cast collision_id from string to UInt32 (0 on parse error)
- Normalize borough and street name columns: strip whitespace, uppercase
- Fill remaining string nulls with empty string

Cleaned rows are inserted into ClickHouse: <CLICKHOUSE_DB>.nyc_collisions.
Rows dropped during cleaning are reported but not stored.
"""

import pandas as pd
from deltalake import DeltaTable

import src.common.global_variables as config
from src.common.clickhouse_client import get_clickhouse_client


_UINT16_COLS = [
    "number_of_persons_injured",
    "number_of_persons_killed",
    "number_of_pedestrians_injured",
    "number_of_pedestrians_killed",
    "number_of_cyclist_injured",
    "number_of_cyclist_killed",
    "number_of_motorist_injured",
    "number_of_motorist_killed",
]

_STRING_UPPER_COLS = [
    "borough",
    "on_street_name",
    "cross_street_name",
    "off_street_name",
]

_CREATE_DATABASE_DDL = f"CREATE DATABASE IF NOT EXISTS {config.CLICKHOUSE_DB}"

_CREATE_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.CLICKHOUSE_DB}.{config.TRUSTED_STRUCTURED_TABLE} (
    crash_date                      Date,
    crash_time                      String,
    borough                         LowCardinality(String),
    zip_code                        String,
    latitude                        Nullable(Float64),
    longitude                       Nullable(Float64),
    on_street_name                  String,
    cross_street_name               String,
    off_street_name                 String,
    number_of_persons_injured       UInt16,
    number_of_persons_killed        UInt16,
    number_of_pedestrians_injured   UInt16,
    number_of_pedestrians_killed    UInt16,
    number_of_cyclist_injured       UInt16,
    number_of_cyclist_killed        UInt16,
    number_of_motorist_injured      UInt16,
    number_of_motorist_killed       UInt16,
    contributing_factor_vehicle_1   String,
    contributing_factor_vehicle_2   String,
    contributing_factor_vehicle_3   String,
    contributing_factor_vehicle_4   String,
    contributing_factor_vehicle_5   String,
    collision_id                    UInt32,
    vehicle_type_code_1             String,
    vehicle_type_code_2             String,
    vehicle_type_code_3             String,
    vehicle_type_code_4             String,
    vehicle_type_code_5             String,
    source_file                     String,
    trusted_at                      DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (crash_date, collision_id)
"""

_FINAL_COLUMNS = [
    "crash_date", "crash_time",
    "borough", "zip_code", "latitude", "longitude",
    "on_street_name", "cross_street_name", "off_street_name",
    "number_of_persons_injured", "number_of_persons_killed",
    "number_of_pedestrians_injured", "number_of_pedestrians_killed",
    "number_of_cyclist_injured", "number_of_cyclist_killed",
    "number_of_motorist_injured", "number_of_motorist_killed",
    "contributing_factor_vehicle_1", "contributing_factor_vehicle_2",
    "contributing_factor_vehicle_3", "contributing_factor_vehicle_4",
    "contributing_factor_vehicle_5",
    "collision_id",
    "vehicle_type_code_1", "vehicle_type_code_2", "vehicle_type_code_3",
    "vehicle_type_code_4", "vehicle_type_code_5",
    "source_file",
]


def _delta_storage_options() -> dict:
    return {
        "AWS_ACCESS_KEY_ID": config.MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": config.MINIO_SECRET_KEY,
        "AWS_ENDPOINT_URL": config.MINIO_ENDPOINT_URL,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def load_delta_table() -> pd.DataFrame:
    """Read the structured Delta table from MinIO as a pandas DataFrame."""
    print("[STRUCTURED] Loading Delta table from Landing Zone...")
    dt = DeltaTable(
        config.TRUSTED_LANDING_STRUCTURED_URI,
        storage_options=_delta_storage_options(),
    )
    df = dt.to_pandas()
    print(f"[STRUCTURED] Loaded {len(df):,} rows, {len(df.columns)} columns.")
    return df


def clean_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Apply all cleaning transformations to the raw Delta DataFrame.
    Returns the cleaned DataFrame and the number of dropped rows.
    """
    initial = len(df)

    # Drop exact duplicates
    df = df.drop_duplicates()

    # Drop rows without a crash_date (critical field)
    df = df[df["crash_date"].notna() & (df["crash_date"].str.strip() != "")]

    dropped = initial - len(df)
    if dropped:
        print(f"[STRUCTURED] Dropped {dropped:,} rows (duplicates or missing crash_date).")

    # Parse crash_date: the API returns MM/DD/YYYY or YYYY-MM-DDT00:00:00.000
    df["crash_date"] = pd.to_datetime(df["crash_date"], errors="coerce").dt.date
    invalid_dates = df["crash_date"].isna().sum()
    if invalid_dates:
        print(f"[STRUCTURED][WARN] {invalid_dates:,} rows with unparseable crash_date — dropped.")
    df = df[df["crash_date"].notna()]

    # Cast latitude / longitude to float (null on error)
    for col in ("latitude", "longitude"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Cast injury/death counts to int, default 0
    for col in _UINT16_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("uint16")
        else:
            df[col] = 0

    # Cast collision_id to uint32, default 0
    if "collision_id" in df.columns:
        df["collision_id"] = pd.to_numeric(df["collision_id"], errors="coerce").fillna(0).astype("uint32")
    else:
        df["collision_id"] = 0

    # Normalize borough and street names
    for col in _STRING_UPPER_COLS:
        if col in df.columns:
            df[col] = df[col].fillna("").str.strip().str.upper()
        else:
            df[col] = ""

    # Fill all remaining string columns with empty string
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].fillna("")

    # Ensure all expected columns exist (add missing ones as empty/zero)
    for col in _FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = "" if col not in _UINT16_COLS and col != "collision_id" else 0

    return df[_FINAL_COLUMNS], dropped


def ensure_schema(client) -> None:
    """Create the ClickHouse database and table if they do not already exist."""
    client.command(_CREATE_DATABASE_DDL)
    client.command(_CREATE_TABLE_DDL)
    print(
        f"[STRUCTURED] ClickHouse schema ready: "
        f"{config.CLICKHOUSE_DB}.{config.TRUSTED_STRUCTURED_TABLE}"
    )


def insert_to_clickhouse(client, df: pd.DataFrame) -> None:
    """Insert the cleaned DataFrame into ClickHouse in one batch."""
    if df.empty:
        print("[STRUCTURED] No rows to insert.")
        return

    client.insert_df(
        f"{config.CLICKHOUSE_DB}.{config.TRUSTED_STRUCTURED_TABLE}",
        df,
    )
    print(f"[STRUCTURED] Inserted {len(df):,} rows into ClickHouse.")


def main():
    df_raw = load_delta_table()
    df_clean, dropped = clean_dataframe(df_raw)

    print(
        f"[STRUCTURED] Clean rows: {len(df_clean):,} | "
        f"Dropped: {dropped:,} | "
        f"Total: {len(df_raw):,}"
    )

    client = get_clickhouse_client()
    try:
        ensure_schema(client)
        insert_to_clickhouse(client, df_clean)
    finally:
        client.close()

    print("[STRUCTURED] Done.")


if __name__ == "__main__":
    main()
