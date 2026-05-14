"""
Trusted Zone pipeline for structured data (NYC motor vehicle collisions).

Reads the Delta Lake table produced by the Landing Zone from MinIO,
applies cleaning and type-casting transformations using PySpark,
and inserts the results into a ClickHouse table for analytical querying.

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
Rows rejected during cleaning (null/empty/unparseable crash_date) are saved
as CSV to trusted-zone/structured/skipped/ in MinIO for traceability.
"""

from datetime import datetime

from deltalake import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, to_date, trim, upper
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType

import src.common.global_variables as config
from src.common.clickhouse_client import get_clickhouse_client
from src.common.minio_manager import write_object_bytes


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

_NUMERIC_COLS = set(_UINT16_COLS) | {"collision_id"}


def _delta_storage_options() -> dict:
    return {
        "AWS_ACCESS_KEY_ID": config.MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": config.MINIO_SECRET_KEY,
        "AWS_ENDPOINT_URL": config.MINIO_ENDPOINT_URL,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def load_delta_table(spark: SparkSession):
    """Read the structured Delta table from MinIO as a Spark DataFrame."""
    print("[STRUCTURED] Loading Delta table from Landing Zone...")
    dt = DeltaTable(
        config.TRUSTED_LANDING_STRUCTURED_URI,
        storage_options=_delta_storage_options(),
    )
    df = spark.createDataFrame(dt.to_pandas())
    total = df.count()
    print(f"[STRUCTURED] Loaded {total:,} rows, {len(df.columns)} columns.")
    return df, total


def clean_dataframe(df, total: int):
    """
    Apply all cleaning transformations using PySpark.

    Returns (df_clean, df_rejected, dropped):
      - df_clean    : accepted rows with all transformations applied (cached)
      - df_rejected : raw rows that failed crash_date validation, for traceability
      - dropped     : total row count reduction (duplicates + invalid dates)
    """
    # Deduplicate on original columns
    df = df.dropDuplicates()

    # Parse crash_date into a temporary column to decide accept/reject in one pass
    df = df.withColumn(
        "_parsed_date",
        coalesce(
            to_date(col("crash_date"), "MM/dd/yyyy"),
            to_date(col("crash_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
            to_date(col("crash_date"), "yyyy-MM-dd"),
        ),
    )

    # Cache here so both the rejected and accepted branches read from memory
    df.cache()

    is_valid = (
        col("crash_date").isNotNull()
        & (trim(col("crash_date")) != "")
        & col("_parsed_date").isNotNull()
    )

    df_rejected = df.filter(~is_valid).drop("_parsed_date")

    df = (
        df.filter(is_valid)
        .withColumn("crash_date", col("_parsed_date"))
        .drop("_parsed_date")
    )

    # Cast latitude / longitude to double (null on parse error)
    for c in ("latitude", "longitude"):
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(DoubleType()))

    # Cast injury/death counts to int, default 0
    for c in _UINT16_COLS:
        if c in df.columns:
            df = df.withColumn(c, coalesce(col(c).cast(IntegerType()), lit(0)))
        else:
            df = df.withColumn(c, lit(0))

    # Cast collision_id to long, default 0
    if "collision_id" in df.columns:
        df = df.withColumn(
            "collision_id", coalesce(col("collision_id").cast(LongType()), lit(0))
        )
    else:
        df = df.withColumn("collision_id", lit(0))

    # Normalize borough and street names: strip whitespace, uppercase
    for c in _STRING_UPPER_COLS:
        if c in df.columns:
            df = df.withColumn(c, upper(trim(coalesce(col(c), lit("")))))
        else:
            df = df.withColumn(c, lit(""))

    # Fill all remaining string columns with empty string
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in string_cols:
        df = df.withColumn(c, coalesce(col(c), lit("")))

    # Add any columns missing from the Delta table with sensible defaults
    existing = set(df.columns)
    for c in _FINAL_COLUMNS:
        if c not in existing:
            df = df.withColumn(c, lit(0) if c in _NUMERIC_COLS else lit(""))

    df_clean = df.select(*_FINAL_COLUMNS)
    df_clean.cache()
    clean_count = df_clean.count()
    dropped = total - clean_count

    return df_clean, df_rejected, dropped


def save_rejected_rows(df_rejected) -> None:
    """Write rejected rows as a timestamped CSV to the skipped folder in MinIO."""
    pandas_df = df_rejected.toPandas()
    if pandas_df.empty:
        return

    csv_bytes = pandas_df.to_csv(index=False).encode("utf-8")
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    key = f"{config.TRUSTED_SKIPPED_ROWS_PREFIX}skipped_{timestamp}.csv"

    write_object_bytes(config.TRUSTED_BUCKET, key, csv_bytes, content_type="text/csv")
    print(f"[STRUCTURED] Saved {len(pandas_df):,} rejected rows → {key}")


def ensure_schema(client) -> None:
    """Create the ClickHouse database and table if they do not already exist."""
    client.command(_CREATE_DATABASE_DDL)
    client.command(_CREATE_TABLE_DDL)
    print(
        f"[STRUCTURED] ClickHouse schema ready: "
        f"{config.CLICKHOUSE_DB}.{config.TRUSTED_STRUCTURED_TABLE}"
    )


def insert_to_clickhouse(client, df) -> None:
    """Convert the cached Spark DataFrame to pandas and insert into ClickHouse."""
    pandas_df = df.toPandas()
    if pandas_df.empty:
        print("[STRUCTURED] No rows to insert.")
        return

    for c in _UINT16_COLS:
        pandas_df[c] = pandas_df[c].fillna(0).astype("uint16")
    pandas_df["collision_id"] = pandas_df["collision_id"].fillna(0).astype("uint32")

    client.insert_df(
        f"{config.CLICKHOUSE_DB}.{config.TRUSTED_STRUCTURED_TABLE}",
        pandas_df,
    )
    print(f"[STRUCTURED] Inserted {len(pandas_df):,} rows into ClickHouse.")


def main():
    spark = (
        SparkSession.builder
        .appName("TrustedZone-Structured")
        .master("local[*]")
        .getOrCreate()
    )
    try:
        df_raw, total = load_delta_table(spark)
        df_clean, df_rejected, dropped = clean_dataframe(df_raw, total)

        print(
            f"[STRUCTURED] Clean rows: {total - dropped:,} | "
            f"Dropped: {dropped:,} | "
            f"Total: {total:,}"
        )

        client = get_clickhouse_client()
        try:
            ensure_schema(client)
            insert_to_clickhouse(client, df_clean)
        finally:
            client.close()

        save_rejected_rows(df_rejected)

        df_clean.unpersist()
        df_raw.unpersist()
    finally:
        spark.stop()

    print("[STRUCTURED] Done.")


if __name__ == "__main__":
    main()
