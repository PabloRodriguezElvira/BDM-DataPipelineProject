"""
Trusted Zone pipeline for semi-structured data (weather reports).

Reads weather JSON files from the Landing Zone persistent storage in MinIO,
applies information-preserving cleaning and normalization transformations
using Spark, and writes the results to a MongoDB collection.

Transformations applied
-----------------------
Weather JSON files:
  - Flattening: Extract deeply nested objects from 'periods' arrays into a tabular format.
  - Schema alignment: Map disparate location names (e.g., "lower_manhattan") to official NYC Boroughs.
  - Handling missing fields: Assign default values (0.0) to 'dewpoint' for 12h-forecasts
    to ensure schema consistency with Hourly reports.
  - Structural rules: Standardize temporal data to YYYY-MM-DD for JOIN compatibility
    and clean 'windSpeed' by removing unit strings (mph).
  - Data casting: Force temperature and humidity fields into numeric types.

Processed records are written to the MongoDB 'weather_data' collection.
Files that fail basic JSON validation are skipped for traceability.

Also handles duplicates storaging deduplicates in MinIO and. It also storages raws that do not
fulfill the Data Quality constraints for Data Governance
"""

import json
from datetime import datetime

from pymongo import UpdateOne
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, when, lit, upper, split, regexp_extract
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, DoubleType, IntegerType
)

import src.common.global_variables as config
from src.common.minio_manager import list_objects, read_object_bytes, write_object_bytes
from src.common.mongo_client import get_mongo_client


def _save_skipped(df_dupes, label: str, prefix: str) -> None:
    """
    Saves rows that failed quality checks or were duplicates into a csv file in MinIO.
    This helps us keep track of 'bad data' for future analysis or debugging.
    """
    pandas_df = df_dupes.toPandas()
    if pandas_df.empty:
        return
    csv_bytes = pandas_df.to_csv(index=False).encode("utf-8")
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    key = f"{prefix}skipped_{timestamp}.csv"
    write_object_bytes(config.TRUSTED_BUCKET, key, csv_bytes, content_type="text/csv")
    print(f"[{label}] Saved {len(pandas_df):,} duplicate rows → {key}")

WEATHER_PREFIX = f"{config.LANDING_PERSISTENT_PATH}semi_structured/data/"


def _load_raw_rows(keys: list[str]) -> tuple[list[Row], int]:
    """
    Parses raw weather JSON files from the landing zone in MiniO, extracting relevant forecast 
    periods and flattening them into a list of Spark Rows for processing. 
    It handles missing nested fields by setting safe default values.
    """
    rows = []
    skipped = 0
    for key in keys:
        raw = read_object_bytes(config.LANDING_BUCKET, key)
        if raw is None:
            skipped += 1
            continue
        try:
            # Parse JSON and being able to access to nested structure
            data = json.loads(raw)
            location = data.get("metadata", {}).get("location", {}).get("name", "")
            periods = data.get("data", {}).get("properties", {}).get("periods", [])
            # Extract metereological metrics for each period
            for period in periods:
                dewpoint = period.get("dewpoint", {})
                dewpoint_val = dewpoint.get("value") if isinstance(dewpoint, dict) else None

                precip = period.get("probabilityOfPrecipitation", {})
                precip_val = precip.get("value") if isinstance(precip, dict) else None

                humidity = period.get("relativeHumidity", {})
                humidity_val = humidity.get("value") if isinstance(humidity, dict) else None
                # Append standardized row for Spark DataFrame creation
                rows.append(Row(
                    raw_location=location,
                    start_time=str(period.get("startTime", "")),
                    temperature_raw=str(period.get("temperature", "")),
                    is_daytime=bool(period.get("isDaytime", False)),
                    wind_speed_raw=str(period.get("windSpeed", "")),
                    dewpoint_raw=float(dewpoint_val) if dewpoint_val is not None else 0.0,
                    precip_prob_raw=int(precip_val) if precip_val is not None else 0,
                    humidity_raw=int(humidity_val) if humidity_val is not None else 0,
                    weather_description=str(period.get("shortForecast", "")),
                ))
        except Exception as exc:
            print(f"[WARN] Skipping {key}: {exc}")
            skipped += 1
    return rows, skipped


def process_weather_to_trusted(spark: SparkSession):
    """
    Orchestrates the ingestion, cleaning, and governance pipeline for weather data.
    Maps raw locations to boroughs, enforces strict quality thresholds, and 
    handles record deduplication before final ingestion into MongoDB.
    Saves the rejected anomalies for Data Governance and duplicates in MinIO for further analysis.
    """

    print(f"[WEATHER] Listing files from: {WEATHER_PREFIX}")
    keys = [k for k in list_objects(config.LANDING_BUCKET, WEATHER_PREFIX) if k.endswith(".json")]
    print(f"[WEATHER] Found {len(keys)} JSON files.")

    rows, skipped = _load_raw_rows(keys)
    if not rows:
        print(f"[WEATHER] No records loaded ({skipped} files skipped).")
        return
    # Define schema and initial transformation to standardize units
    schema = StructType([
        StructField("raw_location",      StringType(),  False),
        StructField("start_time",        StringType(),  False),
        StructField("temperature_raw",   StringType(),  False),
        StructField("is_daytime",        BooleanType(), True),
        StructField("wind_speed_raw",    StringType(),  False),
        StructField("dewpoint_raw",      DoubleType(),  True),
        StructField("precip_prob_raw",   IntegerType(), True),
        StructField("humidity_raw",      IntegerType(), True),
        StructField("weather_description", StringType(), True),
    ])

    df = spark.createDataFrame(rows, schema=schema)
    # Map geographic locations to official NYC Boroughs and clean formats
    df_transformed = df.withColumn(
        "borough",
        when(col("raw_location").rlike("(?i)harlem|upper_east|upper_west|manhattan"), "MANHATTAN")
        .when(col("raw_location").rlike("(?i)brooklyn"), "BROOKLYN")
        .when(col("raw_location").rlike("(?i)queens"), "QUEENS")
        .when(col("raw_location").rlike("(?i)bronx"), "BRONX")
        .when(col("raw_location").rlike("(?i)staten"), "STATEN ISLAND")
        .otherwise("UNKNOWN")
    ).withColumn(
        "station_name", upper(col("raw_location"))
    ).withColumn(
        "crash_date", split(col("start_time"), "T")[0]
    ).withColumn(
        "temperature", regexp_extract(col("temperature_raw"), r"(\d+)", 1).cast("double")
    ).withColumn(
        "wind_speed_mph", regexp_extract(col("wind_speed_raw"), r"(\d+)", 1).cast("int")
    ).withColumn(
        "dewpoint_celsius", col("dewpoint_raw")
    ).withColumn(
        "precip_prob", col("precip_prob_raw")
    ).withColumn(
        "humidity", col("humidity_raw")
    ).drop("raw_location", "start_time", "temperature_raw", "wind_speed_raw",
           "dewpoint_raw", "precip_prob_raw", "humidity_raw")
    
    # APPLY DATA GOVERNANCE for Data Quality Validation
    # Enforce meteorological logic constraints to ensure only 'Trusted' data passes through
    df_validated = df_transformed.withColumn(
        "is_valid",
        # Extreme temperatures — NWS API reports in Fahrenheit; thresholds cover NYC's historical extremes
        when((col("temperature") > 110) | (col("temperature") < -22), lit(False))
        # Invalid percentages (humidity and precipitation probability must be 0-100)
        .when((col("humidity") < 0) | (col("humidity") > 100), lit(False))
        .when((col("precip_prob") < 0) | (col("precip_prob") > 100), lit(False))
        # Negative wind speed
        .when(col("wind_speed_mph") < 0, lit(False))
        # Meteorological inconsistency (dew point cannot exceed air temperature)
        .when(col("dewpoint_celsius") > col("temperature"), lit(False))
        .otherwise(lit(True))
    ).withColumn(
        "rejection_reason",
        when((col("temperature") > 110) | (col("temperature") < -22), lit("Gov: Temp out of bounds [-22, 110] °F"))
        .when((col("humidity") < 0) | (col("humidity") > 100), lit("Gov: Humidity must be 0-100"))
        .when((col("precip_prob") < 0) | (col("precip_prob") > 100), lit("Gov: Precip prob must be 0-100"))
        .when(col("wind_speed_mph") < 0, lit("Gov: Negative wind speed"))
        .when(col("dewpoint_celsius") > col("temperature"), lit("Gov: Dewpoint exceeds temperature"))
        .otherwise(lit("Valid"))
    )

    # Separate clean and wrong data:
    df_clean = df_validated.filter(col("is_valid") == True).drop("is_valid", "rejection_reason")# we drop the unnecessary columns
    df_rejected = df_validated.filter(col("is_valid") == False)

    # Save the lineage of the rejected records using the anaomalies suffix
    _save_skipped(df_rejected, "WEATHER_GOVERNANCE_REJECTS", f"{config.TRUSTED_WEATHER_SKIPPED_PREFIX}anomalies_")

    # Drop and save the rejected deduplicates into MinIO 
    df_deduped = df_clean.dropDuplicates(["station_name", "crash_date", "is_daytime"])
    df_dupes = df_clean.exceptAll(df_deduped)
    _save_skipped(df_dupes, "WEATHER", config.TRUSTED_WEATHER_SKIPPED_PREFIX)

    records = [row.asDict() for row in df_deduped.collect()]
    print(f"[WEATHER] Processed {len(records)} records ({skipped} files skipped).")

    # Insert into MongoDB
    client = get_mongo_client()
    collection = client[config.MONGO_DB][config.TRUSTED_WEATHER_COLLECTION]
    ops = [
        UpdateOne(
            {"station_name": r["station_name"], "crash_date": r["crash_date"], "is_daytime": r["is_daytime"]},
            {"$set": r},
            upsert=True,
        )
        for r in records
    ]
    result = collection.bulk_write(ops, ordered=False)
    client.close()
    print(f"[WEATHER] Upserted {result.upserted_count} new, modified {result.modified_count} existing records into '{config.TRUSTED_WEATHER_COLLECTION}'.")


def main():
    """Initialize Spark and run the weather data trusted zone pipeline."""
    spark = (
        SparkSession.builder
        .appName("TrustedZone-Weather")
        .master("local[2]")
        .getOrCreate()
    )
    try:
        process_weather_to_trusted(spark)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
