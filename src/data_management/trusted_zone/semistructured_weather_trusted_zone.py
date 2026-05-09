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
"""

import json

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, when, lit, upper, split, regexp_extract
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, DoubleType, IntegerType
)

import src.common.global_variables as config
from src.common.minio_manager import list_objects, read_object_bytes
from src.common.mongo_client import get_mongo_client

WEATHER_PREFIX = f"{config.LANDING_PERSISTENT_PATH}semi_structured/data/"


def _load_raw_rows(keys: list[str]) -> tuple[list[Row], int]:
    """Read JSON files from MinIO and return one Row per forecast period."""
    rows = []
    skipped = 0
    for key in keys:
        raw = read_object_bytes(config.LANDING_BUCKET, key)
        if raw is None:
            skipped += 1
            continue
        try:
            data = json.loads(raw)
            location = data.get("metadata", {}).get("location", {}).get("name", "")
            periods = data.get("data", {}).get("properties", {}).get("periods", [])
            for period in periods:
                dewpoint = period.get("dewpoint", {})
                dewpoint_val = dewpoint.get("value") if isinstance(dewpoint, dict) else None

                precip = period.get("probabilityOfPrecipitation", {})
                precip_val = precip.get("value") if isinstance(precip, dict) else None

                humidity = period.get("relativeHumidity", {})
                humidity_val = humidity.get("value") if isinstance(humidity, dict) else None

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
    print(f"[WEATHER] Listing files from: {WEATHER_PREFIX}")
    keys = [k for k in list_objects(config.LANDING_BUCKET, WEATHER_PREFIX) if k.endswith(".json")]
    print(f"[WEATHER] Found {len(keys)} JSON files.")

    rows, skipped = _load_raw_rows(keys)
    if not rows:
        print(f"[WEATHER] No records loaded ({skipped} files skipped).")
        return

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

    records = [row.asDict() for row in df_transformed.collect()]
    print(f"[WEATHER] Processed {len(records)} records ({skipped} files skipped).")

    client = get_mongo_client()
    client[config.MONGO_DB][config.TRUSTED_WEATHER_COLLECTION].insert_many(records)
    client.close()
    print(f"[WEATHER] Inserted {len(records)} records into '{config.TRUSTED_WEATHER_COLLECTION}'.")


def main():
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
