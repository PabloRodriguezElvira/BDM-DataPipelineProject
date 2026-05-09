"""
Trusted Zone pipeline for unstructured metadata (camera traffic reports).

Reads JSON metadata files generated from image processing models located in
the Landing Zone persistent storage (MinIO). It handles a hierarchical
directory structure (Nd_O, SW_O, NT4, NT2, etc.), applies schema standardization
and data cleansing using Spark, and stores the results in MongoDB.

Transformations applied
-----------------------
Camera Metadata files (JSON):
  - Recursive Ingestion: Automatically scan and process multiple camera
    subdirectories for unified processing.
  - Flattening: Convert nested detection objects (avg_per_frame) into an
    accessible tabular format.
  - Schema alignment: Standardize camera identifiers by prefix mapping
    to official NYC Borough names (MANHATTAN, QUEENS, BROOKLYN, STATEN ISLAND).
  - Handling missing fields: Detect absent vehicle classes (e.g., e-Rickshaw)
    and assign default values (0.0) to ensure a consistent schema.
  - Structural rules: Cast detection averages to Float and standardize
    date formats for cross-dataset joining.

Accepted records are written to the MongoDB 'camera_aggregates' collection.
"""

import json

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, when, upper, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

import src.common.global_variables as config
from src.common.minio_manager import list_objects, read_object_bytes
from src.common.mongo_client import get_mongo_client

CAMERAS_PREFIX = f"{config.LANDING_PERSISTENT_PATH}unstructured/images/metadata/"

EXPECTED_OBJECTS = ["MotorBike", "Pedestrian", "Bike", "LMV", "Auto", "LCV", "e-Rickshaw"]


def _load_raw_rows(keys: list[str]) -> tuple[list[Row], int]:
    """Read JSON files from MinIO and return one Row per camera window."""
    rows = []
    skipped = 0
    for key in keys:
        raw = read_object_bytes(config.LANDING_BUCKET, key)
        if raw is None:
            skipped += 1
            continue
        try:
            data = json.loads(raw)
            avg = data.get("avg_per_frame", {})
            rows.append(Row(
                camera_id=str(data.get("camera_id", "")),
                crash_date=str(data.get("date", "")),
                total_frames_processed=int(data.get("total_frames_processed", 0)),
                MotorBike=float(avg.get("MotorBike", 0.0)),
                Pedestrian=float(avg.get("Pedestrian", 0.0)),
                Bike=float(avg.get("Bike", 0.0)),
                LMV=float(avg.get("LMV", 0.0)),
                Auto=float(avg.get("Auto", 0.0)),
                LCV=float(avg.get("LCV", 0.0)),
                eRickshaw=float(avg.get("e-Rickshaw", 0.0)),
            ))
        except Exception as exc:
            print(f"[WARN] Skipping {key}: {exc}")
            skipped += 1
    return rows, skipped


def process_cameras_to_trusted(spark: SparkSession):
    print(f"[CAMERAS] Scanning MinIO path: {CAMERAS_PREFIX}")
    keys = [k for k in list_objects(config.LANDING_BUCKET, CAMERAS_PREFIX) if k.endswith(".json")]
    print(f"[CAMERAS] Found {len(keys)} JSON files.")

    rows, skipped = _load_raw_rows(keys)
    if not rows:
        print(f"[CAMERAS] No records loaded ({skipped} files skipped).")
        return

    schema = StructType([
        StructField("camera_id",               StringType(),  False),
        StructField("crash_date",              StringType(),  True),
        StructField("total_frames_processed",  IntegerType(), True),
        StructField("MotorBike",               DoubleType(),  True),
        StructField("Pedestrian",              DoubleType(),  True),
        StructField("Bike",                    DoubleType(),  True),
        StructField("LMV",                     DoubleType(),  True),
        StructField("Auto",                    DoubleType(),  True),
        StructField("LCV",                     DoubleType(),  True),
        StructField("eRickshaw",               DoubleType(),  True),
    ])

    df = spark.createDataFrame(rows, schema=schema)

    df_transformed = df.withColumn(
        "borough",
        when(col("camera_id").rlike("^Nd"), "MANHATTAN")
        .when(col("camera_id").rlike("^NT2"), "BROOKLYN")
        .when(col("camera_id").rlike("^NT4"), "QUEENS")
        .when(col("camera_id").rlike("^SW"), "STATEN ISLAND")
        .otherwise("UNKNOWN")
    ).withColumn(
        "camera_id_original", upper(col("camera_id"))
    ).withColumnRenamed("eRickshaw", "e-Rickshaw")

    records = [row.asDict() for row in df_transformed.collect()]
    print(f"[CAMERAS] Processed {len(records)} records ({skipped} files skipped).")

    client = get_mongo_client()
    client[config.MONGO_DB][config.TRUSTED_CAMERA_COLLECTION].insert_many(records)
    client.close()
    print(f"[CAMERAS] Inserted {len(records)} records into '{config.TRUSTED_CAMERA_COLLECTION}'.")


def main():
    spark = (
        SparkSession.builder
        .appName("TrustedZone-Camera-Metadata")
        .master("local[2]")
        .getOrCreate()
    )
    try:
        process_cameras_to_trusted(spark)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
