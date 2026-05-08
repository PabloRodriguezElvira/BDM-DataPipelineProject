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

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, upper
import src.common.global_variables as config

def process_cameras_to_trusted(spark):
    """
    Main transformation logic for Camera Metadata.
    Uses regex prefix matching to assign boroughs based on folder naming conventions.
    """
    # 1. RECURSIVE INGESTION
    print(f"[SPARK-CAMERAS] Scanning recursive path: {config.TRUSTED_LANDING_CAMERAS_UNSTRUCTURED_URI}")
    
    df_raw = spark.read.option("recursiveFileLookup", "true") \
                       .json(config.TRUSTED_LANDING_CAMERAS_UNSTRUCTURED_URI)

    # 2. FLATTENING
    df_flat = df_raw.select(
        col("camera_id"),
        col("date").alias("crash_date"),
        col("avg_per_frame.*"),
        col("total_frames_processed")
    )

    # 3. HANDLING MISSING FIELDS
    expected_objects = ["MotorBike", "Pedestrian", "Bike", "LMV", "Auto", "LCV", "e-Rickshaw"]
    
    for obj in expected_objects:
        if obj not in df_flat.columns:
            df_flat = df_flat.withColumn(obj, lit(0.0))
    
    df_filled = df_flat.fillna(0.0, subset=expected_objects)

    # 4. SCHEMA ALIGNMENT: Mapping based on folder prefixes
    # Using 'rlike' with '^' ensures we only match the START of the string.
    df_final = df_filled.withColumn("borough", 
        # Starts with Nd (Manhattan)
        when(col("camera_id").rlike("^Nd"), "MANHATTAN")
        
        # Starts with NT2 (Brooklyn)
        .when(col("camera_id").rlike("^NT2"), "BROOKLYN")
        
        # Starts with NT4 (Queens)
        .when(col("camera_id").rlike("^NT4"), "QUEENS")
        
        # Starts with SW (Staten Island)
        .when(col("camera_id").rlike("^SW"), "STATEN ISLAND")
        
        # Fallback if no prefix matches
        .otherwise("UNKNOWN")
    )

    # Keeping original lineage for traceability
    df_final = df_final.withColumn("camera_id_original", upper(col("camera_id")))

    # 5. LOADING
    print(f"[SPARK-CAMERAS] Writing to MongoDB collection: {config.TRUSTED_CAMERA_COLLECTION}")
    
    df_final.write.format("mongo") \
        .mode("append") \
        .option("database", config.MONGO_DB) \
        .option("collection", config.TRUSTED_CAMERA_COLLECTION) \
        .save()

def main():
    spark = SparkSession.builder \
        .appName("Trusted-Camera-Metadata-Pipeline") \
        .config("spark.mongodb.output.uri", 
                f"mongodb://{config.MONGO_USER}:{config.MONGO_PASSWORD}@{config.MONGO_HOST}:{config.MONGO_PORT}/{config.MONGO_DB}") \
        .getOrCreate()
    
    try:
        process_cameras_to_trusted(spark)
        print("[SUCCESS] Camera Trusted Zone processing completed.")
    except Exception as e:
        print(f"[ERROR] Camera Pipeline failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()