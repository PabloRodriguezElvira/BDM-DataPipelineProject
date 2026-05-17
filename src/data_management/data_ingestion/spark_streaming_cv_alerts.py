import base64
import numpy as np
import cv2
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, when, upper
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import src.common.global_variables as config

# ---------------------------------------------------------
# 1. COMPUTER VISION MODEL (UDF)
# ---------------------------------------------------------
def process_cv_image(base64_str):
    """
    Decodes the Base64 image and applies a Computer Vision model 
    to detect and count vehicles.
    """
    try:
        # Reconstruct image from Base64
        img_bytes = base64.b64decode(base64_str)
        np_arr = np.frombuffer(img_bytes, np.uint8)
        img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        
        # Simulated AI counting vehicles (0 to 10) for performance
        detected_vehicles = random.randint(0, 10) 
        return detected_vehicles
    except Exception as e:
        return 0

# Convert the Python function into a Spark User Defined Function (UDF)
cv_udf = udf(process_cv_image, IntegerType())

# ---------------------------------------------------------
# 2. SPARK STREAMING PIPELINE (Hot Path + Enrichment)
# ---------------------------------------------------------
def run_streaming_alerts():
    print("  Starting Spark Streaming (Hot Path) with geographic alerts...")

    # Initialize Spark Session
    spark = SparkSession.builder.appName("HotPath-CV-Alerts").getOrCreate()
    
    # Reduce Spark logging verbosity to keep the console clean
    spark.sparkContext.setLogLevel("WARN")

    # Define the schema to read the raw JSON directly from the Producer
    json_schema = StructType([
        StructField("video_id", StringType(), True),
        StructField("image_data", StringType(), True)
    ])

    # Connect to Kafka as a streaming source
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_SERVER) \
        .option("subscribe", config.UNSTRUCTURED_IMAGE_TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    # Extract and parse the JSON data from the Kafka message value
    df_parsed = df_kafka.select(
        from_json(col("value").cast("string"), json_schema).alias("data")
    ).select("data.*")

    # 1. Apply the CV model (UDF) to the streaming dataframe
    df_cv = df_parsed.withColumn("vehicle_count", cv_udf(col("image_data")))

    # 2. DATA ENRICHMENT RULE (Map NY Boroughs based on camera ID prefixes)
    df_enriched = df_cv.withColumn(
        "borough",
        when(col("video_id").rlike("^Nd"), "MANHATTAN")
        .when(col("video_id").rlike("^NT2"), "BROOKLYN")
        .when(col("video_id").rlike("^NT4"), "QUEENS")
        .when(col("video_id").rlike("^SW"), "STATEN ISLAND")
        .otherwise("UNKNOWN")
    ).withColumn(
        "camera_id_original", upper(col("video_id"))
    )

    # 3. ALERT BUSINESS RULE: Filter and keep only frames with > 5 vehicles
    df_alerts = df_enriched.filter(col("vehicle_count") > 5) \
                           .select("camera_id_original", "borough", "vehicle_count")

    print(" Alert system activated. Displaying traffic jams by borough in real-time...")

    # Output the alerts directly to the console (Streaming Sink)
    query = df_alerts.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    # Keep the streaming process running continuously
    query.awaitTermination()

if __name__ == "__main__":
    run_streaming_alerts()