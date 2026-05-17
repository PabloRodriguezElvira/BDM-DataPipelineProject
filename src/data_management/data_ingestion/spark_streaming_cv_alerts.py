import base64
import numpy as np
import cv2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, when, upper, current_timestamp, date_format, window, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import src.common.global_variables as config

# ---------------------------------------------------------
# 1. COMPUTER VISION MODEL (MobileNet-SSD via OpenCV DNN)
# ---------------------------------------------------------
# Paths to the MobileNet-SSD files
PROTOTXT_PATH = "src/models/MobileNetSSD_deploy.prototxt"
MODEL_PATH = "src/models/MobileNetSSD_deploy.caffemodel"

# This variable will hold the model locally on each Spark worker node
net_local = None

# MobileNet-SSD (PASCAL VOC) class IDs for all types of vehicles:
# 2: bicycle, 6: bus, 7: car, 14: motorbike
VEHICLE_CLASSES = {2, 6, 7, 14}

def process_cv_image(base64_str):
    """
    Decodes the Base64 image and uses MobileNet-SSD to detect all types 
    of vehicles (cars, buses, bikes, motorbikes) efficiently on CPU.
    """
    global net_local
    try:
        if not base64_str: 
            return 0
        
        # FIX: Load the network inside the worker node only once to avoid Pickling/Serialization errors
        if net_local is None:
            net_local = cv2.dnn.readNetFromCaffe(PROTOTXT_PATH, MODEL_PATH)
        
        # 1. Reconstruct image from Base64
        img_bytes = base64.b64decode(base64_str)
        np_arr = np.frombuffer(img_bytes, np.uint8)
        img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        
        # 2. Prepare the image for the Neural Network 
        # MobileNet requires images to be resized to 300x300 pixels
        blob = cv2.dnn.blobFromImage(cv2.resize(img, (300, 300)), 0.007843, (300, 300), 127.5)
        net_local.setInput(blob)
        
        # 3. Run the forward pass (Inference) to get predictions
        detections = net_local.forward()
        
        vehicle_count = 0
        
        # 4. Iterate over all object detections found in the image
        for i in range(detections.shape[2]):
            confidence = detections[0, 0, i, 2]
            
            # Filter out weak detections (Confidence must be > 40%)
            if confidence > 0.4:
                class_id = int(detections[0, 0, i, 1])
                # If the detected object is a vehicle, increase the counter
                if class_id in VEHICLE_CLASSES:
                    vehicle_count += 1
                    
        return vehicle_count
    except Exception:
        return 0

# Convert the Python function into a Spark User Defined Function (UDF)
cv_udf = udf(process_cv_image, IntegerType())

# ---------------------------------------------------------
# 2. SPARK STREAMING PIPELINE (Hot Path + Enrichment)
# ---------------------------------------------------------
def run_streaming_alerts():
    print("Starting Spark Streaming (Hot Path) with MobileNet-SSD AI and Time Windows.")

    # Initialize Spark Session (Clean local session for spark-submit deployment)
    spark = SparkSession.builder \
        .appName("HotPath-CV-Alerts") \
        .getOrCreate()
        
    # We use NY time globally in Spark
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    
    # Reduce Spark logging verbosity to keep the console clean
    spark.sparkContext.setLogLevel("WARN")

    # Define the schema: Includes the 'timestamp' sent by the Producer
    json_schema = StructType([
        StructField("video_id", StringType(), True),
        StructField("image_data", StringType(), True),
        StructField("timestamp", DoubleType(), True) # <-- Timestamp needed for windows
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

    # Convert the Unix timestamp (Double) from the Producer to a Spark Timestamp type
    df_parsed = df_parsed.withColumn("event_time", col("timestamp").cast("timestamp"))

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

    # ---------------------------------------------------------
    # 3. ANTI-SPAM RULE: 15-SECOND TUMBLING WINDOWS
    # ---------------------------------------------------------
    # Group events in 15-second blocks to avoid alert spam
    df_windowed = df_enriched \
        .withWatermark("event_time", "15 seconds") \
        .groupBy(
            window(col("event_time"), "15 seconds"),
            col("camera_id_original"),
            col("borough")
        ) \
        .agg(spark_max("vehicle_count").alias("max_vehicles"))

    # Extract the start time of the window and format it for the console
    df_windowed = df_windowed.withColumn("alert_time", date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss"))

    # 4. FINAL ALERT FILTER: Keep only windows where max vehicles > 3 (For testing)
    df_alerts = df_windowed.filter(col("max_vehicles") > 3) \
                           .select("alert_time", "borough", "camera_id_original", "max_vehicles")

    print(" Alert system activated. Displaying grouped traffic alerts (>3 vehicles) in New York.")

    # Output the alerts directly to the console (Streaming Sink)
    query = df_alerts.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .outputMode("update") \
        .start()

    # 🟢 CAMBIO AQUÍ: Forzamos a Spark a mantener vivo el streaming pase lo que pase
    try:
        import time
        while query.isActive:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping streaming query manually...")
        query.stop()

if __name__ == "__main__":
    run_streaming_alerts()