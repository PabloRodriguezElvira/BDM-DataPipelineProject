import base64
import numpy as np
import cv2
import random
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, when, upper, date_format, window, max as spark_max, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import src.common.global_variables as config


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
        
        if net_local is None:
            net_local = cv2.dnn.readNetFromCaffe(config.PROTOTXT_PATH, config.MODEL_PATH)# we add the paths from CV model
        
        # 1 Reconstruct image from Base64
        img_bytes = base64.b64decode(base64_str)
        np_arr = np.frombuffer(img_bytes, np.uint8)
        img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        
        # 2 Prepare the image for the Neural Network 
        blob = cv2.dnn.blobFromImage(cv2.resize(img, (300, 300)), 0.007843, (300, 300), 127.5)
        net_local.setInput(blob)
        
        # 3 Run the forward pass (Inference) to get predictions
        detections = net_local.forward()
        
        vehicle_count = 0
        
        # 4 Iterate over all object detections found in the image
        for i in range(detections.shape[2]):
            confidence = detections[0, 0, i, 2]
            
            if confidence > 0.1:
                class_id = int(detections[0, 0, i, 1])
                # If the detected object is a vehicle, increase the counter
                if class_id in VEHICLE_CLASSES:
                    vehicle_count += 1
                    
        return vehicle_count
    except Exception:
        return 0

# Convert the Python function into a Spark User Defined Function (UDF)
cv_udf = udf(process_cv_image, IntegerType())



# Dictionary mapping each borough to 4 random streets
BOROUGH_STREETS = {
    "MANHATTAN": ["5th Avenue", "Broadway", "Times Square Blvd", "Wall Street"],
    "BROOKLYN": ["Flatbush Avenue", "Atlantic Avenue", "Bedford Avenue", "Fulton Street"],
    "QUEENS": ["Queens Boulevard", "Astoria Boulevard", "Northern Boulevard", "Roosevelt Avenue"],
    "STATEN ISLAND": ["Richmond Terrace", "Hylan Boulevard", "Victory Boulevard", "Bay Street"]
}

def get_random_street(borough_name):
    """Returns a random street from the given borough dictionary."""
    if borough_name in BOROUGH_STREETS:
        return random.choice(BOROUGH_STREETS[borough_name])
    return "Unknown Street"

# Register the Python function as a Spark UDF
street_udf = udf(get_random_street, StringType())



# ---------------------------------------------------------
# NEW: UNIFIED SINK (CONSOLE + TXT FILES + IMAGES) WITH CACHE
# ---------------------------------------------------------
def process_and_save_alerts(df_batch, batch_id):
    """
    Saves the dataframe in RAM to avoid executing the heavy AI twice.
    Prints to console and creates nested folders (Alerts/Borough/Time) 
    containing the .txt report and the .jpg image.
    """
    # 1. Freeze the table in RAM (Super important to prevent crashes!)
    df_batch.persist()

    # 2. Print to console (dropping image_data so it doesn't flood the terminal with Base64 text)
    print(f"\n-------------------------------------------")
    print(f"Batch: {batch_id}")
    print(f"-------------------------------------------")
    df_batch.drop("image_data").show(truncate=False)
    
    # 3. Collect data for the files
    alerts_list = df_batch.collect()
    
    # 4. Clear the RAM so the next Batch doesn't explode
    df_batch.unpersist()
    
    # If there are no alerts in this batch, we stop here
    if not alerts_list:
        return
        
    for row in alerts_list:
        borough = row["borough"]
        street = row["street_name"]
        vehicles = row["max_vehicles"]
        alert_time = row["alert_time"]
        camera_id = row["camera_id_original"]
        image_b64 = row["image_data"]
        
        # Format time safely for folder names (no colons or spaces)
        safe_time = alert_time.replace(" ", "_").replace(":", "-")
        
        # Create nested folder structure: Alerts/Borough/Time
        alert_dir = os.path.join("Alerts", borough, safe_time)
        os.makedirs(alert_dir, exist_ok=True)
        
        # 1. Save the .txt alert file
        alert_message = (
            f"---------------------------------------------------\n"
            f"[CRITICAL TRAFFIC CONGESTION ALERT]\n"
            f"---------------------------------------------------\n"
            f"Timestamp : {alert_time}\n"
            f"Borough   : {borough}\n"
            f"Street    : {street}\n"
            f"Camera ID : {camera_id}\n"
            f"Details   : Heavy traffic detected. A total of {vehicles} vehicles \n"
            f"            were counted in the recent 15-second window.\n"
            f"---------------------------------------------------\n"
        )
        
        txt_filepath = os.path.join(alert_dir, f"ALERT_{camera_id}.txt")
        try:
            with open(txt_filepath, "w", encoding="utf-8") as file:
                file.write(alert_message)
        except Exception as e:
            print(f"Error writing TXT file: {e}")

        # 2. Decode Base64 and save the .jpg image
        if image_b64:
            try:
                img_bytes = base64.b64decode(image_b64)
                img_filepath = os.path.join(alert_dir, f"PHOTO_{camera_id}.jpg")
                with open(img_filepath, "wb") as img_file:
                    img_file.write(img_bytes)
            except Exception as e:
                print(f"Error saving Image file: {e}")



# Spark streaming pipeline 
def run_streaming_alerts():
    print("Starting Spark Streaming (Hot Path) with MobileNet-SSD AI and Time Windows.")

    # 1. THE ENGINE: Initialize Spark (2GB RAM to prevent Docker crashes)
    spark = SparkSession.builder \
        .appName("HotPath-CV-Alerts") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
        
    # NY time to make it realistic
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    
    # Silence yellow warnings
    spark.sparkContext.setLogLevel("ERROR")

    # JSON Schema
    json_schema = StructType([
        StructField("video_id", StringType(), True),
        StructField("image_data", StringType(), True),
        StructField("timestamp", DoubleType(), True)
    ])

    # Connect to kafka limiting to 10 seconds offsets
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_SERVER) \
        .option("subscribe", config.UNSTRUCTURED_IMAGE_TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 10) \
        .load()

    # Extract data from kafka
    df_parsed = df_kafka.select(
        from_json(col("value").cast("string"), json_schema).alias("data")
    ).select("data.*")

    # Convert to timestamp
    df_parsed = df_parsed.withColumn("event_time", col("timestamp").cast("timestamp"))

    # Apply the AI algorithm
    df_base_cv = df_parsed.withColumn("raw_count", cv_udf(col("image_data")))

    # Base simulation logic: sum 19 to the raw count
    df_cv = df_base_cv.withColumn("vehicle_count", col("raw_count") + 19)

    # Add borough attribute based on camera ID
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

    # 15 seconds tumbling window grouping
    df_windowed = df_enriched \
        .withWatermark("event_time", "15 seconds") \
        .groupBy(
            window(col("event_time"), "15 seconds"),
            col("camera_id_original"),
            col("borough")
        ) \
        .agg(
            # PACK: (vehicles, image). max() will evaluate based on the first item (vehicles)
            spark_max(struct(col("vehicle_count"), col("image_data"))).alias("max_combo")
        )

    # UNPACK: Extract the vehicles and the exact image that triggered that max count
    df_windowed = df_windowed \
        .withColumn("max_vehicles", col("max_combo.vehicle_count")) \
        .withColumn("image_data", col("max_combo.image_data")) \
        .withColumn("alert_time", date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss"))

    # Apply the random street UDF to the borough column
    df_windowed_streets = df_windowed.withColumn("street_name", street_udf(col("borough")))

    # Alert >= 20 vehicles. Select all necessary columns including image_data
    df_alerts = df_windowed_streets.filter(col("max_vehicles") >= 20) \
                                   .select("alert_time", "borough", "street_name", "camera_id_original", "max_vehicles", "image_data")

    print("Alert system activated. Displaying grouped traffic alerts (>=20 vehicles) in New York. \n" \
    "Press Ctrl + C to stop")

    # ONE SINGLE UNIFIED QUERY USING FOREACHBATCH
    query = df_alerts.writeStream \
        .foreachBatch(process_and_save_alerts) \
        .outputMode("update") \
        .start()

    # Keep streaming alive
    try:
        import time
        while query.isActive:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping streaming query manually.")
        query.stop()

if __name__ == "__main__":
    run_streaming_alerts()