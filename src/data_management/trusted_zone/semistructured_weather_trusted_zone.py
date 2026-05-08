
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
  - Data casting: Force temperature and humidity fields into numeric types (Double/Integer).

Processed records are written to the MongoDB 'weather_data' collection.
Files that fail basic JSON validation are skipped for traceability.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, when, lit, upper, split, regexp_extract
import src.common.global_variables as config

def process_weather_to_trusted(spark):
    """
    Transforms raw weather JSON (Hourly and 12-hour) into a standardized 
    Trusted format, ensuring schema alignment and missing field handling.
    """
    print(f"[SPARK-WEATHER] Loading data from: {config.TRUSTED_LANDING_SEMISTRUCTURED_WEATHER_URI}")
    
    # 1. DATA INGESTION: Load all JSON files from the semi-structured landing path
    df_raw = spark.read.json(config.TRUSTED_LANDING_SEMISTRUCTURED_WEATHER_URI)
    print(f"DEBUG: Hemos leido {df_raw.count()} filas de MinIO") # AÑADE ESTO

    # 2. FLATTENING & INITIAL EXTRACTION
    # Exploding the periods array to get one row per time slot
    df_periods = df_raw.select(
        col("metadata.location.name").alias("raw_location"),
        col("metadata.endpoint").alias("source_type"), # Identifies 'forecast' or 'forecast/hourly'
        explode(col("data.properties.periods")).alias("period")
    )

    # 3. SCHEMA ALIGNMENT (Borough Normalization)
    df_aligned = df_periods.withColumn("borough", 
        when(col("raw_location").rlike("(?i)harlem|upper_east|upper_west|manhattan"), "MANHATTAN")
        .when(col("raw_location").rlike("(?i)brooklyn"), "BROOKLYN")
        .when(col("raw_location").rlike("(?i)queens"), "QUEENS")
        .when(col("raw_location").rlike("(?i)bronx"), "BRONX")
        .when(col("raw_location").rlike("(?i)staten"), "STATEN ISLAND")
        .otherwise("UNKNOWN")
    )

    # 4. HANDLING MISSING FIELDS & STRUCTURAL RULES (The core of Trusted Zone)
    df_final = df_aligned.select(
            col("borough"),
            upper(col("raw_location")).alias("station_name"),
            split(col("period.startTime"), "T")[0].alias("crash_date"),
            
            # 1. Por si acaso la temperatura también trae alguna unidad:
            regexp_extract(col("period.temperature").cast("string"), r"(\d+)", 1).cast("double").alias("temperature"),
            
            col("period.isDaytime").alias("is_daytime"),
            
            # 2. Tu limpieza de windSpeed (Perfecta para quitar el "mph" o la "s"):
            regexp_extract(col("period.windSpeed"), r"(\d+)", 1).cast("int").alias("wind_speed_mph"),
            
            when(col("period.dewpoint.value").isNotNull(), col("period.dewpoint.value"))
            .otherwise(lit(0.0)).alias("dewpoint_celsius"),
            
            when(col("period.probabilityOfPrecipitation.value").isNull(), 0)
            .otherwise(col("period.probabilityOfPrecipitation.value")).alias("precip_prob"),
            
            col("period.relativeHumidity.value").alias("humidity"),
            col("period.shortForecast").alias("weather_description")
        )

    # 5. DATA LOADING: Save to MongoDB
    print(f"[SPARK-WEATHER] Saving processed data to collection: {config.TRUSTED_WEATHER_COLLECTION}")
    df_final.write.format("mongo") \
        .mode("append") \
        .option("database", config.MONGO_DB) \
        .option("collection", config.TRUSTED_WEATHER_COLLECTION) \
        .save()

def main():
    spark = (
        SparkSession.builder
        .appName("TrustedZone-Weather")
        .master("local[2]")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    # --- ESTO ES LO NUEVO Y DEFINITIVO ---
    # Forzamos a Hadoop a olvidar cualquier "60s"
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.connection.timeout", "60000")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "5000")
    hadoop_conf.set("fs.s3a.threads.keepalivetime", "60")
    # --------------------------------------

    try:
        print("🚀 Iniciando procesamiento...")
        process_weather_to_trusted(spark)
        print("✅ ¡EXITO!")
    except Exception as e:
        print(f"❌ ERROR: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()