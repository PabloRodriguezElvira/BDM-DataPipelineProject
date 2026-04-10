import json
import pandas as pd
from deltalake.writer import write_deltalake
from minio import Minio
import src.common.global_variables as config
def process_unstructured_image_metadata_to_delta(client: Minio, object_name: str):
    """    
    This function handles the 'Warm Path' by:
    1. Downloading the raw JSON from the landing bucket.
    2. Flattening nested vehicle detections into a tabular format.
    3. Appending the data to a Delta Table using Schema Evolution (merge).
    """
    # Download the raw JSON object from MinIO
    response = client.get_object(config.LANDING_BUCKET, object_name)
    try:
        content = response.read().decode("utf-8")
        data = json.loads(content)
    finally:
        response.close()
        response.release_conn()

    # Data Transformation & Flattening
    # Map top-level keys and merge with nested detection counts
    row = {
        "camera_id": data.get("camera_id"),
        "timestamp": data.get("date"),
        "total_frames": data.get("total_frames_processed"),
        "source_file": object_name
    }
    
    # Dynamically expand vehicle counts (car, truck, etc.) into individual columns
    # This ensures flexibility if new object classes are detected in the future
    if "avg_per_frame" in data and isinstance(data["avg_per_frame"], dict):
        row.update(data["avg_per_frame"])
    
    # Create a DataFrame and fill missing values with 0.0 for schema consistency
    df = pd.DataFrame([row]).fillna(0.0)

    # Dynamic Path Setup
    # The Delta Table is identified by its URI (partitioned by camera_id)
    camera_id = data.get("camera_id", "unknown_camera")
    delta_path = f"{config.LANDING_PERSISTENT_PATH}unstructured/images/delta/{camera_id}/"
    uri = f"s3://{config.LANDING_BUCKET}/{delta_path}"

    # 4. Storage Configuration
    # Defined internally to keep landing_zone.py logic clean and decoupled
    storage_options = {
        "AWS_ACCESS_KEY_ID": config.MINIO_ROOT_USER,
        "AWS_SECRET_ACCESS_KEY": config.MINIO_ROOT_PASSWORD,
        "AWS_ENDPOINT_URL": config.MINIO_ENDPOINT_URL,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    # Write to Delta Lake
    # 'mode=append' adds new rows, 'schema_mode=merge' allows for new columns
    write_deltalake(
        uri, 
        df, 
        mode="append", 
        schema_mode="merge",
        storage_options=storage_options,
    )
    
    # Successful integration log
    print(f"[DELTA SUCCESS] Integrated {object_name} into analytical table at: {delta_path}")
def process_metadata_to_delta(metadata_payload: dict, delta_folder: str):
    """
    Generic function to convert any metadata dictionary (Audio, Text, Weather)
    into a row within a Delta Table.
    """
    # Flatten the nested JSON into a single-row DataFrame
    # json_normalize converts nested structures (like file_metadata) into flat columns
    df = pd.json_normalize(metadata_payload).fillna(0.0)

    # Build the destination URI 
    uri = f"s3://{config.LANDING_BUCKET}/{config.LANDING_PERSISTENT_PATH}{delta_folder}"

    # Storage configuration 
    storage_options = {
        "AWS_ACCESS_KEY_ID": config.MINIO_ROOT_USER,
        "AWS_SECRET_ACCESS_KEY": config.MINIO_ROOT_PASSWORD,
        "AWS_ENDPOINT_URL": config.MINIO_ENDPOINT_URL,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    # Write to Delta Lake using Schema Evolution (schema_mode="merge")
    write_deltalake(
        uri, 
        df, 
        mode="append", 
        schema_mode="merge",
        storage_options=storage_options,
    )
    print(f"[DELTA SUCCESS] Metadata integrated into table at: {delta_folder}")
    
  