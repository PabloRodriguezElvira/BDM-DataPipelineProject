import os
import json
import base64
import time
from datetime import datetime
import io
from pathlib import Path
import pytz

from kafka import KafkaConsumer
import src.common.global_variables as config
from src.common.minio_client import get_minio_client

# To make a realistic simulation we use New York time
ny_timezone = pytz.timezone("America/New_York")

def consume_and_aggregate():
    """Consume image frames, aggregate counts, and upload reports."""
    base_dir = Path(config.UNSTRUCTURED_IMAGE_BASE_DIR)
    base_dir.mkdir(parents=True, exist_ok=True)

    try:
        consumer = KafkaConsumer(
            config.UNSTRUCTURED_IMAGE_TOPIC_NAME,
            bootstrap_servers=[config.KAFKA_SERVER],
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        client = get_minio_client()
    except Exception as e:
        print(f"CRITICAL: Connection failed - {e}")
        return

    state = {}

    print(f"STATUS: Consumer active. Connected to Kafka at {config.KAFKA_SERVER}")
    print(f"ACTION: Reporting status every {config.UNSTRUCTURED_IMAGE_LOG_INTERVAL}s. Press Ctrl+C to stop.\n")

    try:
        while True:
            # Non-blocking polling loop
            records = consumer.poll(timeout_ms=1000)
            current_time = time.time()
            today = datetime.now().strftime("%Y-%m-%d")

            # Process incoming messages
            if records:
                for topic_partition, messages in records.items():
                    for message in messages:
                        data = message.value
                        camera_id = data.get("video_id")
                        frame_id = data.get("frame_id")
                        detections = data.get("detections", {})
                        img_b64 = data.get("image_data", "")

                        time_str = datetime.now().strftime("%H%M%S")

                        if camera_id not in state:
                            state[camera_id] = {
                                "counts": {},
                                "frames": 0,
                                "start": current_time,
                                "last_log": current_time,
                            }

                        buffer = state[camera_id]
                        buffer["frames"] += 1

                        # Create directory structure and persist image locally
                        camera_path = base_dir / camera_id / today
                        camera_path.mkdir(parents=True, exist_ok=True)

                        image_filename = f"{time_str}_{frame_id}.jpg"
                        full_image_path = camera_path / image_filename

                        try:
                            full_image_path.write_bytes(base64.b64decode(img_b64))
                        except PermissionError as exc:
                            print(f"WARNING: Local image persistence skipped for {full_image_path} - {exc}")

                        for label, count in detections.items():
                            buffer["counts"][label] = buffer["counts"].get(label, 0) + count

            # Evaluate time windows for all active cameras
            cameras_to_reset = []

            for cid, buffer in state.items():
                if buffer["frames"] > 0:
                    
                    if current_time - buffer["last_log"] >= config.UNSTRUCTURED_IMAGE_LOG_INTERVAL:
                        print(f"INFO: Camera {cid} has collected {buffer['frames']} frames so far.")
                        buffer["last_log"] = current_time

                    if current_time - buffer["start"] >= config.UNSTRUCTURED_IMAGE_WINDOW_SECONDS:
                        total_frames = buffer["frames"]
                        averages = {
                            label: round(total / total_frames, 2)
                            for label, total in buffer["counts"].items()
                        }

                        start_dt = datetime.fromtimestamp(buffer["start"], tz=pytz.utc).astimezone(ny_timezone)
                        report_filename = f"meta_unstructured_report_{cid}_{start_dt.strftime('%H%M%S')}.json"
                        
                
                        report = {
                            "camera_id": cid,
                            "date": today,
                            "avg_per_frame": averages,
                            "total_frames_processed": total_frames,
                        }

                        report_data = json.dumps(report, indent=4).encode('utf-8')
                        report_stream = io.BytesIO(report_data)

                        try:
                            minio_target = f"{config.LANDING_TEMPORAL_PATH.strip('/')}/{report_filename}"
                            
                            # Upload directly from memory stream
                            client.put_object(
                                config.LANDING_BUCKET,
                                minio_target,
                                report_stream,
                                len(report_data),
                                content_type='application/json'
                            )
                            print(f"SUCCESS: Metadata report uploaded to MinIO - {minio_target}")
                        except Exception as e:
                            print(f"ERROR: MinIO transfer failed - {e}")

                        print(f"PROCESS: Window closed for {cid}. Final averages: {averages}\n")
                        cameras_to_reset.append(cid)

            # Reset state for cameras whose aggregation window has closed
            for cid in cameras_to_reset:
                state[cid] = {
                    "counts": {},
                    "frames": 0,
                    "start": current_time,
                    "last_log": current_time,
                }

    except KeyboardInterrupt:
        print("\nSTATUS: Closing system due to user interrupt.")
    finally:
        consumer.close()
        print("STATUS: Execution ended successfully.")


if __name__ == "__main__":
    consume_and_aggregate()
