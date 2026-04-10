import os
import json
import base64
import time
from datetime import datetime
from kafka import KafkaConsumer

# --- PROJECT CONFIGURATION & MINIO INTEGRATION ---
import src.common.global_variables as config
from src.common.minio_client import get_minio_client
# -------------------------------------------------

IMAGE_BASE_DIR = "src/downloaded_data/unstructured/real_time_images"
TOPIC_NAME = "traffic-images"
WINDOW_SECONDS = 10
LOG_INTERVAL = 5  # Seconds between each status update in console

def consume_and_aggregate():
    os.makedirs(IMAGE_BASE_DIR, exist_ok=True)
        
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[config.KAFKA_SERVER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        client = get_minio_client()
    except Exception as e:
        print(f"CRITICAL: Connection failed - {e}")
        return

    state = {} 
    
    print(f"STATUS: Consumer active. Connected to Kafka at {config.KAFKA_SERVER}")
    print(f"ACTION: Reporting status every {LOG_INTERVAL}s. Press Ctrl+C to stop.\n")
    
    try:
        for message in consumer:
            data = message.value
            camera_id = data.get("video_id")
            frame_id = data.get("frame_id")
            detections = data.get("detections", {})
            img_b64 = data.get("image_data", "")
            msg_timestamp = data.get("timestamp", time.time())

            today = datetime.now().strftime("%Y-%m-%d")
            time_str = datetime.now().strftime("%H%M%S")
            
            if camera_id not in state:
                # 'last_log' tracks when we last printed the status for this camera
                state[camera_id] = {"counts": {}, "frames": 0, "start": msg_timestamp, "last_log": msg_timestamp}

            buffer = state[camera_id]
            buffer["frames"] += 1
            
            # --- LOGGING EVERY 5 SECONDS (OR LOG_INTERVAL) ---
            if msg_timestamp - buffer["last_log"] >= LOG_INTERVAL:
                print(f"INFO: Camera {camera_id} has collected {buffer['frames']} frames so far...")
                buffer["last_log"] = msg_timestamp # Reset log timer

            # 1. Handle Unstructured Data (Images) - Quietly
            camera_path = os.path.join(IMAGE_BASE_DIR, camera_id, today)
            os.makedirs(camera_path, exist_ok=True)
            image_filename = f"{time_str}_{frame_id}.jpg"
            full_image_path = os.path.join(camera_path, image_filename)
            
            with open(full_image_path, "wb") as f:
                f.write(base64.b64decode(img_b64))

            # 2. Accumulate Detections
            for label, count in detections.items():
                buffer["counts"][label] = buffer["counts"].get(label, 0) + count

            # 3. Window Completion (10 Seconds)
            if msg_timestamp - buffer["start"] >= WINDOW_SECONDS:
                total_frames = buffer["frames"]
                averages = {label: round(total / total_frames, 2) for label, total in buffer["counts"].items()}

                start_dt = datetime.fromtimestamp(buffer['start'])
                report_filename = f"report_{camera_id}_{start_dt.strftime('%H%M%S')}.json"
                local_report_path = os.path.join(camera_path, report_filename)
                
                report = {
                    "camera_id": camera_id,
                    "date": today,
                    "avg_per_frame": averages,
                    "total_frames_processed": total_frames
                }
                
                with open(local_report_path, "w") as f:
                    json.dump(report, f, indent=4)

                # Upload to MinIO
                try:
                    minio_target = f"{config.LANDING_TEMPORAL_PATH.strip('/')}/{report_filename}"
                    client.fput_object(config.LANDING_BUCKET, minio_target, local_report_path)
                    print(f"SUCCESS: Metadata report uploaded to MinIO - {minio_target}")
                except Exception as e:
                    print(f"ERROR: MinIO transfer failed - {e}")
                
                print(f"PROCESS: 10s window closed for {camera_id}. Final averages: {averages}\n")
                
                # Reset window and log timer
                state[camera_id] = {"counts": {}, "frames": 0, "start": msg_timestamp, "last_log": msg_timestamp}

    except KeyboardInterrupt:
        print("\nSIGINT: Closing system resources...")
    finally:
        consumer.close()
        print("STATUS: Execution terminated successfully.")

if __name__ == "__main__":
    consume_and_aggregate()