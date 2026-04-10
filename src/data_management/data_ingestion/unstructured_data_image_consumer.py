import os
import json
import base64
import time
from datetime import datetime

from kafka import KafkaConsumer

import src.common.global_variables as config
from src.common.minio_client import get_minio_client


def consume_and_aggregate():
    os.makedirs(config.UNSTRUCTURED_IMAGE_BASE_DIR, exist_ok=True)

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
    print(
        f"ACTION: Reporting status every {config.UNSTRUCTURED_IMAGE_LOG_INTERVAL}s. "
        "Press Ctrl+C to stop.\n"
    )

    try:
        for message in consumer:
            data = message.value
            camera_id = data.get("video_id")
            frame_id = data.get("frame_id")
            detections = data.get("detections", {})
            img_b64 = data.get("image_data", "")

            current_time = time.time()

            today = datetime.now().strftime("%Y-%m-%d")
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

            if current_time - buffer["last_log"] >= config.UNSTRUCTURED_IMAGE_LOG_INTERVAL:
                print(f"The Camera {camera_id} has collected {buffer['frames']} frames so far.")
                buffer["last_log"] = current_time

            camera_path = os.path.join(config.UNSTRUCTURED_IMAGE_BASE_DIR, camera_id, today)
            os.makedirs(camera_path, exist_ok=True)

            image_filename = f"{time_str}_{frame_id}.jpg"
            full_image_path = os.path.join(camera_path, image_filename)

            with open(full_image_path, "wb") as f:
                f.write(base64.b64decode(img_b64))

            for label, count in detections.items():
                buffer["counts"][label] = buffer["counts"].get(label, 0) + count

            if current_time - buffer["start"] >= config.UNSTRUCTURED_IMAGE_WINDOW_SECONDS:
                total_frames = buffer["frames"]
                averages = {
                    label: round(total / total_frames, 2)
                    for label, total in buffer["counts"].items()
                }

                start_dt = datetime.fromtimestamp(buffer["start"])
                report_filename = (
                    f"meta_unstructured_report_{camera_id}_{start_dt.strftime('%H%M%S')}.json"
                )
                local_report_path = os.path.join(camera_path, report_filename)

                report = {
                    "camera_id": camera_id,
                    "date": today,
                    "avg_per_frame": averages,
                    "total_frames_processed": total_frames,
                }

                with open(local_report_path, "w") as f:
                    json.dump(report, f, indent=4)

                try:
                    minio_target = (
                        f"{config.LANDING_TEMPORAL_PATH.strip('/')}/{report_filename}"
                    )
                    client.fput_object(
                        config.LANDING_BUCKET,
                        minio_target,
                        local_report_path,
                    )
                    print(f"SUCCESS: Metadata report uploaded to MinIO - {minio_target}")
                except Exception as e:
                    print(f"ERROR: MinIO transfer failed - {e}")

                print(f"PROCESS: Window closed for {camera_id}. Final averages: {averages}\n")

                state[camera_id] = {
                    "counts": {},
                    "frames": 0,
                    "start": current_time,
                    "last_log": current_time,
                }

    except KeyboardInterrupt:
        print("\nClosing system.")
    finally:
        consumer.close()
        print("STATUS: Execution ended successfully.")


if __name__ == "__main__":
    consume_and_aggregate()