import os
import json
import base64
import time
from datetime import datetime
from kafka import KafkaConsumer

"""
Local Aggregated Data Consumer
Description: Saves real-time images and performs stream processing to 
calculate 10-second averages. Outputs structured JSON reports exclusively 
to the local camera directories for validation.
"""

# --- CONFIGURATION ---
IMAGE_BASE_DIR = "src/downloaded_data/unstructured/real_time_images"
TOPIC_NAME = "traffic-images"
KAFKA_SERVER = "localhost:9092"
WINDOW_SECONDS = 10

def consume_and_aggregate():
    # Ensure local base directory exists
    os.makedirs(IMAGE_BASE_DIR, exist_ok=True)
        
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_SERVER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return

    # Buffer memory per camera: { camera_id: { counts, frames, start_time } }
    state = {} 
    
    print("Consumer active. Saving images and calculating 10-second local averages...")
    print("Press Ctrl+C at any time to safely stop the consumer.\n")
    
    try:
        for message in consumer:
            data = message.value
            camera_id = data.get("video_id") # Cleaned ID (e.g., Nd_O)
            frame_id = data.get("frame_id")
            detections = data.get("detections", {})
            img_b64 = data.get("image_data", "")
            msg_timestamp = data.get("timestamp", time.time())

            today = datetime.now().strftime("%Y-%m-%d")
            time_str = datetime.now().strftime("%H%M%S")
            
            # 1. Store Unstructured Data (Images)
            # Path logic ensures continuity across original fragmented folders
            camera_path = os.path.join(IMAGE_BASE_DIR, camera_id, today)
            os.makedirs(camera_path, exist_ok=True)
            
            # Save image with execution timestamp to prevent overwriting
            image_filename = f"{time_str}_{frame_id}.jpg"
            full_image_path = os.path.join(camera_path, image_filename)
            
            with open(full_image_path, "wb") as f:
                f.write(base64.b64decode(img_b64))
                
            # Print to know where the image has been saved
            print(f"Frame saved in: {full_image_path}")

            # 2. Accumulate Data for Stream Aggregation
            if camera_id not in state:
                # Initialize state with the timestamp of the VERY FIRST frame
                state[camera_id] = {"counts": {}, "frames": 0, "start": msg_timestamp}

            buffer = state[camera_id]
            buffer["frames"] += 1
            for label, count in detections.items():
                buffer["counts"][label] = buffer["counts"].get(label, 0) + count

            # 3. Time Window Trigger (Calculate Averages & Generate Reports)
            if msg_timestamp - buffer["start"] >= WINDOW_SECONDS:
                total_frames = buffer["frames"]
                
                # Calculate the average objects per frame over the window
                averages = {}
                for label, total in buffer["counts"].items():
                    averages[label] = round(total / total_frames, 2)

                # Format the exact start and end times
                start_dt = datetime.fromtimestamp(buffer['start'])
                end_dt = datetime.fromtimestamp(msg_timestamp)
                
                # Structure the final report
                report = {
                    "camera_id": camera_id,
                    "date": today,
                    "time_window_start": start_dt.strftime('%H:%M:%S'),
                    "time_window_end": end_dt.strftime('%H:%M:%S'),
                    "exact_window_seconds": round(msg_timestamp - buffer["start"], 2),
                    "total_frames_processed": total_frames,
                    "total_detections": buffer["counts"],
                    "average_per_frame": averages
                }
                
                # Naming convention indicates the exact start time of the first frame
                start_time_str = start_dt.strftime('%H%M%S')
                report_filename = f"report_{camera_id}_start_{start_time_str}.json"
                
                # Save locally within the camera's specific directory
                reports_dir = os.path.join(camera_path, "reports")
                os.makedirs(reports_dir, exist_ok=True)
                with open(os.path.join(reports_dir, report_filename), "w") as f:
                    json.dump(report, f, indent=4)
                
                print(f"\n[{camera_id}] 10s Window Closed. Generated: {report_filename} | Averages: {averages}\n")
                
                # Reset the aggregation window for this camera
                state[camera_id] = {"counts": {}, "frames": 0, "start": msg_timestamp}

    except KeyboardInterrupt:
        print("\nManual interruption detected (Ctrl+C).")
        print("Stopping consumer")
    finally:
        consumer.close()
        print("Consumer connection closed successfully.")

if __name__ == "__main__":
    consume_and_aggregate()