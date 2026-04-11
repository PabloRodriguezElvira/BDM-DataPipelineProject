import os
import time
import json
import base64
import random
from pathlib import Path

from kafka import KafkaProducer
import src.common.global_variables as config

"""
Traffic Data Producer (Round Robin + Sequential Continuity)
Description: Streams enriched image sequences with dynamic metadata.
Handles continuous video sequences split across multiple folders,
updating metadata from JSON on every folder switch.
"""

def get_clean_camera_id(folder_name):
    """
    Extracts the root camera ID by splitting at the second underscore.
    Example: 'Nd_O_725515' -> 'Nd_O'
    """
    parts = folder_name.split("_")
    return f"{parts[0]}_{parts[1]}" if len(parts) >= 2 else folder_name

def on_send_error(excp):
    """Log Kafka send failures for image messages."""
    print(f"KAFKA ERROR (Image discarded): {excp}")

def run_producer():
    """Stream image frames to Kafka in round-robin order."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[config.KAFKA_SERVER],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            max_request_size=10485760  # Increased threshold to support large image files (10MB)
        )
    except Exception as e:
        print(f"CRITICAL: Failed to connect to Kafka: {e}")
        return

    folders = sorted([d for d in config.UNSTRUCTURED_IMAGE_SOURCE_DATA_PATH.iterdir() if d.is_dir()])
    camera_groups = {}

    for folder in folders:
        camera_id = get_clean_camera_id(folder.name)
        if camera_id not in camera_groups:
            camera_groups[camera_id] = []
        camera_groups[camera_id].append(folder)

    camera_data = {}
    for camera_id, folders_list in camera_groups.items():
        # Initialize state without loading the JSON file yet (loaded_folder_idx = -1)
        camera_data[camera_id] = {
            "folders": folders_list,
            "current_folder_idx": 0,
            "current_file_ptr": 0,
            "loaded_folder_idx": -1, 
            "state": {},
            "timer": time.time(),
        }

    print("STATUS: Producer initialized. Broadcasting channels via Round Robin")

    active = True
    while active:
        active = False

        for camera_id, info in camera_data.items():
            folders = info["folders"]
            folder_idx = info["current_folder_idx"]

            if folder_idx >= len(folders):
                continue

            active = True
            current_folder = folders[folder_idx]

            # Dynamic JSON reading upon folder transition
            if info["loaded_folder_idx"] != folder_idx:
                new_state = {}
                json_path = current_folder / "frame0.json"
                if json_path.exists():
                    with open(json_path, "r") as f:
                        data = json.load(f)
                        for shape in data.get("shapes", []):
                            label = shape["label"]
                            new_state[label] = new_state.get(label, 0) + 1
                
                info["state"] = new_state
                info["loaded_folder_idx"] = folder_idx
                info["timer"] = time.time()

            # Retrieve all valid image extensions
            files = []
            for ext in ("*.jpg", "*.JPG", "*.png", "*.jpeg"):
                files.extend(list(current_folder.glob(ext)))
            files = sorted(files)
            
            ptr = info["current_file_ptr"]

            # Traffic variation update cycle
            if time.time() - info["timer"] > config.UNSTRUCTURED_IMAGE_TRAFFIC_UPDATE_SECONDS:
                for label in info["state"]:
                    current_value = info["state"][label]
                    change = (
                        random.randint(-current_value, current_value)
                        if current_value > 0
                        else random.randint(0, 1)
                    )
                    info["state"][label] = max(0, current_value + change)
                info["timer"] = time.time()

            # Batch transmission
            for _ in range(config.UNSTRUCTURED_IMAGE_BATCH_SIZE):
                if ptr < len(files):
                    img_path = files[ptr]
                    with open(img_path, "rb") as f:
                        img_b64 = base64.b64encode(f.read()).decode("utf-8")

                    payload = {
                        "video_id": camera_id,
                        "source_folder": current_folder.name,
                        "frame_id": img_path.stem,
                        "image_data": img_b64,
                        "detections": info["state"],
                        "timestamp": time.time(),
                    }
                    producer.send(config.UNSTRUCTURED_IMAGE_TOPIC_NAME, value=payload).add_errback(on_send_error)
                    ptr += 1
                else:
                    # End of current directory reached. Advance to next folder and reset file pointer.
                    info["current_folder_idx"] += 1
                    ptr = 0
                    break

            info["current_file_ptr"] = ptr
            time.sleep(config.UNSTRUCTURED_IMAGE_SLEEP_SECONDS)

    producer.flush()
    print("STATUS: Image transmission to Kafka completed successfully.")


if __name__ == "__main__":
    run_producer()
