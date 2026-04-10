import os
import time
import json
import base64
import random
from pathlib import Path
from kafka import KafkaProducer

"""
Traffic Data Producer (Round Robin + Sequential Continuity)
Description: Streams enriched image sequences with dynamic metadata.
Handles continuous video sequences split across multiple folders.
"""

# --- CONFIGURATION ---
SOURCE_DATA_PATH = Path("src/downloaded_data/unstructured/images")
TOPIC_NAME = "traffic-images"
KAFKA_SERVER = "localhost:9092"
BATCH_SIZE = 5

def get_clean_camera_id(folder_name):
    """
    Extracts the root camera ID by splitting at the second underscore.
    Example: 'Nd_O_725515' -> 'Nd_O'
    """
    parts = folder_name.split('_')
    return f"{parts[0]}_{parts[1]}" if len(parts) >= 2 else folder_name

def run_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return

    # 1. Group sequential folders by their clean camera ID
    folders = sorted([d for d in SOURCE_DATA_PATH.iterdir() if d.is_dir()])
    camera_groups = {}
    for f in folders:
        camera_id = get_clean_camera_id(f.name)
        if camera_id not in camera_groups: 
            camera_groups[camera_id] = []
        camera_groups[camera_id].append(f)

    # 2. Initialize camera states and metadata
    camera_data = {}
    for camera_id, folders_list in camera_groups.items():
        first_folder = folders_list[0]
        base_metadata = {}
        
        # Read the initial JSON to detect vehicle types dynamically
        json_path = first_folder / "frame0.json"
        if json_path.exists():
            with open(json_path, 'r') as f:
                data = json.load(f)
                for shape in data.get('shapes', []):
                    label = shape['label']
                    base_metadata[label] = base_metadata.get(label, 0) + 1

        # Store the tracking state for each camera group
        camera_data[camera_id] = {
            "folders": folders_list,
            "current_folder_idx": 0,
            "current_file_ptr": 0,
            "state": base_metadata,
            "timer": time.time()
        }

    print("Producer initialized. Broadcasting channels via Round Robin...")

    active = True
    while active:
        active = False
        
        for camera_id, info in camera_data.items():
            folders = info["folders"]
            folder_idx = info["current_folder_idx"]
            
            # Skip if this camera has processed all its fragmented folders
            if folder_idx >= len(folders): 
                continue 
            
            active = True
            current_folder = folders[folder_idx]
            files = sorted(list(current_folder.glob("*.jpg")))
            ptr = info["current_file_ptr"]

            # 3. Dynamic Traffic Variation (10-second window)
            if time.time() - info["timer"] > 10:
                for label in info["state"]:
                    current_value = info["state"][label]
                    # Add/Subtract a random value proportional to the current traffic
                    change = random.randint(-current_value, current_value) if current_value > 0 else random.randint(0, 1)
                    info["state"][label] = max(0, current_value + change)
                info["timer"] = time.time()

            # 4. Transmit a batch of images (Round Robin approach)
            for _ in range(BATCH_SIZE):
                if ptr < len(files):
                    img_path = files[ptr]
                    with open(img_path, "rb") as f:
                        img_b64 = base64.b64encode(f.read()).decode('utf-8')

                    payload = {
                        "video_id": camera_id,             # Clean ID (e.g., Nd_O)
                        "source_folder": current_folder.name, # Original folder for traceability
                        "frame_id": img_path.stem,
                        "image_data": img_b64,
                        "detections": info["state"],       # Current dynamic metadata
                        "timestamp": time.time()
                    }
                    producer.send(TOPIC_NAME, value=payload)
                    ptr += 1
                else:
                    # End of current folder reached; shift to the next fragment seamlessly
                    info["current_folder_idx"] += 1
                    info["current_file_ptr"] = 0
                    break
            
            info["current_file_ptr"] = ptr
            time.sleep(0.01) # Brief pause to stabilize network throughput

    producer.flush()
    print("Images to kafka completed successfully.")

if __name__ == "__main__":
    run_producer()