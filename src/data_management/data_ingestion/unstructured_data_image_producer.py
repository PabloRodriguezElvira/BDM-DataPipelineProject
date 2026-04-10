import time
import json
import base64
import random

from kafka import KafkaProducer

import src.common.global_variables as config

"""
Traffic Data Producer (Round Robin + Sequential Continuity)
Description: Streams enriched image sequences with dynamic metadata.
Handles continuous video sequences split across multiple folders.
"""


def get_clean_camera_id(folder_name):
    """
    Extracts the root camera ID by splitting at the second underscore.
    Example: 'Nd_O_725515' -> 'Nd_O'
    """
    parts = folder_name.split("_")
    return f"{parts[0]}_{parts[1]}" if len(parts) >= 2 else folder_name


def run_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[config.KAFKA_SERVER],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
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
        first_folder = folders_list[0]
        base_metadata = {}

        json_path = first_folder / "frame0.json"
        if json_path.exists():
            with open(json_path, "r") as f:
                data = json.load(f)
                for shape in data.get("shapes", []):
                    label = shape["label"]
                    base_metadata[label] = base_metadata.get(label, 0) + 1

        camera_data[camera_id] = {
            "folders": folders_list,
            "current_folder_idx": 0,
            "current_file_ptr": 0,
            "state": base_metadata,
            "timer": time.time(),
        }

    print("Producer initialized. Broadcasting channels via Round Robin...")

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
            files = sorted(list(current_folder.glob("*.jpg")))
            ptr = info["current_file_ptr"]

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
                    producer.send(config.UNSTRUCTURED_IMAGE_TOPIC_NAME, value=payload)
                    ptr += 1
                else:
                    info["current_folder_idx"] += 1
                    info["current_file_ptr"] = 0
                    break

            info["current_file_ptr"] = ptr
            time.sleep(config.UNSTRUCTURED_IMAGE_SLEEP_SECONDS)

    producer.flush()
    print("Images to kafka completed successfully.")


if __name__ == "__main__":
    run_producer()