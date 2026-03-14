"""
He añadido que la cantidad de carpetas se pase como argumento en la terminal.
Donde cada carpeta tiene 58 frames del vídeo.
Para ejecutar: python -m src.data_management.data_ingestion.unstructured_data --max-folders 2 (el que queramos)
"""
import base64
import argparse
import time
from pathlib import Path
from typing import Optional
from src.common.kafka_client import get_kafka_producer
from src.common.progress_bar import ProgressBar

# --- CONFIGURACIÓN DE RUTAS Y KAFKA ---
BASE_DIR = Path("src/downloaded_data/unstructured/images")

# AQUÍ TIENES LA VARIABLE QUE ME HAS PEDIDO:
TOPIC_NAME_KAFKA = "traffic-images" 

def ingest_unstructured_data(max_folders: Optional[int] = None):
    """
    Lee los frames de las carpetas locales y los envía al topic de Kafka.
    """
    # 1. Validación de la ruta de las imágenes
    if not BASE_DIR.exists():
        print(f"Not able to find the path {BASE_DIR.absolute()}")
        return

    # 2. Conexión con el Productor de Kafka
    try:
        producer = get_kafka_producer()
    except Exception as e:
        print(f"Not able to connect to Kafka {e}")
        return

    # 3. Obtener y filtrar las carpetas de vídeos (como Nd_O_...)
    video_folders = sorted([f for f in BASE_DIR.iterdir() if f.is_dir()])
    
    if max_folders is not None:
        video_folders = video_folders[:max_folders]
        print(f"Ingesting only {max_folders} folders.")

    # 4. Listar todos los archivos .jpg de esas carpetas
    all_frames = []
    for folder in video_folders:
        # CAMBIO AQUÍ: Ahora acepta .jpg, .JPG, .jpeg y .JPEG
        frames_in_folder = [f for f in folder.iterdir() if f.suffix.lower() in ['.jpg', '.jpeg']]
        all_frames.extend(frames_in_folder)
    
    total_frames = len(all_frames)
    
    if total_frames == 0:
        print(f"No images .jpg to send.")
        return

    print(f"Sending images to: {TOPIC_NAME_KAFKA}")

    
    with ProgressBar(total=total_frames, description="Sending to Kafka", unit="imgs") as progress:
        for frame_path in all_frames:
            try:
                video_id = frame_path.parent.name
                
                # Leemos la imagen y la pasamos a Base64
                with open(frame_path, "rb") as f:
                    img_b64 = base64.b64encode(f.read()).decode('utf-8')

                # Metadatos del mensaje
                payload = {
                    "escenario": "Traffic_NYC", 
                    "video_id": video_id,
                    "frame_id": frame_path.stem,
                    "image_data": img_b64,
                    "timestamp": time.time()
                }
                producer.send(TOPIC_NAME_KAFKA, value=payload)
                
                progress.update(1)
            
            except Exception as e:
                progress.write(f"Error in the frame {frame_path.name}: {e}")

    # Forzamos que se envíen todos los mensajes pendientes
    producer.flush()
    print(f"\n Ingestion finished in '{TOPIC_NAME_KAFKA}'.")

def parse_args():
    parser = argparse.ArgumentParser(description="Image ingestion to Kafka.")
    
    parser.add_argument(
        "--max-folders",
        type=int,
        default=None,
        help="Number of folders to process.",
    )
    
    args = parser.parse_args()
    if args.max_folders is not None and args.max_folders <= 0:
        parser.error("The number of folders must be positive.")

    return args

if __name__ == "__main__":
    cli_args = parse_args()
    # Ejecutamos la función con el límite que pongamos en la consola
    ingest_unstructured_data(max_folders=cli_args.max_folders)