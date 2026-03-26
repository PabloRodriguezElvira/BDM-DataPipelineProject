import os
import json
import base64
from kafka import KafkaConsumer
'''
Coge las imagenes que se han enviado a kafka en base 64 junto con el frame id y video id, las descodifica
y las envia a una carpeta que he creado aquiBDM-LandingZoneProject\src\downloaded_data\Unstructured\real_time_images"
la ruta seguramente habra que cambiarla 
'''

# --- CONFIGURACIÓN DE RUTAS Y KAFKA ---
# La ruta exacta que me pasaste donde queremos que caigan las alertas
OUTPUT_DIR = r"C:\Users\adals\OneDrive\Escritorio\Máster\BDM\Project\BDM-LandingZoneProject\src\downloaded_data\Unstructured\real_time_images"
TOPIC_NAME = "traffic-images"

def consume_images():
    """
    Lee los mensajes de Kafka en tiempo real, decodifica las imágenes
    y las guarda en la carpeta de alertas.
    """
    # 1. Crear la carpeta destino si no existe
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Iniciando consumidor... Las imágenes irán a:\n{OUTPUT_DIR}\n")
    
    # 2. Configurar el Consumidor de Kafka
    # Usamos json.loads para entender el payload que mandó tu Productor
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest', # Lee las fotos que ya están en Kafka esperando
            enable_auto_commit=True,
            group_id='traffic-alerts-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception as e:
        print(f"❌ No se pudo conectar a Kafka: {e}")
        return
        
    print(f"🎧 Escuchando alertas en el topic '{TOPIC_NAME}'... (Pulsa Ctrl+C para salir)")
    
    # 3. Bucle infinito para recibir datos en tiempo real
    try:
        for message in consumer:
            payload = message.value
            
            # Extraer los datos exactos que enviaste en el Productor
            video_id = payload.get("video_id", "unknown_video")
            frame_id = payload.get("frame_id", "unknown_frame")
            img_b64 = payload.get("image_data", "")
            
            if not img_b64:
                print("Mensaje recibido sin datos de imagen, saltando...")
                continue
                
            # Decodificar la imagen de Base64 a formato original (bytes)
            image_bytes = base64.b64decode(img_b64)
            
            # Crear un nombre de archivo bonito y único (Ej: Nd_O_..._frame2.jpg)
            file_name = f"{video_id}_{frame_id}.jpg"
            file_path = os.path.join(OUTPUT_DIR, file_name)
            
            # Guardar físicamente la imagen en tu disco duro
            with open(file_path, "wb") as f:
                f.write(image_bytes)
                
            print(f"✅ ¡Alerta guardada! -> {file_name}")
            
    except KeyboardInterrupt:
        print("\n🛑 Consumidor detenido por el usuario.")
    except Exception as e:
        print(f"❌ Error inesperado durante el consumo: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_images()