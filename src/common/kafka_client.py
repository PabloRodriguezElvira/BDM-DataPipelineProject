import json
from kafka import KafkaProducer, KafkaConsumer

def get_kafka_producer():
    """
    Crea un Productor de Kafka. 
    Se usa en el script de 'Ingesta' para enviar los frames de las imágenes.
    """
    return KafkaProducer(
        # 'kafka:9092' si se ejecuta dentro de Docker, 'localhost:9092' desde fuera
        bootstrap_servers=['localhost:9092'],
        # Convierte automáticamente los diccionarios de Python a JSON para enviarlos
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Aumentamos el tamaño máximo del mensaje para que quepan frames de alta calidad
        max_request_size=5242880 # 5MB
    )

def get_kafka_consumer(topic_name):
    """
    Crea un Consumidor de Kafka.
    Se usa en el script de 'Landing' para escuchar el canal y guardar en MinIO.
    """
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        # Lee desde el principio si es la primera vez que se conecta
        auto_offset_reset='earliest',
        # Habilita que el consumidor confirme que ha leído el mensaje
        enable_auto_commit=True,
        # Convierte los datos de JSON de vuelta a un diccionario de Python
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        # Debe coincidir con el tamaño del productor
        max_partition_fetch_bytes=5242880 # 5MB
    )