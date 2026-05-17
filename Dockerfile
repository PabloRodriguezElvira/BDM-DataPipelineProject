# =========================================================
# VERSIÓN ANTIGUA (Tu código original)
# (Comentado para que Docker no lo instale dos veces)
# =========================================================
# FROM quay.io/jupyter/base-notebook:python-3.13
# 
# WORKDIR /app
# # he añadido esto
# USER root
# RUN apt-get update && apt-get install -y default-jdk && apt-get clean
# 
# # Volvemos al usuario normal por seguridad
# USER jovyan
# # ---
# 
# COPY requirements.txt .
# # COPY requirements-ml.txt .
# RUN pip install --no-cache-dir -r requirements.txt
# # RUN pip install --no-cache-dir -r requirements-ml.txt


# =========================================================
# =========================================================
# 🟢 VERSIÓN NUEVA (Orden de permisos corregido)
# =========================================================
FROM quay.io/jupyter/base-notebook:python-3.13

WORKDIR /app

# 1. Instalamos las herramientas de sistema (Java y wget) como ROOT
USER root
RUN apt-get update && apt-get install -y default-jdk wget && apt-get clean

# 2. Volvemos al usuario normal ANTES de instalar Python
USER jovyan

# 3. Instalamos las librerías de Python (Esto instalará pyspark con permisos correctos)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Ahora que PySpark está instalado, descargamos los JARs compatibles en su carpeta
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.4.1/spark-sql-kafka-0-10_2.13-3.4.1.jar -O /opt/conda/lib/python3.13/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.13-3.4.1.jar && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/3.4.1/spark-token-provider-kafka-0-10_2.13-3.4.1.jar -O /opt/conda/lib/python3.13/site-packages/pyspark/jars/spark-token-provider-kafka-0-10_2.13-3.4.1.jar && \
    wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar -O /opt/conda/lib/python3.13/site-packages/pyspark/jars/commons-pool2-2.11.1.jar && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar -O /opt/conda/lib/python3.13/site-packages/pyspark/jars/kafka-clients-3.4.1.jar