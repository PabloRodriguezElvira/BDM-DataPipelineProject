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

# 1. Instalamos Java como ROOT (lo único que necesita el sistema operativo)
USER root
RUN apt-get update && apt-get install -y default-jdk && apt-get clean

# 2. Volvemos al usuario normal
USER jovyan

# 3. Instalamos tus librerías de Python (donde está tu nuevo pyspark==3.5.1)
COPY requirements.txt .
COPY requirements-ml.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -r requirements-ml.txt