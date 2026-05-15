FROM quay.io/jupyter/base-notebook:python-3.13

WORKDIR /app
# he añadido esto
USER root
RUN apt-get update && apt-get install -y default-jdk && apt-get clean

# Volvemos al usuario normal por seguridad
USER jovyan
# ---

COPY requirements.txt .
# COPY requirements-ml.txt .
RUN pip install --no-cache-dir -r requirements.txt
# RUN pip install --no-cache-dir -r requirements-ml.txt
