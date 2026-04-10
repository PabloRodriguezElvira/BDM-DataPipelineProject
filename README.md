  Landing Zone BDM project. List of APIs we can use:

  # Structured data:
  - NYC Motor Vehicle Collisions (csv): 
  - Link: https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data
  (El dataset tiene 2246028 filas. Usando la API se pueden obtener filas de 1000 en 1000, de alguna manera se puede hacer para descargar todo el dataset y dividirlo en varios CSV.)

  # Semi-structured data: Weather Forecast
  - Link: https://www.weather.gov/documentation/services-web-api

  # Unstructured data

  ## Traffic images
  - Information about the dataset: https://math-ml-x.github.io/TrafficCAM/
  - Download the data: https://github.com/Math-ML-X/TrafficCAM/blob/main/TrafficCAM-download.md
  (este dataset se podría usar con Kafka, si vemos que 2GB de imágenes son muchas recortamos)

  ## 911 recordings (audio)
  - Link: https://www.kaggle.com/datasets/louisteitelbaum/911-recordings-first-6-seconds
  (en el link hay informacion de como usar la API de Kaggle para descargar el dataset)
  - Ingestion script:
    `python -m src.data_management.data_ingestion.unstructured_data_audio --max-files 20`
  - Kaggle auth options (required):
    1. `.env` with `KAGGLE_API_TOKEN`
    2. `.env` with `KAGGLE_USERNAME` (or `KAGGLE_API_USERNAME`)
    3. If token is `username:key`, username is inferred automatically

  ## Optional Texts.



  ## Cambios
  Dockerfile -> añadido el pip install requirements ahí, solo hace falta hacer docker compose up --build -d

  data_ingestion solo tiene la lógica para descargar los datasets en local. upload_to_temporal se encarga de mover los datasets
  a temporal bucket y landing_zone mueve de temporal a persistent.

  he quitado que en minio_manager se generen las subcarpetas de la landing_zone, no hacen falta.

  ## Cómo ejecutar
  Se puede ejecutar el proyecto usando Docker:
  - docker compose up --build -d

  Para ejecutar cada uno de los scripts:
  - docker compose exec app python -m src.data_management.data_ingestion.unstructured_data_audio

  También se puede ejecutar con una instalación de Python en local.
  - pip install -r requirements.txt

  Para ejecutar cada uno de los scripts:
  - python -m src.data_management.data_ingestion.unstructured_data_audio

  ## TODO
  - guardar metadatos de todos los datos con Delta Lake
  - mirar tipo de dato no estructurado texto -> Pablo
  - hot path: que el kafka envie los datos a una carpeta de fuera de la landing zone -> Pau

