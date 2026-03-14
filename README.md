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

