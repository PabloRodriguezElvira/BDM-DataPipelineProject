# BDM Landing Zone Project

## Description

This project implements a landing zone pipeline for a small data platform. It ingests structured, semi-structured, and unstructured datasets, uploads them to a temporal area in MinIO, and then processes them into a persistent area. During this process, the project also generates Delta Lake tables for structured data and metadata.

The platform includes:

- Batch ingestion scripts for multiple dataset types
- A landing zone process from temporal to persistent storage
- Delta Lake integration
- Kafka-based streaming for image data
- Airflow orchestration for batch and streaming workflows

## Main Flow

1. Download datasets locally into `downloaded_data/`
2. Upload files to `temporal_landing/` in MinIO
3. Process them into `persistent_landing/`
4. Write Delta Lake tables where applicable

## Dataset Types

- Structured: NYC motor vehicle collisions CSV files
- Semi-structured: weather forecast JSON files
- Unstructured audio: Kaggle audio files
- Unstructured text: text-based news data
- Unstructured images: Kafka streaming flow

## Run With Docker

Start the full environment with:

```bash
docker compose up --build -d
```

This builds the project app image from `requirements.txt` and a dedicated Airflow image from `requirements.txt` plus `requirements-airflow.txt`.
During startup, `minio_manager.py` is executed automatically to create the MinIO landing bucket.
Airflow dependencies are installed at image build time instead of being injected with `_PIP_ADDITIONAL_REQUIREMENTS` at container startup.

Main services:

- Airflow: `http://localhost:8080`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9090`
- Kafka UI: `http://localhost:8081`

## Run With Python

Install dependencies:

```bash
pip install -r requirements.txt
```

Example command:

```bash
python -m src.data_management.data_ingestion.structured_data
```

## Execution Options

The project can be executed in two ways:

- From the terminal, by running the Python modules directly
- From Airflow, using the DAGs defined in `dags/orchestrator.py`

Airflow includes:

- `bdm_batch_pipeline` for batch ingestion and landing zone processing
- `bdm_streaming_bootstrap` for the streaming image flow

Default Airflow credentials:

- Username: `admin`
- Password: `admin`

Default MinIO credentials:

- Username: `admin`
- Password: `admin123`

## Common Commands

Download structured data:

```bash
python -m src.data_management.data_ingestion.structured_data --limit 50000 --max-csvs 5
```

Run the same command inside Docker:

```bash
docker compose exec app python -m src.data_management.data_ingestion.structured_data --limit 50000 --max-csvs 5
```

Upload local files to temporal landing:

```bash
python -m src.data_management.landing_zone.upload_to_temporal
```

Process the landing zone:

```bash
python -m src.data_management.landing_zone.landing_zone
```

Show CLI help:

```bash
python -m src.data_management.landing_zone.upload_to_temporal --help
```

## Notes

- `structured_to_delta.py` converts structured CSV data before it is written as Delta Lake
- `landing_zone.py` is responsible for persisting processed data into MinIO
- `.env` is optional for `docker compose up`; it is only needed for ingestion flows that require credentials, especially Kaggle-based ones
- Shared runtime dependencies live in `requirements.txt`
- Airflow-only dependencies live in `requirements-airflow.txt`
- If you add MongoDB, ClickHouse, Milvus or similar connectors used by your project code, add them to `requirements.txt` so both the app container and the Airflow containers receive them
- For the Kafka image streaming workflow, a small test dataset will be provided in `downloaded_data/unstructured/images` with 11 folders and around 30 images per folder
- If the full image dataset is needed, it can be downloaded from: `https://github.com/Math-ML-X/TrafficCAM/blob/main/TrafficCAM-download.md`
