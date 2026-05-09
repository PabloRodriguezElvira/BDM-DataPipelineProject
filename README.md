# BDM Landing Zone Project

## Description

This project implements a landing zone pipeline for a small data platform. It ingests structured, semi-structured, and unstructured datasets, uploads them to a temporal area in MinIO, and then processes them into a persistent area. During this flow, it also generates Delta Lake tables for structured data and metadata.

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

## Run Docker environment

Start the Docker environment with:

```bash
docker compose up --build -d
```

This builds the project app image from `requirements.txt` and a dedicated Airflow image from `requirements.txt` plus `requirements-airflow.txt`.
During startup, `minio_manager.py` is executed automatically to create the MinIO landing bucket.

Main services:

- Apache Airflow: `http://localhost:8080`
  Username: `admin`
  Password: `admin`
- MinIO Console: `http://localhost:9090`
  Username: `admin`
  Password: `admin123`
- MinIO API: `http://localhost:9000`
- Apache Kafka UI: `http://localhost:8081`
- ClickHouse HTTP: `http://localhost:8123`
  Username: `default`
  Password: `clickhouse`
- ClickHouse native: `localhost:9001`

To run any project module inside the `app` container, use:

```bash
docker compose exec app python -m <package.module> [--flag value ...]
```

Example:

```bash
docker compose exec app python -m src.data_management.data_ingestion.structured_data --limit 50000 --max-csvs 5
```

## Run Python scripts locally

Install dependencies locally on your machine:

```bash
pip install -r requirements.txt
```

Example command to execute the structured data ingestion:

```bash
python -m src.data_management.data_ingestion.structured_data
```

## Execution Options

The project can be executed in two ways:

- From the terminal, by running the Python modules directly with a local Python interpreter or inside the Docker `app` container.
- From Airflow, using the DAGs defined in `dags/`

Airflow includes:

- `data_ingestion_landing_zone` for batch ingestion and landing zone processing
- `apache_kafka_streaming` for the streaming image flow


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

## ClickHouse

ClickHouse stores the cleaned structured data (NYC collisions) in the trusted zone.

**Web SQL interface:** `http://localhost:8123/play`
Login with username `default` and password `clickhouse`.

Useful queries:

```sql
-- Count total rows
SELECT count() FROM trusted_zone.nyc_collisions

-- Preview data
SELECT * FROM trusted_zone.nyc_collisions LIMIT 100

-- Check for duplicate collision IDs
SELECT collision_id, count() AS cnt
FROM trusted_zone.nyc_collisions
GROUP BY collision_id
HAVING cnt > 1
ORDER BY cnt DESC

-- Clear all data (then re-run the trusted zone pipeline to reload)
TRUNCATE TABLE trusted_zone.nyc_collisions
```

To run the trusted zone pipeline that populates ClickHouse:

```bash
docker compose exec app python -m src.data_management.trusted_zone.structured_trusted_zone
```

## Notes

- `structured_csv_to_arrow.py` converts structured CSV data before it is written as Delta Lake
- `landing_zone.py` is responsible for persisting processed data into MinIO
- `.env` is optional for `docker compose up`; it is only needed for ingestion flows that require credentials, especially Kaggle-based ones
- Shared runtime dependencies live in `requirements.txt`
- Airflow-only dependencies live in `requirements-airflow.txt`
- For the Kafka image streaming workflow, a small test dataset will be provided in `downloaded_data/unstructured/images` with 10 folders and around 30 images per folder
- If the full image dataset is needed, it can be downloaded from: `https://github.com/Math-ML-X/TrafficCAM/blob/main/TrafficCAM-download.md`
