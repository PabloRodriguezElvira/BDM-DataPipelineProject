# BDM Data Pipeline Project

## Description

This project implements a multi-zone data lake pipeline that ingests structured, semi-structured, and unstructured datasets through Landing → Trusted → Exploitation zones, stored in MinIO, ClickHouse, and MongoDB, orchestrated by Airflow with Kafka streaming.

The platform includes:

- Batch ingestion scripts for multiple dataset types
- Landing zone processing from temporal to persistent storage with Delta Lake integration
- Trusted zone cleaning and loading into ClickHouse, MongoDB, and MinIO
- Exploitation zone producing a unified Data Product for analysis
- Kafka-based streaming for image data
- Airflow orchestration for all batch and streaming workflows

## Main Flow

1. Download datasets locally into `downloaded_data/`
2. Upload files to `temporal_landing/` in MinIO
3. Process them into `persistent_landing/` and write Delta Lake tables
4. Clean and load into the Trusted Zone (ClickHouse, MongoDB, MinIO)
5. Integrate and enrich into the Exploitation Zone (ClickHouse `exploitation_zone`)

## Dataset Types

- Structured: NYC motor vehicle collisions CSV files
- Semi-structured: weather forecast JSON files and traffic camera aggregates
- Unstructured audio: Kaggle audio files
- Unstructured text: text-based news data
- Unstructured images: Kafka streaming flow

## Project Structure

```
BDM-DataPipelineProject/
├── Alerts/
│   ├── BROOKLYN/
│   ├── MANHATTAN/
│   ├── QUEENS/
│   └── STATEN ISLAND/
├── dags/
│   ├── landing_zone_dag.py
│   ├── trusted_zone_dag.py
│   ├── exploitation_zone_dag.py
│   └── streaming_dag.py
├── downloaded_data/
│   ├── structured/
│   ├── semi_structured/
│   └── unstructured/
├── models/
│   ├── MobileNetSSD_deploy.caffemodel
│   └── MobileNetSSD_deploy.prototxt
├── src/
│   ├── common/
│   │   ├── global_variables.py
│   │   ├── minio_client.py
│   │   ├── minio_manager.py
│   │   ├── clickhouse_client.py
│   │   ├── mongo_client.py
│   │   ├── kafka_client.py
│   │   ├── milvus_client.py
│   │   ├── load_env.py
│   │   └── progress_bar.py
│   ├── data_management/
│   │   ├── data_ingestion/
│   │   │   ├── structured_data.py
│   │   │   ├── semi_structured_data.py
│   │   │   ├── unstructured_data_audio.py
│   │   │   ├── unstructured_data_text.py
│   │   │   ├── unstructured_data_image_producer.py
│   │   │   └── unstructured_data_image_consumer.py
│   │   ├── landing_zone/
│   │   │   ├── upload_to_temporal.py
│   │   │   ├── landing_zone.py
│   │   │   ├── structured_csv_to_arrow.py
│   │   │   └── process_metadata_to_delta.py
│   │   ├── trusted_zone/
│   │   │   ├── structured_trusted_zone.py
│   │   │   ├── unstructured_trusted_zone.py
│   │   │   ├── semistructured_weather_trusted_zone.py
│   │   │   └── semistructured_aggregated_trusted_zone.py
│   │   └── exploitation_zone/
│   │       ├── structured_exploitation_zone.py
│   │       ├── semistructured_exploitation_zone.py
│   │       └── unstructured_exploitation_zone.py
│   └── data_consumption/
│       ├── structured_data/
│       │   ├── trafic_collisions_dashboard.py
│       │   └── risk_prediction_dashboard.py
│       └── unstructured_data/
│           ├── nlp_similarity_search.py
│           └── spark_streaming_cv_alerts.py
├── data_MinIO/
├── compose.yaml
├── Dockerfile
├── Dockerfile.airflow
├── requirements.txt
├── requirements-airflow.txt
└── requirements-ml.txt
```

## Run Docker Environment

Start the Docker environment with:

```bash
docker compose up --build -d
```

This builds the project app image from `requirements.txt` and a dedicated Airflow image from `requirements.txt` plus `requirements-airflow.txt`.
During startup, `minio_manager.py` is executed automatically to create the required MinIO buckets.

Main services:

- Apache Airflow: `http://localhost:8080`
  Username: `admin` / Password: `admin`
- MinIO Console: `http://localhost:9090`
  Username: `admin` / Password: `admin123`
- MinIO API: `http://localhost:9000`
- Apache Kafka UI: `http://localhost:8081`
- MongoDB UI (Mongo Express): `http://localhost:8082`
- ClickHouse HTTP: `http://localhost:8123`
  Username: `default` / Password: `clickhouse`
- ClickHouse native: `localhost:9001`

To run any project module inside the `app` container:

```bash
docker compose exec app python -m <package.module> [--flag value ...]
```

## Airflow DAGs

All DAGs are available in the Airflow UI at `http://localhost:8080`.

| DAG | Trigger | Description |
|-----|---------|-------------|
| `data_ingestion_landing_zone` | Manual / scheduled | Ingestion + landing zone (temporal → persistent) |
| `trusted_zone_pipeline` | Manual / scheduled | Cleaning into ClickHouse, MongoDB, and MinIO |
| `exploitation_zone_pipeline` | Manual / scheduled | Integration and enrichment into the Exploitation Zone |
| `apache_kafka_streaming` | Manual | Image producer/consumer streaming via Kafka |

To trigger a DAG from the terminal:

```bash
docker compose exec airflow-webserver airflow dags trigger data_ingestion_landing_zone
docker compose exec airflow-webserver airflow dags trigger trusted_zone_pipeline
docker compose exec airflow-webserver airflow dags trigger exploitation_zone_pipeline
docker compose exec airflow-webserver airflow dags trigger apache_kafka_streaming
```

## Common Commands

All commands can be run locally or inside the Docker `app` container by prepending `docker compose exec app`.

**Data Ingestion:**

```bash
python -m src.data_management.data_ingestion.structured_data --limit 50000 --max-csvs 5
python -m src.data_management.data_ingestion.semi_structured_data
python -m src.data_management.data_ingestion.unstructured_data_audio
python -m src.data_management.data_ingestion.unstructured_data_text
```

**Landing Zone:**

```bash
python -m src.data_management.landing_zone.upload_to_temporal
python -m src.data_management.landing_zone.landing_zone
```

**Trusted Zone:**

```bash
python -m src.data_management.trusted_zone.structured_trusted_zone
python -m src.data_management.trusted_zone.unstructured_trusted_zone
python -m src.data_management.trusted_zone.semistructured_weather_trusted_zone
python -m src.data_management.trusted_zone.semistructured_aggregated_trusted_zone
```

**Exploitation Zone:**

```bash
python -m src.data_management.exploitation_zone.structured_exploitation_zone
python -m src.data_management.exploitation_zone.semistructured_exploitation_zone
python -m src.data_management.exploitation_zone.unstructured_exploitation_zone
```

**Data Consumption (run locally, ClickHouse at `localhost:8123`):**

```bash
streamlit run src/data_consumption/structured_data/dashboard.py
streamlit run src/data_consumption/structured_data/risk_prediction.py
```

## ClickHouse

ClickHouse stores the cleaned structured data in the trusted zone and the enriched Data Product in the exploitation zone.

**Web SQL interface:** `http://localhost:8123/play`
Login with username `default` and password `clickhouse`.

**Trusted zone queries:**

```sql
SELECT count() FROM trusted_zone.nyc_collisions
SELECT * FROM trusted_zone.nyc_collisions LIMIT 100
TRUNCATE TABLE trusted_zone.nyc_collisions
```

**Exploitation zone queries:**

```sql
SELECT count() FROM exploitation_zone.collisions_weather
SELECT * FROM exploitation_zone.collisions_weather LIMIT 100
TRUNCATE TABLE exploitation_zone.collisions_weather
```

### Data Governance - Embedded Metadata

The exploitation zone table carries persistent documentation embedded in the ClickHouse catalogue.

**Table-level comment** (product name, owner, lineage, status):

```sql
SELECT comment
FROM system.tables
WHERE database = 'exploitation_zone' AND name = 'collisions_weather';
```

**Column-level comments** (semantic category and description per field):

```sql
SELECT name, type, comment
FROM system.columns
WHERE database = 'exploitation_zone' AND table = 'collisions_weather';
```

## Notes

- `.env` is only needed for ingestion flows that require Kaggle credentials (`KAGGLE_USERNAME`, `KAGGLE_KEY`)
- Shared runtime dependencies live in `requirements.txt`; Airflow-only dependencies in `requirements-airflow.txt`
- For the Kafka image streaming workflow, we have put test images in `downloaded_data/unstructured/images` (10 folders, ~30 images each)
- Full image dataset: `https://github.com/Math-ML-X/TrafficCAM/blob/main/TrafficCAM-download.md`
