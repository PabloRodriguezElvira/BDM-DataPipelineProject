from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

import src.common.global_variables as config


DEFAULT_ARGS = {
    "owner": config.AIRFLOW_DEFAULT_OWNER,
    "depends_on_past": config.AIRFLOW_DEFAULT_DEPENDS_ON_PAST,
    "retries": config.AIRFLOW_DEFAULT_RETRIES,
    "retry_delay": timedelta(minutes=config.AIRFLOW_DEFAULT_RETRY_DELAY_MINUTES),
}

with DAG(
    dag_id=config.AIRFLOW_BATCH_DAG_ID,
    description=config.AIRFLOW_BATCH_DESCRIPTION,
    default_args=DEFAULT_ARGS,
    start_date=config.AIRFLOW_BATCH_START_DATE,
    schedule=config.AIRFLOW_BATCH_SCHEDULE,
    catchup=config.AIRFLOW_BATCH_CATCHUP,
    max_active_runs=config.AIRFLOW_BATCH_MAX_ACTIVE_RUNS,
    tags=config.AIRFLOW_BATCH_TAGS,
) as dag:

    with TaskGroup(group_id="data_ingestion") as data_ingestion:
        ingest_structured = BashOperator(
            task_id="ingest_structured_data",
            bash_command=(
                f"cd {config.PROJECT_ROOT} && "
                f"{config.PYTHON_BIN} -m src.data_management.data_ingestion.structured_data "
                f"--limit {config.AIRFLOW_STRUCTURED_LIMIT} "
                f"--max-csvs {config.AIRFLOW_STRUCTURED_MAX_CSVS}"
            ),
        )

        ingest_semi_structured = BashOperator(
            task_id="ingest_semi_structured_data",
            bash_command=(
                f"cd {config.PROJECT_ROOT} && "
                f"{config.PYTHON_BIN} -m src.data_management.data_ingestion.semi_structured_data "
                f"--max-locations {config.AIRFLOW_SEMI_STRUCTURED_MAX_LOCATIONS}"
            ),
        )

        ingest_unstructured_text = BashOperator(
            task_id="ingest_unstructured_text_data",
            bash_command=(
                f"cd {config.PROJECT_ROOT} && "
                f"{config.PYTHON_BIN} -m src.data_management.data_ingestion.unstructured_data_text "
                f"--max-files {config.AIRFLOW_UNSTRUCTURED_TEXT_MAX_FILES}"
            ),
        )

        ingest_unstructured_audio = BashOperator(
            task_id="ingest_unstructured_audio_data",
            bash_command=(
                f"cd {config.PROJECT_ROOT} && "
                f"{config.PYTHON_BIN} -m src.data_management.data_ingestion.unstructured_data_audio "
                f"--max-files {config.AIRFLOW_UNSTRUCTURED_AUDIO_MAX_FILES}"
            ),
        )

        [
            ingest_structured,
            ingest_semi_structured,
            ingest_unstructured_text,
            ingest_unstructured_audio,
        ]

    with TaskGroup(group_id="landing_zone") as landing_zone:
        upload_to_temporal = BashOperator(
            task_id="upload_to_temporal",
            bash_command=(
                f"cd {config.PROJECT_ROOT} && "
                f"{config.PYTHON_BIN} -m src.data_management.landing_zone.upload_to_temporal"
            ),
        )

        process_landing_zone = BashOperator(
            task_id="process_landing_zone",
            bash_command=(
                f"cd {config.PROJECT_ROOT} && "
                f"{config.PYTHON_BIN} -m src.data_management.landing_zone.landing_zone"
            ),
        )

        upload_to_temporal >> process_landing_zone

    data_ingestion >> landing_zone


with DAG(
    dag_id=config.AIRFLOW_STREAMING_DAG_ID,
    description=config.AIRFLOW_STREAMING_DESCRIPTION,
    default_args=DEFAULT_ARGS,
    start_date=config.AIRFLOW_STREAMING_START_DATE,
    schedule=config.AIRFLOW_STREAMING_SCHEDULE,
    catchup=config.AIRFLOW_STREAMING_CATCHUP,
    max_active_runs=config.AIRFLOW_STREAMING_MAX_ACTIVE_RUNS,
    tags=config.AIRFLOW_STREAMING_TAGS,
) as streaming_dag:

    start_consumer = BashOperator(
        task_id="start_image_consumer",
        bash_command=(
            f"cd {config.PROJECT_ROOT} && "
            f"timeout {config.AIRFLOW_STREAMING_TIMEOUT_SECONDS} "
            f"{config.PYTHON_BIN} -m src.data_management.data_ingestion.unstructured_data_image_consumer"
        ),
    )

    start_producer = BashOperator(
        task_id="start_image_producer",
        bash_command=(
            f"cd {config.PROJECT_ROOT} && "
            f"timeout {config.AIRFLOW_STREAMING_TIMEOUT_SECONDS} "
            f"{config.PYTHON_BIN} -m src.data_management.data_ingestion.unstructured_data_image_producer"
        ),
    )

    start_consumer >> start_producer