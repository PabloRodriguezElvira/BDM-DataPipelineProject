from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
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
    dag_id=config.AIRFLOW_LZ_DAG_ID,
    description=config.AIRFLOW_LZ_DESCRIPTION,
    default_args=DEFAULT_ARGS,
    start_date=config.AIRFLOW_LZ_START_DATE,
    schedule=config.AIRFLOW_LZ_SCHEDULE,
    catchup=config.AIRFLOW_LZ_CATCHUP,
    max_active_runs=config.AIRFLOW_LZ_MAX_ACTIVE_RUNS,
    tags=config.AIRFLOW_LZ_TAGS,
    params={
        "structured_limit": Param(
            100,
            type="integer",
            minimum=1,
            title="Row limit for CSV files",
            description="Maximum number of rows per CSV file to ingest.",
        ),
        "structured_max_csvs": Param(
            50,
            type="integer",
            minimum=1,
            title="Max CSV files",
            description="Maximum number of CSV files to ingest.",
        ),
        "semi_structured_max_locations": Param(
            5,
            type="integer",
            minimum=1,
            title="Max locations for semi-structured files",
            description="Maximum number of locations to query.",
        ),
        "semi_structured_no_hourly": Param(
            False,
            type="boolean",
            title="Set daily forecasts for semi-structured files",
            description="Set to true to request daily forecasts, or false to request hourly forecasts.",
        ),
        "unstructured_text_max_files": Param(
            50,
            type="integer",
            minimum=1,
            title="Unstructured text max files",
            description="Maximum number of text files to ingest.",
        ),
        "unstructured_audio_max_files": Param(
            50,
            type="integer",
            minimum=1,
            title="Unstructured audio max files",
            description="Maximum number of audio files to ingest.",
        ),
        "upload_to_temporal_only": Param(
            "all",
            type="string",
            title="Upload dataset type",
            description="Dataset type to upload to temporal landing: all, structured, semi_structured, unstructured_audio or unstructured_text.",
        ),
        "upload_to_temporal_max_files": Param(
            100,
            type="integer",
            minimum=1,
            title="Upload max files",
            description="Maximum number of files to upload per dataset type to temporal landing.",
        ),
    },
) as dag:
    with TaskGroup(group_id="data_ingestion") as data_ingestion:
        ingest_structured = BashOperator(
            task_id="ingest_structured_data",
            bash_command=(
                f"cd {config.PROJECT_ROOT} && "
                f"{config.PYTHON_BIN} -m src.data_management.data_ingestion.structured_data "
                "--limit {{ params.structured_limit }} "
                "--max-csvs {{ params.structured_max_csvs }}"
            ),
        )

        ingest_semi_structured = BashOperator(
            task_id="ingest_semi_structured_data",
            bash_command=(
                f"cd {config.PROJECT_ROOT} && "
                f"{config.PYTHON_BIN} -m src.data_management.data_ingestion.semi_structured_data "
                "--max-locations {{ params.semi_structured_max_locations }} "
                "{% if params.semi_structured_no_hourly %}--no-hourly{% endif %}"
            ),
        )

        ingest_unstructured_text = BashOperator(
            task_id="ingest_unstructured_text_data",
            bash_command=(
                f"cd {config.PROJECT_ROOT} && "
                f"{config.PYTHON_BIN} -m src.data_management.data_ingestion.unstructured_data_text "
                "--max-files {{ params.unstructured_text_max_files }}"
            ),
        )

        ingest_unstructured_audio = BashOperator(
            task_id="ingest_unstructured_audio_data",
            bash_command=(
                f"cd {config.PROJECT_ROOT} && "
                f"{config.PYTHON_BIN} -m src.data_management.data_ingestion.unstructured_data_audio "
                "--max-files {{ params.unstructured_audio_max_files }}"
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
                f"{config.PYTHON_BIN} -m src.data_management.landing_zone.upload_to_temporal "
                "--only {{ params.upload_to_temporal_only }} "
                "--max-files {{ params.upload_to_temporal_max_files }}"
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
