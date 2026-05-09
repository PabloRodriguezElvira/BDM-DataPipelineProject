from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

import src.common.global_variables as config


DEFAULT_ARGS = {
    "owner": config.AIRFLOW_DEFAULT_OWNER,
    "depends_on_past": config.AIRFLOW_DEFAULT_DEPENDS_ON_PAST,
    "retries": config.AIRFLOW_DEFAULT_RETRIES,
    "retry_delay": timedelta(minutes=config.AIRFLOW_DEFAULT_RETRY_DELAY_MINUTES),
}


with DAG(
    dag_id=config.AIRFLOW_TZ_DAG_ID,
    description=config.AIRFLOW_TZ_DESCRIPTION,
    default_args=DEFAULT_ARGS,
    start_date=config.AIRFLOW_TZ_START_DATE,
    schedule=config.AIRFLOW_TZ_SCHEDULE,
    catchup=config.AIRFLOW_TZ_CATCHUP,
    max_active_runs=config.AIRFLOW_TZ_MAX_ACTIVE_RUNS,
    tags=config.AIRFLOW_TZ_TAGS,
) as dag:

    trusted_zone_structured = BashOperator(
        task_id="trusted_zone_structured",
        bash_command=(
            f"cd {config.PROJECT_ROOT} && "
            f"{config.PYTHON_BIN} -m src.data_management.trusted_zone.structured_trusted_zone"
        ),
    )

    trusted_zone_unstructured = BashOperator(
        task_id="trusted_zone_unstructured",
        bash_command=(
            f"cd {config.PROJECT_ROOT} && "
            f"{config.PYTHON_BIN} -m src.data_management.trusted_zone.unstructured_trusted_zone"
        ),
    )

    trusted_zone_semistructured_weather = BashOperator(
        task_id="trusted_zone_semistructured_weather",
        bash_command=(
            f"cd {config.PROJECT_ROOT} && "
            f"{config.PYTHON_BIN} -m src.data_management.trusted_zone.semistructured_weather_trusted_zone"
        ),
    )

    trusted_zone_semistructured_cameras = BashOperator(
        task_id="trusted_zone_semistructured_cameras",
        bash_command=(
            f"cd {config.PROJECT_ROOT} && "
            f"{config.PYTHON_BIN} -m src.data_management.trusted_zone.semistructured_aggregated_trusted_zone"
        ),
    )

    [trusted_zone_structured, trusted_zone_unstructured,
     trusted_zone_semistructured_weather, trusted_zone_semistructured_cameras]
