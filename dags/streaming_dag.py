from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import src.common.global_variables as config


DEFAULT_ARGS = {
    "owner": config.AIRFLOW_DEFAULT_OWNER,
    "depends_on_past": config.AIRFLOW_DEFAULT_DEPENDS_ON_PAST,
    "retries": config.AIRFLOW_DEFAULT_RETRIES,
    "retry_delay": timedelta(minutes=config.AIRFLOW_DEFAULT_RETRY_DELAY_MINUTES),
}


with DAG(
    dag_id=config.AIRFLOW_STREAMING_DAG_ID,
    description=config.AIRFLOW_STREAMING_DESCRIPTION,
    default_args=DEFAULT_ARGS,
    start_date=config.AIRFLOW_STREAMING_START_DATE,
    schedule=config.AIRFLOW_STREAMING_SCHEDULE,
    catchup=config.AIRFLOW_STREAMING_CATCHUP,
    max_active_runs=config.AIRFLOW_STREAMING_MAX_ACTIVE_RUNS,
    tags=config.AIRFLOW_STREAMING_TAGS,
    params={
        "streaming_timeout_seconds": Param(
            600,
            type="integer",
            minimum=1,
            title="Streaming timeout seconds",
            description="Maximum execution time for producer and consumer",
        ),
    },
) as dag:
    def _reset_topic():
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import UnknownTopicOrPartitionError

        admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_SERVER)
        try:
            admin.delete_topics([config.UNSTRUCTURED_IMAGE_TOPIC_NAME])
        except UnknownTopicOrPartitionError:
            pass
        import time; time.sleep(2)
        admin.create_topics([
            NewTopic(
                name=config.UNSTRUCTURED_IMAGE_TOPIC_NAME,
                num_partitions=1,
                replication_factor=1,
            )
        ])
        admin.close()

    reset_topic = PythonOperator(
        task_id="reset_topic",
        python_callable=_reset_topic,
    )

    run_image_stream = BashOperator(
        task_id="run_image_stream",
        bash_command=(
            f"""
            cd {config.PROJECT_ROOT}
            timeout {{{{ params.streaming_timeout_seconds }}}} {config.PYTHON_BIN} -m src.data_management.data_ingestion.unstructured_data_image_consumer &
            consumer_pid=$!

            sleep 5

            timeout {{{{ params.streaming_timeout_seconds }}}} {config.PYTHON_BIN} -m src.data_management.data_ingestion.unstructured_data_image_producer
            producer_exit=$?

            wait $consumer_pid
            consumer_exit=$?

            if [ "$producer_exit" -ne 0 ]; then
              exit "$producer_exit"
            fi

            if [ "$consumer_exit" -ne 0 ] && [ "$consumer_exit" -ne 124 ]; then
              exit "$consumer_exit"
            fi
            """
        ),
    )

    run_cv_alerts = BashOperator(
        task_id="run_cv_alerts",
        bash_command=(
            f"""
            cd {config.PROJECT_ROOT}
            timeout {{{{ params.streaming_timeout_seconds }}}} {config.PYTHON_BIN} -m src.data_consumption.unstructured_data.spark_streaming_cv_alerts
            exit_code=$?

            if [ "$exit_code" -ne 0 ] && [ "$exit_code" -ne 124 ]; then
              exit "$exit_code"
            fi
            """
        ),
    )

    reset_topic >> [run_image_stream, run_cv_alerts]
