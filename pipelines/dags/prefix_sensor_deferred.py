"""DAG to test Data Lineage."""

import pendulum
from airflow.models import DAG
from pipelines.custom_operators.airflow_sensor_wait_for_files_deferred import \
    DeferredSensorGCSPrefix

args = {
    "owner": "Test",
    "start_date": pendulum.today(),
    "provide_context": True,
    "retries": 0,
    "schedule": None,
}

with DAG(
    "gcs_deferrable_sensor_prefix_test",
    description="data lineage",
    default_args=args,
    catchup=False,
    max_active_runs=5,
    doc_md="""
    # Trigger cloudrun test dag
    A simple dag to demonstrate using cloudrun with a deferrable operator.

    This will run a task in cloudrun which will return the status and the logs to airflow, so the logs are unified in one place.
    """,
) as dag:
    DeferredSensorGCSPrefix(
        task_id="wait_for_files",
        bucket_name="tim-ellis-smith-testing-inputs",
        prefix="test/",
        timeout=2,
    )
