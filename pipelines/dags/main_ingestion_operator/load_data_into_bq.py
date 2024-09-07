"""Simple DAG to handle GCS object to BigQuery."""
import os
from datetime import date, datetime

from airflow.decorators import task
from airflow.models.dag import DAG

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import \
    GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import \
    GCSToGCSOperator
from airflow.sensors.base import PokeReturnValue
from pipelines.dags.main_ingestion_operator.main_schema_definitions import \
    MainIngestionDag
from pipelines.shared.dag_loaders import generate_dags

project_id = os.getenv("GCP_PROJECT")
local = os.getenv("LOCAL")


def create_dag(dag_config: MainIngestionDag) -> DAG:
    """Create a dag from the given configuration.

    Args:
        dag_config (MainIngestionDag): The configuration for the DAG.

    Returns:
        DAG: The DAG to be created.
    """
    default_args = {
        "start_date": dag_config.airflow_variables.start_date,
        "owner": dag_config.airflow_variables.owner,
        "retries": dag_config.airflow_variables.retries,
        "catchup": dag_config.airflow_variables.catchup,
        "schedule_interval": dag_config.airflow_variables.schedule,
    }

    with DAG(
            dag_id=dag_config.dag_id,
            default_args=default_args,
            tags=dag_config.tags,
            max_active_runs=dag_config.airflow_variables.max_active_runs,
    ) as dag:
        insert_into_bq = GCSToBigQueryOperator(
            bucket=dag_config.source_bucket,
            destination_project_dataset_table=dag_config.project_dataset_table,
            source_objects="yet_another_cat_pick.jpg",
            task_id="load_data_into_gcp"
        )

        move_to_archive = GCSToGCSOperator(
            task_id="move_to_archive",
            source_bucket=dag_config.source_bucket,
            source_objects=[f"{dag_config.series_name}/*"],
            destination_bucket=dag_config.archive_bucket,
            destination_object="census_archived.csv",
            move_object=True,
        )

        move_to_unprocessed = GCSToGCSOperator(
            task_id="move_to_unprocessed",
            source_bucket=dag_config.source_bucket,
            source_objects=[f"{dag_config.series_name}/*"],
            destination_bucket=dag_config.unprocessed_bucket,
            destination_object="census_unprocessed.csv",
            trigger_rule="all_failed",
            move_object=True,
        )
        if dag_config.airflow_variables.enable_sensors:
            wait_for_files = GCSObjectsWithPrefixExistenceSensor(
                task_id="wait_for_files",
                deferrable=True,
                bucket="tims-random-bucket",
                prefix=dag_config.table_name,
            )

            wait_for_files.set_downstream(insert_into_bq)

            # Using a sensor operator to wait for the upstream data to be ready.

            @task.sensor(poke_interval=60, timeout=3600, mode="reschedule")
            def wait_for_files() -> PokeReturnValue:
                hook = GCSHook()
                if hook.exists("tims-random-bucket", "yet_another_cat_pick.jpg"):
                    return PokeReturnValue(is_done=True, xcom_value="")

        insert_into_bq >>  [move_to_archive, move_to_unprocessed]
    return dag


dags = generate_dags(directory="main_dags")
for dag_config in dags:
    globals()[dag_config.dag_id] = create_dag(dag_config)
