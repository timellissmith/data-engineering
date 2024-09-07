"""Simple DAG to handle GCS object to BigQuery."""
import os
from datetime import date, datetime

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.google.cloud.sensors.gcs import \
    GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import \
    GCSToGCSOperator
from airflow.utils.dag_parsing_context import get_parsing_context

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
            dag_id=f"{dag_config.dag_id}_2",
            default_args=default_args,
            tags=dag_config.tags,
            max_active_runs=dag_config.airflow_variables.max_active_runs,
    ) as dag:
        @task()
        def generate_raw_schema():
            return ",".join([f"{col.column_name:STRING}" for col in dag_config.series_columns])

        # load raw file into bq as table
        load_raw_file = GCSToBigQueryOperator(
            task_id="load_raw_files",
            bucket=dag_config.source_bucket,
            source_objects=dag_config.series_name,
            schema_fields="{{ task_instance.xcom_pull(task_ids='generate_raw_schema') }}",
            autodetect=False,
            source_format=dag_config.series_source.datasource,
            destination_project_dataset_table=dag_config.project_dataset_table,
        )

        # Calculate transformed SQL statement
        @task()
        def generate_sql_for_transformation():
            starting_statement = f"INSERT INTO {dag_config.project_dataset_table} SELECT"
            column_statements = []
            for col in dag_config.series_columns:
                statement = ""
                if col.column_data_type == "STRING":
                    statement = f"\t{col.column_name}"
                elif col.column_data_type in ["INTEGER", "DECIMAL"]:
                    statement = f"\tSAFE_CAST({col.column_name} AS {col.column_data_type}"
                elif col.column_name == "DATE":
                    statement = (f"\tPARSE_DATE({col.column_format, col.column_name}")
                column_statements += statement
            end_sql = "FROM "
            full_sql = starting_statement + "\n".join(column_statements) + end_sql


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
                bucket=dag_config.source_bucket,
                prefix=dag_config.series_name,
                deferrable=True
            )

        generate_raw_schema() >> load_raw_file
    return dag


dags = generate_dags(directory="main_dags")
current_dag_id = get_parsing_context().dag_id
for dag_config in dags:
    if current_dag_id is not None and current_dag_id != dag_config.dag_id:
        globals()[dag_config.dag_id] = create_dag(dag_config)
