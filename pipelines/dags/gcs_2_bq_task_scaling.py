"""Simple DAG to handle GCS object to BigQuery."""

import logging
import os

from airflow import XComArg, models
from airflow.providers.google.cloud.sensors.gcs import \
    GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import \
    GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import \
    GCSToGCSOperator
from airflow.utils.dates import days_ago

default_args = {
    "start_date": days_ago(1),
    "owner": "data engineering",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # "catchup": False,
}
PROJECT_NAME = os.environ.get("GCP_PROJECT")
DATASET_NAME = "mvp_dataset"
TABLE_NAME = "usa_census"
SOURCE_BUCKET = "tim-ellis-smith-sandbox-de-census-public-source"
ARCHIVE_BUCKET = "tim-ellis-smith-sandbox-de-census-public-archive"
UNPROCESSED_BUCKET = f"{PROJECT_NAME}-unprocessed-bucket"

with models.DAG(
    dag_id="gcs_2_bq_dynamic",
    default_args=default_args,
    schedule_interval=None,
    tags=["census", "public_census"],
) as dag:

    wait_for_file = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_file", bucket=SOURCE_BUCKET, prefix="files", do_xcom_push=True
    )

    insert_into_bq = GCSToBigQueryOperator.partial(
        task_id="insert_into_bq",
        bucket=SOURCE_BUCKET,
        source_format="CSV",
        create_disposition="CREATE_IF_NEEDED",
        destination_project_dataset_table=f"{PROJECT_NAME}.{DATASET_NAME}.{TABLE_NAME}",
        skip_leading_rows=1,
        schema_fields=[
            {"name": "key", "type": "STRING", "mode": "NULLABLE"},
            {"name": "population", "type": "STRING", "mode": "NULLABLE"},
            {"name": "population_male", "type": "STRING", "mode": "NULLABLE"},
            {"name": "population_female", "type": "STRING", "mode": "NULLABLE"},
            {"name": "population_age_00_09", "type": "STRING", "mode": "NULLABLE"},
            {"name": "population_age_10_19", "type": "STRING", "mode": "NULLABLE"},
            {"name": "population_age_20_29", "type": "STRING", "mode": "NULLABLE"},
            {"name": "population_age_30_39", "type": "STRING", "mode": "NULLABLE"},
            {"name": "population_age_40_49", "type": "STRING", "mode": "NULLABLE"},
            {"name": "population_age_50_59", "type": "STRING", "mode": "NULLABLE"},
            {"name": "population_age_60_69", "type": "STRING", "mode": "NULLABLE"},
            {"name": "population_age_70_79", "type": "STRING", "mode": "NULLABLE"},
            {
                "name": "population_age_80_and_older",
                "type": "STRING",
                "mode": "NULLABLE",
            },
        ],
        write_disposition="WRITE_TRUNCATE",
    ).expand(source_objects=XComArg(wait_for_file))
    logging.info("inserted or appended records into big query")

    move_to_archive = GCSToGCSOperator(
        task_id="move_to_archive",
        source_bucket=SOURCE_BUCKET,
        source_objects=["sample_datasets/census.csv"],
        destination_bucket=ARCHIVE_BUCKET,
        destination_object="census_archived.csv",
        move_object=True,
    )

    move_to_unprocessed = GCSToGCSOperator(
        task_id="move_to_unprocessed",
        source_bucket=SOURCE_BUCKET,
        source_objects=["sample_datasets/census.csv"],
        destination_bucket=UNPROCESSED_BUCKET,
        destination_object="census_unprocessed.csv",
        trigger_rule="all_failed",
        move_object=True,
    )

    wait_for_file >> insert_into_bq >> [move_to_archive, move_to_unprocessed]
