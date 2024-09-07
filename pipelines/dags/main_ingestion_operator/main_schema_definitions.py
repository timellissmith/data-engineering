"""The Schema definitions for DAGS."""

from datetime import datetime
from os import getenv
from typing import List, Union

from attr import define, field, validators  # type: ignore
from pendulum import yesterday
from pipelines.dags.main_ingestion_operator.dbt_schema import (
    DbtTable, Model, PiiRedactionVars, Source, SourceTable)
from pipelines.shared.converters import (convert_to_dataset_name,
                                         split_keywords, version_to_int)
from pipelines.shared.validators import (check_for_duplicate_col_names,
                                         is_greater_than_one,
                                         validate_column_name)

VALID_TYPES = ["STRING", "INTEGER", "DECIMAL"]
VALID_CLASSIFICATIONS = ["RED", "ORANGE", "GREEN"]
VALID_SOURCES = ["avro", "parquet", "oracle", "mssql", "csv"]



@define(frozen=True)
class AirflowVariables:
    """Default Airflow variables."""

    owner: str = field(default="Data Engineering")
    enable_sensors: bool = field(default=True)
    bigquery_location: str = field(default="europe-west2")
    schedule: Union[str, None] = field(default=None)
    max_active_runs: int = field(default=1)
    catchup: bool = field(default=False)
    email_on_failure: bool = field(default=False)
    start_date: datetime = field(default=yesterday(), init=True)
    gcp_connection: str = field(default="google_cloud_default")
    retries: int = field(default=0)


@define()
class Schema:
    """Define the schema of a column."""
    column_name: str = field(
        validator=[validators.instance_of(str), validate_column_name]
    )
    column_data_type: str = field(validator=[validators.in_(VALID_TYPES)])
    column_description: str = field(default="")
    column_format: str = field(default="")
    column_is_pk: bool = field(default=False, converter=bool)
    column_is_fk: bool = field(default=False, converter=bool)
    column_nullable: bool = field(default=False, converter=bool)
    column_is_pii: bool = field(default=False)


@define
class SeriesSource:
    file_path: str
    datasource: str = field(validator=[validators.in_(VALID_SOURCES)])
    file_quote_character: str = field(default="\"")
    file_separator: str = field(default=",")
    file_number_header_records: int = field(default=0)
    file_number_footer_records: int = field(default=0)


@define
class MainIngestionDag:
    """Define the config of a main ingestion DAG."""

    dataset_name: str
    dataset_supplier: str
    series_columns: List[Schema] = field(validator=[check_for_duplicate_col_names])
    series_source: SeriesSource
    dataset_domain: str
    series_name: str
    series_security_classification: str = field(
        validator=validators.in_(VALID_CLASSIFICATIONS)
    )
    dataset_search_keywords: str = field(default="", converter=split_keywords)
    project: str = getenv("GCP_PROJECT", "TEST")
    schema_version: str = field(
        default=1,
        converter=version_to_int,
        validator=[validators.instance_of(int), is_greater_than_one],
    )
    airflow_variables: AirflowVariables = AirflowVariables()
    description: str = field(default="Description goes here!")
    paused: bool = field(default=False)
    dataset: str = field(default="", init=False)
    dag_id: str = field(default="", init=False)
    tags: list = field(default="", init=False)
    source_bucket: str = field(default="", init=False)
    unprocessed_bucket: str = field(default="", init=False)
    archive_bucket: str = field(default="", init=False)
    project_dataset_table: str = field(default="", init=False)
    code_bucket: str = field(default="", init=False)
    temp_bucket: str = field(default="", init=False)
    schema_string: str = field(default="", init=False)
    dataset_dash: str = field(default="", init=False)
    table_name: str = field(default="", init=False)

    def __attrs_post_init__(self):
        """Define the variables post init."""
        self.dataset = convert_to_dataset_name(self.dataset_name)
        self.dataset_dash = self.dataset.replace("_", "-")
        self.tags = [self.dataset, self.series_name] + self.dataset_search_keywords  # type: ignore
        self.project_dataset_table = (
            f"{self.project}.{self.dataset}.{self.series_name}_{self.schema_version}"
        )
        self.dag_id = f"{self.dataset}_{self.series_name}_v{self.schema_version}"
        self.source_bucket = f"{self.project}-{self.dataset_dash}-source"
        self.unprocessed_bucket = f"{self.project}-{self.dataset_dash}-unprocessed"
        self.archive_bucket = f"{self.project}-{self.dataset_dash}-archive"
        self.code_bucket = f"{self.project}-code"
        self.temp_bucket = f"{self.project}-temp"
        self.schema_string = ",".join(
            [f"{col.column_name} {col.column_data_type}" for col in self.series_columns]
        )
        self.table_name = f"{self.series_name}_v{self.schema_version}"