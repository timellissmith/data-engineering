"""Schema for DBT models."""

from typing import Any, Dict, List, Optional, Union

from attr import field, frozen  # type: ignore


@frozen
class DataTest:
    """Schema for tests.

    Args:
        name: The name of the test.
        parameters: The parameters of the test.
    """

    name: str
    parameters: Dict[str, Any]


@frozen
class DbtTable:
    """
    Dataclass describing a table created by the model.

    Args:
        name: The name of the table
        schema: the dataset of the table
        sql: the sql file relating to the model. This should be stored in ./sql and the name should not contain ./sql"
        materialized: Can be view, table, incremental, or ephemeral
        description: A description of the table
        alias: An optional alias for the table (this will override the table name)
        project: The GCP project of the table (overrides the settings in profiles.yml
        tests: any test definitions
        columns: the columns to include
    """

    name: str
    schema: str
    sql: str
    materialized: str = field(default="table")
    description: str = field(default="")
    alias: str = field(default="")
    project: str = field(default="")
    tests: List[DataTest] = field(default=[])
    columns: List[Dict] = field(default=[])


@frozen
class PiiRedactionVars:
    """Specific variables for the PiiRedactionVars model type."""

    not_pii_data: str
    table_source: str
    dataset_source: str


ModelTypes = Union[PiiRedactionVars]


@frozen
class SourceTable:
    """Define the name of source tables. This lies under the dataset name in the yaml structure.

    Args:
          identifier: The name of the table as it appears in the source
          name: The optional name that you can use to reference the table in SQL
    """

    name: str
    source_format: str
    source_location: str
    identifier: Optional[str] = None


@frozen
class Source:
    """The name of any source datasets used.

    Args:
          name: The optional name that you can use to reference the dataset in the SQL
          source_tables: The source tables in the dataset (schema).
          schema: The name of the dataset as it appears in the BigQuery.
          database: equivalent to gcp project.
    """

    name: str
    source_tables: List[SourceTable]
    schema: Optional[str] = None
    database: Optional[str] = None


@frozen
class Model:
    """Define a DBT Model.

    Args:
        variables: Any variables needing to be passed to the model.
        name: the name of the model
        sources: Any data sources used in the model.
        tables: The list of tables created by the model
        description: A description of the model
    """

    variables: ModelTypes
    name: str
    sources: List[Source]
    tables: List[DbtTable]
    description: Optional[str] = None
