"""Classes for unit-testing."""

from pipelines.dags.main_ingestion_operator.dbt_schema import (
    DbtTable, Model, PiiRedactionVars, Source, SourceTable)


class DAGConfig:
    """Class for unit testing DBT model creation."""

    def ___init__(self):
        """Perform post init activities.

        This includes the following:
        *   Instantiate the transform configurations for dbt
        *   Instantiate the validation models for dbt
        """
        self.dbt_model: Model = Model(
            name="test",
            variables=PiiRedactionVars(not_pii_data="", table_source=""),
            tables=[
                DbtTable(name="test", alias="test", schema="test", sql="test"),
            ],
            sources=[Source(name="test", source_tables=[SourceTable(name="test")])],
        )
