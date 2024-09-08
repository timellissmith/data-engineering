"""Unit tests for writing models to dbt."""

import builtins
import io
from os import makedirs, remove
from os.path import isdir, isfile

from pytest import fixture
from yaml import safe_load  # type: ignore

from pipelines.dags.main_ingestion_operator.dbt_schema import (
    DataTest, DbtTable, Model, PiiRedactionVars, Source, SourceTable)
from scripts.python_helpers.helpers import (construct_model_yaml,
                                            create_source_tables,
                                            deploy_sql_files, list_sql_files,
                                            make_model_directory,
                                            search_and_filter_model_files,
                                            search_for_dbt_model_directories,
                                            write_model_configuration,
                                            write_source_table_configuration,
                                            write_variable_information)

table_list = [DbtTable(name="table1", schema="default", sql="one.sql")]  # type: ignore

table1_list = [
    DbtTable(
        name="table3",
        description="test_table3",
        materialized="table",
        schema="default",
        alias="table4",
        project="this",  # type: ignore
        tests=[
            DataTest(  # type: ignore # Ignored due to error in mypy
                name="dbt_utils.equality",
                parameters={"compare_model": "ref('error_stream')"},
            )
        ],
        sql="test.sql",
    )
]  # type: ignore

model_name = "unittest"

sql_list = ["one.sql"]

model_variables = PiiRedactionVars(not_pii_data="one", table_source="this_table")  # type: ignore

model = Model(description="unittest", name="unittest", tables=table_list, variables=model_variables, sources=[])  # type: ignore

model_with_tests = Model(description="test", name="test", tables=table1_list, variables=model_variables, sources=[])  # type: ignore

source_tables = [SourceTable(identifier="source1", name="s1"), SourceTable(identifier="source2", name="s2")]  # type: ignore

sources = [Source(name="source", source_tables=source_tables)]  # type: ignore


def patch_open(open_func, files):
    """Get all created files."""

    def open_patched(
        path,
        mode="r",
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
        closefd=True,
        opener=None,
    ):
        if "w" in mode and not isfile(path):
            files.append(path)
        return open_func(
            path,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            closefd=closefd,
            opener=opener,
        )

    return open_patched


@fixture(autouse=True)
def cleanup_files(monkeypatch):
    """Remove all files written by the unit tests."""
    files = []
    monkeypatch.setattr(builtins, "open", patch_open(builtins.open, files))
    monkeypatch.setattr(io, "open", patch_open(io.open, files))
    yield
    for file in files:
        remove(file)


def test_create_model_directory():
    """Test that the model directory is created correctly."""
    model_dir = "test_dir"
    make_model_directory(model_dir)
    assert isdir(f"dbt/models/{model_dir}")


def test_list_sql_files():
    """Test list_sql files returns a list of SQL files."""
    assert list_sql_files(table_list) == sql_list


def test_deploy_sql_files():
    """Test that the sql files are deployed correctly."""
    with open("pipelines/sql/one.sql", "w") as f:
        f.write("$__not_pii_data__")
    deploy_sql_files(model_name, sql_list)
    assert isfile(f"dbt/models/{model_name}/{model_name}_one.sql")


def test_construct_model_yaml():
    """Test that the model yaml is constructed correctly."""
    assert construct_model_yaml(model) == [
        {"name": "table1", "config": {"materialized": "table", "schema": "default"}}
    ]


def test_write_model_configuration():
    """Test that the model configuration is formed correctly."""
    makedirs(f"models/{model_name}", mode=0o755, exist_ok=True)
    write_model_configuration(model)
    with open(f"dbt/models/{model_name}/schema.yml", "r") as f:
        result = safe_load(f)

    assert result == {
        "version": 2,
        "models": [
            {
                "name": "table1",
                "config": {
                    "materialized": "table",
                    "schema": "default",
                },
            }
        ],
    }


def test_construct_model_with_tests():
    """Test that a model that includes tests, database and alias is configured correctly."""
    assert construct_model_yaml(model_with_tests) == [
        {
            "name": "table3",
            "description": "test_table3",
            "config": {
                "materialized": "table",
                "alias": "table4",
                "database": "this",
                "schema": "default",
            },
            "tests": [{"dbt_utils.equality": {"compare_model": "ref('error_stream')"}}],
        }
    ]


def test_create_source_tables():
    """Check that the source table list is correctly created."""
    table_list_return = create_source_tables(source_tables)
    assert table_list_return == [
        {"identifier": "source1", "name": "s1"},
        {"identifier": "source2", "name": "s2"},
    ]


def test_write_source_table_configuration():
    """Test that the source tables yaml configuration is written correctly."""
    makedirs(f"models/{model_name}", mode=0o755, exist_ok=True)
    write_source_table_configuration(sources, "unittest")
    with open(f"dbt/models/{model_name}/sources.yml", "r") as f:
        result = safe_load(f)

    assert result == {
        "version": 2,
        "sources": [
            {
                "name": "source",
                "tables": [
                    {"identifier": "source1", "name": "s1"},
                    {"identifier": "source2", "name": "s2"},
                ],
            }
        ],
    }


def test_write_variable_information():
    """Test that the variables are formed correctly to be written to yaml."""
    with open("dbt/models/unittest/unittest_one.sql", "w") as f:
        f.write("$__not_pii_data__")

    write_variable_information(model_variables, sql_list, model_name)
    with open("dbt/models/unittest/unittest_one.sql", "r") as f:
        assert f.read() == "one"


def test_search_for_model_directories():
    """Test that the search for model files returns the correct files."""
    [
        makedirs(f"temp/models/{model_name}", exist_ok=True)
        for model_name in ["test_model", "test_model2"]
    ]
    assert set(search_for_dbt_model_directories("temp")) == {
        "temp/models/test_model",
        "temp/models/test_model2",
    }


def test_search_and_filter_model_files():
    """Test that the function returns only python files from different directories."""
    [
        makedirs(f"temp/models/{model_name}", exist_ok=True)
        for model_name in ["test_model"]
    ]
    for file in ["temp/models/test_model/one.py"]:
        with open(file, "w") as f:
            f.write("test")
    files = search_and_filter_model_files(["temp/models/test_model"])
    assert files == ["temp.models.test_model.one"]


# def test_search_in_airflow_dags():
#     """Test that the function returns a dbt model from an airflow dataclass."""
#     result = next(
#         obj
#         for obj in generate_dags(directory="metadata/main_dags")
#         if obj.dag_id == "census_public_census_v1"
#     )
#     assert result.dbt_model == Model(
#         variables=PiiRedactionVars(
#             not_pii_data=(
#                 "\tkey,\n\tpopulation,\n\tpopulation_female,\n\tpopulation_age_00_09,\n\tpopulation_age_10_19,\n"
#                 "\tpopulation_age_20_29,\n\tpopulation_age_30_39,\n\tpopulation_age_40_49,\n\tpopulation_age_50_59,\n"
#                 "\tpopulation_age_60_69,\n\tpopulation_age_70_79,\n\tpopulation_age_80_and_older"
#             ),
#             table_source="source('census_public', 'census_v1')",
#         ),
#         name="census_public_census_v1",
#         sources=[
#             Source(
#                 name="census_public",
#                 source_tables=[SourceTable(name="census_v1", identifier=None)],
#                 schema="census_public",
#                 database=None,
#             )
#         ],
#         tables=[
#             DbtTable(
#                 name="census_public_census_v1_filter_pii_data",
#                 schema="census_public_restricted",
#                 sql="filter_pii_data.sql",
#                 materialized="view",
#                 description="census with PII redacted",
#                 alias="census_v1",
#                 project="",
#                 tests=[],
#                 columns=[
#                     {"name": "key", "description": "reference_key"},
#                     {"name": "population", "description": "Total Population"},
#                     {"name": "population_female", "description": "Female population"},
#                     {
#                         "name": "population_age_00_09",
#                         "description": "Population under 10",
#                     },
#                     {
#                         "name": "population_age_10_19",
#                         "description": "Population between 10 and 20",
#                     },
#                     {
#                         "name": "population_age_20_29",
#                         "description": "Population between 20 and 30",
#                     },
#                     {
#                         "name": "population_age_30_39",
#                         "description": "Population between 30 and 40",
#                     },
#                     {
#                         "name": "population_age_40_49",
#                         "description": "Population between 40 and 50",
#                     },
#                     {
#                         "name": "population_age_50_59",
#                         "description": "Population between 50 and 60",
#                     },
#                     {
#                         "name": "population_age_60_69",
#                         "description": "Population between 60 and 70",
#                     },
#                     {
#                         "name": "population_age_70_79",
#                         "description": "Population between 70 and 80",
#                     },
#                     {
#                         "name": "population_age_80_and_older",
#                         "description": "Population over 80",
#                     },
#                 ],
#             )
#         ],
#         description="Description goes here!",
#     )
