"""Helpers for write dbt models script."""


import contextlib
import logging
from os import listdir, makedirs, walk
from os.path import isdir, join, splitext
from pathlib import Path
from shutil import copyfile
from typing import Any, Dict, List

try:
    from attrs import asdict  # type: ignore
except ModuleNotFoundError:
    from attr import asdict  # type: ignore

from pipelines.dags.main_ingestion_operator.dbt_schema import (DbtTable, Model,
                                                               ModelTypes,
                                                               Source,
                                                               SourceTable)
from pipelines.shared.dag_loaders import generate_dags
from yaml import safe_dump  # type: ignore


def make_model_directory(model_name: str) -> None:
    """
    Create a directory for the model to reside in.

    Args:
        model_name: The name of the model
    """
    makedirs(f"dbt/models/{model_name}", mode=0o755, exist_ok=True)


def list_sql_files(tables: List[DbtTable]) -> List[str]:
    """
    Return a list of the SQL files needing to be copied for the model.

    Args:
        tables: The list of tables defined in the model.

    Returns: None

    """
    return [table.sql for table in tables]


def deploy_sql_files(model_name: str, sql_files: List[str]) -> None:
    """
    Copy SQL files into place.

    Args:
        model_name: the name of the model (used to determine directory structure)
        sql_files: the sql files relating to the model

    Returns: None

    """
    logging.info("Deploying SQL files")
    make_model_directory(model_name)
    for file in sql_files:
        copyfile(
            f"pipelines/sql/{file}", f"dbt/models/{model_name}/{model_name}_{file}"
        )


def construct_model_yaml(model: Model) -> List[Dict[str, Any]]:
    """
    Construct the model dictionary to be written to the yaml file.

    Args:
        model: The model dataclass

    Returns: The structure for the model to be written to yaml

    """
    result: List[Any] = []
    for table in model.tables:
        model_dict: Dict[str, Any] = {
            "name": table.name,
            "config": {"schema": table.schema, "materialized": table.materialized},
        }
        if table.description:
            model_dict["description"] = table.description
        if table.alias:
            model_dict["config"]["alias"] = table.alias
        if table.project:
            model_dict["config"]["database"] = table.project
        if table.tests:
            model_dict["tests"] = [{test.name: test.parameters} for test in table.tests]
        if table.columns:
            model_dict["columns"] = table.columns
        result.append(model_dict)
    return result


def write_model_configuration(model: Model) -> None:
    """
    Update the config with the model information to be written.

    Args:
        model: the model to be added to the config

    Returns: None
    """
    make_model_directory(model.name)
    logging.info("Updating models")
    model_config = {"version": 2, "models": construct_model_yaml(model)}
    with open(f"dbt/models/{model.name}/schema.yml", "w") as f:
        safe_dump(model_config, f)


def create_source_tables(tables: List[SourceTable]) -> List[Dict[Any, Any]]:
    """
    Create the configuration for the source tables to be appended to the source datasets.

    Args:
        tables: the dataclass of the source tables

    Returns: a list of the table configs
    """
    table_list: List[Any] = []
    for table in tables:
        dict1: Dict[Any, Any] = {}
        if table.identifier:
            dict1["identifier"] = table.identifier
        dict1["name"] = table.name
        table_list.append(dict1)
    return table_list


def write_source_table_configuration(sources: List[Source], model_name: str):
    """
    Write the source table information.

    Args:
        sources: the list of sources
        model_name: the name of the model where the config file is to be written

    Returns: None
    """
    result: Dict[Any, Any] = {"version": 2, "sources": []}
    for source in sources:
        s_dict: Dict[str, Any] = {"name": source.name}
        if source.schema:
            s_dict["schema"] = source.schema
        if source.database:
            s_dict["database"] = source.database
        s_dict["tables"] = create_source_tables(source.source_tables)
        result["sources"].append(s_dict)  # type: ignore
        with open(f"dbt/models/{model_name}/sources.yml", "w") as f:
            safe_dump(result, f)


def write_variable_information(
    variables: ModelTypes, sql_files: List[str], model_name: str
):
    """
    Append the constructed variables to the list of variables already present.

    Args:
        variables: The variables dataclass
        sql_files: The files to be updated
        model_name: the name of the model (to prefix the variable name with)

    Returns: None
    """
    make_model_directory(model_name)
    logging.info("Updating variables")
    for file in sql_files:
        with open(f"dbt/models/{model_name}/{model_name}_{file}", "r") as f:
            lines = f.readlines()
        with open(f"dbt/models/{model_name}/{model_name}_{file}", "w") as f:
            for line in lines:
                for key, value in asdict(variables).items():
                    line = line.replace(f"$__{key}__", f"{value}")
                f.write(line)


def search_for_dbt_model_directories(file_path: str) -> List[str]:
    """
    Return a list of models from within the specified directory.

    Args:
        file_path: The path to be searched

    Returns:
        The list of model paths to be processed
    """
    return [
        join(model_dir, model)
        for model_dir in Path(file_path).rglob("models")
        for model in listdir(model_dir)
    ]


def search_and_filter_model_files(file_paths: List[str]) -> List[str]:
    """
    Return a list of files to be imported using dot notation (dir.dir.filename).

    This will allow the file to be easily imported in python.

    Args:
        file_paths: The list of file paths to be processed

    Returns:
        A combined list of all python files contained within the file paths in an importable python format.
    """
    files_to_be_processed: List[Any] = []
    for file_dir in file_paths:
        for _, _, files in walk(file_dir):
            for file in files:
                dbt_model, ext = splitext(file)
                if dbt_model != "__init__" and ext == ".py":
                    files_to_be_processed.append(
                        join(file_dir, dbt_model).replace("/", ".")
                    )
    return files_to_be_processed


def search_in_airflow_dags(airflow_dags_dir: str, project: str) -> List[Model]:
    """
    Search for DBT Models in Airflow DAGS.

    This will typically either be pipelines/dags/ or pipelines/samples/

    Args:
        airflow_dags_dir: The directory to search in.
        project: The name of the Google Cloud Project

    Returns: the models to be processed

    """
    models: List[Any] = []
    for directory in listdir(airflow_dags_dir):
        if directory != "__pycache__" and isdir(join(airflow_dags_dir, directory)):
            logging.info(f"Processing file: {directory}")
            # TODO: Remove hard-coding
            dags = generate_dags(
                f"/Users/timellis-smith/workspace/cookiecutter-data-engineering/{airflow_dags_dir}"
            )
            for dag in dags:
                print(f"{dir(dag)}")
                with contextlib.suppress(AttributeError):
                    models.append(dag.dbt_model)
    return models
