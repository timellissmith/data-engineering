"""Utilities for loading and processing DAGs."""

import contextlib
from copy import deepcopy
from os import getcwd, walk
from os.path import isfile, join, splitext
from typing import List, Set, Tuple, Union

import yaml  # type: ignore
from cattr import structure
from pipelines.dags.main_ingestion_operator.main_schema_definitions import \
    MainIngestionDag
from pipelines.shared.airflow_utils import query_airflow


def flatten_and_get_dag(file: dict) -> List[dict]:
    """Split out the configuration to allow for separate DAGs per dataset_series to be created.

    Args:
        file (dict): The contents of the file to be parsed loaded as a dict.

    Returns:
        List[dict]: The dict of the separate DAGs.
    """
    result = []
    for f in file["dataset_series"]:
        temp = deepcopy(file)
        temp.pop("dataset_series")
        result.append({**temp, **f})
    return result


def get_directory(directory, base_path: str = "") -> str:
    """Search for a configuration directory based on whether airflow is run in a local container or composer.

    Returns:
        str: The path of the configuration files.
    """
    base_path = base_path or getcwd()
    if "opt" in base_path:  # Used for airflow local
        base_directory = "/opt/airflow/dags/metadata/"
    elif "home" in base_path:
        base_directory = "/home/airflow/gcs/dags/metadata/"  # Used for composer
    else:
        base_directory = f"{base_path}/"
    return join(base_directory, directory)


def find_files(expected_file_types: Tuple[str, ...], directory: str = "") -> List[str]:
    """Find files of expected types and return a list of the contents of each file."""
    files_contents = []
    full_directory = directory or get_directory(directory)
    for dirpath, _, filenames in walk(full_directory):
        if "__pycache__" in dirpath:
            continue
        for filename in filenames:
            file_w_path = join(dirpath, filename)
            if isfile(file_w_path) and splitext(filename)[1] in expected_file_types:
                with open(file_w_path, "r") as f:
                    files_contents.append(f.read())
    return files_contents


def parse_files(full_path: str = "", directory: str = "") -> List[Union[dict]]:
    """
    Generate the DAG according to the MainIngestionDag class.

    This should be run with either full_path or directory defined.

    Args:
        full_path: The full path to the DAG directory to be searched.
        directory: the directory of the dataclass declaration of different prefix

    Returns: List[MainIngestionDag]
    """
    full_directory = full_path or get_directory(directory)
    valid_types = ".json", ".yaml"
    loaded_contents = []
    # yaml surprisingly can load both yaml and json
    loader = yaml.safe_load

    files_contents = find_files(valid_types, full_directory)
    for file in files_contents:
        with contextlib.suppress(AttributeError):
            loaded_contents.append(loader(file))
            continue
    return loaded_contents


def generate_dags(full_path: str = "", directory: str = "") -> List[MainIngestionDag]:
    """Generate the DAG config by finding files and loading the config into classes."""
    dags_list: List[MainIngestionDag] = []
    for item in parse_files(full_path, directory):
        dags_json = flatten_and_get_dag(item)
        dags_list.extend(structure(dag, MainIngestionDag) for dag in dags_json)
    return dags_list


def get_paused_or_unpaused_dags_from_config(
    dags: List[MainIngestionDag], pause_state: bool
) -> List[str]:
    """Return a list of paused DAGS from the config files."""
    return [dag.dag_id for dag in dags if dag.paused is pause_state]


def get_paused_dags_from_running_dags_list(directory: str):
    """Find all text files defining paused dags and get the list of DAGS from them."""
    paused_dags = []
    for file in find_files(expected_file_types=(".txt",), directory=directory):
        result = file.splitlines()
        paused_dags += result
    return [dag for dag in paused_dags if dag != ""]


def get_union(set1, set2):
    """Return a union of two sets."""
    return set(set1).union(set(set2))


def get_running_and_paused_dags(local_tests: bool = False):
    """Query airflow and return lists of running and paused DAGs."""
    dag_list = query_airflow(command="list", local_tests=local_tests)
    if local_tests:
        running_dags = [
            dag["dag_id"] for dag in dag_list["dags"] if not dag["is_paused"]
        ]
        paused_dags = [dag["dag_id"] for dag in dag_list["dags"] if dag["is_paused"]]
    else:
        running_dags = [dag["dag_id"] for dag in dag_list if dag["paused"] == "False"]
        paused_dags = [dag["dag_id"] for dag in dag_list if dag["paused"] == "True"]
    return running_dags, paused_dags


def get_dags_to_pause(
    current_paused_dags: List[str], expected_paused_dags: List[str]
) -> Set[str]:
    """Calculate which DAGs to set to pause based on the diff between expected and actual."""
    return set(expected_paused_dags) - set(current_paused_dags)


def get_dags_to_unpause(
    current_paused_dags: List[str],
    expected_paused_dags: List[str],
    expected_running_dags: List[str],
):
    """Calculate which DAGS to enable based on the diff between paused and expected dags but excluding unknown about DAGs."""
    return (set(current_paused_dags) - set(expected_paused_dags)).intersection(
        expected_running_dags
    )
