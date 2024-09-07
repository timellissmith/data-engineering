#! /usr/bin/env python
"""Ensure that DAGs are in the correct paused / running state."""
from distutils.util import strtobool
from os import getcwd, getenv
from os.path import join

from pipelines.shared.airflow_utils import query_airflow
from pipelines.shared.dag_loaders import (
    generate_dags, get_dags_to_pause, get_dags_to_unpause,
    get_paused_dags_from_running_dags_list,
    get_paused_or_unpaused_dags_from_config, get_running_and_paused_dags,
    get_union)


def set_correct_status():
    """Set the Paused / Running of the DAG states in airflow to match config."""
    local_tests = bool(strtobool(getenv("LOCAL_TESTS", "False")))
    path = join(getcwd(), "metadata")
    config_driven_dags = generate_dags(full_path=path)
    config_paused_dags = get_paused_or_unpaused_dags_from_config(
        config_driven_dags, pause_state=True
    )
    config_running_dags = get_paused_or_unpaused_dags_from_config(
        config_driven_dags, pause_state=False
    )
    paused_dags_from_lists = get_paused_dags_from_running_dags_list(directory=path)

    union_of_paused_dags = get_union(config_paused_dags, paused_dags_from_lists)

    actual_running_dags, actual_paused_dags = get_running_and_paused_dags(
        local_tests=local_tests
    )

    dags_to_pause = get_dags_to_pause(actual_paused_dags, union_of_paused_dags)
    dags_to_unpause = get_dags_to_unpause(
        actual_paused_dags, union_of_paused_dags, config_running_dags
    )

    [
        query_airflow(command="pause", extra_args=dag, local_tests=local_tests)
        for dag in dags_to_pause
    ]
    [
        query_airflow(command="unpause", extra_args=dag, local_tests=local_tests)
        for dag in dags_to_unpause
    ]


if __name__ == "__main__":
    """Run the script."""
    set_correct_status()
