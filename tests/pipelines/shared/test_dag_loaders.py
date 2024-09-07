"""Tests for dag_loaders.py."""
from dataclasses import dataclass

import pytest
from pipelines.shared.dag_loaders import (
    flatten_and_get_dag, get_dags_to_pause, get_dags_to_unpause, get_directory,
    get_paused_or_unpaused_dags_from_config)

test_dict = {
    "airflow_variables": {"enable_sensors": True},
    "dataset_series": [{"series_name": "a"}, {"series_name": "b"}],
}

expected_result = [
    {"airflow_variables": {"enable_sensors": True}, "series_name": "a"},
    {"airflow_variables": {"enable_sensors": True}, "series_name": "b"},
]

expected_paused_dags = ["dag1", "dag2"]
current_paused_dags = ["dag2", "dag3"]
expected_running_dags = ["dag3"]


@dataclass
class TestDag:
    """DAG for tests below."""

    paused: bool
    dag_id: str
    __test__ = False


dags_list = [TestDag(paused=True, dag_id="dag1"), TestDag(paused=False, dag_id="dag2")]


def test_flatten_and_get_dag_returns_correct():
    """Test flatten_and_get_dag returns correct results."""
    assert flatten_and_get_dag(test_dict) == expected_result


@pytest.mark.parametrize(
    "path, result",
    [
        ("/opt/airflow", "/opt/airflow/dags/metadata/"),
        ("/home/airflow", "/home/airflow/gcs/dags/metadata/"),
    ],
)
def test_get_directory(path, result):
    """Assert that the get directory function returns the expected results."""
    assert get_directory("", path) == result


@pytest.mark.parametrize("pause_state, expected", [(True, ["dag1"]), (False, ["dag2"])])
def test_get_paused_or_unpaused_dags_from_config(pause_state, expected):
    """Assert that paused and running dags are correctly ascertained from config."""
    assert get_paused_or_unpaused_dags_from_config(dags_list, pause_state) == expected


def test_get_dags_to_pause():
    """Assert that the get_dags_to_pause returns the correct result."""
    assert get_dags_to_pause(current_paused_dags, expected_paused_dags) == {"dag1"}


def test_get_dags_to_unpause():
    """Assert that get_dags_to_unpause returns the correct results."""
    assert get_dags_to_unpause(
        current_paused_dags, expected_paused_dags, expected_running_dags
    ) == {"dag3"}
