"""Utilities for querying airflow."""

from json import JSONDecodeError, loads
from os import getenv, popen


def query_airflow(command: str, extra_args: str = "-o json", local_tests: bool = False):
    """Generate the pairings of expected vs actual dags for each dag type."""
    environment = getenv("COMPOSER_ENVIRONMENT")
    location = getenv("COMPOSER_LOCATION")
    if local_tests:
        if command == "list":
            run_command = (
                "curl --silent 'http://localhost:8080/api/v1/dags' --user 'admin:admin'"
            )
        elif command == "pause":
            run_command = (
                f"curl --silent -X PATCH 'http://localhost:8080/api/v1/dags/{extra_args}?update_mask=is_paused'"
                " -H 'Content-Type: application/json' --user 'admin:admin' -d '{'is_paused': true}'"
            )
        elif command == "unpause":
            run_command = (
                f"curl --silent -X PATCH 'http://localhost:8080/api/v1/dags/{extra_args}?update_mask=is_paused'"
                " -H 'Content-Type: application/json' --user 'admin:admin' -d '{\"is_paused\": false}'"
            )
    else:
        extra_args = f"-- {extra_args}" if extra_args else ""
        run_command = f"gcloud composer environments run {environment} --location={location} dags {command} -- {extra_args}"
    stream = popen(run_command)  # nosec
    string_result = stream.read()
    try:
        return loads(string_result)
    except JSONDecodeError:
        return {}
