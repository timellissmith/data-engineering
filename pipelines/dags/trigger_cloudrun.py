"""DAG to test Data Lineage."""

from airflow.models import DAG
from airflow.utils import dates
from pipelines.custom_operators.trigger_cloudrun import RunCloudbuildOperator

args = {
    "owner": "Test",
    "start_date": dates.days_ago(1),
    "provide_context": True,
    "retries": 0,
    "schedule": None,
}

with DAG(
    "cloudrun_trigger",
    description="data lineage",
    default_args=args,
    catchup=False,
    max_active_runs=5,
    doc_md="""
    # Trigger cloudrun test dag
    A simple dag to demonstrate using cloudrun with a deferrable operator.

    This will run a task in cloudrun which will return the status and the logs to airflow, so the logs are unified in one place.
    """,
) as dag:

    RunCloudbuildOperator(
        task_id="run_cloudrun",
        url="https://test-a32qwqjo2a-nw.a.run.app/events",
        audience="https://test-a32qwqjo2a-nw.a.run.app",
    )
