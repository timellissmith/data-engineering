"""Custom Operator to Trigger a CloudRun and wait for completion.

This uses a deferrable operator.
"""
from asyncio import sleep
from typing import Any, Dict, List, Optional, Tuple

from aiohttp import ClientSession
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from gcloud.aio.storage import Bucket, Storage


class DeferredSensorGCSPrefix(BaseSensorOperator):
    """The RunCloudBuild Custom Operator."""

    def __init__(self, bucket_name: str, prefix: str, **kwargs):
        """Initialize the class."""
        self.bucket_name = bucket_name
        self.prefix = prefix
        super().__init__(**kwargs)

    def execute(self, context: Dict) -> Any:
        """Execute the cloudrun."""
        self.defer(
            trigger=SensorTrigger(bucket_name=self.bucket_name, prefix=self.prefix),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict, event: Optional[List[str]]):
        """Run when the deferrable part has completed."""
        print(f"{event=}")
        return event


class SensorTrigger(BaseTrigger):
    """The part that's passed to the Triggerer."""

    def __init__(self, bucket_name: str, prefix: str):
        """Initialize the function."""
        from airflow.providers.google.common.hooks import base_google

        super().__init__()
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.base_hook = base_google.GoogleBaseHook()

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Return the function in serialized form."""
        return (
            "pipelines.custom_operators.airflow_sensor_wait_for_files_deferred.SensorTrigger",
            {"bucket_name": self.bucket_name, "prefix": self.prefix},
        )

    async def run(self) -> TriggerEvent:
        """Run the trigger."""
        with self.base_hook.provide_gcp_credential_file_as_context() as keyfile:
            async with ClientSession() as session:
                async with Storage(session=session, service_file=keyfile) as client:
                    bucket = Bucket(storage=client, name=self.bucket_name)
                    while True:
                        files = await bucket.list_blobs(prefix=self.prefix)
                        if filtered_files := [
                            file for file in files if file != self.prefix
                        ]:
                            yield TriggerEvent(filtered_files)
                        await sleep(60)
