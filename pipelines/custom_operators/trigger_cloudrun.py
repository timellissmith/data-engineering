"""Custom Operator to Trigger a CloudRun and wait for completion.

This uses a deferrable operator.
"""
import logging
from typing import Any, Dict, Optional, Tuple

import google.auth.transport.requests
import google.oauth2.id_token
from aiohttp import ClientSession
from airflow.providers.google.common.hooks import base_google
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent


class RunCloudbuildOperator(BaseSensorOperator):
    """The RunCloudBuild Custom Operator."""

    def __init__(
        self,
        url: str,
        audience: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        """Initialize the class."""
        self.url = url
        self.audience = audience
        self.headers = headers or {}
        self.body = body
        self.base_hook = base_google.GoogleBaseHook()
        super().__init__(**kwargs)

    def execute(self, context: Dict) -> Any:
        """Execute the cloudrun."""
        url = self.url
        headers = self.headers
        body = self.body
        audience = self.audience
        auth_req = google.auth.transport.requests.Request()
        with self.base_hook.provide_gcp_credential_file_as_context():
            id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience)
        self.defer(
            trigger=CloudRunTrigger(
                url=url, id_token=id_token, headers=headers, body=body
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict, event: Tuple[Dict[str, str], int]):
        """Execute any post deferred tasks."""
        logging.info(
            "------------------CloudRun logs start here-----------------------------"
        )
        print(event[0]["logs"])
        logging.info(
            "------------------CloudRun logs end here-----------------------------"
        )
        if event[1] != 200:
            raise Exception(event[0], event[0]["status"])


class CloudRunTrigger(BaseTrigger):
    """The deferrable part of the operator passed to the trigger."""

    def __init__(
        self, url: str, id_token: str, headers: Optional[Dict[str, str]], body: Optional[Dict[str, str]]
    ):
        """Initialize the class."""
        super().__init__()
        self.url = url
        self.headers = headers or {}
        self.id_token = id_token
        self.body = body

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serialize the code for passing to the Trigger."""
        return "pipelines.custom_operators.trigger_cloudrun.CloudRunTrigger", {
            "url": self.url,
            "id_token": self.id_token,
            "headers": self.headers,
            "body": self.body,
        }

    async def run(self) -> TriggerEvent:
        """Run this code async."""
        async with ClientSession() as session:
            self.headers["Authorization"] = f"Bearer {self.id_token}"
            async with session.post(
                self.url, headers=self.headers, json=self.body
            ) as resp:
                response = await resp.json()
        yield TriggerEvent((response, resp.status))
