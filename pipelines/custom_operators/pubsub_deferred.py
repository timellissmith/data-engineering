"""Custom Operator to Trigger a PubSub and wait for a corresponding message to be returned.

This uses a deferrable operator.
"""
import logging
from asyncio import sleep
from typing import Any, Dict, Tuple

from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
from airflow.providers.google.common.hooks import base_google
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent


class PubSubAwaitTaskOperator(BaseSensorOperator):
    """The RunCloudBuild Custom Operator."""

    def __init__(self, send_topic: str, receive_topic: str, pipeline, **kwargs):
        """Initialize the class."""
        self.send_topic = send_topic
        self.receive_topic = receive_topic
        self.pipeline = pipeline
        super().__init__(**kwargs)

    def execute(self, context: Dict) -> Any:
        """Execute the cloudrun."""
        self.base_hook = base_google.GoogleBaseHook()
        # credentials = self.base_hook._get_credentials()

        hook = PubSubHook()
        hook.publish(
            topic=self.send_topic,
            messages=[{"attributes": {"pipeline": self.pipeline}}],
        )
        self.defer(
            trigger=AwaitPubSubMessage(
                topic=self.receive_topic, pipeline=self.pipeline
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict, event: Tuple[Dict[str, str], int]):
        """Run the post trigger tasks."""
        logging.info(
            "------------------CloudRun logs start here-----------------------------"
        )
        print(event)
        logging.info(
            "------------------CloudRun logs end here-----------------------------"
        )


class AwaitPubSubMessage(BaseTrigger):
    """Trigger for awaiting pubsub message."""

    def __init__(self, topic: str, pipeline: str):
        """Initialize the trigger."""
        super().__init__()
        self.topic = topic
        self.pipeline = pipeline

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serialize the trigger."""
        return "pipelines.custom_operators.pubsub_deferred.AwaitPubSubMessage", {
            "topic": self.topic,
            "pipeline": self.pipeline,
        }

    async def run(self) -> TriggerEvent:
        """Run the trigger."""
        from airflow.providers.google.cloud.hooks.pubsub import PubSubHook

        hook = PubSubHook()
        done = False
        while not done:
            res = hook.pull(subscription="task_feedback-sub", max_messages=10)
            for msg in res:
                print(msg.message)
                if msg.message.attributes["pipeline"] == self.pipeline:
                    hook.acknowledge(
                        subscription="task_feedback-sub", ack_ids=[msg.ack_id]
                    )
                    logs = msg.message.data.decode()
                    done = True
                    yield TriggerEvent(logs)
            await sleep(5)

    # async def run(self) -> TriggerEvent:
    #     async with ClientSession() as session:
    #         self.headers["Authorization"] = f"Bearer {self.id_token}"
    #         async with session.post(self.url, headers=self.headers, json=self.body) as resp:
    #             response = await resp.json()
    #     yield TriggerEvent((response, resp.status))
