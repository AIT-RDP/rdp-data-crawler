import asyncio
import logging
from asyncio import Task

import pydantic
from typing import Any, Dict, Optional
from data_crawler.sinks.abc.abstract_sink import AbstractSinkAPI, SinkParameters, SinkMetadata
from rdp_mqtt.mqtt_client import MqttClient, MqttSettings
from rdp_mqtt.mqtt_parameters import MqttBaseParameters, MqttBatchingParameters


class MqttSinkParameters(SinkParameters, MqttBaseParameters, MqttBatchingParameters):
    pass


class MqttSinkMetadata(SinkMetadata):
    topic: Optional[str] = pydantic.Field(
        description="The topic to publish to (overrides default)",
        default=None
    )


class MqttSink(AbstractSinkAPI):
    """
    MQTT sink implementation.

    Publishes data to an MQTT broker using the rdp_mqtt client.
    """

    def __init__(self, mqtt_client: MqttClient, default_topic: str, include_metadata: bool):
        self.mqtt_client = mqtt_client
        self.default_topic = default_topic
        self.logger = logging.getLogger(__name__)
        self._include_metadata = include_metadata

    @classmethod
    def create(cls, sink_parameters: MqttSinkParameters, **kwargs) -> "MqttSink":
        """Factory method to create a sink instance"""

        mqtt_settings_dict = sink_parameters.model_dump()

        if mqtt_settings_dict.get("password"):
            from pydantic import SecretStr
            mqtt_settings_dict["password"] = SecretStr(mqtt_settings_dict["password"])

        mqtt_settings = MqttSettings.model_validate(mqtt_settings_dict)
        mqtt_settings.subscribe = False  # Sinks don't subscribe

        client = MqttClient(mqtt_settings)

        # Don't try to setup here - let it be done lazily in _async_insert

        return cls(client, sink_parameters.topic, sink_parameters.include_metadata)

    def insert_data(self, data: Dict[str, Any], metadata: MqttSinkMetadata) -> Task[None] | None:
        """Insert data into the MQTT sink"""

        topic = metadata.topic if metadata.topic else self.default_topic

        try:
            loop = asyncio.get_running_loop()
            # Create task and store reference to prevent it from being garbage collected
            # For test environments, we might want to wait for completion
            return loop.create_task(self._async_insert(data, topic))
        except RuntimeError:
            # No event loop running, run synchronously
            asyncio.run(self._async_insert(data, topic))

    async def _async_insert(self, data: Dict[str, Any], topic: str) -> None:
        """Async helper for inserting data"""
        try:
            await self.mqtt_client.publish(data, topic)
        except Exception as e:
            self.logger.error(f"Failed to publish data to MQTT: {e}")
            # If publish fails, try to setup and retry once
            try:
                await self.mqtt_client.setup()
                await self.mqtt_client.publish(data, topic)
            except Exception as retry_e:
                self.logger.error(f"Failed to publish data to MQTT after retry: {retry_e}")

    @staticmethod
    def parameter_model() -> type[MqttSinkParameters]:
        """Return the parameter model for this sink"""
        return MqttSinkParameters

    @staticmethod
    def metadata_model() -> type[MqttSinkMetadata]:
        """Return the metadata model for this sink"""
        return MqttSinkMetadata

    @property
    def include_metadata(self) -> bool:
        """
        Whether to include metadata in the data.
        """
        return self._include_metadata
