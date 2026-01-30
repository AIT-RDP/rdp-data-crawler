"""
Defines common MQTT functions for testing the MQTT implementation

The module is mainly concerned with providing the MQTT broker configuration for different test cases.
"""

import os
import logging
from dataclasses import dataclass

import pytest

logger = logging.getLogger(__name__)


@dataclass
class MqttBrokerConfig:
    """Configuration for MQTT broker connection."""
    host: str
    port: int


@pytest.fixture
def mqtt_broker_plaintext() -> MqttBrokerConfig:
    """Fixture providing MQTT broker configuration for plaintext connections.

    Reads from environment variables:
    - DATA_CRAWLER_MQTT_HOST
    - DATA_CRAWLER_MQTT_PORT

    Falls back to broker.emqx.io:1883 if not set simultaneously.
    """
    if "DATA_CRAWLER_MQTT_HOST" in os.environ and "DATA_CRAWLER_MQTT_PORT" in os.environ:
        host, port = os.getenv("DATA_CRAWLER_MQTT_HOST"), int(os.getenv("DATA_CRAWLER_MQTT_PORT"))
    else:
        host, port = "broker.emqx.io", 1883
    logger.info(f"Using MQTT plaintext broker at {host}:{port}")
    return MqttBrokerConfig(host=host, port=port)


@pytest.fixture
def mqtt_broker_ssl() -> MqttBrokerConfig:
    """Fixture providing MQTT broker configuration for SSL connections.

    Reads from environment variables:
    - DATA_CRAWLER_MQTT_HOST
    - DATA_CRAWLER_MQTT_SSL_PORT

    Falls back to broker.emqx.io:8883 if not set simultaneously.
    """
    if "DATA_CRAWLER_MQTT_HOST" in os.environ and "DATA_CRAWLER_MQTT_SSL_PORT" in os.environ:
        host, port = os.getenv("DATA_CRAWLER_MQTT_HOST"), int(os.getenv("DATA_CRAWLER_MQTT_SSL_PORT"))
    else:
        host, port = "broker.emqx.io", 8883
    logger.info(f"Using MQTT SSL broker at {host}:{port}")
    return MqttBrokerConfig(host=host, port=port)
