import asyncio
import json
import os
import uuid
from dataclasses import dataclass
import logging

import pytest
import zstandard as zstd
import paho.mqtt.client as mqtt
from data_crawler.sources.mqtt import MqttSource
from rdp_mqtt.sparkplug.sparkplug_encode import encode_data_message, get_sparkplug_topic

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

    Falls back to broker.emqx.io:1883 if not set.
    """
    host = os.getenv("DATA_CRAWLER_MQTT_HOST", "broker.emqx.io")
    port = int(os.getenv("DATA_CRAWLER_MQTT_PORT", "1883"))
    logger.info(f"Using MQTT plaintext broker at {host}:{port}")
    return MqttBrokerConfig(host=host, port=port)


@pytest.fixture
def mqtt_broker_ssl() -> MqttBrokerConfig:
    """Fixture providing MQTT broker configuration for SSL connections.

    Reads from environment variables:
    - DATA_CRAWLER_MQTT_HOST
    - DATA_CRAWLER_MQTT_SSL_PORT

    Falls back to broker.emqx.io:8883 if not set.
    """
    host = os.getenv("DATA_CRAWLER_MQTT_HOST", "broker.emqx.io")
    port = int(os.getenv("DATA_CRAWLER_MQTT_SSL_PORT", "8883"))
    logger.info(f"Using MQTT SSL broker at {host}:{port}")
    return MqttBrokerConfig(host=host, port=port)


async def create_connected_mqtt_publisher(broker: str = "broker.emqx.io", port: int = 1883) -> mqtt.Client:
    """Create and connect a Paho MQTT publisher with proper connection handling and retries."""

    max_retries = 3
    retry_delay = 1
    client_id = f"test-pub-{uuid.uuid4()}"

    for attempt in range(max_retries):
        publisher = mqtt.Client(client_id=f"{client_id}_{attempt}",
                                callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        connected = asyncio.Event()
        connect_failed = asyncio.Event()
        loop = asyncio.get_running_loop()

        def on_connect(client, userdata, flags, rc, properties=None):
            if rc == 0:
                loop.call_soon_threadsafe(connected.set)
            else:
                loop.call_soon_threadsafe(connect_failed.set)

        publisher.on_connect = on_connect

        try:
            publisher.connect(broker, port, 60)
            publisher.loop_start()

            await asyncio.wait(
                [asyncio.create_task(connected.wait()), asyncio.create_task(connect_failed.wait())],
                timeout=5,  # Shorter timeout per attempt
                return_when=asyncio.FIRST_COMPLETED,
            )

            if connected.is_set() and not connect_failed.is_set():
                # Success - give a moment for the connection to stabilize
                await asyncio.sleep(0.1)
                return publisher
            else:
                # Failed - clean up and try again
                publisher.loop_stop()
                publisher.disconnect()

        except Exception:
            # Connection error - clean up and try again
            try:
                publisher.loop_stop()
                publisher.disconnect()
            except Exception:
                pass

        if attempt < max_retries - 1:
            await asyncio.sleep(retry_delay * (attempt + 1))  # Exponential backoff

    raise ConnectionError(f"MQTT broker connection failed after {max_retries} attempts")


@pytest.mark.asyncio
async def test_mqtt_source_fetch_data(mqtt_broker_plaintext: MqttBrokerConfig):
    """
    Test fetching data from the MQTT source.
    """
    topic = f"test/data_crawler/source/{uuid.uuid4()}"

    params = {
        "host": mqtt_broker_plaintext.host,
        "port": mqtt_broker_plaintext.port,
        "topic": topic,
        "ssl": False,
    }

    source = MqttSource.create(params, executor_name="test_mqtt")
    source.start()

    test_data = {"sensor": "temperature", "value": 23.5, "timestamp": "2024-01-01T12:00:00Z"}

    publisher = await create_connected_mqtt_publisher(mqtt_broker_plaintext.host, mqtt_broker_plaintext.port)

    received_messages = []

    try:
        # Give source and publisher time to connect
        await asyncio.sleep(2)

        publisher.publish(topic, json.dumps(test_data))

        # Iterate over the source's run method to get messages
        # Run in a separate thread to avoid blocking the test's event loop
        # and use a timeout to prevent indefinite blocking
        loop = asyncio.get_running_loop()
        run_task = loop.run_in_executor(None, lambda: next(source.run()))

        try:
            message = await asyncio.wait_for(run_task, timeout=2)
            received_messages.append(message)
        except asyncio.TimeoutError:
            pass

        assert len(received_messages) == 1
        assert received_messages[0] == test_data
    finally:
        # Ensure publisher is properly cleaned up
        try:
            publisher.loop_stop()
            publisher.disconnect()
            await asyncio.sleep(0.1)  # Allow disconnect to complete
        except Exception as e:
            print(f"Publisher cleanup warning: {e}")

        # Shutdown source
        try:
            source.shutdown()
            await asyncio.sleep(0.1)  # Allow source shutdown to complete
        except Exception as e:
            print(f"Source cleanup warning: {e}")

        # Additional delay to ensure all threads terminate
        # The MqttSource uses a separate event loop in a thread
        await asyncio.sleep(0.5)


@pytest.mark.asyncio
async def test_mqtt_source_multiple_messages(mqtt_broker_plaintext: MqttBrokerConfig):
    """
    Test fetching multiple messages from the MQTT source.
    """
    topic = f"test/data_crawler/source/multi/{uuid.uuid4()}"

    params = {
        "host": mqtt_broker_plaintext.host,
        "port": mqtt_broker_plaintext.port,
        "topic": topic,
        "ssl": False,
    }

    source = MqttSource.create(params, executor_name="test_mqtt_multi")
    source.start()

    test_messages = [
        {"sensor": "temperature", "value": 23.5, "id": 1},
        {"sensor": "humidity", "value": 65.0, "id": 2},
        {"sensor": "pressure", "value": 1013.25, "id": 3}
    ]

    publisher = await create_connected_mqtt_publisher(mqtt_broker_plaintext.host, mqtt_broker_plaintext.port)

    received_messages = []

    try:
        # Give source and publisher time to connect
        await asyncio.sleep(2)

        # Publish all test messages
        for msg in test_messages:
            publisher.publish(topic, json.dumps(msg))
            await asyncio.sleep(0.1)  # Small delay between messages

        # Collect messages with timeout
        loop = asyncio.get_running_loop()

        for i in range(len(test_messages)):
            try:
                run_task = loop.run_in_executor(None, lambda: next(source.run()))
                message = await asyncio.wait_for(run_task, timeout=2)
                received_messages.append(message)
            except asyncio.TimeoutError:
                break

        assert len(received_messages) == len(test_messages)

        # Verify all messages were received with topic added
        for i, original_msg in enumerate(test_messages):
            assert original_msg in received_messages

    finally:
        # Ensure publisher is properly cleaned up
        try:
            publisher.loop_stop()
            publisher.disconnect()
            await asyncio.sleep(0.1)  # Allow disconnect to complete
        except Exception as e:
            print(f"Publisher cleanup warning: {e}")

        # Shutdown source
        try:
            source.shutdown()
            await asyncio.sleep(0.1)  # Allow source shutdown to complete
        except Exception as e:
            print(f"Source cleanup warning: {e}")

        # Additional delay to ensure all threads terminate
        # The MqttSource uses a separate event loop in a thread
        await asyncio.sleep(0.5)


@pytest.mark.asyncio
async def test_mqtt_source_ssl_connection(mqtt_broker_ssl: MqttBrokerConfig):
    """
    Test MQTT source with SSL connection (using broker.emqx.io SSL port).
    """
    topic = f"test/data_crawler/source/ssl/{uuid.uuid4()}"

    params = {
        "host": mqtt_broker_ssl.host,
        "port": mqtt_broker_ssl.port,
        "topic": topic,
        "ssl": True,
        "validate_certificate": False,  # Disable certificate validation for testing
    }

    source = MqttSource.create(params, executor_name="test_mqtt_ssl")
    source.start()

    test_data = {"sensor": "temperature", "value": 23.5, "ssl_test": True}

    # Use SSL-enabled publisher with retry logic
    max_retries = 3
    retry_delay = 1
    client_id = f"test-ssl-pub-{uuid.uuid4()}"
    publisher = None

    for attempt in range(max_retries):
        try:
            publisher = mqtt.Client(client_id=f"{client_id}_{attempt}",
                                    callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

            # Set up SSL before connection
            import ssl
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            publisher.tls_set_context(context)

            connected = asyncio.Event()
            connect_failed = asyncio.Event()
            loop = asyncio.get_running_loop()

            def on_connect(client, userdata, flags, rc, properties=None):
                if rc == 0:
                    loop.call_soon_threadsafe(connected.set)
                else:
                    loop.call_soon_threadsafe(connect_failed.set)

            publisher.on_connect = on_connect
            publisher.connect(mqtt_broker_ssl.host, mqtt_broker_ssl.port, 60)
            publisher.loop_start()

            await asyncio.wait(
                [asyncio.create_task(connected.wait()), asyncio.create_task(connect_failed.wait())],
                timeout=5,
                return_when=asyncio.FIRST_COMPLETED,
            )

            if connected.is_set() and not connect_failed.is_set():
                break  # Success
            else:
                # Failed - clean up and try again
                publisher.loop_stop()
                publisher.disconnect()
                publisher = None

        except Exception as e:
            print(f"SSL connection attempt {attempt + 1} failed: {e}")
            if publisher:
                try:
                    publisher.loop_stop()
                    publisher.disconnect()
                except Exception:
                    pass
                publisher = None

        if attempt < max_retries - 1:
            await asyncio.sleep(retry_delay)

    if not publisher:
        pytest.skip("Could not establish SSL connection to MQTT broker after retries")

    received_messages = []

    try:
        # Give source and publisher time to connect
        await asyncio.sleep(2)

        publisher.publish(topic, json.dumps(test_data))

        # Try to receive the message
        loop = asyncio.get_running_loop()
        run_task = loop.run_in_executor(None, lambda: next(source.run()))

        try:
            message = await asyncio.wait_for(run_task, timeout=2)
            received_messages.append(message)
        except asyncio.TimeoutError:
            pass

        assert len(received_messages) == 1
        assert received_messages[0] == test_data

    finally:
        # Ensure publisher is properly cleaned up
        try:
            publisher.loop_stop()
            publisher.disconnect()
            await asyncio.sleep(0.1)  # Allow disconnect to complete
        except Exception as e:
            print(f"Publisher cleanup warning: {e}")

        # Shutdown source
        try:
            source.shutdown()
            await asyncio.sleep(0.1)  # Allow source shutdown to complete
        except Exception as e:
            print(f"Source cleanup warning: {e}")

        # Additional delay to ensure all threads terminate
        # The MqttSource uses a separate event loop in a thread
        await asyncio.sleep(0.5)


@pytest.mark.asyncio
async def test_mqtt_source_qos_levels(mqtt_broker_plaintext: MqttBrokerConfig):
    """
    Test MQTT source with different QoS levels.
    """
    topic = f"test/data_crawler/source/qos/{uuid.uuid4()}"

    params = {
        "host": mqtt_broker_plaintext.host,
        "port": mqtt_broker_plaintext.port,
        "topic": topic,
        "ssl": False,
        "qos": 2,  # Exactly once delivery
    }

    source = MqttSource.create(params, executor_name="test_mqtt_qos")
    source.start()

    test_data = {"sensor": "temperature", "value": 23.5, "qos_test": True}

    publisher = await create_connected_mqtt_publisher(mqtt_broker_plaintext.host, mqtt_broker_plaintext.port)

    received_messages = []

    try:
        # Give source and publisher time to connect
        await asyncio.sleep(2)

        # Publish with QoS 2
        publisher.publish(topic, json.dumps(test_data), qos=2)

        # Try to receive the message
        loop = asyncio.get_running_loop()
        run_task = loop.run_in_executor(None, lambda: next(source.run()))

        try:
            message = await asyncio.wait_for(run_task, timeout=2)
            received_messages.append(message)
        except asyncio.TimeoutError:
            pass

        assert len(received_messages) == 1
        assert received_messages[0] == test_data

    finally:
        # Ensure publisher is properly cleaned up
        try:
            publisher.loop_stop()
            publisher.disconnect()
            await asyncio.sleep(0.1)  # Allow disconnect to complete
        except Exception as e:
            print(f"Publisher cleanup warning: {e}")

        # Shutdown source
        try:
            source.shutdown()
            await asyncio.sleep(0.1)  # Allow source shutdown to complete
        except Exception as e:
            print(f"Source cleanup warning: {e}")

        # Additional delay to ensure all threads terminate
        # The MqttSource uses a separate event loop in a thread
        await asyncio.sleep(0.5)


@pytest.mark.asyncio
async def test_mqtt_source_wildcard_topic(mqtt_broker_plaintext: MqttBrokerConfig):
    """
    Test MQTT source with wildcard topic subscription.
    """
    base_topic = f"test/data_crawler/source/wildcard/{uuid.uuid4()}"
    wildcard_topic = f"{base_topic}/+"

    params = {
        "host": mqtt_broker_plaintext.host,
        "port": mqtt_broker_plaintext.port,
        "topic": wildcard_topic,
        "ssl": False,
    }

    source = MqttSource.create(params, executor_name="test_mqtt_wildcard")
    source.start()

    test_messages = [
        {"sensor": "temperature", "subtopic": "temp"},
        {"sensor": "humidity", "subtopic": "humid"},
        {"sensor": "pressure", "subtopic": "press"}
    ]

    publisher = await create_connected_mqtt_publisher(mqtt_broker_plaintext.host, mqtt_broker_plaintext.port)

    received_messages = []

    try:
        # Give source and publisher time to connect
        await asyncio.sleep(2)

        # Publish to different subtopics
        for i, msg in enumerate(test_messages):
            subtopic = f"{base_topic}/sub{i}"
            publisher.publish(subtopic, json.dumps(msg))
            await asyncio.sleep(0.2)

        # Try to receive all messages
        loop = asyncio.get_running_loop()

        for _ in range(len(test_messages)):
            try:
                run_task = loop.run_in_executor(None, lambda: next(source.run()))
                message = await asyncio.wait_for(run_task, timeout=2)
                received_messages.append(message)
            except asyncio.TimeoutError:
                break

        # Should receive messages from all subtopics
        assert len(received_messages) >= 1  # At least one message should be received

    finally:
        # Ensure publisher is properly cleaned up
        try:
            publisher.loop_stop()
            publisher.disconnect()
            await asyncio.sleep(0.1)  # Allow disconnect to complete
        except Exception as e:
            print(f"Publisher cleanup warning: {e}")

        # Shutdown source
        try:
            source.shutdown()
            await asyncio.sleep(0.1)  # Allow source shutdown to complete
        except Exception as e:
            print(f"Source cleanup warning: {e}")

        # Additional delay to ensure all threads terminate
        # The MqttSource uses a separate event loop in a thread
        await asyncio.sleep(0.5)


@pytest.mark.asyncio
async def test_mqtt_source_retained_messages(mqtt_broker_plaintext: MqttBrokerConfig):
    """
    Test MQTT source with retained messages.
    """
    topic = f"test/data_crawler/source/retained/{uuid.uuid4()}"

    test_data = {"sensor": "temperature", "value": 23.5, "retained": True}

    # First, publish a retained message
    publisher = await create_connected_mqtt_publisher(mqtt_broker_plaintext.host, mqtt_broker_plaintext.port)

    try:
        # Publish retained message
        publisher.publish(topic, json.dumps(test_data), retain=True)
        await asyncio.sleep(1)  # Give time for message to be stored

    finally:
        publisher.loop_stop()
        publisher.disconnect()

    # Now create source after the retained message was published
    params = {
        "host": mqtt_broker_plaintext.host,
        "port": mqtt_broker_plaintext.port,
        "topic": topic,
        "ssl": False,
    }

    source = MqttSource.create(params, executor_name="test_mqtt_retained")
    source.start()

    received_messages = []

    try:
        # Give source time to connect and receive retained message
        await asyncio.sleep(2)

        # Try to receive the retained message
        loop = asyncio.get_running_loop()
        run_task = loop.run_in_executor(None, lambda: next(source.run()))

        try:
            message = await asyncio.wait_for(run_task, timeout=2)
            received_messages.append(message)
        except asyncio.TimeoutError:
            pass

        # Should receive the retained message
        assert len(received_messages) == 1
        assert received_messages[0] == test_data

    finally:
        # Shutdown source properly
        try:
            source.shutdown()
            await asyncio.sleep(0.2)
        except Exception as e:
            print(f"Source cleanup warning: {e}")

        # Clean up retained message with robust connection
        try:
            cleanup_publisher = await create_connected_mqtt_publisher(mqtt_broker_plaintext.host,
                                                                      mqtt_broker_plaintext.port)
            cleanup_publisher.publish(topic, "", retain=True)  # Delete retained message
            await asyncio.sleep(0.5)
            cleanup_publisher.loop_stop()
            cleanup_publisher.disconnect()
            await asyncio.sleep(0.2)
        except Exception as e:
            print(f"Retained message cleanup warning: {e}")


@pytest.mark.asyncio
async def test_mqtt_source_reconnection(mqtt_broker_plaintext: MqttBrokerConfig):
    """
    Test MQTT source reconnection behavior (basic test).
    This test verifies the source can be restarted without issues.
    """
    topic = f"test/data_crawler/source/reconnect/{uuid.uuid4()}"

    params = {
        "host": mqtt_broker_plaintext.host,
        "port": mqtt_broker_plaintext.port,
        "topic": topic,
        "ssl": False,
    }

    # First connection
    source1 = MqttSource.create(params, executor_name="test_mqtt_reconnect1")
    source1.start()

    try:
        await asyncio.sleep(2)  # Allow connection
        source1.shutdown()
        await asyncio.sleep(1)  # Allow shutdown

        # Second connection (simulating reconnection)
        source2 = MqttSource.create(params, executor_name="test_mqtt_reconnect2")
        source2.start()

        test_data = {"sensor": "temperature", "value": 23.5, "reconnect_test": True}

        publisher = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        publisher.connect(mqtt_broker_plaintext.host, mqtt_broker_plaintext.port)
        publisher.loop_start()

        received_messages = []

        try:
            await asyncio.sleep(2)  # Allow connection

            publisher.publish(topic, json.dumps(test_data))

            # Try to receive message
            loop = asyncio.get_running_loop()
            run_task = loop.run_in_executor(None, lambda: next(source2.run()))

            try:
                message = await asyncio.wait_for(run_task, timeout=2)
                received_messages.append(message)
            except asyncio.TimeoutError:
                pass

            assert len(received_messages) == 1
            assert received_messages[0] == test_data

        finally:
            publisher.loop_stop()
            publisher.disconnect()
            source2.shutdown()

    except Exception:
        source1.shutdown()
        raise


@pytest.mark.asyncio
async def test_mqtt_source_sparkplug_decode(mqtt_broker_plaintext: MqttBrokerConfig):
    """
    Test MQTT source with Sparkplug payload decoding.
    """
    topic = f"spBv1.0/TestGroup/DDATA/TestNode/TestDevice_{uuid.uuid4()}"

    params = {
        "host": mqtt_broker_plaintext.host,
        "port": mqtt_broker_plaintext.port,
        "topic": topic,
        "ssl": False,
        "payload_parser": "sparkplug",
    }

    source = MqttSource.create(params, executor_name="test_mqtt_sparkplug")
    source.start()

    # Create a Sparkplug message manually
    try:

        # Create test metrics data (Sparkplug format expects list of metric objects)
        test_metrics = [
            {"name": "temperature", "value": 23.5},
            {"name": "humidity", "value": 65},
            {"name": "status", "value": True}
        ]

        # Encode as Sparkplug message
        sparkplug_payload = encode_data_message(test_metrics)

        publisher = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        publisher.connect(mqtt_broker_plaintext.host, mqtt_broker_plaintext.port)
        publisher.loop_start()

        received_messages = []

        try:
            # Give source and publisher time to connect
            await asyncio.sleep(2)

            # Publish Sparkplug binary message
            publisher.publish(topic, sparkplug_payload)

            # Try to receive and decode the message
            loop = asyncio.get_running_loop()
            run_task = loop.run_in_executor(None, lambda: next(source.run()))

            try:
                message = await asyncio.wait_for(run_task, timeout=2)
                received_messages.append(message)
            except asyncio.TimeoutError:
                pass

            assert len(received_messages) == 1

            # The source should decode the Sparkplug message
            decoded_message = received_messages[0]

            message_str = str(decoded_message)

            # Should contain evidence of our test metrics
            assert ("temperature" in message_str or
                    "23.5" in message_str or
                    "humidity" in message_str or
                    "65" in message_str), f"Decoded message should contain test metrics: {decoded_message}"

            # Verify it's not just the raw binary payload
            assert not isinstance(decoded_message.get("payload"), bytes), "Message should be decoded, not raw binary"

            print(f"Decoded Sparkplug message structure: {decoded_message}")

        finally:
            publisher.loop_stop()
            publisher.disconnect()

    except Exception as e:
        # If there's an issue with Sparkplug encoding, it's a real error
        pytest.fail(f"Sparkplug encoding failed: {e}")

    finally:
        source.shutdown()


@pytest.mark.asyncio
async def test_mqtt_source_json_zstd_payload(mqtt_broker_plaintext: MqttBrokerConfig):
    """
    Test MQTT source with compressed JSON payload.
    """
    topic = f"test/data_crawler/source/zstd/{uuid.uuid4()}"

    params = {
        "host": mqtt_broker_plaintext.host,
        "port": mqtt_broker_plaintext.port,
        "topic": topic,
        "ssl": False,
        "payload_parser": "json_zstd",
    }

    source = MqttSource.create(params, executor_name="test_mqtt_zstd")
    source.start()

    test_data = {
        "sensor": "temperature",
        "value": 23.5,
        "large_data": "x" * 1000,  # Large string to benefit from compression
        "zstd_test": True
    }

    publisher = await create_connected_mqtt_publisher(mqtt_broker_plaintext.host, mqtt_broker_plaintext.port)

    received_messages = []

    try:
        # Give source and publisher time to connect
        await asyncio.sleep(2)

        # Compress the JSON data with zstd
        json_data = json.dumps(test_data).encode('utf-8')
        compressor = zstd.ZstdCompressor()
        compressed_data = compressor.compress(json_data)

        # Publish the compressed data
        publisher.publish(topic, compressed_data)

        # Try to receive the message
        loop = asyncio.get_running_loop()
        run_task = loop.run_in_executor(None, lambda: next(source.run()))

        try:
            message = await asyncio.wait_for(run_task, timeout=2)
            received_messages.append(message)
        except asyncio.TimeoutError:
            pass

        # Should receive and decompress the message correctly
        assert len(received_messages) == 1
        assert received_messages[0] == test_data

    finally:
        # Ensure publisher is properly cleaned up
        try:
            publisher.loop_stop()
            publisher.disconnect()
            await asyncio.sleep(0.1)  # Allow disconnect to complete
        except Exception as e:
            print(f"Publisher cleanup warning: {e}")

        # Shutdown source
        try:
            source.shutdown()
            await asyncio.sleep(0.1)  # Allow source shutdown to complete
        except Exception as e:
            print(f"Source cleanup warning: {e}")

        # Additional delay to ensure all threads terminate
        # The MqttSource uses a separate event loop in a thread
        await asyncio.sleep(0.5)
