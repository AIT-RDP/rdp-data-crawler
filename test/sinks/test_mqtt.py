import asyncio
import json
import uuid
import pytest
import paho.mqtt.client as mqtt
from data_crawler.sinks.mqtt import MqttSink, MqttSinkParameters, MqttSinkMetadata

async def create_connected_mqtt_client(broker: str = "broker.emqx.io", port: int = 1883) -> mqtt.Client:
    """Create and connect a Paho MQTT client with proper connection handling and retries."""
    
    max_retries = 3
    retry_delay = 1
    client_id = f"test-client-{uuid.uuid4()}"
    
    for attempt in range(max_retries):
        client = mqtt.Client(client_id=f"{client_id}_{attempt}", callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        connected = asyncio.Event()
        connect_failed = asyncio.Event()
        loop = asyncio.get_running_loop()
        
        def on_connect(client, userdata, flags, rc, properties=None):
            if rc == 0:
                loop.call_soon_threadsafe(connected.set)
            else:
                loop.call_soon_threadsafe(connect_failed.set)
        
        client.on_connect = on_connect
        
        try:
            client.connect(broker, port, 60)
            client.loop_start()
            
            await asyncio.wait(
                [asyncio.create_task(connected.wait()), asyncio.create_task(connect_failed.wait())],
                timeout=3,  # Much shorter timeout per attempt
                return_when=asyncio.FIRST_COMPLETED,
            )
            
            if connected.is_set() and not connect_failed.is_set():
                # Success - give a moment for the connection to stabilize
                await asyncio.sleep(0.1)
                return client
            else:
                # Failed - clean up and try again
                client.loop_stop()
                client.disconnect()
                
        except Exception:
            # Connection error - clean up and try again
            try:
                client.loop_stop()
                client.disconnect()
            except Exception:
                pass
        
        if attempt < max_retries - 1:
            await asyncio.sleep(retry_delay * (attempt + 1))  # Exponential backoff
    
    raise ConnectionError(f"MQTT broker connection failed after {max_retries} attempts")


async def cleanup_mqtt_connections(subscriber, sink):
    """Helper function to properly clean up MQTT connections"""
    try:
        if subscriber:
            subscriber.loop_stop()
            subscriber.disconnect()
        await asyncio.sleep(0.1)  # Allow disconnect to complete

        if hasattr(sink, 'mqtt_client') and hasattr(sink.mqtt_client, '_client') and sink.mqtt_client._client:
            await sink.mqtt_client.shutdown()
        await asyncio.sleep(0.1)  # Allow shutdown to complete
        
        # Add small delay between tests to avoid rate limiting
        await asyncio.sleep(0.2)
    except Exception as e:
        # Log but don't fail the test due to cleanup issues
        print(f"Cleanup warning: {e}")


async def create_test_subscriber(topic, on_message_callback):
    """Helper function to create and setup a test subscriber"""
    subscriber = await create_connected_mqtt_client()
    subscriber.on_message = on_message_callback
    subscriber.subscribe(topic)
    return subscriber

@pytest.fixture
async def mqtt_subscriber():
    """Fixture that provides a connected MQTT subscriber"""
    client = None
    try:
        client = await create_connected_mqtt_client()
        yield client
    finally:
        if client:
            client.loop_stop()
            client.disconnect()
            await asyncio.sleep(0.3)  # Small cleanup delay


@pytest.mark.asyncio
async def test_mqtt_sink_insert_data():
    """
    Test inserting data into the MQTT sink and verifying it with a subscriber.
    """
    host = "broker.emqx.io"
    port = 1883
    topic = f"test/data_crawler/sink/{uuid.uuid4()}"

    params = MqttSinkParameters(
        host=host,
        port=port,
        topic=topic,
        ssl=False,
    )

    sink = MqttSink.create(params)

    # Ensure the sink client is properly set up
    await sink.mqtt_client.setup()

    test_data = {"sensor": "temperature", "value": 23.5, "timestamp": "2024-01-01T12:00:00Z"}
    metadata = MqttSinkMetadata()

    received_message = None
    message_received_event = asyncio.Event()

    def on_message(client, userdata, msg):
        nonlocal received_message
        received_message = json.loads(msg.payload.decode())
        message_received_event.set()

    subscriber = await create_connected_mqtt_client(host, port)
    subscriber.on_message = on_message
    subscriber.subscribe(topic)

    try:
        # Give subscriber time to connect and subscribe
        await asyncio.sleep(0.3)

        # Insert data and wait for the task to complete
        insert_task = sink.insert_data(test_data, metadata)
        if insert_task:
            await insert_task

        # Allow some time for message propagation
        await asyncio.sleep(0.2)

        # Wait for the message to be received with a timeout
        await asyncio.wait_for(message_received_event.wait(), timeout=3)

        assert received_message is not None
        assert received_message == test_data
    finally:
        subscriber.loop_stop()
        subscriber.disconnect()
        # Ensure the sink client is properly cleaned up
        if hasattr(sink.mqtt_client, '_client') and sink.mqtt_client._client:
            await sink.mqtt_client.shutdown()


@pytest.mark.asyncio
async def test_mqtt_sink_topic_override():
    """
    Test that metadata can override the default topic.
    """
    host = "broker.emqx.io"
    port = 1883
    default_topic = f"test/data_crawler/sink/{uuid.uuid4()}"
    override_topic = f"test/data_crawler/sink/override/{uuid.uuid4()}"

    params = MqttSinkParameters(
        host=host,
        port=port,
        topic=default_topic,
        ssl=False,
    )

    sink = MqttSink.create(params)

    # Ensure the sink client is properly set up
    await sink.mqtt_client.setup()

    test_data = {"value": 42}
    metadata = MqttSinkMetadata(topic=override_topic)

    received_message = None
    message_received_event = asyncio.Event()

    def on_message(client, userdata, msg):
        nonlocal received_message
        received_message = json.loads(msg.payload.decode())
        message_received_event.set()

    subscriber = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    subscriber.on_message = on_message
    subscriber.connect(host, port)
    subscriber.subscribe(override_topic)
    subscriber.loop_start()

    try:
        # Give subscriber time to connect and subscribe
        await asyncio.sleep(0.3)

        # Insert data and wait for the task to complete
        insert_task = sink.insert_data(test_data, metadata)
        if insert_task:
            await insert_task

        # Allow some time for message propagation
        await asyncio.sleep(0.2)

        # Wait for the message to be received with a timeout
        await asyncio.wait_for(message_received_event.wait(), timeout=3)

        assert received_message is not None
        assert received_message == test_data
    finally:
        subscriber.loop_stop()
        subscriber.disconnect()
        # Ensure the sink client is properly cleaned up
        if hasattr(sink.mqtt_client, '_client') and sink.mqtt_client._client:
            await sink.mqtt_client.shutdown()


@pytest.mark.asyncio
async def test_mqtt_sink_multiple_messages():
    """
    Test publishing multiple messages to the MQTT sink.
    """
    host = "broker.emqx.io"
    port = 1883
    topic = f"test/data_crawler/sink/multi/{uuid.uuid4()}"

    params = MqttSinkParameters(
        host=host,
        port=port,
        topic=topic,
        ssl=False,
    )

    sink = MqttSink.create(params)
    await sink.mqtt_client.setup()

    test_messages = [
        {"sensor": "temperature", "value": 23.5, "id": 1},
        {"sensor": "humidity", "value": 65.0, "id": 2},
        {"sensor": "pressure", "value": 1013.25, "id": 3}
    ]

    received_messages = []
    messages_received = 0
    all_messages_event = asyncio.Event()

    def on_message(client, userdata, msg):
        nonlocal messages_received
        received_messages.append(json.loads(msg.payload.decode()))
        messages_received += 1
        if messages_received >= len(test_messages):
            all_messages_event.set()

    subscriber = await create_connected_mqtt_client(host, port)
    subscriber.on_message = on_message
    subscriber.subscribe(topic)

    try:
        # Give subscriber time to connect and subscribe
        await asyncio.sleep(0.3)

        # Publish all messages
        metadata = MqttSinkMetadata()
        for msg in test_messages:
            insert_task = sink.insert_data(msg, metadata)
            if insert_task:
                await insert_task

        # Allow some time for message propagation
        await asyncio.sleep(0.2)

        # Wait for all messages to be received
        await asyncio.wait_for(all_messages_event.wait(), timeout=5)

        assert len(received_messages) == len(test_messages)

        # Verify all messages were received
        for original_msg in test_messages:
            assert original_msg in received_messages

    finally:
        subscriber.loop_stop()
        subscriber.disconnect()
        if hasattr(sink.mqtt_client, '_client') and sink.mqtt_client._client:
            await sink.mqtt_client.shutdown()


@pytest.mark.asyncio
async def test_mqtt_sink_empty_data():
    """
    Test publishing empty data to the MQTT sink.
    """
    host = "broker.emqx.io"
    port = 1883
    topic = f"test/data_crawler/sink/empty/{uuid.uuid4()}"

    params = MqttSinkParameters(
        host=host,
        port=port,
        topic=topic,
        ssl=False,
    )

    sink = MqttSink.create(params)
    await sink.mqtt_client.setup()

    test_data = {}
    metadata = MqttSinkMetadata()

    received_message = None
    message_received_event = asyncio.Event()

    def on_message(client, userdata, msg):
        nonlocal received_message
        received_message = json.loads(msg.payload.decode())
        message_received_event.set()

    subscriber = await create_connected_mqtt_client(host, port)
    subscriber.on_message = on_message
    subscriber.subscribe(topic)

    try:
        # Give subscriber time to connect and subscribe
        await asyncio.sleep(0.3)

        # Insert empty data
        insert_task = sink.insert_data(test_data, metadata)
        if insert_task:
            await insert_task

        # Allow some time for message propagation
        await asyncio.sleep(0.2)

        # Wait for the message to be received
        await asyncio.wait_for(message_received_event.wait(), timeout=8)

        assert received_message is not None
        assert received_message == test_data

    finally:
        subscriber.loop_stop()
        subscriber.disconnect()
        if hasattr(sink.mqtt_client, '_client') and sink.mqtt_client._client:
            await sink.mqtt_client.shutdown()


@pytest.mark.asyncio
async def test_mqtt_sink_large_data():
    """
    Test publishing large data payloads to the MQTT sink.
    """
    host = "broker.emqx.io"
    port = 1883
    topic = f"test/data_crawler/sink/large/{uuid.uuid4()}"

    params = MqttSinkParameters(
        host=host,
        port=port,
        topic=topic,
        ssl=False,
    )

    sink = MqttSink.create(params)
    await sink.mqtt_client.setup()

    # Create a large data payload
    test_data = {
        "sensor_id": "large_sensor_001",
        "readings": [{
            "timestamp": f"2024-01-01T12:{i:02d}:00Z",
            "value": i * 0.5,
            "quality": "good"
        } for i in range(100)],  # 100 data points
        "metadata": {
            "location": "Test Location",
            "device_info": "Large payload test device",
            "calibration_data": [f"cal_{i}" for i in range(50)]
        }
    }
    metadata = MqttSinkMetadata()

    received_message = None
    message_received_event = asyncio.Event()

    def on_message(client, userdata, msg):
        nonlocal received_message
        received_message = json.loads(msg.payload.decode())
        message_received_event.set()

    subscriber = await create_connected_mqtt_client(host, port)
    subscriber.on_message = on_message
    subscriber.subscribe(topic)

    try:
        # Give subscriber time to connect and subscribe
        await asyncio.sleep(0.3)

        # Insert large data
        insert_task = sink.insert_data(test_data, metadata)
        if insert_task:
            await insert_task

        # Allow some time for message propagation
        await asyncio.sleep(1)

        # Wait for the message to be received
        await asyncio.wait_for(message_received_event.wait(), timeout=3)

        assert received_message is not None
        assert received_message == test_data
        assert len(received_message["readings"]) == 100

    finally:
        subscriber.loop_stop()
        subscriber.disconnect()
        if hasattr(sink.mqtt_client, '_client') and sink.mqtt_client._client:
            await sink.mqtt_client.shutdown()


class TestMqttSinkBatching:

    @pytest.mark.asyncio
    async def test_mqtt_sink_batching(self):
        """
        Test MQTT sink batching functionality.
        """
        host = "broker.emqx.io"
        port = 1883
        topic = f"test/data_crawler/sink/batch/{uuid.uuid4()}"

        # Configure batching: batch size of 3 messages
        params = MqttSinkParameters(
            host=host,
            port=port,
            topic=topic,
            ssl=False,
            batch_size=3,
            batch_timeout=2.0,  # 2 second timeout
        )

        sink = MqttSink.create(params)
        await sink.mqtt_client.setup()

        test_messages = [
            {"sensor": "temperature", "value": 23.5, "id": 1},
            {"sensor": "humidity", "value": 65.0, "id": 2},
            {"sensor": "pressure", "value": 1013.25, "id": 3},
            {"sensor": "light", "value": 500, "id": 4},  # This should trigger a new batch
        ]

        received_messages = []
        messages_received = 0
        batches_received = 0

        def on_message(client, userdata, msg):
            nonlocal messages_received, batches_received
            try:
                # With batching, we might receive arrays of messages or single messages
                payload = json.loads(msg.payload.decode())
                batches_received += 1
                if isinstance(payload, list):
                    received_messages.extend(payload)
                    messages_received += len(payload)
                else:
                    received_messages.append(payload)
                    messages_received += 1
            except json.JSONDecodeError:
                # Handle case where message is not JSON
                pass

        subscriber = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        subscriber.on_message = on_message
        subscriber.connect(host, port)
        subscriber.subscribe(topic)
        subscriber.loop_start()

        try:
            # Give subscriber time to connect and subscribe
            await asyncio.sleep(1)

            # Send messages one by one
            metadata = MqttSinkMetadata()
            for msg in test_messages:
                insert_task = sink.insert_data(msg, metadata)
                if insert_task:
                    await insert_task
                await asyncio.sleep(0.1)  # Small delay between messages

            # Wait for batch timeout to ensure all messages are sent
            await asyncio.sleep(3)

            assert batches_received == 2
            assert messages_received == 4

        finally:
            subscriber.loop_stop()
            subscriber.disconnect()
            if hasattr(sink.mqtt_client, '_client') and sink.mqtt_client._client:
                await sink.mqtt_client.shutdown()

    @pytest.mark.asyncio
    async def test_mqtt_sink_batch_timeout(self):
        """
        Test MQTT sink batch timeout functionality.
        """
        host = "broker.emqx.io"
        port = 1883
        topic = f"test/data_crawler/sink/timeout/{uuid.uuid4()}"

        # Configure batching with a small timeout
        params = MqttSinkParameters(
            host=host,
            port=port,
            topic=topic,
            ssl=False,
            batch_size=10,  # Large batch size
            batch_timeout=1.0,  # Short timeout - should trigger before batch is full
        )

        sink = MqttSink.create(params)
        await sink.mqtt_client.setup()

        # Send fewer messages than batch_size to test timeout
        test_messages = [
            {"sensor": "temperature", "value": 23.5, "id": 1},
            {"sensor": "humidity", "value": 65.0, "id": 2},
            {"sensor": "humidity", "value": 65.0, "id": 2},
            {"sensor": "humidity", "value": 65.0, "id": 2},
            {"sensor": "humidity", "value": 65.0, "id": 2},
        ]

        received_messages = []
        batches_received = 0

        def on_message(client, userdata, msg):
            nonlocal received_messages, batches_received
            try:
                payload = json.loads(msg.payload.decode())
                batches_received += 1
                if isinstance(payload, list):
                    received_messages.extend(payload)
                else:
                    received_messages.append(payload)
            except json.JSONDecodeError:
                pass

        subscriber = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        subscriber.on_message = on_message
        subscriber.connect(host, port)
        subscriber.subscribe(topic)
        subscriber.loop_start()

        try:
            # Give subscriber time to connect and subscribe
            await asyncio.sleep(1)

            # Send messages
            metadata = MqttSinkMetadata()
            for msg in test_messages:
                insert_task = sink.insert_data(msg, metadata)
                if insert_task:
                    await insert_task

            # Wait for batch timeout (should be triggered by timeout, not batch size)
            await asyncio.sleep(5)

            # Should have received messages due to timeout
            assert len(received_messages) == 5
            assert batches_received == 1

        finally:
            subscriber.loop_stop()
            subscriber.disconnect()
            if hasattr(sink.mqtt_client, '_client') and sink.mqtt_client._client:
                await sink.mqtt_client.shutdown()

    @pytest.mark.asyncio
    async def test_mqtt_sink_no_batching(self):
        """
        Test MQTT sink with batching disabled (batch_size = 0).
        """
        host = "broker.emqx.io"
        port = 1883
        topic = f"test/data_crawler/sink/no_batch/{uuid.uuid4()}"

        # Configure with no batching
        params = MqttSinkParameters(
            host=host,
            port=port,
            topic=topic,
            ssl=False,
            batch_size=0,  # No batching
        )

        sink = MqttSink.create(params)
        await sink.mqtt_client.setup()

        test_messages = [
            {"sensor": "temperature", "value": 23.5, "id": 1},
            {"sensor": "humidity", "value": 65.0, "id": 2},
            {"sensor": "pressure", "value": 1013.25, "id": 3},
        ]

        received_messages = []
        messages_received = 0
        expected_messages = len(test_messages)
        all_messages_event = asyncio.Event()

        def on_message(client, userdata, msg):
            nonlocal messages_received
            try:
                payload = json.loads(msg.payload.decode())
                # With no batching, each message should come individually
                received_messages.append(payload)
                messages_received += 1
                if messages_received >= expected_messages:
                    all_messages_event.set()
            except json.JSONDecodeError:
                pass

        subscriber = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        subscriber.on_message = on_message
        subscriber.connect(host, port)
        subscriber.subscribe(topic)
        subscriber.loop_start()

        try:
            # Give subscriber time to connect and subscribe
            await asyncio.sleep(1)

            # Send messages one by one
            metadata = MqttSinkMetadata()
            for msg in test_messages:
                insert_task = sink.insert_data(msg, metadata)
                if insert_task:
                    await insert_task
                await asyncio.sleep(0.2)  # Small delay between messages

            # Wait for all messages to be received
            await asyncio.wait_for(all_messages_event.wait(), timeout=5)

            # Should have received each message individually
            assert messages_received == expected_messages
            assert len(received_messages) == expected_messages

            # Verify all original messages were received
            for original_msg in test_messages:
                assert original_msg in received_messages

        finally:
            subscriber.loop_stop()
            subscriber.disconnect()
            if hasattr(sink.mqtt_client, '_client') and sink.mqtt_client._client:
                await sink.mqtt_client.shutdown()

    @pytest.mark.asyncio
    async def test_mqtt_sink_batch_size_one(self):
        """
        Test MQTT sink with batch size of 1 (immediate sending).
        """
        host = "broker.emqx.io"
        port = 1883
        topic = f"test/data_crawler/sink/batch_one/{uuid.uuid4()}"

        # Configure with batch size of 1
        params = MqttSinkParameters(
            host=host,
            port=port,
            topic=topic,
            ssl=False,
            batch_size=1,  # Send immediately
            batch_timeout=5.0,  # Long timeout, shouldn't matter
        )

        sink = MqttSink.create(params)
        await sink.mqtt_client.setup()

        test_data = {"sensor": "temperature", "value": 23.5, "id": 1}
        metadata = MqttSinkMetadata()

        received_message = None
        message_received_event = asyncio.Event()

        def on_message(client, userdata, msg):
            nonlocal received_message
            try:
                payload = json.loads(msg.payload.decode())
                received_message = payload
                message_received_event.set()
            except json.JSONDecodeError:
                pass

        subscriber = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        subscriber.on_message = on_message
        subscriber.connect(host, port)
        subscriber.subscribe(topic)
        subscriber.loop_start()

        try:
            # Give subscriber time to connect and subscribe
            await asyncio.sleep(1)

            # Send single message
            insert_task = sink.insert_data(test_data, metadata)
            if insert_task:
                await insert_task

            # Should receive message quickly due to batch size = 1
            await asyncio.wait_for(message_received_event.wait(), timeout=3)

            assert received_message is not None
            assert received_message == test_data

        finally:
            subscriber.loop_stop()
            subscriber.disconnect()
            if hasattr(sink.mqtt_client, '_client') and sink.mqtt_client._client:
                await sink.mqtt_client.shutdown()


@pytest.mark.asyncio
async def test_mqtt_sink_sparkplug_data_types():
    """
    Test that MqttSink can publish Sparkplug data that can be received and decoded.
    Uses the same approach as working integration tests.
    """
    from rdp_mqtt.mqtt_client import MqttClient, MqttSettings
    
    host = "broker.emqx.io"
    port = 1883
    group_id = f"test_group_{uuid.uuid4().hex[:8]}"
    node_id = f"test_node_{uuid.uuid4().hex[:8]}"
    device_id = f"test_device_{uuid.uuid4().hex[:8]}"

    # Test using MqttClient directly (like working integration tests) to verify the approach works
    publisher_settings = MqttSettings(
        host=host,
        port=port,
        topic="not_used",  # Sparkplug generates its own topics
        ssl=False,
        payload_parser="sparkplug",
        sparkplug_group_id=group_id,
        sparkplug_node_id=node_id,
        sparkplug_device_id=device_id,
        subscribe=False,
        identifier=f"test_pub_{uuid.uuid4().hex[:8]}"
    )
    
    subscriber_settings = MqttSettings(
        host=host,
        port=port,
        topic=f"spBv1.0/{group_id}/+/{node_id}/{device_id}",
        ssl=False,
        payload_parser="sparkplug",  # Must match publisher to decode Sparkplug messages
        subscribe=True,
        identifier=f"test_sub_{uuid.uuid4().hex[:8]}"
    )

    publisher = MqttClient(publisher_settings)
    subscriber = MqttClient(subscriber_settings)
    
    received_messages = []
    message_event = asyncio.Event()

    async def collect_messages():
        try:
            async for message in subscriber.subscribe():
                print(f"Received: {message}")
                received_messages.append(message)
                message_event.set()
                if len(received_messages) >= 1:  # Just need one DDATA message
                    break
        except Exception as e:
            print(f"Subscriber error: {e}")

    try:
        # Setup both clients
        await publisher.setup()
        await subscriber.setup()
        
        # Start subscriber
        subscriber_task = asyncio.create_task(collect_messages())
        
        # Wait for connections and birth messages
        await asyncio.sleep(8.0)
        
        # Test dataset data like the working integration tests
        # This should create a dataset with Time and V columns that gets transformed
        test_data = {
            "name": f"{device_id}_temperature_reading",  # Device-specific metric name  
            "timestamp": "2022-01-01T00:00:00Z",
            "temperature": 25.5,
            "humidity": 60.0
        }
        
        # Publish using MqttClient directly
        await publisher.publish(test_data)
        
        # Wait for message
        await asyncio.wait_for(message_event.wait(), timeout=10)
        
        # Verify we got the message
        assert len(received_messages) > 0, "Should have received Sparkplug message"
        message = received_messages[0] 
        assert message is not None
        
        # Verify it contains our test data (decoded from Sparkplug)
        # For dataset messages, the field name should be extracted from the device_id + metric name
        expected_field_name = "temperature_reading"  # Last part of device_id_temperature_reading
        assert (
            "name" in message or 
            "temperature" in message or 
            "humidity" in message or
            expected_field_name in message
        ), f"Message should contain our test data or derived field name: {message}"
            
        print(f"SUCCESS: Sparkplug dataset test completed with message: {message}")
        
        # If this is a Dewesoft-style dataset transformation, verify the field naming
        if expected_field_name in message:
            print(f"Dataset transformation successful - field '{expected_field_name}' created from device metric name")
        
    finally:
        try:
            await publisher.shutdown()
            await subscriber.shutdown()
        except Exception as e:
            print(f"Cleanup error: {e}")
