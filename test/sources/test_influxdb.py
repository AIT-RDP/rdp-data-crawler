"""
Tests the InfluxDB data source

The file is mostly generated with the help of Claude Sonnet 4.5
"""
import datetime
import os
import time
from dataclasses import dataclass
from typing import Optional

import pytest
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

import data_crawler.sources.influxdb as influxdb_source

# Test bucket name
TEST_BUCKET = "test_data_crawler"


@dataclass
class InfluxDBTestConfig:
    """Encapsulates InfluxDB connection configuration for tests"""
    url: str
    org: str
    username: str
    password: str
    token: Optional[str] = None
    org_id: Optional[str] = None


def _get_test_config() -> Optional[InfluxDBTestConfig]:
    """
    Retrieves test configuration from environment variables.
    Returns None if required variables are not set.
    """
    url = os.environ.get("DATA_CRAWLER_INFLUX_URL")
    org = os.environ.get("DATA_CRAWLER_INFLUX_ORG")
    username = os.environ.get("DATA_CRAWLER_INFLUX_USER")
    password = os.environ.get("DATA_CRAWLER_INFLUX_PASSWORD")

    if not all([url, org, username, password]):
        return None

    return InfluxDBTestConfig(
        url=url,
        org=org,
        username=username,
        password=password
    )


def _get_skip_reason() -> str:
    """Returns the reason message for skipping tests"""
    return (
        "Environment variables DATA_CRAWLER_INFLUX_URL, DATA_CRAWLER_INFLUX_ORG, "
        "DATA_CRAWLER_INFLUX_USER, or DATA_CRAWLER_INFLUX_PASSWORD not set"
    )


def _create_token(config: InfluxDBTestConfig, description: str = "test_token") -> str:
    """
    Creates a new token for testing using user credentials.
    Returns the token string. Also stores the org_id in the config.
    """
    # Create a client with username/password authentication
    client = influxdb_client.InfluxDBClient(
        url=config.url,
        username=config.username,
        password=config.password,
        org=config.org
    )

    try:
        # Verify connection
        if not client.ping():
            raise ConnectionError(f"Unable to connect to InfluxDB at {config.url}")

        # Get organization ID and store it in config
        orgs_api = client.organizations_api()
        orgs = orgs_api.find_organizations()
        org_id = None
        for org in orgs:
            if org.name == config.org:
                org_id = org.id
                break

        if org_id is None:
            raise ValueError(f"Organization '{config.org}' not found")

        # Store org_id in config for later use
        config.org_id = org_id

        # Create authorization/token
        tokens_api = client.authorizations_api()

        # Create Authorization object with correct imports
        from influxdb_client import Permission, PermissionResource

        # Create permissions for all necessary resources
        permissions = [
            Permission(action="read", resource=PermissionResource(type="buckets", org_id=org_id)),
            Permission(action="write", resource=PermissionResource(type="buckets", org_id=org_id)),
            Permission(action="read", resource=PermissionResource(type="orgs", org_id=org_id)),
            Permission(action="read", resource=PermissionResource(type="authorizations", org_id=org_id)),
            Permission(action="write", resource=PermissionResource(type="authorizations", org_id=org_id)),
        ]

        # Create authorization using the API method
        created_auth = tokens_api.create_authorization(org_id=org_id, permissions=permissions)

        return created_auth.token
    finally:
        client.close()


def _delete_token(config: InfluxDBTestConfig, token: str) -> None:
    """
    Deletes a token using user credentials.
    """
    # Create a client with the token to be deleted
    client = influxdb_client.InfluxDBClient(
        url=config.url,
        token=token,
        org=config.org
    )

    try:
        tokens_api = client.authorizations_api()

        # Find the authorization by token
        authorizations = tokens_api.find_authorizations()
        for auth in authorizations:
            if auth.token == token:
                tokens_api.delete_authorization(auth.id)
                break
    except Exception:
        # If deletion fails, try with username/password
        client.close()
        client = influxdb_client.InfluxDBClient(
            url=config.url,
            username=config.username,
            password=config.password,
            org=config.org
        )
        try:
            tokens_api = client.authorizations_api()
            authorizations = tokens_api.find_authorizations()
            for auth in authorizations:
                if auth.token == token:
                    tokens_api.delete_authorization(auth.id)
                    break
        finally:
            client.close()
    finally:
        if client:
            client.close()


@pytest.fixture(scope="module")
def influxdb_test_config():
    """Provides test configuration and manages token lifecycle"""
    config = _get_test_config()

    if config is None:
        pytest.skip(_get_skip_reason())

    # Create a token for the test session
    token = _create_token(config, description="test_data_crawler_token")
    config.token = token

    yield config

    # Cleanup: delete the token
    try:
        _delete_token(config, token)
    except Exception:
        pass  # Ignore cleanup errors


@pytest.fixture(scope="module")
def influxdb_client_instance(influxdb_test_config):
    """Creates an InfluxDB client for testing"""
    client = influxdb_client.InfluxDBClient(
        url=influxdb_test_config.url,
        token=influxdb_test_config.token,
        org=influxdb_test_config.org
    )

    # Verify connection
    if not client.ping():
        pytest.skip(f"Unable to connect to InfluxDB at {influxdb_test_config.url}")

    yield client

    client.close()


@pytest.fixture(scope="module")
def influxdb_bucket(influxdb_client_instance, influxdb_test_config):
    """Creates and manages a test bucket in InfluxDB"""
    buckets_api = influxdb_client_instance.buckets_api()

    # Check if bucket exists, delete if it does
    existing_bucket = buckets_api.find_bucket_by_name(TEST_BUCKET)
    if existing_bucket:
        buckets_api.delete_bucket(existing_bucket)

    # Create test bucket with 1 hour retention using org_id
    bucket = buckets_api.create_bucket(
        bucket_name=TEST_BUCKET,
        org_id=influxdb_test_config.org_id,
        retention_rules=[{"everySeconds": 3600, "type": "expire"}]
    )

    yield bucket

    # Cleanup: delete the test bucket
    buckets_api.delete_bucket(bucket)


@pytest.fixture
def influxdb_test_data(influxdb_client_instance, influxdb_bucket, influxdb_test_config):
    """Fixture that populates InfluxDB with test data and cleans up afterwards"""
    write_api = influxdb_client_instance.write_api(write_options=SYNCHRONOUS)
    delete_api = influxdb_client_instance.delete_api()

    # Clean any existing data in the bucket before starting
    start_delete = "1970-01-01T00:00:00Z"
    stop_delete = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(hours=1)
    try:
        delete_api.delete(
            start_delete,
            stop_delete.isoformat(),
            '',
            bucket=TEST_BUCKET,
            org=influxdb_test_config.org
        )
        time.sleep(0.5)  # Wait for delete to complete
    except Exception:
        pass  # Ignore errors if there's no data to delete

    # Prepare test data with different data types
    base_time = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(minutes=30)

    test_points = []

    # Create test data with float, int, and string types
    for i in range(5):
        timestamp = base_time + datetime.timedelta(minutes=i * 2)

        # Measurement 1: sensor data with mixed types
        point1 = (
            influxdb_client.Point("sensor_data")
            .tag("location", "room_a")
            .tag("device_id", "device_001")
            .field("temperature", 20.5 + i * 0.5)  # float
            .field("humidity", 45 + i)  # int
            .field("status", f"reading_{i}")  # string
            .time(timestamp)
        )
        test_points.append(point1)

        # Measurement 2: power data
        point2 = (
            influxdb_client.Point("power_data")
            .tag("location", "building_b")
            .tag("meter_id", "meter_002")
            .field("power_kw", 100.0 + i * 10.0)  # float
            .field("energy_kwh", 1000 + i * 50)  # int
            .field("phase", f"phase_{i % 3}")  # string
            .time(timestamp)
        )
        test_points.append(point2)

    # Write initial test data
    write_api.write(bucket=TEST_BUCKET, org=influxdb_test_config.org, record=test_points)

    # Wait a bit for data to be written
    time.sleep(0.5)

    yield {
        "write_api": write_api,
        "base_time": base_time,
        "bucket": TEST_BUCKET,
        "initial_points": len(test_points)
    }

    # Cleanup: delete all data written during this test
    try:
        delete_api.delete(
            start_delete,
            stop_delete.isoformat(),
            '',
            bucket=TEST_BUCKET,
            org=influxdb_test_config.org
        )
    except Exception:
        pass  # Ignore cleanup errors


def test_influxdb_source_configuration():
    """Tests the InfluxDBSourceConfiguration model"""
    config = influxdb_source.InfluxDBSourceConfiguration(
        url="http://localhost:8086",
        token="test_token",
        org="test_org",
        query='from(bucket: "test") |> range(start: _start_time, stop: _stop_time)',
        initial_history=datetime.timedelta(hours=2),
        lag_time=datetime.timedelta(minutes=5)
    )

    assert config.url == "http://localhost:8086"
    assert config.token == "test_token"
    assert config.org == "test_org"
    assert config.initial_history == datetime.timedelta(hours=2)
    assert config.lag_time == datetime.timedelta(minutes=5)


def test_influxdb_source_initialization(influxdb_test_config):
    """Tests that the InfluxDB source can be initialized properly"""
    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": 'from(bucket: "test") |> range(start: _start_time, stop: _stop_time)',
        "initial_history": "1h",
        "lag_time": "0s"
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    assert source._config.url == influxdb_test_config.url
    assert source._config.org == influxdb_test_config.org
    assert source._client is None  # Not started yet


def test_influxdb_source_start_stop(influxdb_test_data, influxdb_test_config):
    """Tests starting and stopping the InfluxDB source"""
    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": f'from(bucket: "{TEST_BUCKET}") |> range(start: _start_time, stop: _stop_time)',
        "initial_history": "1h",
        "lag_time": "0s"
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    assert source._client is None

    source.start()
    assert source._client is not None

    source.stop()
    assert source._client is None


def test_influxdb_source_fetch_data(influxdb_test_data, influxdb_test_config):
    """Tests fetching data from InfluxDB"""
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "1s"
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        messages = list(source.fetch_data_bundle())

        # We should get at least one message (one table)
        assert len(messages) > 0

        # Check the first message structure
        message = messages[0]
        assert isinstance(message, dict)

        # Verify that the message contains expected fields
        assert "temperature" in message
        assert "humidity" in message
        assert "status" in message

        # Verify data types
        assert isinstance(message["temperature"], list)
        assert isinstance(message["humidity"], list)
        assert isinstance(message["status"], list)

        # Verify we got multiple readings
        assert len(message["temperature"]) == 5
        assert len(message["humidity"]) == 5
        assert len(message["status"]) == 5

        # Verify float values
        assert all(isinstance(v, float) for v in message["temperature"])
        # Verify int values
        assert all(isinstance(v, int) for v in message["humidity"])
        # Verify string values
        assert all(isinstance(v, str) for v in message["status"])

    finally:
        source.stop()


def test_influxdb_source_multiple_measurements(influxdb_test_data, influxdb_test_config):
    """Tests fetching data from multiple measurements"""
    # Query that returns data from both measurements
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "1s"
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        messages = list(source.fetch_data_bundle())

        # We should get two messages (one per measurement)
        assert len(messages) == 2

        # Verify both measurements are present
        measurements = []
        for message in messages:
            if "temperature" in message and "humidity" in message:
                measurements.append("sensor_data")
            elif "power_kw" in message and "energy_kwh" in message:
                measurements.append("power_data")

        assert "sensor_data" in measurements
        assert "power_data" in measurements

    finally:
        source.stop()


def test_influxdb_source_iterative_fetch_with_new_data(influxdb_test_data, influxdb_test_config):
    """Tests iterative calls to fetch_data_bundle with newly arriving data"""
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "0s"  # No lag time for this test to get all data immediately
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        # First fetch - should get initial 5 data points
        messages_first = list(source.fetch_data_bundle())
        assert len(messages_first) > 0
        first_message = messages_first[0]
        initial_count = len(first_message["temperature"])
        assert initial_count == 5

        # Record the time after the first fetch
        # The source's internal _start_time has now moved forward
        time_after_first_fetch = datetime.datetime.now(tz=datetime.timezone.utc)

        # Wait a moment to create separation
        time.sleep(2)

        # Add new data points with timestamps AFTER the first fetch but BEFORE now
        write_api = influxdb_test_data["write_api"]
        # Use timestamps starting from 1 second after the first fetch
        new_timestamp = time_after_first_fetch + datetime.timedelta(seconds=1)

        new_points = []
        for i in range(3):
            # Space timestamps 1 second apart
            timestamp = new_timestamp + datetime.timedelta(seconds=i)
            point = (
                influxdb_client.Point("sensor_data")
                .tag("location", "room_a")
                .tag("device_id", "device_001")
                .field("temperature", 25.0 + i * 0.5)
                .field("humidity", 50 + i)
                .field("status", f"new_reading_{i}")
                .time(timestamp)
            )
            new_points.append(point)

        write_api.write(bucket=TEST_BUCKET, org=influxdb_test_config.org, record=new_points)

        # Wait for data to be written and indexed
        time.sleep(2)

        # Second fetch - should get the 3 new data points
        messages_second = list(source.fetch_data_bundle())

        # Verify we got the new data
        assert len(messages_second) > 0, "Expected to receive new data but got no messages"

        second_message = messages_second[0]
        second_count = len(second_message["temperature"])

        # Should get exactly the 3 new points
        assert second_count == 3, f"Expected 3 new data points, got {second_count}"

        # Verify the new data
        assert all(isinstance(v, float) for v in second_message["temperature"])
        assert all(isinstance(v, int) for v in second_message["humidity"])
        assert all(isinstance(v, str) for v in second_message["status"])

        # Check that new values are in expected range
        assert all(v >= 25.0 for v in second_message["temperature"])

        # Third fetch with no new data - should get empty result
        time.sleep(1)
        messages_third = list(source.fetch_data_bundle())

        # Should get no messages or empty messages
        assert len(messages_third) == 0, "Expected no messages when there's no new data"

    finally:
        source.stop()


def test_influxdb_source_data_types(influxdb_test_data, influxdb_test_config):
    """Tests that different data types (float, int, string) are handled correctly"""
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> filter(fn: (r) => r._measurement == "power_data")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "1s"
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        messages = list(source.fetch_data_bundle())
        assert len(messages) > 0

        message = messages[0]

        # Check float field
        assert "power_kw" in message
        assert isinstance(message["power_kw"], list)
        assert len(message["power_kw"]) == 5
        assert all(isinstance(v, float) for v in message["power_kw"])
        assert all(v >= 100.0 for v in message["power_kw"])

        # Check int field
        assert "energy_kwh" in message
        assert isinstance(message["energy_kwh"], list)
        assert len(message["energy_kwh"]) == 5
        assert all(isinstance(v, int) for v in message["energy_kwh"])
        assert all(v >= 1000 for v in message["energy_kwh"])

        # Check string field
        assert "phase" in message
        assert isinstance(message["phase"], list)
        assert len(message["phase"]) == 5
        assert all(isinstance(v, str) for v in message["phase"])
        assert all(v.startswith("phase_") for v in message["phase"])

    finally:
        source.stop()


def test_influxdb_source_empty_result(influxdb_test_data, influxdb_test_config):
    """Tests behavior when query returns no data"""
    # Query for non-existent measurement
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> filter(fn: (r) => r._measurement == "nonexistent_measurement")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "1s"
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        messages = list(source.fetch_data_bundle())
        # Should return empty list or list with empty messages
        assert isinstance(messages, list)

    finally:
        source.stop()


def test_influxdb_source_connection_error():
    """Tests behavior when connection to InfluxDB fails"""
    config = {
        "url": "http://invalid-host:8086",
        "token": "invalid_token",
        "org": "invalid_org",
        "query": 'from(bucket: "test") |> range(start: _start_time, stop: _stop_time)',
        "initial_history": "1h",
        "lag_time": "0s"
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    # Should raise ConnectionError when trying to start
    with pytest.raises(ConnectionError):
        source.start()


def test_influxdb_source_fetch_timed_historic_data_basic(influxdb_test_data, influxdb_test_config):
    """Tests fetching historic data within a specific time range"""
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "0s"
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        # Define a time range that covers the middle 3 data points
        # The test data has 5 points at: base_time, base_time+2min, base_time+4min, base_time+6min, base_time+8min
        base_time = influxdb_test_data["base_time"]
        start_time = base_time + datetime.timedelta(minutes=1)  # After first point
        end_time = base_time + datetime.timedelta(minutes=7)    # Before last point

        # Fetch historic data
        messages = list(source.fetch_timed_historic_data_bundle(start_time, end_time, {}))

        # Should get exactly one message (one table for sensor_data)
        assert len(messages) > 0

        message = messages[0]

        # Should get exactly 3 data points (at 2min, 4min, 6min)
        assert len(message["temperature"]) == 3, f"Expected 3 data points, got {len(message['temperature'])}"

        # Verify the values are correct (second, third, and fourth readings)
        expected_temps = [21.0, 21.5, 22.0]  # 20.5 + i*0.5 for i=1,2,3
        expected_humidity = [46, 47, 48]      # 45 + i for i=1,2,3

        assert message["temperature"] == expected_temps
        assert message["humidity"] == expected_humidity

    finally:
        source.stop()


def test_influxdb_source_fetch_timed_historic_data_excludes_early_data(influxdb_test_data, influxdb_test_config):
    """Tests that data before the start_time is excluded"""
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "0s"
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        # Define a time range that excludes the first two data points
        base_time = influxdb_test_data["base_time"]
        start_time = base_time + datetime.timedelta(minutes=5)  # After second point
        end_time = base_time + datetime.timedelta(minutes=10)   # After all points

        messages = list(source.fetch_timed_historic_data_bundle(start_time, end_time, {}))

        assert len(messages) > 0
        message = messages[0]

        # Should only get the last 2 data points (at 6min and 8min)
        assert len(message["temperature"]) == 2, f"Expected 2 data points, got {len(message['temperature'])}"

        # Verify these are the last two readings
        expected_temps = [22.0, 22.5]  # 20.5 + i*0.5 for i=3,4
        expected_humidity = [48, 49]    # 45 + i for i=3,4

        assert message["temperature"] == expected_temps
        assert message["humidity"] == expected_humidity

    finally:
        source.stop()


def test_influxdb_source_fetch_timed_historic_data_excludes_late_data(influxdb_test_data, influxdb_test_config):
    """Tests that data after the end_time is excluded"""
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "0s"
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        # Define a time range that excludes the last two data points
        base_time = influxdb_test_data["base_time"]
        start_time = base_time - datetime.timedelta(minutes=1)  # Before all points
        end_time = base_time + datetime.timedelta(minutes=3)    # After first point, before second

        messages = list(source.fetch_timed_historic_data_bundle(start_time, end_time, {}))

        assert len(messages) > 0
        message = messages[0]

        # Should only get the first 2 data points (at 0min and 2min)
        assert len(message["temperature"]) == 2, f"Expected 2 data points, got {len(message['temperature'])}"

        # Verify these are the first two readings
        expected_temps = [20.5, 21.0]  # 20.5 + i*0.5 for i=0,1
        expected_humidity = [45, 46]    # 45 + i for i=0,1

        assert message["temperature"] == expected_temps
        assert message["humidity"] == expected_humidity

    finally:
        source.stop()


def test_influxdb_source_fetch_timed_historic_data_exact_boundaries(influxdb_test_data, influxdb_test_config):
    """Tests fetching data with exact timestamp boundaries"""
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "0s"
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        # Use exact timestamps of data points as boundaries
        base_time = influxdb_test_data["base_time"]
        start_time = base_time + datetime.timedelta(minutes=2)  # Exact timestamp of second point
        end_time = base_time + datetime.timedelta(minutes=6)    # Exact timestamp of fourth point

        messages = list(source.fetch_timed_historic_data_bundle(start_time, end_time, {}))

        assert len(messages) > 0
        message = messages[0]

        # InfluxDB range is [start, stop), so should get points at 2min and 4min only (not 6min)
        assert len(message["temperature"]) == 2, f"Expected 2 data points, got {len(message['temperature'])}"

        # Verify the correct readings
        expected_temps = [21.0, 21.5]  # 20.5 + i*0.5 for i=1,2
        expected_humidity = [46, 47]    # 45 + i for i=1,2

        assert message["temperature"] == expected_temps
        assert message["humidity"] == expected_humidity

    finally:
        source.stop()


def test_influxdb_source_fetch_timed_historic_data_empty_range(influxdb_test_data, influxdb_test_config):
    """Tests fetching data when no data exists in the specified time range"""
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "0s"
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        # Define a time range where no data exists (far in the past)
        base_time = influxdb_test_data["base_time"]
        start_time = base_time - datetime.timedelta(hours=2)
        end_time = base_time - datetime.timedelta(hours=1)

        messages = list(source.fetch_timed_historic_data_bundle(start_time, end_time, {}))

        # Should get empty result
        assert len(messages) == 0, f"Expected no messages, got {len(messages)}"

    finally:
        source.stop()


def test_influxdb_source_fetch_timed_historic_data_multiple_measurements(influxdb_test_data, influxdb_test_config):
    """Tests fetching historic data from multiple measurements with time filtering"""
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "0s"
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        # Fetch a subset of the data
        base_time = influxdb_test_data["base_time"]
        start_time = base_time + datetime.timedelta(minutes=1)
        end_time = base_time + datetime.timedelta(minutes=5)

        messages = list(source.fetch_timed_historic_data_bundle(start_time, end_time, {}))

        # Should get two messages (one per measurement)
        assert len(messages) == 2

        # Verify both measurements have the correct number of points
        for message in messages:
            # Each measurement should have 2 data points in this range (at 2min and 4min)
            if "temperature" in message:
                assert len(message["temperature"]) == 2
            elif "power_kw" in message:
                assert len(message["power_kw"]) == 2

    finally:
        source.stop()


def test_influxdb_source_fetch_timed_historic_data_does_not_affect_fetch_data_bundle(influxdb_test_data, influxdb_test_config):
    """Tests that fetch_timed_historic_data_bundle does not affect the internal state used by fetch_data_bundle"""
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "0s"
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        # First, use fetch_data_bundle to establish the internal state
        messages_first = list(source.fetch_data_bundle())
        assert len(messages_first) > 0
        first_count = len(messages_first[0]["temperature"])

        # Now use fetch_timed_historic_data_bundle
        base_time = influxdb_test_data["base_time"]
        start_time = base_time + datetime.timedelta(minutes=1)
        end_time = base_time + datetime.timedelta(minutes=5)

        historic_messages = list(source.fetch_timed_historic_data_bundle(start_time, end_time, {}))
        assert len(historic_messages) > 0

        # Add new data
        time.sleep(1)
        write_api = influxdb_test_data["write_api"]
        new_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        new_point = (
            influxdb_client.Point("sensor_data")
            .tag("location", "room_a")
            .tag("device_id", "device_001")
            .field("temperature", 30.0)
            .field("humidity", 60)
            .field("status", "new_after_historic")
            .time(new_timestamp)
        )
        write_api.write(bucket=TEST_BUCKET, org=influxdb_test_config.org, record=[new_point])
        time.sleep(1)

        # fetch_data_bundle should still work correctly and only fetch new data
        messages_second = list(source.fetch_data_bundle())

        # Should get the new data point only
        assert len(messages_second) > 0
        assert len(messages_second[0]["temperature"]) == 1
        assert messages_second[0]["temperature"][0] == 30.0

    finally:
        source.stop()


def test_influxdb_source_batching_mechanism(influxdb_test_data, influxdb_test_config):
    """Tests that the batching mechanism correctly breaks up large time ranges into smaller batches"""
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    # Configure with batch_duration of 3 minutes
    # With test data at 0, 2, 4, 6, 8 minutes, and a 10-minute range,
    # we should get batches: [0-3min], [3-6min], [6-9min], [9-10min]
    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "0s",
        "batch_duration": "3m"  # 3 minute batches
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        base_time = influxdb_test_data["base_time"]
        start_time = base_time - datetime.timedelta(minutes=1)
        end_time = base_time + datetime.timedelta(minutes=10)

        messages = list(source.fetch_timed_historic_data_bundle(start_time, end_time, {}))

        # We should get multiple messages due to batching
        # Since we have 5 data points spread over ~8 minutes with 3-minute batches,
        # we should get multiple batches
        assert len(messages) > 0, "Should receive at least one message"

        # Collect all temperature readings across all messages
        all_temps = []
        for message in messages:
            if "temperature" in message:
                all_temps.extend(message["temperature"])

        # Should get all 5 data points total
        assert len(all_temps) == 5, f"Expected 5 total data points across all batches, got {len(all_temps)}"

        # Verify the data is correct
        expected_temps = [20.5, 21.0, 21.5, 22.0, 22.5]
        assert all_temps == expected_temps, f"Expected {expected_temps}, got {all_temps}"

    finally:
        source.stop()


def test_influxdb_source_batching_with_small_batches(influxdb_test_data, influxdb_test_config):
    """Tests batching with very small batch duration to ensure each batch contains only relevant data"""
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    # Use 1-minute batches - should result in some batches with data and some without
    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "0s",
        "batch_duration": "1m"  # 1 minute batches
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        base_time = influxdb_test_data["base_time"]
        # Query from base_time to base_time + 10 minutes
        start_time = base_time
        end_time = base_time + datetime.timedelta(minutes=10)

        messages = list(source.fetch_timed_historic_data_bundle(start_time, end_time, {}))

        # Collect all data points
        all_temps = []
        all_humidity = []
        for message in messages:
            if "temperature" in message:
                all_temps.extend(message["temperature"])
                all_humidity.extend(message["humidity"])

        # Should get all 5 data points
        assert len(all_temps) == 5, f"Expected 5 data points, got {len(all_temps)}"
        assert len(all_humidity) == 5, f"Expected 5 data points, got {len(all_humidity)}"

        # Verify sequential order is maintained
        for i in range(len(all_temps) - 1):
            assert all_temps[i] <= all_temps[i + 1], "Temperature readings should be in order"

    finally:
        source.stop()


def test_influxdb_source_batching_vs_no_batching_consistency(influxdb_test_data, influxdb_test_config):
    """Tests that batching produces the same results as non-batching, just in smaller chunks"""
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    base_time = influxdb_test_data["base_time"]
    start_time = base_time - datetime.timedelta(minutes=1)
    end_time = base_time + datetime.timedelta(minutes=10)

    # Fetch without batching
    config_no_batch = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "0s",
        "batch_duration": None
    }

    source_no_batch = influxdb_source.InfluxDBSource(
        source_parameters=config_no_batch,
        executor_name="test_executor_no_batch"
    )

    source_no_batch.start()
    try:
        messages_no_batch = list(source_no_batch.fetch_timed_historic_data_bundle(start_time, end_time, {}))
    finally:
        source_no_batch.stop()

    # Fetch with batching
    config_with_batch = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "0s",
        "batch_duration": "2m"  # 2 minute batches
    }

    source_with_batch = influxdb_source.InfluxDBSource(
        source_parameters=config_with_batch,
        executor_name="test_executor_with_batch"
    )

    source_with_batch.start()
    try:
        messages_with_batch = list(source_with_batch.fetch_timed_historic_data_bundle(start_time, end_time, {}))
    finally:
        source_with_batch.stop()

    # Collect all data from both approaches
    temps_no_batch = []
    for message in messages_no_batch:
        if "temperature" in message:
            temps_no_batch.extend(message["temperature"])

    temps_with_batch = []
    for message in messages_with_batch:
        if "temperature" in message:
            temps_with_batch.extend(message["temperature"])

    # Should have the same data
    assert len(temps_no_batch) == len(temps_with_batch), \
        f"Batching should produce same number of data points: {len(temps_no_batch)} vs {len(temps_with_batch)}"
    assert temps_no_batch == temps_with_batch, "Batching should produce identical data"


def test_influxdb_source_batching_with_fetch_data_bundle(influxdb_test_data, influxdb_test_config):
    """Tests that batching also works with fetch_data_bundle (not just historic data)"""
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "0s",
        "batch_duration": "2m"  # 2 minute batches
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        # Fetch data using fetch_data_bundle
        messages = list(source.fetch_data_bundle())

        # Should get messages
        assert len(messages) > 0, "Should receive at least one message"

        # Collect all data points
        all_temps = []
        for message in messages:
            if "temperature" in message:
                all_temps.extend(message["temperature"])

        # Should get all 5 initial data points
        assert len(all_temps) == 5, f"Expected 5 data points, got {len(all_temps)}"

    finally:
        source.stop()


def test_influxdb_source_batching_with_multiple_measurements(influxdb_test_data, influxdb_test_config):
    """Tests that batching works correctly when query returns multiple measurements"""
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "0s",
        "batch_duration": "3m"  # 3 minute batches
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        base_time = influxdb_test_data["base_time"]
        start_time = base_time - datetime.timedelta(minutes=1)
        end_time = base_time + datetime.timedelta(minutes=10)

        messages = list(source.fetch_timed_historic_data_bundle(start_time, end_time, {}))

        # Should get messages from both measurements
        assert len(messages) > 0, "Should receive at least one message"

        # Count data from each measurement
        sensor_data_count = 0
        power_data_count = 0

        for message in messages:
            if "temperature" in message:
                sensor_data_count += len(message["temperature"])
            elif "power_kw" in message:
                power_data_count += len(message["power_kw"])

        # Should have data from both measurements
        assert sensor_data_count == 5, f"Expected 5 sensor_data points, got {sensor_data_count}"
        assert power_data_count == 5, f"Expected 5 power_data points, got {power_data_count}"

    finally:
        source.stop()


def test_influxdb_source_batching_edge_case_exact_boundaries(influxdb_test_data, influxdb_test_config):
    """Tests batching when time range exactly aligns with batch boundaries"""
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    # Use batch size that aligns with data points (2 minute batches, data at 0, 2, 4, 6, 8 min)
    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "0s",
        "batch_duration": "2m"  # 2 minute batches
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        base_time = influxdb_test_data["base_time"]
        # Query from base to base+8 minutes (exactly 4 batches of 2 minutes each)
        start_time = base_time
        end_time = base_time + datetime.timedelta(minutes=8)

        messages = list(source.fetch_timed_historic_data_bundle(start_time, end_time, {}))

        # Collect all data
        all_temps = []
        for message in messages:
            if "temperature" in message:
                all_temps.extend(message["temperature"])

        # Should get first 4 data points (at 0, 2, 4, 6 minutes)
        # The 5th point at 8 minutes should be excluded due to range being [start, stop)
        assert len(all_temps) == 4, f"Expected 4 data points, got {len(all_temps)}"

    finally:
        source.stop()


def test_influxdb_source_batching_single_large_batch(influxdb_test_data, influxdb_test_config):
    """Tests that a very large batch_duration effectively disables batching"""
    query = f'''
    from(bucket: "{TEST_BUCKET}")
        |> range(start: _start_time, stop: _stop_time)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    config = {
        "url": influxdb_test_config.url,
        "token": influxdb_test_config.token,
        "org": influxdb_test_config.org,
        "query": query,
        "initial_history": "1h",
        "lag_time": "0s",
        "batch_duration": "24h"  # Very large batch - should get everything in one batch
    }

    source = influxdb_source.InfluxDBSource(
        source_parameters=config,
        executor_name="test_executor"
    )

    source.start()

    try:
        base_time = influxdb_test_data["base_time"]
        start_time = base_time - datetime.timedelta(minutes=1)
        end_time = base_time + datetime.timedelta(minutes=10)

        messages = list(source.fetch_timed_historic_data_bundle(start_time, end_time, {}))

        # Should get one message (single batch contains everything)
        assert len(messages) == 1, f"Expected 1 message with large batch, got {len(messages)}"

        message = messages[0]
        assert len(message["temperature"]) == 5, f"Expected 5 data points, got {len(message['temperature'])}"

    finally:
        source.stop()

