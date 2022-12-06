"""
Test the query executor services
"""
import datetime
import logging
import os
import time
import warnings
from typing import Dict, Any

import pytest
import redis

import data_crawler.query_executors as query_executors
import data_crawler.sources.abc.abstract_source as abstract_sources

logger = logging.getLogger(__name__)


class MockupSourceAPI(abstract_sources.AbstractSourceAPI):
    """Test API source that just counts function invocations"""

    def __init__(self, source_parameters, **kwargs):
        """Stores the configuration and initializes the object"""
        self.config = source_parameters
        self.fetch_invocations = 0
        self.start_invocations = 0
        self.stop_invocations = 0

        self.fetch_ts = []

    def start(self):
        """Counts the start and performs some basic checks"""
        assert self.start_invocations == self.stop_invocations
        self.start_invocations += 1

    def fetch_data(self) -> Dict[str, Any]:
        """Generates some content and returns it"""

        self.fetch_ts.append(datetime.datetime.utcnow())
        self.fetch_invocations += 1

        assert self.start_invocations == self.stop_invocations + 1

        if self.config.get("no odd invocations", False) and self.fetch_invocations % 2 == 1:
            raise ValueError("That's odd.")

        return {
            "invocations": self.fetch_invocations,
            "data": "some-test-nonsense",
            "duplicate": "api-key"
        }

    def stop(self):
        """Counts the stop and performs some basic checks"""
        assert self.start_invocations == self.stop_invocations + 1
        self.stop_invocations += 1


@pytest.fixture()
def mockup_service_config(appended_test_path):
    """Returns the configuration of a simple mockup service"""

    return {
        "type": "test_query_executors.MockupSourceAPI",
        "source parameter": {
            "key": "<keep it secret>"
        },
        "polling": {
            "frequency": "0.5s"
        },
        "redis": {
            "stream": "my-stream",
            "tags": {
                "source type": "mockup-test",
                "empty": "",
                "my-number": 0.2,
                "duplicate": "config-key"
            },
        },
    }


@pytest.fixture()
def redis_pool() -> redis.ConnectionPool:
    """Opens a Redis pool and tests the connection"""

    host = os.environ.get("DATA_CRAWLER_REDIS_HOST", "localhost")
    port = os.environ.get("DATA_CRAWLER_REDIS_PORT", "6379")
    db = os.environ.get("DATA_CRAWLER_REDIS_DB", "0")

    logger.debug(f"Initialize redis pool connecting to host={host}, port={port}, db={db}")

    pool = redis.ConnectionPool(host=host, port=port, db=db, decode_responses=True)
    client = redis.Redis(connection_pool=pool)

    client.ping()
    return pool


@pytest.fixture()
def redis_stream_name(redis_pool) -> str:
    """Returns the name of a managed REDIS stream"""

    redis_client = redis.Redis(connection_pool=redis_pool)
    stream_name = "test.stream"

    stream_content = redis_client.xrange(stream_name)  # Read to implicitly create the stream
    if len(stream_content) == 0:
        warnings.warn(f"There are already {len(stream_content)} items in the redis stream '{stream_name}'")
    yield stream_name

    redis_client.xtrim(stream_name, maxlen=0)
    redis_client.delete(stream_name)  # Delete the stream again


def test_thread_executor_lifecycle(mockup_service_config, redis_pool):
    """Tests the basic lifecycle of a threaded executor"""

    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool)

    with pytest.raises(AssertionError):
        executor.join()

    executor.start()
    executor.stop()
    with pytest.raises(AssertionError):
        executor.stop()

    executor.join()


def test_thread_executor_api_instantiation(mockup_service_config, redis_pool):
    """Tests the API instantiation function using the mockup API"""

    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool)
    assert isinstance(executor.source_api, MockupSourceAPI)

    api: MockupSourceAPI = executor.source_api
    assert "key" in api.config
    assert api.config["key"] == "<keep it secret>"


def test_thread_executor_api_lifecycle(mockup_service_config, redis_pool):
    """Tests the API instantiation function using the mockup API"""

    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool)
    assert isinstance(executor.source_api, MockupSourceAPI)

    api: MockupSourceAPI = executor.source_api
    assert api.fetch_invocations == 0
    assert api.start_invocations == 0
    assert api.stop_invocations == 0

    executor.start()
    time.sleep(0.6)
    executor.stop()
    executor.join()

    assert api.fetch_invocations >= 1
    assert api.start_invocations == 1
    assert api.stop_invocations == 1


def test_thread_executor_invalid_api_name(mockup_service_config, redis_pool):
    """Tests an invalid API name"""

    mockup_service_config["type"] = "data_crawler.sources.nsa.Prism"
    with pytest.raises(ModuleNotFoundError, match="data_crawler\\.sources\\.nsa"):
        executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool)


def test_thread_executor_invalid_api_class(mockup_service_config, redis_pool):
    """Tests an invalid API class"""

    mockup_service_config["type"] = "threading.Thread"
    with pytest.raises(ModuleNotFoundError, match="Thread"):
        executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool)


def test_thread_executor_fetch_invocation(mockup_service_config, redis_pool):
    """Tests whether the API fetch function is correctly invoked"""

    api = MockupSourceAPI(source_parameters={})
    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool, source_api=api)

    assert api.fetch_invocations == 0

    executor.start()
    time.sleep(1.25)
    executor.stop()
    executor.join()

    assert 2 <= api.fetch_invocations <= 4
    assert ((api.fetch_ts[-1].microsecond < 0.1e6) or (api.fetch_ts[-1].microsecond > 0.9e6) or
            (0.4e6 < api.fetch_ts[-1].microsecond < 0.6e6))  # Check alignment


def test_thread_executor_timing_no_force_initial(mockup_service_config, redis_pool):
    """Tests the timing when no initial sample is forced"""

    mockup_service_config["polling"]["frequency"] = "1s"
    mockup_service_config["polling"]["force initial"] = False

    api = MockupSourceAPI(source_parameters={})
    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool, source_api=api)

    executor.start()
    time.sleep(2.0)
    executor.stop()
    executor.join()

    assert 1 <= api.fetch_invocations <= 3
    for i, ts in enumerate(api.fetch_ts):
        assert 0.9e6 < ts.microsecond or ts.microsecond < 0.1e6, f"Alignment error in sample {i}"  # Check alignment


def test_thread_executor_timing_force_initial(mockup_service_config, redis_pool):
    """Tests the timing when the initial sample is forced"""

    mockup_service_config["polling"]["frequency"] = "1s"
    mockup_service_config["polling"]["force initial"] = True

    api = MockupSourceAPI(source_parameters={})
    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool, source_api=api)

    executor.start()
    ts_now = datetime.datetime.utcnow()
    time.sleep(2.0)
    executor.stop()
    executor.join()

    assert 2 <= api.fetch_invocations <= 3
    assert -0.1 <= (api.fetch_ts[0] - ts_now).total_seconds() <= 0.1  # The first sample must be triggered immediately
    for i, ts in enumerate(api.fetch_ts[1:]):
        assert 0.9e6 < ts.microsecond or ts.microsecond < 0.1e6, f"Alignment error in sample {i}"  # Check alignment


def test_thread_executor_slot_mechanism(mockup_service_config, redis_pool):
    """Tests whether the API fetch function is correctly invoked"""

    mockup_service_config["polling"]["frequency"] = "1s"
    mockup_service_config["polling"]["slot count"] = 2
    mockup_service_config["polling"]["slot id"] = "1"  # The function must also support string inputs
    mockup_service_config["polling"]["force initial"] = False

    api = MockupSourceAPI(source_parameters={})
    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool, source_api=api)

    assert api.fetch_invocations == 0

    executor.start()
    time.sleep(2.0)
    executor.stop()
    executor.join()

    assert 1 <= api.fetch_invocations <= 3
    for i, ts in enumerate(api.fetch_ts):
        assert 0.4e6 < ts.microsecond < 0.6e6, f"Alignment error in sample {i}"  # Check alignment


def test_thread_executor_fetch_error(mockup_service_config, redis_pool):
    """Tests whether the API fetch function is correctly invoked"""

    api = MockupSourceAPI(source_parameters={"no odd invocations": True})
    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool, source_api=api)

    assert api.fetch_invocations == 0

    executor.start()
    time.sleep(1.25)
    executor.stop()
    executor.join()

    assert 2 <= api.fetch_invocations <= 4
    assert ((api.fetch_ts[-1].microsecond < 0.1e6) or (api.fetch_ts[-1].microsecond > 0.9e6) or
            (0.4e6 < api.fetch_ts[-1].microsecond < 0.6e6))  # Check alignment


def test_thread_executor_redis_export(mockup_service_config, redis_pool, redis_stream_name):
    """Tests the executor's capabilities in writing Redis streams"""

    mockup_service_config["redis"]["stream"] = redis_stream_name
    api = MockupSourceAPI(source_parameters={})
    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool, source_api=api)

    redis_client = redis.Redis(connection_pool=redis_pool)

    executor.start()
    time.sleep(0.51)
    executor.stop()
    executor.join()

    messages = redis_client.xrange(redis_stream_name)
    assert messages is not None
    assert 2 <= len(messages) <= 3
    assert len(messages) == api.fetch_invocations

    assert messages[0][-1]["source type"] == '"mockup-test"'
    assert messages[0][-1]["empty"] == '""'
    assert messages[0][-1]["my-number"] == '0.2'
    assert messages[0][-1]["duplicate"] == '"config-key"'
    assert messages[0][-1]["invocations"] == '1'  # Number of invocations including the current one
    assert messages[0][-1]["data"] == '"some-test-nonsense"'

    assert messages[1][-1]["source type"] == '"mockup-test"'
    assert messages[1][-1]["empty"] == '""'
    assert messages[1][-1]["my-number"] == '0.2'
    assert messages[1][-1]["duplicate"] == '"config-key"'
    assert messages[1][-1]["invocations"] == '2'
    assert messages[1][-1]["data"] == '"some-test-nonsense"'


def test_thread_executor_redis_templated_export(mockup_service_config, redis_pool, redis_stream_name):
    """Specifically assesses the templated export mechanism"""

    mockup_service_config["redis"]["stream"] = redis_stream_name + ".${data}"
    api = MockupSourceAPI(source_parameters={})
    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool, source_api=api)

    redis_client = redis.Redis(connection_pool=redis_pool)

    executor.start()
    time.sleep(0.51)
    executor.stop()
    executor.join()

    messages = redis_client.xrange(f"{redis_stream_name}.some-test-nonsense")
    redis_client.delete(f"{redis_stream_name}.some-test-nonsense")
    assert messages is not None
    assert 2 <= len(messages) <= 3
    assert len(messages) == api.fetch_invocations
