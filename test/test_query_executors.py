"""
Test the query executor services
"""
import asyncio
import copy
import datetime
import logging
import time
from typing import Dict, Any, Optional

import pytest
import redis

import data_crawler.query_executors as query_executors
import data_crawler.access.storage as storage

import sources.mockup as mockup

logger = logging.getLogger(__name__)


@pytest.fixture()
def mockup_service_config(appended_test_path):
    """Returns the configuration of a simple mockup service"""

    return {
        "type": "sources.mockup.PassiveMockupSourceAPI",
        "source parameter": {
            "key": "<keep it secret>",
            "some_list": [1, 2, 4],
            "keep": "it"
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
def mockup_service_config_dynamic(appended_test_path):
    """Returns the novel, dynamic sink configuration"""
    return {
        "type": "sources.mockup.PassiveMockupSourceAPI",
        "source parameter": {
            "key": "<keep it secret>",
            "some_list": [1, 2, 4],
            "keep": "it"
        },
        "polling": {
            "frequency": "0.5s"
        },
        "sink_type": "data_crawler.sinks.redis.RedisStream",
        "sink_parameters": {
            "tags": {
                "source type": "mockup-test",
                "empty": "",
                "my-number": 0.2,
                "duplicate": "config-key"
            },
        },
    }


def test_thread_executor_lifecycle(mockup_service_config, redis_pool):
    """Tests the basic lifecycle of a threaded executor"""

    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool)

    with pytest.raises(AssertionError):
        executor.join()

    executor.start()
    executor.shutdown()
    with pytest.raises(AssertionError):
        executor.shutdown()

    executor.join()


def test_thread_executor_api_instantiation(mockup_service_config, redis_pool):
    """Tests the API instantiation function using the mockup API"""

    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool)
    assert isinstance(executor.source_api, mockup.PassiveMockupSourceAPI)

    api: mockup.PassiveMockupSourceAPI = executor.source_api
    assert "key" in api.config
    assert api.config["key"] == "<keep it secret>"

    assert "persistent_store" in api.kwargs
    assert isinstance(api.kwargs["persistent_store"], storage.PersistentAPIStorage)


def test_thread_executor_api_lifecycle(mockup_service_config, redis_pool):
    """Tests the API instantiation function using the mockup API"""

    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool)
    assert isinstance(executor.source_api, mockup.PassiveMockupSourceAPI)

    api: mockup.PassiveMockupSourceAPI = executor.source_api
    assert api.fetch_invocations == 0
    assert api.start_invocations == 0
    assert api.stop_invocations == 0
    assert api.history_invocations == 0

    executor.start()
    time.sleep(0.6)
    executor.shutdown()
    executor.join()

    assert api.fetch_invocations >= 1
    assert api.start_invocations == 1
    assert api.stop_invocations == 1
    assert api.history_invocations == 0


def test_thread_executor_api_status(mockup_service_config, redis_pool):
    """Tests the API instantiation function using the mockup API"""

    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool)
    assert isinstance(executor.source_api, mockup.PassiveMockupSourceAPI)

    status = executor.get_activity_status()
    assert status.last_wakeup is None
    assert status.last_cycle_complete is None
    assert status.max_permitted_cycle_time == datetime.timedelta(seconds=0.5)

    ts_start = datetime.datetime.now(tz=datetime.timezone.utc)
    executor.start()
    time.sleep(0.6)
    executor.shutdown()
    executor.join()

    status = executor.get_activity_status()
    assert status.last_wakeup is not None
    assert status.last_wakeup >= ts_start

    assert status.last_cycle_complete is not None
    assert status.last_cycle_complete >= ts_start

    assert status.max_permitted_cycle_time == datetime.timedelta(seconds=0.5)


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

    api = mockup.PassiveMockupSourceAPI(source_parameters={})
    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool, source_api=api)

    assert api.fetch_invocations == 0

    executor.start()
    time.sleep(1.25)
    executor.shutdown()
    executor.join()

    assert 2 <= api.fetch_invocations <= 4
    assert ((api.fetch_ts[-1].microsecond < 0.1e6) or (api.fetch_ts[-1].microsecond > 0.9e6) or
            (0.4e6 < api.fetch_ts[-1].microsecond < 0.6e6))  # Check alignment


def test_thread_executor_fetch_error(mockup_service_config, redis_pool):
    """Tests whether the API fetch function is correctly invoked"""

    api = mockup.PassiveMockupSourceAPI(source_parameters={"no odd invocations": True})
    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool, source_api=api)

    assert api.fetch_invocations == 0

    executor.start()
    time.sleep(1.25)
    executor.shutdown()
    executor.join()

    assert 2 <= api.fetch_invocations <= 4
    assert ((api.fetch_ts[-1].microsecond < 0.1e6) or (api.fetch_ts[-1].microsecond > 0.9e6) or
            (0.4e6 < api.fetch_ts[-1].microsecond < 0.6e6))  # Check alignment


def test_thread_executor_startup_error(mockup_service_config, redis_pool):
    """Tests whether the API fetch function is correctly invoked"""

    api = mockup.PassiveMockupSourceAPI(source_parameters={"raise_on_startup": True})
    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool, source_api=api)

    assert api.fetch_invocations == 0
    assert api.start_invocations == 0

    executor.start()
    assert not executor.is_alive(), "Crash on startup expected"

    executor.shutdown()
    executor.join()


@pytest.mark.parametrize("dry_run", [None, False, "FaLsE", "0"])
def test_thread_executor_redis_export(dry_run, mockup_service_config, redis_pool, redis_stream_name):
    """Tests the executor's capabilities in writing Redis streams"""

    if dry_run is not None:
        mockup_service_config["dry_run"] = dry_run

    mockup_service_config["redis"]["stream"] = redis_stream_name
    api = mockup.PassiveMockupSourceAPI(source_parameters={})
    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool, source_api=api)

    redis_client = redis.Redis(connection_pool=redis_pool)

    executor.start()
    time.sleep(0.51)
    executor.shutdown()
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


@pytest.mark.parametrize("dry_run", [True, "TrUe", "1", 1])
def test_thread_executor_dry_run(dry_run, mockup_service_config, redis_pool, redis_stream_name):
    """Tests the executor's capabilities in ignoring data in a dry_run situation"""

    mockup_service_config["dry_run"] = dry_run

    mockup_service_config["redis"]["stream"] = redis_stream_name
    api = mockup.PassiveMockupSourceAPI(source_parameters={})
    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool, source_api=api)

    redis_client = redis.Redis(connection_pool=redis_pool)

    executor.start()
    time.sleep(0.51)
    executor.shutdown()
    executor.join()

    messages = redis_client.xrange(redis_stream_name)
    assert messages is not None
    assert len(messages) <= 0


@pytest.mark.parametrize("dry_run", [None, "Super Dry", "-1", -1, 0.5])
def test_thread_executor_invalid_dry_run(dry_run, mockup_service_config, redis_pool, redis_stream_name):
    """Tests the executor's capabilities in raising errors on invalid dry run flags"""

    mockup_service_config["dry_run"] = dry_run

    mockup_service_config["redis"]["stream"] = redis_stream_name
    api = mockup.PassiveMockupSourceAPI(source_parameters={})
    with pytest.raises(ValueError, match=".*boolean configuration.*"):
        query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool, source_api=api)


def test_thread_executor_redis_templated_export(mockup_service_config, redis_pool, redis_stream_name):
    """Specifically assesses the templated export mechanism"""

    mockup_service_config["redis"]["stream"] = redis_stream_name + ".${data}"
    api = mockup.PassiveMockupSourceAPI(source_parameters={})
    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool, source_api=api)

    redis_client = redis.Redis(connection_pool=redis_pool)

    executor.start()
    time.sleep(0.51)
    executor.shutdown()
    executor.join()

    messages = redis_client.xrange(f"{redis_stream_name}.some-test-nonsense")
    redis_client.delete(f"{redis_stream_name}.some-test-nonsense")
    assert messages is not None
    assert 2 <= len(messages) <= 3
    assert len(messages) == api.fetch_invocations


def test_thread_executor_sink_dynamic_config(mockup_service_config_dynamic, redis_pool, redis_stream_name):
    """Tests the executor's capabilities in writing Redis streams using metadata"""

    api = mockup.PassiveMockupSourceAPI(source_parameters={}, metadata=dict(stream=redis_stream_name))
    executor = query_executors.ThreadQueryExecutor(mockup_service_config_dynamic, redis_pool, source_api=api)

    redis_client = redis.Redis(connection_pool=redis_pool)

    executor.start()
    time.sleep(0.51)
    executor.shutdown()
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


@pytest.fixture()
def mockup_service_config_active(appended_test_path):
    """Configures a mockup service form the active API"""
    return {
        "type": "sources.mockup.ActiveMockupSourceAPI",
        "source parameter": {},
        "sink_type": "data_crawler.sinks.redis.RedisStream",
        "sink_parameters": {
            "tags": {
                "source type": "active-mockup-test",
                "empty": "",
                "my-number": 0.2,
                "duplicate": "config-key"
            },
        },
    }


def test_thread_executor_active_api_instantiation(mockup_service_config_active, redis_pool):
    """Tests the dynamic instantiation process of the active API"""

    executor = query_executors.ThreadQueryExecutor(mockup_service_config_active, redis_pool)

    assert isinstance(executor.source_api, mockup.ActiveMockupSourceAPI)
    api: mockup.ActiveMockupSourceAPI = executor.source_api

    assert isinstance(api.config, mockup.ActiveMockupSourceParameters)
    assert api.config == mockup.ActiveMockupSourceParameters()

    assert api.start_exec == []
    assert api.run_exec == []
    assert api.shutdown_exec == []
    assert api.stop_exec == []


def test_thread_executor_active_api_lifecycle_calls(mockup_service_config_active: dict, redis_pool, redis_stream_name):
    """Tests the default lifecycle of the thread query executor in an end-to-end fashion"""

    mockup_service_config_active["sink_parameters"]["stream"] = redis_stream_name
    executor = query_executors.ThreadQueryExecutor(mockup_service_config_active, redis_pool)

    assert isinstance(executor.source_api, mockup.ActiveMockupSourceAPI)
    api: mockup.ActiveMockupSourceAPI = executor.source_api

    # Perform a standard life cycle
    executor.start()
    time.sleep(0.51)
    executor.shutdown()
    executor.join()

    # Check the API invocations
    assert api.start_exec == [0]
    assert api.run_exec == [1]
    assert len(api.shutdown_exec) == 1
    assert len(api.stop_exec) == 1
    assert 3 <= len(api.run_message_exec) <= 7
    assert all(i > 1 for i in api.run_message_exec), "Run must be executed strictly after start"
    assert all(i < api.stop_exec[0] for i in api.run_message_exec), "Run must be executed strictly before stop"
    assert api.shutdown_exec[0] < api.stop_exec[0], "Shutdown must be called before stop"


def test_thread_executor_active_api_lifecycle_message(mockup_service_config_active: dict, redis_pool,
                                                      redis_stream_name):
    """Tests the default lifecycle of the thread query executor in an end-to-end fashion"""

    mockup_service_config_active["sink_parameters"]["stream"] = redis_stream_name
    executor = query_executors.ThreadQueryExecutor(mockup_service_config_active, redis_pool)

    assert isinstance(executor.source_api, mockup.ActiveMockupSourceAPI)
    api: mockup.ActiveMockupSourceAPI = executor.source_api

    redis_client = redis.Redis(connection_pool=redis_pool)

    # Perform a standard life cycle
    executor.start()
    time.sleep(0.51)
    executor.shutdown()
    executor.join()

    # Fetch and check the messages
    messages = redis_client.xrange(redis_stream_name)
    assert messages is not None
    assert 3 <= len(messages) <= 7
    assert len(messages) == len(api.run_message_exec)

    assert messages[0][-1]["source type"] == '"active-mockup-test"'
    assert messages[0][-1]["empty"] == '""'
    assert messages[0][-1]["my-number"] == '0.2'
    assert messages[0][-1]["duplicate"] == '"config-key"'
    assert messages[0][-1]["message"] == '"got it"'


@pytest.fixture()
def mockup_executors_config(mockup_service_config):
    """A mockup config including two fake executors"""

    return {
        "first": copy.deepcopy(mockup_service_config),
        "second": copy.deepcopy(mockup_service_config)
    }


def test_one_shot_executor_api_instantiation(mockup_service_config, redis_pool):
    """Tests the API instantiation function using the mockup API"""

    executor = query_executors.OneShotQueryExecutor(mockup_service_config, redis_pool, name="<test>")
    assert isinstance(executor.source_api, mockup.PassiveMockupSourceAPI)

    api: mockup.PassiveMockupSourceAPI = executor.source_api
    assert "key" in api.config
    assert api.config["key"] == "<keep it secret>"

    assert "persistent_store" in api.kwargs
    assert isinstance(api.kwargs["persistent_store"], storage.PersistentAPIStorage)

    assert api.fetch_invocations == 0
    assert api.history_invocations == 0


def test_one_shot_executor_api_lifecycle(mockup_service_config, redis_pool):
    """Tests the API instantiation function using the mockup API"""

    executor = query_executors.OneShotQueryExecutor(mockup_service_config, redis_pool, name="<test>")
    assert isinstance(executor.source_api, mockup.PassiveMockupSourceAPI)

    api: mockup.PassiveMockupSourceAPI = executor.source_api
    assert api.fetch_invocations == 0
    assert api.start_invocations == 0
    assert api.stop_invocations == 0
    assert api.history_invocations == 0

    executor.execute_batch([dict(start_time="2023-09-17T00:00:00Z", end_time="2023-09-18T00:00:00Z")])

    assert api.fetch_invocations == 0
    assert api.start_invocations == 1
    assert api.stop_invocations == 1
    assert api.history_invocations == 1


def test_one_shot_executor_fetch_error(mockup_service_config, redis_pool):
    """Tests whether the API fetch function is correctly invoked"""

    api = mockup.PassiveMockupSourceAPI(source_parameters={"no odd invocations": True})
    executor = query_executors.OneShotQueryExecutor(mockup_service_config, redis_pool, source_api=api, name="<test>")

    assert api.fetch_invocations == 0
    assert api.history_invocations == 0

    with pytest.raises(ValueError, match="That's odd."):
        filters = [dict(start_time="2023-09-17T00:00:00Z", end_time="2023-09-18T00:00:00Z"),
                   dict(start_time="2023-09-18T00:00:00Z", end_time="2023-09-19T00:00:00Z")]
        executor.execute_batch(filters)

    assert api.fetch_invocations == 0
    assert api.history_invocations == 1
    assert api.start_invocations == 1
    assert api.stop_invocations == 1


def test_one_shot_executor_redis_export(mockup_service_config, redis_pool, redis_stream_name):
    """Tests the executor's capabilities in writing Redis streams"""

    mockup_service_config["redis"]["stream"] = redis_stream_name
    api = mockup.PassiveMockupSourceAPI(source_parameters={})
    executor = query_executors.OneShotQueryExecutor(mockup_service_config, redis_pool, source_api=api, name="test")

    redis_client = redis.Redis(connection_pool=redis_pool)

    filters = [dict(start_time="2023-09-17T00:00:00Z", end_time="2023-09-18T00:00:00Z"),
               dict(start_time="2023-09-18T00:00:00Z", end_time="2023-09-19T00:00:00Z")]
    executor.execute_batch(filters)

    messages = redis_client.xrange(redis_stream_name)
    assert messages is not None
    assert len(messages) == 2
    assert len(messages) == api.history_invocations

    assert messages[0][-1]["source type"] == '"mockup-test"'
    assert messages[0][-1]["empty"] == '""'
    assert messages[0][-1]["my-number"] == '0.2'
    assert messages[0][-1]["duplicate"] == '"config-key"'
    assert messages[0][-1]["history_invocations"] == '1'  # Number of invocations including the current one
    assert messages[0][-1]["data"] == '"another-test-nonsense"'

    assert messages[1][-1]["source type"] == '"mockup-test"'
    assert messages[1][-1]["empty"] == '""'
    assert messages[1][-1]["my-number"] == '0.2'
    assert messages[1][-1]["duplicate"] == '"config-key"'
    assert messages[1][-1]["history_invocations"] == '2'
    assert messages[1][-1]["data"] == '"another-test-nonsense"'


def test_execute_one_shot_batches(mockup_executors_config, redis_pool):
    """Tests the complete one shot cycle function"""

    filters = [dict(start_time="2023-09-17T00:00:00Z", end_time="2023-09-18T00:00:00Z"),
               dict(start_time="2023-09-18T00:00:00Z", end_time="2023-09-19T00:00:00Z")]
    targets = ["non-existing*", "secon*"]

    sources = query_executors.execute_one_shot_batches(mockup_executors_config, redis_pool, filters, targets,
                                                       debug_return=True)

    assert len(sources) == 1
    api: mockup.PassiveMockupSourceAPI = sources["second"]

    assert api.start_invocations == 1
    assert api.fetch_invocations == 0
    assert api.history_invocations == 2
    assert api.stop_invocations == 1


def test_execute_one_shot_batches_override(mockup_executors_config, redis_pool):
    """Tests the override functionality of the configuration"""

    filters = [dict(start_time="2023-09-17T00:00:00Z", end_time="2023-09-18T00:00:00Z")]
    targets = ["non-existing*", "secon*"]
    override = {"source parameter": {"key": "Some new key", "LetTheHammer": "Fall", "some_list": [1, 4, 16]}}

    sources = query_executors.execute_one_shot_batches(mockup_executors_config, redis_pool, filters, targets,
                                                       override_config=override, debug_return=True)

    assert len(sources) == 1
    api: mockup.PassiveMockupSourceAPI = sources["second"]

    assert api.config["key"] == "Some new key"
    assert "LetTheHammer" in api.config
    assert api.config["LetTheHammer"] == "Fall"
    assert api.config["some_list"] == [1, 4, 16]
    assert api.config["keep"] == "it"
