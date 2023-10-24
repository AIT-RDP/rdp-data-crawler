"""
Test the query executor services
"""
import copy
import datetime
import logging
import os
import threading
import time
import warnings
from typing import Dict, Any

import pytest
import redis

import data_crawler.query_executors as query_executors
import data_crawler.sources.abc.abstract_source as abstract_sources
import data_crawler.sources.abc.history as history
import data_crawler.access.storage as storage

logger = logging.getLogger(__name__)


class MockupSourceAPI(abstract_sources.AbstractSourceAPI, history.AbstractTimedHistorySourceMixin):
    """Test API source that just counts function invocations"""

    def __init__(self, source_parameters, **kwargs):
        """Stores the configuration and initializes the object"""
        self.kwargs = kwargs

        self.config = source_parameters
        self.fetch_invocations = 0
        self.history_invocations = 0
        self.start_invocations = 0
        self.stop_invocations = 0

        self.fetch_ts = []

        self.enable_fetch = threading.Event()
        self.enable_fetch.set()

    def start(self):
        """Counts the start and performs some basic checks"""
        assert self.start_invocations == self.stop_invocations

        if self.config.get("raise_on_startup", False):
            raise ValueError("Uups!")

        self.start_invocations += 1

    def fetch_data(self) -> Dict[str, Any]:
        """Generates some content and returns it"""

        self.fetch_ts.append(datetime.datetime.utcnow())
        self.fetch_invocations += 1

        assert self.start_invocations == self.stop_invocations + 1

        if self.config.get("no odd invocations", False) and self.fetch_invocations % 2 == 1:
            raise ValueError("That's odd.")

        self.enable_fetch.wait()

        return {
            "invocations": self.fetch_invocations,
            "data": "some-test-nonsense",
            "duplicate": "api-key"
        }

    def fetch_historic_data(self, start_time: datetime.datetime, end_time: datetime.datetime,
                            filter_clauses: Dict[str, Any]) -> Dict[str, Any]:
        """Generates some content and returns it"""

        self.history_invocations += 1

        assert self.start_invocations == self.stop_invocations + 1

        if self.config.get("no odd invocations", False) and self.history_invocations % 2 == 1:
            raise ValueError("That's odd.")

        return {
            "fetch_invocations": self.fetch_invocations,
            "history_invocations": self.history_invocations,
            "data": "another-test-nonsense",
            "duplicate": "same-api-key"
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

    assert "persistent_store" in api.kwargs
    assert isinstance(api.kwargs["persistent_store"], storage.PersistentAPIStorage)


def test_thread_executor_api_lifecycle(mockup_service_config, redis_pool):
    """Tests the API instantiation function using the mockup API"""

    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool)
    assert isinstance(executor.source_api, MockupSourceAPI)

    api: MockupSourceAPI = executor.source_api
    assert api.fetch_invocations == 0
    assert api.start_invocations == 0
    assert api.stop_invocations == 0
    assert api.history_invocations == 0

    executor.start()
    time.sleep(0.6)
    executor.stop()
    executor.join()

    assert api.fetch_invocations >= 1
    assert api.start_invocations == 1
    assert api.stop_invocations == 1
    assert api.history_invocations == 0


def test_thread_executor_api_status(mockup_service_config, redis_pool):
    """Tests the API instantiation function using the mockup API"""

    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool)
    assert isinstance(executor.source_api, MockupSourceAPI)

    status = executor.get_activity_status()
    assert status.last_wakeup is None
    assert status.last_cycle_complete is None
    assert status.max_permitted_cycle_time == datetime.timedelta(seconds=0.5)

    ts_start = datetime.datetime.now(tz=datetime.timezone.utc)
    executor.start()
    time.sleep(0.6)
    executor.stop()
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


def test_thread_executor_startup_error(mockup_service_config, redis_pool):
    """Tests whether the API fetch function is correctly invoked"""

    api = MockupSourceAPI(source_parameters={"raise_on_startup": True})
    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool, source_api=api)

    assert api.fetch_invocations == 0
    assert api.start_invocations == 0

    executor.start()
    assert not executor.is_alive(), "Crash on startup expected"

    executor.stop()
    executor.join()


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


@pytest.fixture()
def mockup_executors_config(mockup_service_config):
    """A mockup config including two fake executors"""

    return {
        "first": copy.deepcopy(mockup_service_config),
        "second": copy.deepcopy(mockup_service_config)
    }


@pytest.fixture()
def mockup_executors_externals(mockup_executors_config) -> Dict[str, MockupSourceAPI]:
    """Instantiates the mockup executors for the collection"""
    return {
        key: MockupSourceAPI(config) for key, config in mockup_executors_config.items()
    }


def test_query_supervisor_life_cycle(mockup_executors_config, mockup_executors_externals, redis_pool):
    """Tests the standard life cycle of the supervisor"""

    supervisor = query_executors.QuerySupervisor(mockup_executors_config, {}, redis_pool, mockup_executors_externals)
    supervisor.start()

    status = supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok"}

    time.sleep(0.6)
    status = supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok"}

    assert mockup_executors_externals["first"].start_invocations == 1
    assert mockup_executors_externals["first"].fetch_invocations >= 1
    assert mockup_executors_externals["first"].stop_invocations == 0

    assert mockup_executors_externals["second"].start_invocations == 1
    assert mockup_executors_externals["second"].fetch_invocations >= 1
    assert mockup_executors_externals["second"].stop_invocations == 0

    supervisor.stop()
    assert mockup_executors_externals["first"].start_invocations == 1
    assert mockup_executors_externals["first"].fetch_invocations >= 1
    assert mockup_executors_externals["first"].stop_invocations == 1

    assert mockup_executors_externals["second"].start_invocations == 1
    assert mockup_executors_externals["second"].fetch_invocations >= 1
    assert mockup_executors_externals["second"].stop_invocations == 1


def test_query_supervisor_restart(mockup_executors_config, mockup_executors_externals, redis_pool):
    """Tests the friendly restart policy of the query supervisor"""
    sup_config = {"dead timeout": "0.1s"}
    supervisor = query_executors.QuerySupervisor(mockup_executors_config, sup_config, redis_pool,
                                                 mockup_executors_externals)
    supervisor.start()

    status = supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok"}

    mockup_executors_externals["second"].enable_fetch.clear()  # Block the execution
    time.sleep(1.1)

    status = supervisor.heartbeat()
    assert status == {"first": "ok", "second": "stop-by-timeout"}
    status = supervisor.heartbeat()
    assert status == {"first": "ok", "second": "blocking"}

    mockup_executors_externals["second"].enable_fetch.set()
    time.sleep(0.1)  # Give the second source some time to terminate

    status = supervisor.heartbeat()
    assert status == {"first": "ok", "second": "restarted"}

    time.sleep(0.6)

    status = supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok"}

    supervisor.stop()


def test_query_supervisor_logging(caplog: pytest.LogCaptureFixture, mockup_executors_config, mockup_executors_externals,
                                  redis_pool):
    """Tests the log messages when restarting some executors"""
    caplog.set_level(logging.DEBUG, logger="data_crawler")
    sup_config = {"dead timeout": "0.1s"}
    supervisor = query_executors.QuerySupervisor(mockup_executors_config, sup_config, redis_pool,
                                                 mockup_executors_externals)
    supervisor.start()
    supervisor.heartbeat()

    mockup_executors_externals["second"].enable_fetch.clear()  # Block the execution
    time.sleep(1.1)

    supervisor.heartbeat()  # second: "stop-by-timeout"
    assert "The source second does not complete its cycle in time" in caplog.text

    supervisor.heartbeat()  # second: "blocking"
    assert "Source second reached a timeout and is marked for restart" in caplog.text

    mockup_executors_externals["second"].enable_fetch.set()
    time.sleep(0.1)  # Give the second source some time to terminate

    supervisor.heartbeat()  # second: "restarted"
    assert "Restart the executor" in caplog.text

    supervisor.heartbeat()
    supervisor.stop()


def test_one_shot_executor_api_instantiation(mockup_service_config, redis_pool):
    """Tests the API instantiation function using the mockup API"""

    executor = query_executors.OneShotQueryExecutor(mockup_service_config, redis_pool, name="<test>")
    assert isinstance(executor.source_api, MockupSourceAPI)

    api: MockupSourceAPI = executor.source_api
    assert "key" in api.config
    assert api.config["key"] == "<keep it secret>"

    assert "persistent_store" in api.kwargs
    assert isinstance(api.kwargs["persistent_store"], storage.PersistentAPIStorage)

    assert api.fetch_invocations == 0
    assert api.history_invocations == 0


def test_one_shot_executor_api_lifecycle(mockup_service_config, redis_pool):
    """Tests the API instantiation function using the mockup API"""

    executor = query_executors.OneShotQueryExecutor(mockup_service_config, redis_pool, name="<test>")
    assert isinstance(executor.source_api, MockupSourceAPI)

    api: MockupSourceAPI = executor.source_api
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

    api = MockupSourceAPI(source_parameters={"no odd invocations": True})
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
    api = MockupSourceAPI(source_parameters={})
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
    api: MockupSourceAPI = sources["second"]

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
    api: MockupSourceAPI = sources["second"]

    assert api.config["key"] == "Some new key"
    assert "LetTheHammer" in api.config
    assert api.config["LetTheHammer"] == "Fall"
    assert api.config["some_list"] == [1, 4, 16]
    assert api.config["keep"] == "it"
