"""
Tests the synchronous active wrapper that encapsulates the polling-based API and provides an active source interface
"""
import contextlib
import datetime
import threading
import time
from typing import Optional

import pytest

import data_crawler.sources._sync_wrapper as wrapper
import data_crawler.sources.abc.active_source_sync as active_source_sync

import sources.mockup as mockup


@pytest.fixture()
def mockup_source_config_base(appended_test_path) -> dict:
    """Returns the configuration of a simple mockup service"""

    return {
        "type": "sources.mockup.MockupSourceAPI",
        "source parameter": {
            "key": "<keep it secret>",
            "some_list": [1, 2, 4],
            "keep": "it"
        },
        "polling": {
            "frequency": "0.5s"
        },
        "some-key": "that must be ignored"
    }


def create_mockup_api(config: dict, metadata: Optional[dict] = None) -> mockup.MockupSourceAPI:
    """Instantiates the mockup API from the given config"""

    return mockup.MockupSourceAPI(config["source parameter"], metadata=metadata)


@contextlib.contextmanager
def send_shutdown(source: active_source_sync.AbstractSyncActiveSourceAPI, delay: float):
    """Triggers the termination request after the given delay in seconds"""

    def _run():
        time.sleep(delay)
        source.shutdown()

    terminator = threading.Thread(target=_run)
    terminator.start()

    yield

    terminator.join()


def test_polling_executor_life_cycle_basic(mockup_source_config_base: dict):
    """tests the basic life cycle functions of the API"""
    api = create_mockup_api(mockup_source_config_base)
    source = wrapper.SyncPollingExecutor(api, mockup_source_config_base, "<test>")

    source.start()
    source.stop()

    assert api.start_invocations == 1
    assert api.stop_invocations == 1
    assert api.fetch_invocations == 0
    assert api.history_invocations == 0


def test_polling_executor_life_cycle_default(mockup_source_config_base: dict):
    """tests the default life cycle of the API"""
    api = create_mockup_api(mockup_source_config_base)
    source = wrapper.SyncPollingExecutor(api, mockup_source_config_base, "<test>")

    source.start()
    with send_shutdown(source, 0.6):
        messages = list(source.run())
    source.stop()

    assert api.start_invocations == 1
    assert api.stop_invocations == 1
    assert 1 <= api.fetch_invocations <= 3
    assert api.history_invocations == 0

    assert len(messages) == api.fetch_invocations

    assert messages[0] == {
        "invocations": 1,
        "data": "some-test-nonsense",
        "duplicate": "api-key"
    }


def test_polling_executor_api_status(mockup_source_config_base: dict):
    """Tests the API status information returned by the polling executor"""

    api = create_mockup_api(mockup_source_config_base)
    source = wrapper.SyncPollingExecutor(api, mockup_source_config_base, "<test>")

    # Check that no spurious status information is available
    status = source.get_activity_status()
    assert status is not None
    assert status.last_wakeup is None
    assert status.last_cycle_complete is None
    assert status.max_permitted_cycle_time == datetime.timedelta(seconds=0.5)

    # Execute the standard life-cycle
    source.start()
    with send_shutdown(source, 0.6):
        start_ts = datetime.datetime.now(tz=datetime.timezone.utc)
        messages = list(source.run())
        end_ts = datetime.datetime.now(tz=datetime.timezone.utc)
    source.stop()

    assert 1 <= len(messages) <= 3

    # check the returned status timing
    status = source.get_activity_status()
    assert status is not None
    assert status.last_wakeup is not None
    assert start_ts <= status.last_wakeup <= end_ts

    assert status.last_cycle_complete is not None
    assert start_ts <= status.last_cycle_complete <= end_ts
    assert status.last_wakeup <= status.last_cycle_complete

    assert status.max_permitted_cycle_time == datetime.timedelta(seconds=0.5)


def test_polling_executor_alignment(mockup_source_config_base: dict):
    """Tests the temporal alignment of the polled data source"""

    api = create_mockup_api(mockup_source_config_base)
    source = wrapper.SyncPollingExecutor(api, mockup_source_config_base, "<test>")

    # Execute the standard life-cycle
    source.start()
    with send_shutdown(source, 1.2):
        start_ts = datetime.datetime.now(tz=datetime.timezone.utc)
        messages = list(source.run())
        end_ts = datetime.datetime.now(tz=datetime.timezone.utc)
    source.stop()

    assert 2 <= len(messages) <= 4
    assert api.fetch_invocations == len(messages)

    # Check the timing parameters
    assert api.fetch_ts is not None
    assert all(start_ts <= ts <= end_ts for ts in api.fetch_ts)
    assert (
            (api.fetch_ts[-1].microsecond < 0.1e6) or (api.fetch_ts[-1].microsecond > 0.9e6) or
            (0.4e6 < api.fetch_ts[-1].microsecond < 0.6e6)
    ), "polling alignment"


def test_polling_executor_alignment_no_force_initial(mockup_source_config_base: dict):
    """Tests the temporal alignment of the polled data source without an initial polling execution"""

    mockup_source_config_base["polling"]["frequency"] = "1s"
    mockup_source_config_base["polling"]["force initial"] = False

    api = create_mockup_api(mockup_source_config_base)
    source = wrapper.SyncPollingExecutor(api, mockup_source_config_base, "<test>")

    # Execute the standard life-cycle
    source.start()
    with send_shutdown(source, 2.1):
        start_ts = datetime.datetime.now(tz=datetime.timezone.utc)
        messages = list(source.run())
        end_ts = datetime.datetime.now(tz=datetime.timezone.utc)
    source.stop()

    assert 1 <= api.fetch_invocations <= 3
    assert api.fetch_invocations == len(messages)

    # Check the event timing and alignment
    for i, ts in enumerate(api.fetch_ts):
        assert start_ts <= ts <= end_ts, f"Sample {i} out of range: {api.fetch_ts}"
        assert 0.9e6 < ts.microsecond or ts.microsecond < 0.1e6, f"Alignment error in sample {i}: {api.fetch_ts}"


def test_polling_executor_alignment_force_initial(mockup_source_config_base: dict):
    """Tests the temporal alignment of the polled data source with an initial polling execution"""

    mockup_source_config_base["polling"]["frequency"] = "1s"
    mockup_source_config_base["polling"]["force initial"] = True

    api = create_mockup_api(mockup_source_config_base)
    source = wrapper.SyncPollingExecutor(api, mockup_source_config_base, "<test>")

    # Execute the standard life-cycle
    source.start()
    with send_shutdown(source, 2.1):
        start_ts = datetime.datetime.now(tz=datetime.timezone.utc)
        messages = list(source.run())
        end_ts = datetime.datetime.now(tz=datetime.timezone.utc)
    source.stop()

    assert 2 <= api.fetch_invocations <= 3
    assert api.fetch_invocations == len(messages)

    # Check the initial invocation
    assert start_ts <= api.fetch_ts[0] <= end_ts
    assert start_ts <= api.fetch_ts[0] <= start_ts + datetime.timedelta(seconds=0.1), "Missing initial invocation"

    # Check the remaining event timing and alignment
    for i, ts in enumerate(api.fetch_ts[1:]):
        assert start_ts <= ts <= end_ts, f"Sample {i} out of range: {api.fetch_ts}"
        assert 0.9e6 < ts.microsecond or ts.microsecond < 0.1e6, f"Alignment error in sample {i}: {api.fetch_ts}"


def test_polling_executor_timing_slots(mockup_source_config_base: dict):
    """Tests the temporal alignment of the polled data source with an initial polling execution"""

    mockup_source_config_base["polling"]["frequency"] = "1s"
    mockup_source_config_base["polling"]["slot count"] = 2
    mockup_source_config_base["polling"]["slot id"] = "1"  # The function must also support string inputs
    mockup_source_config_base["polling"]["force initial"] = False

    api = create_mockup_api(mockup_source_config_base)
    source = wrapper.SyncPollingExecutor(api, mockup_source_config_base, "<test>")

    # Execute the standard life-cycle
    source.start()
    with send_shutdown(source, 2.1):
        start_ts = datetime.datetime.now(tz=datetime.timezone.utc)
        messages = list(source.run())
        end_ts = datetime.datetime.now(tz=datetime.timezone.utc)
    source.stop()

    assert 1 <= api.fetch_invocations <= 3
    assert api.fetch_invocations == len(messages)

    # Check the event timing and alignment
    for i, ts in enumerate(api.fetch_ts):
        assert start_ts <= ts <= end_ts, f"Sample {i} out of range: {api.fetch_ts}"
        assert 0.4e6 < ts.microsecond < 0.6e6, f"Alignment error in sample {i}: {api.fetch_ts}"


def test_polling_executor_fetch_error(mockup_source_config_base: dict):
    """Tests whether temporal fetch exceptions are handled internally without propagating them to the outside"""

    mockup_source_config_base["source parameter"]["no odd invocations"] = True

    api = create_mockup_api(mockup_source_config_base)
    source = wrapper.SyncPollingExecutor(api, mockup_source_config_base, "<test>")

    # Execute the standard life-cycle
    source.start()
    with send_shutdown(source, 1.25):
        messages = list(source.run())
    source.stop()

    assert 2 <= api.fetch_invocations <= 4
    assert 1 <= len(messages) <= 3

    for message in messages:
        assert message["invocations"] % 2 == 0, "No odd invocations allowed"
        assert message["data"] == "some-test-nonsense"


def test_polling_executor_startup_error(mockup_source_config_base: dict):
    """Tests whether startup errors are appropriately re-raised"""

    mockup_source_config_base["source parameter"]["raise_on_startup"] = True
    api = create_mockup_api(mockup_source_config_base)
    source = wrapper.SyncPollingExecutor(api, mockup_source_config_base, "<test>")

    with pytest.raises(ValueError) as err_info:
        source.start()

    assert "Uups!" in str(err_info.value)


def test_polling_executor_history_relay(mockup_source_config_base: dict):
    """Tests whether the history calls are correctly relayed"""

    api = create_mockup_api(mockup_source_config_base)
    source = wrapper.SyncPollingExecutor(api, mockup_source_config_base, "<test>")
    filter_clauses = dict(start_time="2023-01-01T00:00:00+00:00", end_time="2024-01-01T00:00:00+00:00")

    # Check whether no invocation has taken place, yet
    assert api.history_invocations == 0
    assert api.fetch_invocations == 0
    assert api.start_invocations == 0
    assert api.stop_invocations == 0

    # Perform the history cycle
    source.start()
    messages = list(source.fetch_historic_data_bundle(filter_clauses))
    source.stop()

    # Check whether the expected invocations were performed
    assert api.history_invocations == 1
    assert api.fetch_invocations == 0
    assert api.start_invocations == 1
    assert api.stop_invocations == 1

    assert len(messages) == 1
    assert messages[0] == {
        "fetch_invocations": 0,
        "history_invocations": 1,
        "data": "another-test-nonsense",
        "duplicate": "same-api-key"
    }
