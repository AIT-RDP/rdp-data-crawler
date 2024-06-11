"""
Tests the synchronous active wrapper that encapsulates the polling-based API and provides an active source interface
"""
import contextlib
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
    assert 1 <= api.fetch_invocations <= 2
    assert api.history_invocations == 0

    assert len(messages) == api.fetch_invocations

    assert messages[0] == {
        "invocations": 1,
        "data": "some-test-nonsense",
        "duplicate": "api-key"
    }


