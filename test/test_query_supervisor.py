"""
Implements the test cases for the query supervisor
"""

import asyncio
import copy
import logging
from typing import Dict

import pyrdp_commons as commons
import pytest

import data_crawler.query_supervisor as query_supervisor

import sources.mockup as mockup


@pytest.fixture()
def static_mockup_service_config(appended_test_path):
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
def mockup_executors_config(static_mockup_service_config):
    """The mockup configuration without any dynamic elements"""
    return {
        "first": copy.deepcopy(static_mockup_service_config),
        "second": copy.deepcopy(static_mockup_service_config)
    }


@pytest.fixture()
async def mockup_executors_config_container(mockup_executors_config) -> commons.ConfigDict:
    """Returns an exemplary executors configuration as a pyrdp-commons container"""
    return await commons.ConfigDict.create(mockup_executors_config)


@pytest.fixture()
def mockup_executors_externals(mockup_executors_config) -> Dict[str, mockup.PassiveMockupSourceAPI]:
    """Instantiates the mockup executors for the collection"""
    return {
        key: mockup.PassiveMockupSourceAPI(config) for key, config in mockup_executors_config.items()
    }


@pytest.mark.asyncio
async def test_query_supervisor_life_cycle(mockup_executors_config_container, mockup_executors_externals, redis_pool):
    """Tests the standard life cycle of the supervisor"""
    mockup_executors_config_container = await mockup_executors_config_container

    supervisor = query_supervisor.AsyncQuerySupervisor(mockup_executors_config_container, {}, redis_pool,
                                                       mockup_executors_externals)
    await supervisor.start()

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok"}

    await asyncio.sleep(0.6)
    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok"}

    assert mockup_executors_externals["first"].start_invocations == 1
    assert mockup_executors_externals["first"].fetch_invocations >= 1
    assert mockup_executors_externals["first"].stop_invocations == 0

    assert mockup_executors_externals["second"].start_invocations == 1
    assert mockup_executors_externals["second"].fetch_invocations >= 1
    assert mockup_executors_externals["second"].stop_invocations == 0

    await supervisor.stop()
    assert mockup_executors_externals["first"].start_invocations == 1
    assert mockup_executors_externals["first"].fetch_invocations >= 1
    assert mockup_executors_externals["first"].stop_invocations == 1

    assert mockup_executors_externals["second"].start_invocations == 1
    assert mockup_executors_externals["second"].fetch_invocations >= 1
    assert mockup_executors_externals["second"].stop_invocations == 1


@pytest.mark.asyncio
async def test_query_supervisor_restart(mockup_executors_config_container, mockup_executors_externals, redis_pool):
    """Tests the friendly restart policy of the query supervisor"""
    mockup_executors_config_container = await mockup_executors_config_container

    sup_config = {"dead timeout": "0.1s"}
    supervisor = query_supervisor.AsyncQuerySupervisor(mockup_executors_config_container, sup_config,
                                                       redis_pool,
                                                       mockup_executors_externals)
    await supervisor.start()

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok"}

    mockup_executors_externals["second"].enable_fetch.clear()  # Block the execution
    await asyncio.sleep(1.1)

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "stop-by-timeout"}
    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "blocking"}

    mockup_executors_externals["second"].enable_fetch.set()
    await asyncio.sleep(0.1)  # Give the second source some time to terminate

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "restarted"}

    await asyncio.sleep(0.6)

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok"}

    await supervisor.stop()


@pytest.mark.asyncio
async def test_query_supervisor_logging(caplog: pytest.LogCaptureFixture, mockup_executors_config_container,
                                        mockup_executors_externals,
                                        redis_pool):
    """Tests the log messages when restarting some executors"""
    mockup_executors_config_container = await mockup_executors_config_container

    caplog.set_level(logging.DEBUG, logger="data_crawler")
    sup_config = {"dead timeout": "0.1s"}
    supervisor = query_supervisor.AsyncQuerySupervisor(mockup_executors_config_container, sup_config, redis_pool,
                                                       mockup_executors_externals)
    await supervisor.start()
    await supervisor.heartbeat()

    mockup_executors_externals["second"].enable_fetch.clear()  # Block the execution
    await asyncio.sleep(1.1)

    await supervisor.heartbeat()  # second: "stop-by-timeout"
    assert "The source second does not complete its cycle in time" in caplog.text

    await supervisor.heartbeat()  # second: "blocking"
    assert "Source second reached a timeout and is marked for restart" in caplog.text

    mockup_executors_externals["second"].enable_fetch.set()
    await asyncio.sleep(0.1)  # Give the second source some time to terminate

    await supervisor.heartbeat()  # second: "restarted"
    assert "Restart the executor" in caplog.text

    await supervisor.heartbeat()
    await supervisor.stop()
