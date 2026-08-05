"""
Implements the test cases for the query supervisor
"""

import asyncio
import copy
import logging
from typing import Dict, Union, List

import pyrdp_commons as commons
import pytest
import pytest_asyncio
import yaml

import data_crawler.query_supervisor as query_supervisor

import sources.mockup as mockup


@pytest.fixture()
def passive_mockup_service_config(appended_test_path):
    """Returns the configuration of a simple mockup service using the passive interface"""

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
def active_mockup_service_config(appended_test_path):
    """Returns the configuration of a simple mockup service following the active interface"""

    return {
        "type": "sources.mockup.ActiveMockupSourceAPI",
        "source parameter": {
            "sleep_time": 0.5,
            "repeat": True,
            "messages": [{}]
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
def mockup_executors_config(passive_mockup_service_config, active_mockup_service_config):
    """The mockup configuration without any dynamic elements"""
    return {
        "first": copy.deepcopy(passive_mockup_service_config),
        "second": copy.deepcopy(passive_mockup_service_config),
        "third": copy.deepcopy(active_mockup_service_config)
    }


@pytest_asyncio.fixture()
async def mockup_executors_config_container(mockup_executors_config) -> commons.ConfigDict:
    """Returns an exemplary executors configuration as a pyrdp-commons container"""
    return await commons.ConfigDict.create(mockup_executors_config)


@pytest.fixture()
def mockup_executors_externals(
        mockup_executors_config
) -> Dict[str, Union[mockup.PassiveMockupSourceAPI, mockup.ActiveMockupSourceAPI]]:
    """Instantiates the mockup executors for the collection"""
    return {
        "first": mockup.PassiveMockupSourceAPI(mockup_executors_config["first"]["source parameter"]),
        "second": mockup.PassiveMockupSourceAPI(mockup_executors_config["second"]["source parameter"]),
        "third": mockup.ActiveMockupSourceAPI.create(
            mockup.ActiveMockupSourceParameters.model_validate(mockup_executors_config["second"]["source parameter"])
        )
    }


@pytest.mark.asyncio
async def test_query_supervisor_life_cycle(mockup_executors_config_container, mockup_executors_externals, redis_pool):
    """Tests the standard life cycle of the supervisor"""

    supervisor = query_supervisor.AsyncQuerySupervisor(mockup_executors_config_container, {}, redis_pool,
                                                       mockup_executors_externals)
    await supervisor.start()

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok", "third": "ok"}

    await asyncio.sleep(0.6)
    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok", "third": "ok"}

    assert mockup_executors_externals["first"].start_invocations == 1
    assert mockup_executors_externals["first"].fetch_invocations >= 1
    assert mockup_executors_externals["first"].stop_invocations == 0

    assert mockup_executors_externals["second"].start_invocations == 1
    assert mockup_executors_externals["second"].fetch_invocations >= 1
    assert mockup_executors_externals["second"].stop_invocations == 0

    assert len(mockup_executors_externals["third"].start_exec) == 1
    assert len(mockup_executors_externals["third"].run_exec) == 1
    assert len(mockup_executors_externals["third"].run_message_exec) >= 1
    assert len(mockup_executors_externals["third"].shutdown_exec) == 0
    assert len(mockup_executors_externals["third"].stop_exec) == 0

    await supervisor.stop()
    assert mockup_executors_externals["first"].start_invocations == 1
    assert mockup_executors_externals["first"].fetch_invocations >= 1
    assert mockup_executors_externals["first"].stop_invocations == 1

    assert mockup_executors_externals["second"].start_invocations == 1
    assert mockup_executors_externals["second"].fetch_invocations >= 1
    assert mockup_executors_externals["second"].stop_invocations == 1

    assert len(mockup_executors_externals["third"].start_exec) == 1
    assert len(mockup_executors_externals["third"].run_exec) == 1
    assert len(mockup_executors_externals["third"].run_message_exec) >= 1
    assert len(mockup_executors_externals["third"].shutdown_exec) == 1
    assert len(mockup_executors_externals["third"].stop_exec) == 1


@pytest.mark.asyncio
async def test_query_supervisor_restart_passive(mockup_executors_config_container, mockup_executors_externals,
                                                redis_pool):
    """Tests the friendly restart policy of the query supervisor targeting a passive source"""

    sup_config = {"dead timeout": "0.1s"}
    supervisor = query_supervisor.AsyncQuerySupervisor(mockup_executors_config_container, sup_config,
                                                       redis_pool,
                                                       mockup_executors_externals)
    await supervisor.start()

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok", "third": "ok"}

    mockup_executors_externals["second"].enable_fetch.clear()  # Block the execution
    await asyncio.sleep(1.1)

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "stop-by-timeout", "third": "ok"}
    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "blocking", "third": "ok"}

    mockup_executors_externals["second"].enable_fetch.set()
    await asyncio.sleep(0.1)  # Give the second source some time to terminate

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "restarted", "third": "ok"}

    await asyncio.sleep(0.6)

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok", "third": "ok"}

    await supervisor.stop()


@pytest.mark.asyncio
async def test_query_supervisor_restart_active(mockup_executors_config_container, mockup_executors_externals,
                                               redis_pool):
    """Tests the friendly restart policy of the query supervisor targeting an active source"""

    # Custom configuration changes
    mockup_executors_externals["third"].config.track_activity = True

    sup_config = {"dead timeout": "0.1s"}
    supervisor = query_supervisor.AsyncQuerySupervisor(mockup_executors_config_container, sup_config,
                                                       redis_pool,
                                                       mockup_executors_externals)
    await supervisor.start()

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok", "third": "ok"}

    mockup_executors_externals["third"].enable_run.clear()  # Block the execution
    await asyncio.sleep(1.1)

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok", "third": "stop-by-timeout"}
    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok", "third": "blocking"}

    mockup_executors_externals["third"].enable_run.set()
    await asyncio.sleep(0.1)  # Give the second source some time to terminate

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok", "third": "restarted"}

    await asyncio.sleep(0.6)

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok", "third": "ok"}

    await supervisor.stop()


@pytest.mark.asyncio
async def test_query_supervisor_logging(caplog: pytest.LogCaptureFixture, mockup_executors_config_container,
                                        mockup_executors_externals,
                                        redis_pool):
    """Tests the log messages when restarting some executors"""

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


@pytest.fixture()
def exec_config_sequence() -> List[dict]:
    """Returns an iterable of different configuration variants"""

    def get_basic_source_config(variant):
        """Just defines a basic template for every source configuration"""
        return {
            "type": "sources.mockup.PassiveMockupSourceAPI",
            "source parameter": {
                "variant": variant
            },
            "polling": {
                "frequency": "0.5s"
            },
            "redis": {
                "stream": "my-stream",
            }
        }

    # Create and return the actual configuration variants
    return [
        {  # First realization of the configuration
            "first": get_basic_source_config("first"),
            "second": get_basic_source_config("second_v0"),
            "third": get_basic_source_config("third")  # Will be deleted afterwards
        },
        {
            "first": get_basic_source_config("first"),  # Stays the same
            "second": get_basic_source_config("second_v1"),  # Changes content
            "fourth": get_basic_source_config("fourth")  # Newly created
        },
    ]


@pytest.mark.asyncio
async def test_query_supervisor_reconfiguration(exec_config_sequence, redis_pool):
    """
    Tests the reconfiguration facilities of the query supervisor

    The test case will load the initial configuration and test the call status of each API. Afterwards, a
    reconfiguration is triggered having all kinds of changes. Afterwards, the call status of each API is checked again.
    """

    exec_template = mockup.StaticConfigSequenceTemplate(exec_config_sequence)
    exec_config = await commons.ConfigDict.create({yaml.Node("", "", None, None): exec_template})
    supervisor = query_supervisor.AsyncQuerySupervisor(exec_config, {}, redis_pool)

    await supervisor.start()
    await asyncio.sleep(0.8)

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok", "third": "ok"}

    # Test whether all APIs are created as expected
    api_first = supervisor.executors["first"].source_api
    assert isinstance(api_first, mockup.PassiveMockupSourceAPI)
    assert api_first.start_invocations == 1
    assert api_first.fetch_invocations >= 1
    assert api_first.stop_invocations == 0
    assert api_first.config["variant"].startswith("first")

    api_second = supervisor.executors["second"].source_api
    assert isinstance(api_second, mockup.PassiveMockupSourceAPI)
    assert api_second.start_invocations == 1
    assert api_second.fetch_invocations >= 1
    assert api_second.stop_invocations == 0
    assert api_second.config["variant"].startswith("second_v0")

    api_third = supervisor.executors["third"].source_api
    assert isinstance(api_third, mockup.PassiveMockupSourceAPI)
    assert api_third.start_invocations == 1
    assert api_third.fetch_invocations >= 1
    assert api_third.stop_invocations == 0
    assert api_third.config["variant"].startswith("third")

    # Do the configuration transition
    exec_template.event_trigger.release()
    await asyncio.sleep(0.8)

    # Test whether all APIs are recreated as expected
    assert api_first == supervisor.executors["first"].source_api  # No changes
    assert isinstance(api_first, mockup.PassiveMockupSourceAPI)
    assert api_first.start_invocations == 1
    assert api_first.fetch_invocations >= 2
    assert api_first.stop_invocations == 0
    assert api_first.config["variant"].startswith("first")

    assert api_second.start_invocations == 1
    assert api_second.fetch_invocations >= 1
    assert api_second.stop_invocations == 1  # Previous second API must be properly stopped
    assert api_second.config["variant"].startswith("second_v0")

    api_second = supervisor.executors["second"].source_api  # Fetch the new version of "second"
    assert isinstance(api_second, mockup.PassiveMockupSourceAPI)
    assert api_second.start_invocations == 1
    assert api_second.fetch_invocations >= 1
    assert api_second.stop_invocations == 0
    assert api_second.config["variant"].startswith("second_v1")

    assert "third" not in supervisor.executors  # Must be deleted entirely
    assert api_third.start_invocations == 1
    assert api_third.fetch_invocations >= 1
    assert api_third.stop_invocations == 1

    api_fourth = supervisor.executors["fourth"].source_api  # Newly created
    assert isinstance(api_fourth, mockup.PassiveMockupSourceAPI)
    assert api_fourth.start_invocations == 1
    assert api_fourth.fetch_invocations >= 1
    assert api_fourth.stop_invocations == 0
    assert api_fourth.config["variant"].startswith("fourth")

    # Check the heartbeat
    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok", "fourth": "ok"}

    # Stop the executors
    await supervisor.stop()

    # Quickly check the final state
    for ex_name in ["first", "second", "fourth"]:
        api = supervisor.executors[ex_name].source_api
        assert isinstance(api, mockup.PassiveMockupSourceAPI)
        assert api.start_invocations == 1
        assert api.fetch_invocations >= 1
        assert api.stop_invocations == 1


@pytest.mark.asyncio
async def test_query_supervisor_reconfiguration_on_startup(exec_config_sequence, redis_pool):
    """Tests the query supervisor in case a reconfiguration event is directly triggered on startup"""

    exec_template = mockup.StaticConfigSequenceTemplate(exec_config_sequence)
    exec_template.event_trigger.release()  # Directly trigger an event at startup

    exec_config = await commons.ConfigDict.create({yaml.Node("", "", None, None): exec_template})
    supervisor = query_supervisor.AsyncQuerySupervisor(exec_config, {}, redis_pool)

    await supervisor.start()
    status = await supervisor.heartbeat()  # Status may either be the old or new assignment. No state in between.
    assert (
            status == {"first": "ok", "second": "ok", "fourth": "ok"} or
            status == {"first": "ok", "second": "ok", "third": "ok"}
    )

    await asyncio.sleep(0.8)

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok", "fourth": "ok"}

    await supervisor.stop()


@pytest.mark.asyncio
async def test_query_supervisor_multiple_reconfiguration_events(exec_config_sequence, redis_pool):
    """Tests the query supervisor when receiving multiple reconfiguration events at once"""

    exec_template = mockup.StaticConfigSequenceTemplate(exec_config_sequence + exec_config_sequence)  # Double config
    exec_config = await commons.ConfigDict.create({yaml.Node("", "", None, None): exec_template})
    supervisor = query_supervisor.AsyncQuerySupervisor(exec_config, {}, redis_pool)

    await supervisor.start()
    await asyncio.sleep(0.8)

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok", "third": "ok"}

    # Trigger concurrent changes
    exec_template.event_trigger.release()
    exec_template.event_trigger.release()

    await asyncio.sleep(0.8)

    status = await supervisor.heartbeat()
    assert status == {"first": "ok", "second": "ok", "third": "ok"}

    await supervisor.stop()