"""
Assesses the OPCUA Source API
"""
import asyncio
import datetime
import multiprocessing
import socket

import pandas as pd
import pytest
import tenacity

import data_crawler.sources.opc_ua as opc_ua

from asyncua import Server, ua
from asyncua.ua.uatypes import NodeId


@pytest.fixture()
def minimal_opcua_config(opcua_server):
    """Returns a minimal opcua configuration"""

    return {
        "endpoint": "opc.tcp://127.0.0.1:5151/test_server/",
        "register_spec": pd.DataFrame.from_dict({
            "address": ["ns=0;i=10000", "ns=0;i=10001", "ns=0;i=10002", "ns=11;i=20"],
            "name": ["Schalter_1", "Schalter_2", "Schalter_3", "Schalter_4"],
        })
    }


async def _mockup_server():
    server = Server()
    await server.init()
    server.set_endpoint("opc.tcp://0.0.0.0:5151/test_server/")

    await server.nodes.objects.add_variable(NodeId.from_string("ns=0;i=10000"), "Schalter_1", 1234.567)
    await server.nodes.objects.add_variable(NodeId.from_string("ns=0;i=10001"), "Schalter_2", "test_schalter_2")
    await server.nodes.objects.add_variable(NodeId.from_string("ns=0;i=10002"), "Schalter_3", True)
    await server.nodes.objects.add_variable(NodeId.from_string("ns=11;i=20"), "Schalter_4", 1)

    async with server:
        while True:
            await asyncio.sleep(1)


def _run_server():
    asyncio.run(_mockup_server())


@pytest.fixture()
def opcua_server():
    server_process = multiprocessing.Process(target=_run_server)
    server_process.start()

    @tenacity.retry(stop=tenacity.stop_after_delay(10))
    def test_if_server_on():
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 5151))

    test_if_server_on()

    yield server_process

    server_process.terminate()
    server_process.join()


def test_opcua_source(minimal_opcua_config):
    """Tests the very basic operation of the opcua crawler"""

    src_api = opc_ua.OPCUA(source_parameters=minimal_opcua_config, executor_name="<test-opcua>")

    try:
        src_api.start()

        time_start = datetime.datetime.now(tz=datetime.timezone.utc)
        data = src_api.fetch_data()
        time_end = datetime.datetime.now(tz=datetime.timezone.utc)
    finally:
        src_api.stop()

    assert "observation_time" in data
    assert time_start <= datetime.datetime.fromisoformat(data["observation_time"]) <= time_end

    assert data["Schalter_1"] == 1234.567
    assert data["Schalter_2"] == "test_schalter_2"
    assert data["Schalter_3"] is True
    assert data["Schalter_4"] == 1


@pytest.mark.parametrize("connection_error", [
    asyncio.TimeoutError(),  # Reported by a dead watchdog or channel renewal task of the client
    ConnectionError("connection reset"),
    ua.UaError("some protocol error"),
])
def test_opcua_source_reconnect(minimal_opcua_config, monkeypatch, connection_error):
    """Tests whether a broken connection is transparently re-established before the data is fetched

    Once one of the background tasks of the client has died, check_connection() keeps re-raising the stored exception
    on every single call. Hence, the source has to replace the entire client instead of reusing the broken one.
    """

    src_api = opc_ua.OPCUA(source_parameters=minimal_opcua_config, executor_name="<test-opcua-reconnect>")

    try:
        src_api.start()

        stale_client = src_api._client
        disconnect_invocations = []

        # Simulate the permanently broken connection check of a client whose background task has died
        def _failing_check_connection():
            raise connection_error

        # Record the teardown of the stale client. It needs to be released to avoid leaking its socket and its
        # background thread loop.
        stale_disconnect = stale_client.disconnect

        def _recording_disconnect():
            disconnect_invocations.append(True)
            stale_disconnect()

        monkeypatch.setattr(stale_client, "check_connection", _failing_check_connection)
        monkeypatch.setattr(stale_client, "disconnect", _recording_disconnect)

        data = src_api.fetch_data()

        assert src_api._client is not stale_client, "The broken client must be replaced by a fresh one"
        assert disconnect_invocations == [True], "The stale client must be disconnected exactly once"
    finally:
        src_api.stop()

    # The sample must be fetched via the re-established connection
    assert "observation_time" in data
    assert data["Schalter_1"] == 1234.567
    assert data["Schalter_2"] == "test_schalter_2"
    assert data["Schalter_3"] is True
    assert data["Schalter_4"] == 1
