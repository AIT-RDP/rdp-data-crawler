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

from asyncua import Server
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
