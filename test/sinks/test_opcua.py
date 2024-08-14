"""
Assesses the OPCUA Sink API
"""
import asyncio
import multiprocessing
import socket
import time

import pandas as pd
import pytest
import tenacity
from asyncua.server.user_managers import UserManager
from asyncua.server.users import User, UserRole

import data_crawler.sinks.opc_ua as opc_ua

from asyncua import Server
from asyncua.ua.uatypes import NodeId

from data_crawler.shared.opcua import OPCUAParameters


@pytest.fixture()
def pass_test_vars():
    return (multiprocessing.Value('i', 0),
            multiprocessing.Value('i', 0),
            multiprocessing.Value('i', 0),
            multiprocessing.Value('i', 0))


@pytest.fixture()
def minimal_opcua_config(opcua_server):
    """Returns a minimal opcua configuration"""

    return {
        "endpoint": "opc.tcp://127.0.0.1:5151/test_server/",
        "register_spec": pd.DataFrame.from_dict({
            "address": ["ns=0;i=10000", "ns=0;i=10001", "ns=0;i=10002", "ns=11;i=20"],
            "name": ["Schalter_1", "Schalter_2", "Schalter_3", "Schalter_4"],
        }),
        "user": "test_user",
        "password": "test_password"
    }


class UserManager:
    def get_user(self, iserver, username=None, password=None, certificate=None):
        if username == "test_user" and password == "test_password":
            return User(role=UserRole.User)

        return None


async def _mockup_server(pass_test_1, pass_test_2, pass_test_3, pass_test_4):
    server = Server(user_manager=UserManager())
    await server.init()
    server.set_endpoint("opc.tcp://0.0.0.0:5151/test_server/")

    schalter_1 = await server.nodes.objects.add_variable(NodeId.from_string("ns=0;i=10000"), "Schalter_1", 1234.567)
    await schalter_1.set_writable()
    schalter_2 = await server.nodes.objects.add_variable(NodeId.from_string("ns=0;i=10001"), "Schalter_2",
                                                         "test_schalter_2")
    await schalter_2.set_writable()
    schalter_3 = await server.nodes.objects.add_variable(NodeId.from_string("ns=0;i=10002"), "Schalter_3", True)
    await schalter_3.set_writable()
    schalter_4 = await server.nodes.objects.add_variable(NodeId.from_string("ns=11;i=20"), "Schalter_4", 1)
    await schalter_4.set_writable()

    async with server:
        while True:
            await asyncio.sleep(1)

            if await server.get_node("ns=0;i=10000").get_value() == 2213.0002:
                pass_test_1.value = 1
            if await server.get_node("ns=0;i=10001").get_value() == "Hello":
                pass_test_2.value = 1
            if await server.get_node("ns=0;i=10002").get_value() is True:
                pass_test_3.value = 1
            if await server.get_node("ns=11;i=20").get_value() == 800:
                pass_test_4.value = 1


def _run_server(pass_test_1, pass_test_2, pass_test_3, pass_test_4):
    asyncio.run(_mockup_server(pass_test_1, pass_test_2, pass_test_3, pass_test_4))


@pytest.fixture()
def opcua_server(pass_test_vars):
    server_process = multiprocessing.Process(target=_run_server, args=(
        pass_test_vars[0], pass_test_vars[1], pass_test_vars[2], pass_test_vars[3]))
    server_process.start()

    @tenacity.retry(stop=tenacity.stop_after_delay(10))
    def test_if_server_on():
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 5151))

    test_if_server_on()

    yield server_process

    server_process.terminate()
    server_process.join()


def test_opcua_sink(minimal_opcua_config, pass_test_vars):
    """Tests the very basic operation of the opcua crawler"""

    validated_source = OPCUAParameters.validate(minimal_opcua_config)
    src_api = opc_ua.OPCUA.create(sink_parameters=validated_source)

    src_api.insert_data(data=dict(
        {
            "Schalter_1": 2213.0002,
            "Schalter_2": "Hello",
            # Schalter_3 should stay the same
            "Schalter_4": 800
        }
    ), metadata=opc_ua.OPCUAMetadata())

    time.sleep(2)  # Wait for the server to update the values

    assert bool(pass_test_vars[0].value) is True
    assert bool(pass_test_vars[1].value) is True
    assert bool(pass_test_vars[2].value) is True
    assert bool(pass_test_vars[3].value) is True
