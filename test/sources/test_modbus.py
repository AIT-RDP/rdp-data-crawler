"""
Assesses the Modbus Source API
"""
import asyncio
import threading
from typing import Tuple

import pandas as pd
import pymodbus.datastore
import pymodbus.server.async_io
import pytest

import data_crawler.sources.modbus as modbus


@pytest.fixture()
def minimal_modbus_config(mockup_server):
    """Returns a (quite) minimal modus configuration stanza"""

    return {
        "address": mockup_server[0],
        "port": mockup_server[1],
        "register spec": pd.DataFrame.from_dict({
            "Register_start": [100, 110],
            "Register_end": [100, 111],
            "Register_type": ["i", "i"],
            "Data_type": ["INT16", "SINGLE"],
            "Name": ["current_phase_1", "frequency"],
            "Unit": ["A", "Hz"],
            "Scaling": [0.01, 1]
        })
    }


@pytest.fixture()
def mockup_server() -> Tuple[str, int]:
    """Spins up a mockup server"""

    context = pymodbus.datastore.ModbusSimulatorContext()

    sim_description = dict(
        registers=200,  # The total number of registers
        invalid_address=dict(  # List of invalid addresses, Read/Write causes invalid address response.
            registers=[]
        ),
        write_allowed=dict(  # default is ReadOnly, allow write (other addresses causes invalid address response)
            registers=[]
        ),
        type_uint32=dict(
            registers=[dict(
                registers=[110, 111],  # Start, end
                value=1234567890  # static value
            )]
        ),
        type_uint16=dict(
            registers=[dict(
                registers=[100, 100],  # Start, end
                value=12345  # static value
            )]
        ),
        type_string=dict(  # Define strings, variable number of registers (2 bytes)
            registers=[]
        ),
        type_bits=dict(  # Define 16 bit registers
            registers=[]
        ),
        repeat_address=dict(  # Allows to repeat section e.g. for n devices
            registers=[]
        )
    )
    context.load_dict(sim_description, None)

    address = "127.0.0.1"
    port = 5502

    # StartTcpServer/StopServer do not properly shut down the servers. Hence, do all the work manually:
    server = pymodbus.server.async_io.ModbusTcpServer(context, address=(address, port))
    server_thread = threading.Thread(target=asyncio.run, args=(server.serve_forever(),))
    server_thread.start()

    yield address, port

    asyncio.run(server.server_close())
    server_thread.join()


def test_modbus_tcp_basic(minimal_modbus_config):
    """Tests the very basic operation of the modbus crawler"""

    src_api = modbus.ModbusTCP(source_parameters=minimal_modbus_config, executor_name="<test-modbus>")
    src_api.start()

    data = src_api.fetch_data()
    src_api.stop()

    assert data["frequency"] == 1234567890.0
    assert data["current_phase_1"] == 123.45
