"""
Assesses the Modbus Source API
"""
import asyncio
import datetime
import multiprocessing
import threading
import time
from typing import Tuple

import pandas as pd
import pymodbus.datastore
import pymodbus.server.async_io
import pymodbus.server
import pytest

import data_crawler.sources.modbus as modbus


@pytest.fixture()
def minimal_modbus_config(mockup_server):
    """Returns a (quite) minimal modus configuration stanza"""

    return {
        "address": mockup_server[0],
        "port": mockup_server[1],
        "register spec": pd.DataFrame.from_dict({
            "Register_start": [100, "", "", 110],
            "Register_end": ["", "", "", 111],
            "Register_type": ["i", "i", "i", "i"],
            "Data_type": ["UINT16", "DOUBLE", "FloaT", "UINT32"],
            "Name": ["current_phase_1", "some_energy", "crazy number", "frequency"],
            "Unit": ["A", "Wh", "1", "Hz"],
            "Scaling": [0.01, 1.0, 1.0, 1.0]
        })
    }


@pytest.fixture()
def mockup_server() -> Tuple[str, int]:
    """Spins up a mockup server"""

    sim_context = pymodbus.datastore.ModbusSimulatorContext()

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
            registers=[
                # Static value
                dict(registers=[100, 100], value=12345),
                # Double value Big endian: 54830306.71802119   (418A 2527 15BE 81E5)
                dict(registers=[101, 101], value=0x418A),
                dict(registers=[102, 102], value=0x2527),
                dict(registers=[103, 103], value=0x15BE),
                dict(registers=[104, 104], value=0x81E5),
                # Float value, Big endian, value 0.2, (3E4C CCCD)
                dict(registers=[105, 105], value=0x3E4C),
                dict(registers=[106, 106], value=0xCCCD),
            ]
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
    sim_context.load_dict(sim_description, None)
    context = pymodbus.datastore.ModbusServerContext(slaves=sim_context, single=True)

    address = "127.0.0.1"
    port = 5502

    # The server needs to be executed in a dedicated process since threads can't be killed and I was unable to
    # gracefully stop the server process once it is started. (The start function blocks forever.)
    server_process = multiprocessing.Process(target=pymodbus.server.StartTcpServer,
                                             kwargs=dict(context=context, address=(address, port)))
    server_process.start()

    yield address, port

    server_process.terminate()
    server_process.join()


def test_modbus_tcp_basic(minimal_modbus_config):
    """Tests the very basic operation of the modbus crawler"""

    src_api = modbus.ModbusTCP(source_parameters=minimal_modbus_config, executor_name="<test-modbus>")
    src_api.start()

    time_start = datetime.datetime.utcnow()
    data = src_api.fetch_data()
    time_end = datetime.datetime.utcnow()

    src_api.stop()

    assert "observation_time" in data
    assert time_start <= datetime.datetime.fromisoformat(data["observation_time"]) <= time_end

    assert data["frequency"] == 1234567890.0
    assert data["current_phase_1"] == 123.45
    assert data["some_energy"] == 54830306.71802119
    assert data["crazy number"] == pytest.approx(0.2)


@pytest.fixture()
def modbus_config_types(mockup_server):
    """Returns a (quite) minimal modus configuration stanza with unconventional column types"""

    return {
        "address": mockup_server[0],
        "port": mockup_server[1],
        "register spec": pd.DataFrame.from_dict({
            "Register_start": [100, None, None, 110],  # Will be translated to a float column
            "Register_type": ["i", "i", "i", "i"],
            "Data_type": ["UINT16", "DOUBLE", "FloaT", "UINT32"],
            "Name": ["current_phase_1", "some_energy", "crazy number", "frequency"],
            "Unit": ["A", "Wh", "1", "Hz"],
            "Scaling": [0.01, 1.0, 1.0, 1.0]
        })
    }


def test_modbus_tcp_unconventional_types(modbus_config_types):
    """Tests the very basic operation of the modbus crawler"""

    src_api = modbus.ModbusTCP(source_parameters=modbus_config_types, executor_name="<test-modbus>")
    src_api.start()

    time_start = datetime.datetime.utcnow()
    data = src_api.fetch_data()
    time_end = datetime.datetime.utcnow()

    src_api.stop()

    assert "observation_time" in data
    assert time_start <= datetime.datetime.fromisoformat(data["observation_time"]) <= time_end

    assert data["frequency"] == 1234567890.0
    assert data["current_phase_1"] == 123.45
    assert data["some_energy"] == 54830306.71802119
    assert data["crazy number"] == pytest.approx(0.2)


def test_modbus_tcp_invalid_register_spec(modbus_config_types):
    """tests the modbus TCP implementation with an invalid register spec"""

    modbus_config_types["register spec"] = pd.DataFrame.from_dict({
            "Register_start": ["0.1", "x", "x", "110"],  # Invalid register id
            "Register_type": ["i", "i", "i", "i"],
            "Data_type": ["UINT16", "DOUBLE", "FloaT", "UINT32"],
            "Name": ["current_phase_1", "some_energy", "crazy number", "frequency"],
            "Unit": ["A", "Wh", "1", "Hz"],
            "Scaling": [0.01, 1.0, 1.0, 1.0]
        })

    with pytest.raises(expected_exception=ValueError):
        modbus.ModbusTCP(source_parameters=modbus_config_types, executor_name="<test-modbus>")
