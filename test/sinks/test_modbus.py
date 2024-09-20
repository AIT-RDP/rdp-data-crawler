"""
Assesses the Modbus Sink API
"""
import datetime
import multiprocessing
from typing import Tuple

import pandas as pd
import pymodbus.datastore
import pymodbus.server.async_io
import pymodbus.server
import pytest

import data_crawler.sinks.modbus as modbus_sink
import data_crawler.sources.modbus as modbus_source

from data_crawler.shared.modbus import ModbusParameters


@pytest.fixture()
def mockup_server() -> Tuple[str, int]:
    """Spins up a mockup server"""

    sim_description = dict(
        setup={
            "di size": 0,  # Size of discrete input block (8 bit)
            "co size": 0,  # Size of coils block (8 bit)
            "ir size": 0,  # Size of input registers block (16 bit)
            "hr size": 200,  # Size of holding registers block (16 bit)
            "shared blocks": True,  # share memory for all blocks (largest size wins)
            "defaults": {
                "value": {  # Initial values(can be overwritten)
                    "bits": 0,
                    "uint16": 0,
                    "uint32": 0,
                    "float32": 0,
                    "string": " ",
                },
                "action": {  # default action(can be overwritten)
                    "bits": None,
                    "uint16": None,
                    "uint32": None,
                    "float32": None,
                    "string": None,
                },
            },
            "type exception": False,  # Return IO exception if read / write on non boundary
        },
        invalid=[],  # List of invalid addresses or ranges, Read/Write causes invalid address response.
        write=[
            [100, 106],  # --> start, end
            [110, 111]
        ],  # default is ReadOnly, allow write (other addresses causes invalid address response)
        bits=[],  # Define bits (1 register == 1 byte)
        uint16=[  # Define uint16 (1 register == 2 bytes)
            # Static value
            dict(addr=[100, 100], value=0),
            # Double value Big endian: 54830306.71802119   (418A 2527 15BE 81E5)
            dict(addr=[101, 101], value=0),
            dict(addr=[102, 102], value=0),
            dict(addr=[103, 103], value=0),
            dict(addr=[104, 104], value=0),
            # Float value, Big endian, value 0.2, (3E4C CCCD)
            dict(addr=[105, 105], value=0),
            dict(addr=[106, 106], value=0),
        ],
        uint32=[
            dict(
                addr=[110, 111],  # Start, end
                value=1234567890  # static value
            )
        ],
        float32=[],  # Define 32 bit floats (2 registers == 4 bytes)
        string=[],  # Define strings (variable number of registers (each 2 bytes))
        repeat=[]  # allows to repeat section e.g. for n devices
    )
    sim_context = pymodbus.datastore.ModbusSimulatorContext(sim_description, {})
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


def test_modbus_write_read(modbus_config_types):
    """Write and then read from modbus"""

    # Write
    modbus_config_model = ModbusParameters.model_validate(modbus_config_types)
    src_api = modbus_sink.ModbusTCP(modbus_config=modbus_config_model)

    src_api.insert_data({"current_phase_1": 123.45, "some_energy": 54830306.71802119, "crazy number": 0.2,
                         "frequency": 1234567890.0},
                        None)

    # Read
    src_api = modbus_source.ModbusTCP(source_parameters=modbus_config_types, executor_name="<test-modbus>")
    src_api.start()

    time_start = datetime.datetime.now(tz=datetime.timezone.utc)
    data = src_api.fetch_data()
    time_end = datetime.datetime.now(tz=datetime.timezone.utc)

    src_api.stop()

    assert "observation_time" in data
    assert time_start <= datetime.datetime.fromisoformat(data["observation_time"]) <= time_end

    assert data["current_phase_1"] == 123.45
    assert data["some_energy"] == 54830306.71802119
    assert data["crazy number"] == pytest.approx(0.2)
    assert data["frequency"] == 1234567890.0
