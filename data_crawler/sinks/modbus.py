"""
Contains all Modbus-related sink configuration directives
"""
from typing import Any

import data_crawler.sinks.abc.abstract_sink as abstract_sink
from data_crawler.shared.modbus import ModbusParameters
import modbus_crawler.modbus_device_tcp


class ModbusMetadata(abstract_sink.SinkMetadata):
    """Specifies the supported metadata fields"""


class ModbusTCP(abstract_sink.AbstractSinkAPI):
    """
    Implements the Modbus data sink
    """

    def __init__(self, modbus_config: ModbusParameters):
        """Initializes the object"""

        self._device = modbus_crawler.modbus_device_tcp.ModbusTcpDevice(
            ip_address=modbus_config.address,
            modbus_port=modbus_config.port,
            byteorder=modbus_config.byte_order,
            wordorder=modbus_config.word_order,
            auto_connect=False, registers_spec_df=modbus_config.register_spec
        )

        self._device.connect()

    def __del__(self):
        self._device.disconnect()

    @classmethod
    def create(cls, sink_parameters: ModbusParameters, **kwargs) -> abstract_sink.AbstractSinkAPI:
        """
        Factory function that creates a new sink

        :param sink_parameters: The configuration of the Modbus
        :param kwargs: Any additional kwargs that will be gracefully ignored
        :return: The newly constructed data sink instance
        """

        return ModbusTCP(sink_parameters)

    def insert_data(self, data: dict[str, Any], metadata: ModbusMetadata) -> None:
        """
        Pushes the data to the Modbus client

        :param data: The actual message data to push to the client
        :param metadata: Any metadata that alters the behaviour of the function
        """

        for key, value in data.items():
            # Modbus checks if the key is in the register table
            self._device.write_register(key, value)

    @staticmethod
    def parameter_model() -> type[ModbusParameters]:
        return ModbusParameters

    @staticmethod
    def metadata_model() -> type[ModbusMetadata]:
        return ModbusMetadata
