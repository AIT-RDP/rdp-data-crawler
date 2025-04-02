"""
Accesses Christian's famous one and only Modbus Crawler to pick up Modbus values.
"""
import datetime
from typing import Dict, Any

import modbus_crawler.modbus_device_tcp

import data_crawler.sources.abc.abstract_source as abstract_source
from data_crawler.shared.modbus import ModbusParameters


class ModbusTCP(abstract_source.AbstractSourceAPI):
    """Accesses Modbus TCP devices using a pre-configured register tables"""

    def __init__(self, source_parameters, executor_name, **kwargs):
        """
        Initializes an unconnected Modbus TCP object

        :param source_parameters: The source parameters according to the configuration
        :param executor_name: The name of the executor for debugging purpose
        :param kwargs: Any extra arguments that will be sent to the super class
        """

        super(ModbusTCP, self).__init__(source_parameters=source_parameters, executor_name=executor_name, **kwargs)

        source_parameters_model = ModbusParameters.model_validate(source_parameters)

        self._device = modbus_crawler.modbus_device_tcp.ModbusTcpDevice(
            ip_address=source_parameters_model.address,
            modbus_port=source_parameters_model.port,
            byteorder=source_parameters_model.byte_order,
            wordorder=source_parameters_model.word_order,
            auto_connect=False, registers_spec_df=source_parameters_model.register_spec
        )

        self._transactional_connection = source_parameters_model.transactional_connection

    def start(self):
        """Connects to the Modbus server"""

        # If the connection is transactional, we do not want
        # to connect here but in the fetch_data method
        if not self._transactional_connection:
            self._device.connect()

    def fetch_data(self) -> Dict[str, Any]:
        """
        Queries the Modbus TCP device and returns the corresponding message
        """

        if self._transactional_connection:
            self._device.connect()

        out_info: Dict[str, Any] = {"observation_time": datetime.datetime.now(tz=datetime.timezone.utc).isoformat()}
        out_info.update(self._device.read_registers_as_dict())

        if self._transactional_connection:
            self._device.disconnect()

        return out_info

    def stop(self):
        """Disconnects from the Modbus server"""

        # Disconnect anyway
        self._device.disconnect()
