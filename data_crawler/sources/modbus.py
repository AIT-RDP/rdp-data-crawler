"""
Accesses Christian's famous one and only Modbus Crawler to pick up Modbus values.
"""
from typing import Dict, Any

import modbus_crawler
import pandas as pd
import pymodbus.constants

import data_crawler.sources.abc.abstract_source as abstract_source


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

        register_spec = source_parameters["register spec"]
        if not isinstance(register_spec, pd.DataFrame):
            raise ValueError(f"The modbus register spec '{register_spec}' at {executor_name} is not a valid table "
                             "(DataFrame). Please consider the appropriate YAML tags to input one.")

        self._device = modbus_crawler.ModbusTcpDevice(
            ip_address=source_parameters["address"],
            modbus_port=source_parameters.get("port", 502),
            byteorder=self._get_endian_config(source_parameters.get("byte order", "Big")),
            wordorder=self._get_endian_config(source_parameters.get("word order", "Big")),
            auto_connect=False, registers_spec_df=register_spec
        )

    @staticmethod
    def _get_endian_config(config_value: str) -> str:
        """Resolves the Endian value"""

        if not hasattr(pymodbus.constants.Endian, config_value):
            raise ValueError(f"Unknown endian value '{config_value}'")
        return getattr(pymodbus.constants.Endian, config_value)

    def fetch_data(self) -> Dict[str, Any]:
        """
        Queries the Modbus TCP device and returns the corresponding message
        """

        return self._device.read_registers_as_dict()
