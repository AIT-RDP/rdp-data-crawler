import pandas
import pandas as pd
import pydantic
import pymodbus.constants
from pydantic import ConfigDict, AliasChoices, field_validator

from data_crawler.sinks.abc import abstract_sink


class ModbusParameters(abstract_sink.SinkParameters):
    """Modbus configuration parameters"""

    address: str = pydantic.Field(
        description="The address of the Modbus server",
    )

    port: int = pydantic.Field(
        description="The port of the Modbus server",
        default=502,
    )

    register_spec: pd.DataFrame = pydantic.Field(
        description="The register spec of the Modbus server",
        validation_alias=AliasChoices('register_spec', 'register spec')
    )

    byte_order: pymodbus.constants.Endian = pydantic.Field(
        description="The byte order of the Modbus server",
        default=pymodbus.constants.Endian.BIG,
        validation_alias=AliasChoices('byte_order', 'byte order')
    )

    word_order: pymodbus.constants.Endian = pydantic.Field(
        description="The word order of the Modbus server",
        default=pymodbus.constants.Endian.BIG,
        validation_alias=AliasChoices('word_order', 'word order')
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)  # Allow DataFrame to be used as a type

    @field_validator("register_spec", mode="before")
    @classmethod
    def transform(cls, raw: pd.DataFrame | list[dict]) -> pd.DataFrame:
        """Transforms the raw data into a pandas DataFrame if necessary"""

        if isinstance(raw, pd.DataFrame):
            return raw
        # Uses a record in the yaml which can be created with df.to_dict(orient='records')
        return pandas.DataFrame.from_records(raw)
