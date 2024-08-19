import pandas as pd
import pydantic
import pymodbus.constants
from pydantic import ConfigDict, AliasChoices, field_validator
import pandera as pa
from pandera import typing as pt
from typing import Optional

from modbus_crawler.input_data_validation import data_types, register_types, data_type_lookup, register_type_lookup

from data_crawler.sinks.abc import abstract_sink

# TODO: Remove this once pandera is fixed
pd.set_option('future.no_silent_downcasting', True)

class Datapoint(pa.DataFrameModel):
    """
    Datapoint dataframe model
    """
    register_start: pt.Series[str] = pa.Field(
        description="The address of the datapoint used to connect to modbus device directly.",
        coerce=True,
        unique=False,  # Because we can have NaN values
        default=None,
        nullable=True
    )

    name: pt.Series[str] = pa.Field(
        description="The name of the datapoint used to query redis and used int the graphql schema.",
        coerce=True,
        unique=True,
    )

    data_type: pt.Series[str] = pa.Field(
        description="The data type of the datapoint",
        isin=data_types,
        coerce=True,
    )

    register_type: pt.Series[str] = pa.Field(
        description="The modbus register type",
        isin=register_types,
        coerce=True,
    )

    unit: Optional[pt.Series[str]] = pa.Field(
        description="Datapoint Unit",
        coerce=True,
        default=None,
        nullable=True
    )

    scaling: Optional[pt.Series[float]] = pa.Field(
        description="The scaling of the datapoint",
        coerce=True,
        default=1,
    )

    used: Optional[pt.Series[bool]] = pa.Field(
        description="If the datapoint is used or not",
        coerce=True,
        default=True,
    )

    unit_id: Optional[pt.Series[int]] = pa.Field(
        description="The slave id of the modbus device",
        coerce=True,
        default=True,
    )

    description: Optional[pt.Series[str]] = pa.Field(
        description="Textual description of the datapoint",
        coerce=True,
        default=None,
        nullable=True
    )

    @pa.parser("register_type")
    def negate(cls, series):
        series = series.str.lower()
        # Allow alias for register type
        series = series.map(register_type_lookup)
        return series

    @pa.parser("data_type")
    def negate(cls, series):
        series = series.str.lower()
        # Allow alias for data type
        series = series.map(data_type_lookup)
        return series

    @classmethod
    def preprocess(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess the DataFrame before validation. Called before validation in Device manually.
        :param df: Original DataFrame
        :return: Modified DataFrame
        """
        df.columns = df.columns.str.lower()

        # Mapping of aliases to the actual column names
        alias_map = {
            "address": "register_start",  # Allow address to also work

            # Backward compatibility -> we should keep to the ones with underline but existing
            # configs may use this
            "registerstart": "register_start",
            "datatype": "data_type",
            "registertype": "register_type",
            "unitid": "unit_id",
        }
        return df.rename(columns=alias_map)


class ModbusParameters(abstract_sink.SinkParameters):
    """Modbus configuration parameters"""

    address: str = pydantic.Field(
        description="The address of the Modbus server",
    )

    port: int = pydantic.Field(
        description="The port of the Modbus server",
        default=502,
    )

    register_spec: pt.DataFrame[Datapoint] = pydantic.Field(
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
            df = raw
        else:
            # Uses a record in the yaml which can be created with df.to_dict(orient='records')
            df = pd.DataFrame.from_records(raw)

        # Preprocess the DataFrame before validation
        df = Datapoint.preprocess(df)

        return df
