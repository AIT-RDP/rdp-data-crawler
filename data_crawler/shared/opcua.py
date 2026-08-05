from typing import Optional

import pandas as pd
import pydantic
from pydantic import ConfigDict, AliasChoices, SecretStr, field_validator, BaseModel
import pandera.pandas as pa
from pandera.pandas import typing as pt

from data_crawler.sinks.abc import abstract_sink

# TODO: Remove this once pandera is fixed
# Reference warning: FutureWarning:
# Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated and will change in a future version.
# Call result.infer_objects(copy=False) instead. To opt-in to the future behavior, set
# `pd.set_option('future.no_silent_downcasting', True)` check_obj[col_name] = check_obj[col_name].fillna(
pd.set_option('future.no_silent_downcasting', True)

class Datapoint(pa.DataFrameModel):
    """
    Datapoint dataframe model
    """
    address: pt.Series[str] = pa.Field(
        description="The address of the opc ua datapoint as a string e.g. 'ns=1;i=1650'.",
        coerce=True,
        unique=True,
    )
    name: pt.Series[str] = pa.Field(
        description="The name of the datapoint.",
        coerce=True,
        unique=True,
    )

    @classmethod
    def preprocess(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess the DataFrame before validation. Called before validation in Device manually.
        :param df: Original DataFrame
        :return: Modified DataFrame
        """
        # Mapping of aliases to the actual column names
        alias_map = {
            "register_start": "address",  # Allow modbus csv to also work
            # Add more aliases if needed
        }
        return df.rename(columns=alias_map)


class OPCUAEncryption(BaseModel):
    """OPCUA encryption parameters"""
    server_cert_path: str = pydantic.Field(
        description="Server certificate path"
    )

    client_cert_path: str = pydantic.Field(
        description="Client certificate path"
    )

    client_key_path: str = pydantic.Field(
        description="Client certificate key"
    )

    trusted_certs_path: str = pydantic.Field(
        description="Trusted certificates folder path. The certificates must be in the *.der format",
        default=None
    )


class OPCUAParameters(abstract_sink.SinkParameters):
    """OPCUA configuration parameters"""

    endpoint: str = pydantic.Field(
        pattern=r"^opc.tcp://",
        description="The endpoint of the OPCUA server",
    )

    uri: str = pydantic.Field(
        pattern=r"^urn:",
        description="The OPC UA Application URI is a unique identifier used within the OPC Unified Architecture",
        default=None
    )

    register_spec: pt.DataFrame[Datapoint] = pydantic.Field(
        description="The register spec of the OPCUA server",
        validation_alias=AliasChoices('register_spec', 'register spec')
    )

    user: Optional[SecretStr] = pydantic.Field(
        description="The user to connect to the OPCUA server",
        default=None,
    )

    password: Optional[SecretStr] = pydantic.Field(
        description="The password to connect to the OPCUA server",
        default=None,
    )

    encryption: Optional[OPCUAEncryption] = pydantic.Field(
        description="Parameters in case the communication is encrypted",
        default=None
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

        # This is an example list[dict] in yaml

        # register_spec: [{'address': 'ns=1;i=1', 'name': 'Status', 'data_type': 'Int', 'mode': 'r', 'description': ''},
        #   {'address': 'ns=1;i=2', 'name': 'Automatenquittierung_24V_DC', 'data_type': 'Boolean', 'mode': 'w','description': ''},
        #   {'address': 'ns=1;i=16501', 'name': 'Automatenfall_24V_DC', 'data_type': 'Boolean', 'mode': 'r', 'description': ''},
        #   {'address': 'ns=1;i=16502', 'name': 'Schluesselschalter_USV', 'data_type': 'Boolean', 'mode': 'r', 'description': nan}]
