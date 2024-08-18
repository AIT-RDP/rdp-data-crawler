from typing import Optional

import pandas as pd
import pydantic
from pydantic import ConfigDict, AliasChoices, SecretStr, field_validator

from data_crawler.sinks.abc import abstract_sink


class OPCUAParameters(abstract_sink.SinkParameters):
    """OPCUA configuration parameters"""

    endpoint: str = pydantic.Field(
        pattern=r"^opc.tcp://",
        description="The endpoint of the OPCUA server",
    )

    register_spec: pd.DataFrame = pydantic.Field(
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

    model_config = ConfigDict(arbitrary_types_allowed=True)  # Allow DataFrame to be used as a type

    @field_validator("register_spec", mode="before")
    @classmethod
    def transform(cls, raw: pd.DataFrame | list[dict]) -> pd.DataFrame:
        """Transforms the raw data into a pandas DataFrame if necessary"""

        if isinstance(raw, pd.DataFrame):
            return raw
        # Uses a record in the yaml which can be created with df.to_dict(orient='records')
        return pd.DataFrame.from_records(raw)

        # This is an example list[dict] in yaml

        # register_spec: [{'address': 'ns=1;i=1', 'name': 'Status', 'data_type': 'Int', 'mode': 'r', 'description': ''},
        #   {'address': 'ns=1;i=2', 'name': 'Automatenquittierung_24V_DC', 'data_type': 'Boolean', 'mode': 'w','description': ''},
        #   {'address': 'ns=1;i=16501', 'name': 'Automatenfall_24V_DC', 'data_type': 'Boolean', 'mode': 'r', 'description': ''},
        #   {'address': 'ns=1;i=16502', 'name': 'Schluesselschalter_USV', 'data_type': 'Boolean', 'mode': 'r', 'description': nan}]
