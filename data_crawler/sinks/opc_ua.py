"""
Contains all OPCUA-related sink configuration directives
"""
import time
from typing import Any

from asyncua import ua
from asyncua.sync import Client
import data_crawler.sinks.abc.abstract_sink as abstract_sink
from data_crawler.shared.opcua import OPCUAParameters, Datapoint


class OPCUAMetadata(abstract_sink.SinkMetadata):
    """Specifies the supported metadata fields"""


class OPCUA(abstract_sink.AbstractSinkAPI):
    """
    Implements the OPCUA data sink using a shared connection pool to save connections
    """

    def __init__(self, opcua_config: OPCUAParameters):
        """Initializes the object"""

        self._endpoint = opcua_config.endpoint
        self._register_spec = opcua_config.register_spec
        self._client = Client(url=self._endpoint)

        if opcua_config.user is not None and opcua_config.password is not None:
            self._client.set_user(opcua_config.user.get_secret_value())
            self._client.set_password(opcua_config.password.get_secret_value())

        self._client.connect()

    def __del__(self):
        self._client.disconnect()

    @classmethod
    def create(cls, sink_parameters: OPCUAParameters, **kwargs) -> "OPCUA":
        """
        Factory function that creates a new sink

        :param sink_parameters: The configuration of the OPCUA
        :param kwargs: Any additional kwargs that will be gracefully ignored
        :return: The newly constructed data sink instance
        """

        return OPCUA(sink_parameters)

    def insert_data(self, data: dict[str, Any], metadata: OPCUAMetadata) -> None:
        """
        Pushes the data to the OPCUA stream

        :param data: The actual message data to mush to the stream
        :param metadata: Any metadata that alters the behaviour of the function
        """

        # Test if the connection is still alive
        try:
            self._client.check_connection()  # Throws a exception if connection is lost
        except (ConnectionError, ua.UaError):
            # Reconnect if the connection is lost
            time.sleep(2)  # Don't spam the server
            self._client = Client(url=self._endpoint)
            self._client.connect()

        # Get the node id of each key in data dict
        nodes = {
            self._client.get_node(
                self._register_spec.loc[self._register_spec[Datapoint.name] == key, Datapoint.address].iloc[0]
            ): data[key]
            for key in data if (self._register_spec[Datapoint.name] == key).any()}

        self._client.write_values(nodes.keys(), nodes.values())

    @staticmethod
    def parameter_model() -> type[OPCUAParameters]:
        return OPCUAParameters

    @staticmethod
    def metadata_model() -> type[OPCUAMetadata]:
        return OPCUAMetadata
