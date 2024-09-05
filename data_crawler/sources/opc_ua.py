import datetime
import time
from typing import Dict, Any
from pathlib import Path

from asyncua import ua
from asyncua.sync import Client
from asyncua.crypto.security_policies import SecurityPolicyBasic256Sha256
from asyncua.crypto.validator import CertificateValidator, CertificateValidatorOptions
from asyncua.crypto.truststore import TrustStore

import data_crawler.sources.abc.abstract_source as abstract_source
from data_crawler.shared.opcua import OPCUAParameters, Datapoint


class OPCUA(abstract_source.AbstractSourceAPI):
    """Accesses OPC UA devices using a pre-configured register tables"""

    def __init__(self, source_parameters, executor_name, **kwargs):
        """
        Initializes an unconnected OPC UA object

        :param source_parameters: The source parameters according to the configuration
        :param executor_name: The name of the executor for debugging purpose
        :param kwargs: Any extra arguments that will be sent to the super class
        """

        super(OPCUA, self).__init__(source_parameters=source_parameters, executor_name=executor_name, **kwargs)

        # Validate model which is not validated in AbstractSourceAPI (only in AbstractSyncActiveSourceAPI)
        self._source_parameters = OPCUAParameters.model_validate(source_parameters)

        self.create_client(self._source_parameters)

    def create_client(self, source_parameters: OPCUAParameters):
        self._client = Client(url=self._source_parameters.endpoint)

        # Set application URI
        if source_parameters.uri:
            self._client.application_uri = source_parameters.uri

        # Add encryption
        if source_parameters.encryption:
            # Set security mode and policy
            self._client.set_security(
                policy=SecurityPolicyBasic256Sha256,
                certificate=source_parameters.encryption.client_cert_path,
                private_key=source_parameters.encryption.client_key_path,
                server_certificate=source_parameters.encryption.server_cert_path
            )

            # Add trusted store
            if source_parameters.encryption.trusted_certs_path:
                trust_store = TrustStore([Path(source_parameters.encryption.trusted_certs_path)], [])
                trust_store.load()
                validator = CertificateValidator(
                    CertificateValidatorOptions.TRUSTED_VALIDATION | CertificateValidatorOptions.PEER_SERVER,
                    trust_store)
            else:
                validator = CertificateValidator(
                    CertificateValidatorOptions.EXT_VALIDATION | CertificateValidatorOptions.PEER_SERVER)

            self._client.certificate_validator = validator

        if source_parameters.user is not None and source_parameters.password is not None:
            self._client.set_user(source_parameters.user.get_secret_value())
            self._client.set_password(source_parameters.password.get_secret_value())

    def start(self):
        self._client.connect()

    def stop(self):
        self._client.disconnect()

    def fetch_data(self) -> Dict[str, Any]:
        """
        Queries the OPC UA device and returns the corresponding message
        """

        # Test if the connection is still alive
        try:
            # Throws an exception if connection is lost
            self._client.check_connection()
        except (ConnectionError, ua.UaError):
            # Reconnect if the connection is lost
            time.sleep(2)  # Don't spam the server
            self.create_client(self._source_parameters)
            self._client.connect()

        nodes = [self._client.get_node(node_id) for node_id in self._source_parameters.register_spec[Datapoint.address]]
        values = self._client.read_values(nodes)

        # Convert the values to a dictionary with the node_id as key
        values = {node_id: value for node_id, value in zip(self._source_parameters.register_spec[Datapoint.name], values)}

        out_info: Dict[str, Any] = {"observation_time": datetime.datetime.now(tz=datetime.timezone.utc).isoformat()}
        out_info.update(values)

        return out_info
