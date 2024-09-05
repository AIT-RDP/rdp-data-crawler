"""
Assesses the OPCUA Source API
"""
import asyncio
import datetime
import multiprocessing
import socket
import shutil

import pandas as pd
from pathlib import Path
import pytest
import tenacity

import data_crawler.sources.opc_ua as opc_ua

from typing import List, Dict

from asyncua import Server, ua
from asyncua.crypto.permission_rules import SimpleRoleRuleset
from asyncua.crypto.validator import CertificateValidator, CertificateValidatorOptions
from asyncua.ua.uatypes import NodeId

from cryptography import x509
from cryptography.hazmat.primitives.serialization import Encoding  # , load_pem_private_key
from cryptography.x509.oid import ExtendedKeyUsageOID
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from asyncua.crypto.uacrypto import load_certificate,  load_private_key

from asyncua.crypto.cert_gen import generate_private_key, generate_self_signed_app_certificate, dump_private_key_as_pem, generate_app_certificate_signing_request, sign_certificate_request
from asyncua.server.users import User, UserRole


@pytest.fixture()
def opcua_config_with_encryption(opcua_server):
    """Returns a opcua configuration with encryption and generates certificates"""

    return {
        "endpoint": "opc.tcp://localhost:4840/opcua/server/",
        "register_spec": pd.DataFrame.from_dict({
            "address": ["ns=2;i=2003"],
            "name": ["Var1"],
        }),
        "uri": "urn:localhost:freeopcua:python:client",
        "user": "test_user",
        "password": "test_password",
        "encryption": {
            "server_cert_path": "certificates/certs/self_signed_local_server_cert.der",
            "client_cert_path": "certificates/certs/self_signed_local_client_cert.der",
            "client_key_path": "certificates/private/local_client_key.pem",
            "trusted_certs_path": "certificates/certs/"}
    }


def _generate_private_key(key_file: Path):
    key: RSAPrivateKey = generate_private_key()
    key_file.write_bytes(dump_private_key_as_pem(key))


async def _generate_self_signed_certificate(hostname: str,
                                            names: Dict[str, str],
                                            subject_alt_names: List[x509.GeneralName],
                                            cert_key: Path,
                                            cert_file: Path):

    # key: RSAPrivateKey = generate_private_key()
    key = await load_private_key(cert_key)

    client_server_use = [ExtendedKeyUsageOID.CLIENT_AUTH, ExtendedKeyUsageOID.SERVER_AUTH]
    cert: x509.Certificate = generate_self_signed_app_certificate(key,
                                                                  hostname,
                                                                  names,
                                                                  subject_alt_names,
                                                                  extended=client_server_use)

    cert_file.write_bytes(cert.public_bytes(encoding=Encoding.DER))


class UserManager:
    """
    Handler used to validate the username and password
    """
    def get_user(self, iserver, username=None, password=None, certificate=None):

        if username == "test_user" and password == "test_password":
            return User(role=UserRole.Admin, name="test_user")
        return None


async def _mockup_server():
    server = Server(user_manager=UserManager())
    server.set_server_name("TestServer")
    server.set_endpoint("opc.tcp://localhost:4840/opcua/server/")

    await server.init()

    server.set_security_policy(security_policy=[ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt],
                               permission_ruleset=SimpleRoleRuleset())

    # load server certificate and private key. This enables endpoints with signing and encryption.
    await server.load_certificate("certificates/certs/self_signed_local_server_cert.der")
    await server.load_private_key("certificates/private/local_server_key.pem")

    # Set certificate validator
    validator = CertificateValidator(
        options=CertificateValidatorOptions.EXT_VALIDATION | CertificateValidatorOptions.PEER_CLIENT)
    server.set_certificate_validator(validator)

    # Register namespace
    await server.register_namespace("localhost@opcua:server")

    await server.nodes.objects.add_variable(NodeId.from_string("ns=2;i=2003"), "Var1", 9.99)

    async with server:
        while True:
            await asyncio.sleep(1)


def _run_server():
    asyncio.run(_mockup_server())


@pytest.fixture()
def opcua_server():

    # Create certificates

    # setup the paths for the certs, keys and csr
    base = Path('certificates')
    base_private: Path = base / 'private'
    base_certs: Path = base / 'certs'
    base_private.mkdir(parents=True, exist_ok=True)
    base_certs.mkdir(parents=True, exist_ok=True)

    # hostname: str = socket.gethostname()
    hostname: str = "localhost"
    names: Dict[str, str] = {
        'countryName': 'AT',
        'stateOrProvinceName': 'Vienna',
        'localityName': 'Vienna',
        'organizationName': "AIT",
    }

    _generate_private_key(base_private / "local_server_key.pem")
    _generate_private_key(base_private / "local_client_key.pem")

    # Generate server self-signed cert
    subject_alt_names: List[x509.GeneralName] = [x509.UniformResourceIdentifier(f"urn:freeopcua:python:server"),
                                                 x509.DNSName(f"{hostname}")]
    cert_key = base_private / "local_server_key.pem"
    cert_file = base_certs / "self_signed_local_server_cert.der"

    asyncio.run(_generate_self_signed_certificate(hostname="localhost",
                                                  names=names,
                                                  subject_alt_names=subject_alt_names,
                                                  cert_key=cert_key,
                                                  cert_file=cert_file))

    # Generate client self-signed cert
    subject_alt_names: List[x509.GeneralName] = [x509.UniformResourceIdentifier(f"urn:{hostname}:freeopcua:python:client"),
                                                 x509.DNSName(f"{hostname}")]
    cert_key = base_private / "local_client_key.pem"
    cert_file = base_certs / "self_signed_local_client_cert.der"
    asyncio.run(_generate_self_signed_certificate(hostname=hostname,
                                                  names=names,
                                                  subject_alt_names=subject_alt_names,
                                                  cert_key=cert_key,
                                                  cert_file=cert_file))

    server_process = multiprocessing.Process(target=_run_server)
    server_process.start()

    @tenacity.retry(stop=tenacity.stop_after_delay(10))
    def test_if_server_on():
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 4840))

    test_if_server_on()

    yield server_process

    server_process.terminate()
    server_process.join()
    shutil.rmtree("certificates")


def test_opcua_source(opcua_config_with_encryption):
    """Tests the very basic operation of the opcua crawler"""

    src_api = opc_ua.OPCUA(source_parameters=opcua_config_with_encryption,
                           executor_name="<test-opcua>")

    try:
        src_api.start()

        time_start = datetime.datetime.now(tz=datetime.timezone.utc)
        data = src_api.fetch_data()
        time_end = datetime.datetime.now(tz=datetime.timezone.utc)
    finally:
        src_api.stop()

    assert "observation_time" in data
    assert time_start <= datetime.datetime.fromisoformat(data["observation_time"]) <= time_end

    assert round(data["Var1"], 3) == 9.99
