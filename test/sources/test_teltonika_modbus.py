"""
Tests the TeltonikaModbus data source
"""

import concurrent.futures
import datetime
import json
import threading

import fastapi
import pytest
import requests
import requests.auth
import tenacity

import data_crawler.sources.abc.active_source_sync as active_source_sync
import data_crawler.sources.teltonika_modbus as telmod

PORT = 8000

# TODO Multiprocessing coverage
#      The coverage of the subprocess of TeltonikaModbus running the FasAPI server is not working.
#      This should be a solution but I didn't managed to get it running.


@pytest.fixture
def data_translation_voltage() -> list[telmod.DataTranslation]:
    return [
        telmod.DataTranslation(
            request_server_name="UMG_Umrichter_2",
            request_name="Voltage_LL",
            request_data_index=0,
            db_name="U_L1L2",
            db_device_id="UMG_Umrichter_2",
            db_location_code="Theiss",
            db_data_provider="Janiza UMG 96",
            db_unit="V",
        ),
        telmod.DataTranslation(
            request_server_name="UMG_Umrichter_2",
            request_name="Voltage_LL",
            request_data_index=1,
            db_name="U_L2L3",
            db_device_id="UMG_Umrichter_2",
            db_location_code="Theiss",
            db_data_provider="Janiza UMG 96",
            db_unit="V",
        ),
        telmod.DataTranslation(
            request_server_name="UMG_Umrichter_2",
            request_name="Voltage_LL",
            request_data_index=2,
            db_name="U_L3L1",
            db_device_id="UMG_Umrichter_2",
            db_location_code="Theiss",
            db_data_provider="Janiza UMG 96",
            db_unit="V",
        ),
    ]


@pytest.fixture
def usernames_passwords() -> list[telmod.UsernamePasswordParameters]:
    return [
        telmod.UsernamePasswordParameters(username=b"batman", password=b"supername"),
    ]


@pytest.fixture
def config(
        data_translation_voltage: list[telmod.DataTranslation],
        usernames_passwords: list[telmod.UsernamePasswordParameters],
) -> telmod.TeltonikaModbusParameters:
    return telmod.TeltonikaModbusParameters(
        host="0.0.0.0",
        port=PORT,
        data_translation=data_translation_voltage,
        basic_auth=usernames_passwords,
    )


def test_teltonika_modbus_config_classes():
    """Tests the TeltonikaModbus source configuration classes"""

    assert issubclass(telmod.TeltonikaModbus.parameter_model(), telmod.TeltonikaModbusParameters)


def _make_request(
        url: str,
        timestamp: datetime.datetime,
        server_name: str,
        name: str,
        data: list[float],
        username: str,
        password: str,
        wrong_header: bool = False,
) -> requests.Response:
    payload = telmod.PayloadModel(modbus=telmod.ModbusModel(
        timestamp=timestamp,
        server_name=server_name,
        name=name,
        data=data,
    ))

    return requests.post(
        url=url,
        json=json.loads(payload.model_dump_json()) if not wrong_header else None,
        data=payload.model_dump_json().encode() if wrong_header else None,
        auth=requests.auth.HTTPBasicAuth(username, password),
        headers={"content-type": "application/x-www-form-urlencoded"} if wrong_header else None,
    )


def test_teltonika_basic_auth(
        config: telmod.TeltonikaModbusParameters,
        usernames_passwords: list[telmod.UsernamePasswordParameters],
):
    """Tests BasicAuth of the source"""

    source = telmod.TeltonikaModbus.create(source_parameters=config)

    source.start()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # execute `run` in a separate thread since it is blocking,
        # and it has to be called so that the shutdown process works
        run = executor.submit(lambda: [_ for _ in source.run()])

        try:
            url = f"http://localhost:{PORT}/data"
            timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
            server_name = config.data_translation[0].request_server_name
            name = config.data_translation[0].request_name
            data = [0.0, 0.0, 0.0]

            # allow retries (the server needs some time to startup and we don't know exactly how long)
            @tenacity.retry(stop=tenacity.stop_after_delay(1))
            def make_request() -> requests.Response:
                return _make_request(
                    url=url,
                    timestamp=timestamp,
                    server_name=server_name,
                    name=name,
                    data=data,
                    username="Incorrect",
                    password="Incorrect",
                )

            response = make_request()
            assert response.status_code == fastapi.status.HTTP_401_UNAUTHORIZED

            response = _make_request(
                url=url,
                timestamp=timestamp,
                server_name=server_name,
                name=name,
                data=data,
                username=usernames_passwords[0].username.decode("utf-8"),
                password=usernames_passwords[0].password.decode("utf-8"),
            )
            assert response.status_code == fastapi.status.HTTP_200_OK
        finally:
            source.shutdown()
            run.result()
            source.stop()


@pytest.mark.parametrize(
    "wrong_header", [True, False],
)
def test_teltonika_modbus_endpoints(
        config: telmod.TeltonikaModbusParameters,
        data_translation_voltage: list[telmod.DataTranslation],
        usernames_passwords: list[telmod.UsernamePasswordParameters],
        # Teltonika sends the request with a wrong header -> try it with the correct and the wrong header
        wrong_header,
):
    """Tests the endpoint `/data` of the REST server"""

    source = telmod.TeltonikaModbus.create(source_parameters=config)

    source.start()

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    data_sent = [0.0, 10.0, 20.0]

    try:
        # allow retries (the server needs some time to startup and we don't know exactly how long)
        @tenacity.retry(stop=tenacity.stop_after_delay(1))
        def make_request() -> requests.Response:
            return _make_request(
                url=f"http://localhost:{PORT}/data",
                timestamp=now,
                server_name=config.data_translation[0].request_server_name,
                name=config.data_translation[0].request_name,
                data=data_sent,
                username=usernames_passwords[0].username.decode("utf-8"),
                password=usernames_passwords[0].password.decode("utf-8"),
                wrong_header=wrong_header,
            )

        response = make_request()

        assert response.status_code == 200

        for index, data in enumerate(source.run()):
            payload_expected = dict(
                obs_time=now.isoformat(),
                value=data_sent[index],
                name=data_translation_voltage[index].db_name,
                device_id=data_translation_voltage[index].db_device_id,
                location_code=data_translation_voltage[index].db_location_code,
                data_provider=data_translation_voltage[index].db_data_provider,
                unit=data_translation_voltage[index].db_unit,
                metadata=data_translation_voltage[index].db_metadata,
            )

            assert payload_expected == data.payload

            if index == 2:
                source.shutdown()
    except Exception as e:
        # make sure to shut down the source also when an exception occurs
        source.shutdown()
        raise e
    finally:
        source.stop()


def test_teltonika_modbus_missing_translation(
        config: telmod.TeltonikaModbusParameters,
        usernames_passwords: list[telmod.UsernamePasswordParameters],
):
    """Tests missing translations in the config"""

    source = telmod.TeltonikaModbus.create(source_parameters=config)

    source.start()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # execute `run` in a separate thread since it is blocking,
        # and it has to be called so that the shutdown process works
        run = executor.submit(lambda: [_ for _ in source.run()])

        try:
            url = f"http://localhost:{PORT}/data"
            timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
            server_name = config.data_translation[0].request_server_name
            data = [0.0, 0.0, 0.0]

            # allow retries (the server needs some time to startup and we don't know exactly how long)
            @tenacity.retry(stop=tenacity.stop_after_delay(1))
            def make_request() -> requests.Response:
                return _make_request(
                    url=url,
                    timestamp=timestamp,
                    server_name=server_name,
                    name="Devilish Wrong!",
                    data=data,
                    username=usernames_passwords[0].username.decode("utf-8"),
                    password=usernames_passwords[0].password.decode("utf-8"),
                )

            response = make_request()

            assert response.status_code == fastapi.status.HTTP_200_OK

            # TODO it would be nice to catch the logger massages (caplog fxture).
            #      However, they are produced by a subprocess and it does not seem to be supported by pytest
        finally:
            source.shutdown()
            assert [] == run.result()
            source.stop()


def test_teltonika_modbus_activity_status(
        config: telmod.TeltonikaModbusParameters,
        usernames_passwords: list[telmod.UsernamePasswordParameters],
):
    """Test the activity statis of the TeltonikaModbus source"""

    source = telmod.TeltonikaModbus.create(source_parameters=config)

    assert source.get_activity_status() == active_source_sync.ActivityStatus(
        last_wakeup=None,
        last_cycle_complete=None,
        max_permitted_cycle_time=datetime.timedelta(milliseconds=config.block_time_ms),
    )

    source.start()

    start_time = datetime.datetime.now(tz=datetime.timezone.utc)

    # execute `run` in a separate thread since it is blocking,
    # and it has to be called so that the shutdown process works
    run = threading.Thread(target=lambda: [_ for _ in source.run()])
    run.start()

    try:
        url = f"http://localhost:{PORT}/data"
        timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        server_name = config.data_translation[0].request_server_name
        name = config.data_translation[0].request_name
        data = [0.0, 0.0, 0.0]

        # allow retries (the server needs some time to startup and we don't know exactly how long)
        @tenacity.retry(stop=tenacity.stop_after_delay(1))
        def make_request() -> requests.Response:
            return _make_request(
                url=url,
                timestamp=timestamp,
                server_name=server_name,
                name=name,
                data=data,
                username=usernames_passwords[0].username.decode("utf-8"),
                password=usernames_passwords[0].password.decode("utf-8"),
            )

        response = make_request()
        assert response.status_code == fastapi.status.HTTP_200_OK
    finally:
        source.shutdown()
        run.join()
        source.stop()

    stop_time = datetime.datetime.now(tz=datetime.timezone.utc)

    activity_status = source.get_activity_status()

    assert activity_status.max_permitted_cycle_time == datetime.timedelta(milliseconds=config.block_time_ms)
    assert start_time <= activity_status.last_wakeup <= stop_time
    assert start_time <= activity_status.last_cycle_complete <= stop_time
    assert activity_status.last_cycle_complete >= activity_status.last_wakeup
