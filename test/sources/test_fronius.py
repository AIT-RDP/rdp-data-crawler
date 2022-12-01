"""
Tests the fronius interface logic
"""
import datetime

import pytest

import data_crawler.sources.fronius as fronius
import helpers

inverter_rt_device_response_1 = helpers.get_json_fixture(
    "data/test/fronius-solarapi/GetInverterRealtimeData-Device-1-CommonInverterData-1.json"
)
inverter_rt_device_response_2 = helpers.get_json_fixture(
    "data/test/fronius-solarapi/GetInverterRealtimeData-Device-1-CommonInverterData-2.json"
)


@pytest.fixture()
def rt_parameters():
    """Returns a simple real-time configuration"""

    return {
        "address": "10.10.10.126",  # This may or may not be a real device
        "device": "1"
    }


def test_inverter_rt_data_parsing_day(inverter_rt_device_response_1, rt_parameters):
    """Tests the parsing functionality of the realtime data source"""

    api = fronius.FroniusInverterRealtimeData(source_parameters=rt_parameters, executor_name="<test>")
    api.start()
    message = api.fetch_data(raw_data=inverter_rt_device_response_1)
    api.stop()

    assert message is not None
    assert message["active_power_generation"] == 0.049
    assert message["P_AC_tot"] == 49
    assert message["I_AC_tot"] == 0.38
    assert message["I_DC_tot"] == 0.080000000000000002
    assert message["U_AC"] == 226.5
    assert message["U_DC"] == 666
    assert message["P_DC_tot"] == 666 * 0.080000000000000002
    assert message["E_P_exp"] == 6460320
    assert message["E_P_exp_day"] == 68.700000000000003
    assert message["E_P_exp_year"] == 5323129.5

    assert message["error_code"] == 0
    assert message["status_code"] == 7
    assert message["observation_time"] == "2022-11-30T11:06:23+01:00"


def test_inverter_rt_data_parsing_night(inverter_rt_device_response_2, rt_parameters):
    """Tests the parsing functionality of the realtime data source"""

    api = fronius.FroniusInverterRealtimeData(source_parameters=rt_parameters, executor_name="<test>")
    api.start()
    message = api.fetch_data(raw_data=inverter_rt_device_response_2)
    api.stop()

    assert message is not None
    assert message["active_power_generation"] == 0.0
    assert message["P_AC_tot"] == 0.0
    assert message["P_DC_tot"] == 0.0
    assert message["E_P_exp"] == 6461029.5
    assert message["E_P_exp_day"] == 481.5
    assert message["E_P_exp_year"] == 5323839

    assert message["error_code"] == 307
    assert message["status_code"] == 3
    assert message["observation_time"] == "2022-12-01T19:22:17+01:00"


@pytest.mark.xfail(strict=False)
def test_inverter_rt_data_fetch(rt_parameters):
    """Test fetching some data a local device. This test case may fail for most systems."""

    api = fronius.FroniusInverterRealtimeData(source_parameters=rt_parameters, executor_name="<test>")
    api.start()
    dt_now = datetime.datetime.now(tz=datetime.timezone.utc)
    message = api.fetch_data()
    api.stop()

    assert message is not None
    assert message["P_AC_tot"] >= 0
    assert message["active_power_generation"] >= 0
    assert message["P_DC_tot"] >= 0

    assert dt_now - datetime.timedelta(minutes=50) <= datetime.datetime.fromisoformat(message["observation_time"])
    assert datetime.datetime.fromisoformat(message["observation_time"]) <= dt_now + datetime.timedelta(minutes=50)
