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
    assert message["observation_time_device"] == "2022-11-30T11:06:23+01:00"


def test_inverter_rt_data_parsing_time(inverter_rt_device_response_1, rt_parameters):
    """Tests the device time correction functionality"""

    rt_parameters["correct device time"] = True

    api = fronius.FroniusInverterRealtimeData(source_parameters=rt_parameters, executor_name="<test>")
    api.start()
    ts_before = datetime.datetime.now(tz=datetime.timezone.utc)
    message = api.fetch_data(raw_data=inverter_rt_device_response_1)
    ts_after = datetime.datetime.now(tz=datetime.timezone.utc)
    api.stop()

    assert message is not None
    assert message["observation_time_device"] == "2022-11-30T11:06:23+01:00"
    assert ts_before <= datetime.datetime.fromisoformat(message["observation_time"]) <= ts_after


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


inverter_archive_response_1 = helpers.get_json_fixture("data/test/fronius-solarapi/GetArchiveData-System-1.json")
inverter_archive_response_2 = helpers.get_json_fixture("data/test/fronius-solarapi/GetArchiveData-System-2.json")


@pytest.fixture
def archive_parameters() -> dict:
    """Returns a simple Fronius device archive configuration"""

    return {
        "address": "10.10.10.126",  # This may or may not be a real device
        "initial history": "1h",
        "data points": ["EnergyReal_WAC_Sum_Produced", "Current_DC_String_1", "Voltage_DC_String_1",
                        "PowerReal_PAC_Sum"]
    }


def test_inverter_archive_parsing(inverter_archive_response_1, archive_parameters):
    """Tests parsing the archive files"""

    api = fronius.FroniusSystemArchiveData(source_parameters=archive_parameters, executor_name="<test>")
    api.start()
    messages = list(api.fetch_data_bundle(raw_data=inverter_archive_response_1))
    api.stop()

    ref_ts = ["2022-11-30T14:00:00+01:00", "2022-11-30T14:05:00+01:00", "2022-11-30T14:10:00+01:00",
              "2022-11-30T14:15:00+01:00", "2022-11-30T14:20:00+01:00", "2022-11-30T14:25:00+01:00",
              "2022-11-30T14:30:00+01:00", "2022-11-30T14:35:00+01:00", "2022-11-30T14:40:00+01:00",
              "2022-11-30T14:45:00+01:00", "2022-11-30T14:50:00+01:00"]

    assert len(messages) == 5

    assert messages[0]["device_id"] == "1"
    assert messages[0]["observation_time"] == ref_ts
    assert messages[0]["observation_time_device"] == ref_ts
    assert messages[0]["E_P_exp_interval"] == [3.8938888888888887, 3.9494444444444445, 3.6025, 3.4608333333333334,
                                               3.694722222222222, 1.6097222222222223, 2.8500000000000001,
                                               3.6455555555555557, 4.1566666666666663, 3.881388888888889,
                                               0.24638888888888888]
    assert messages[0]["P_AC_avg"] == [46.88294314381271, 47.551839464882946, 43.374581939799334, 41.668896321070235,
                                       44.484949832775918, 19.381270903010034, 34.314381270903013, 43.892976588628763,
                                       50.046822742474909, 46.732441471571903, 2.9665551839464883]
    assert messages[0]["I_DC_S1"] == [0.080000000000000002, 0.089999999999999997, 0.070000000000000007,
                                      0.070000000000000007, 0.070000000000000007, 0.070000000000000007,
                                      0.070000000000000007, 0.080000000000000002, 0.089999999999999997,
                                      0.080000000000000002, 0.050000000000000003]
    assert messages[0]["U_DC_S1"] == [671, 671.20000000000005, 672.10000000000002, 666.40000000000009,
                                      674.80000000000007, 641.5, 690.40000000000009, 681.10000000000002, 687,
                                      672.60000000000002, 656]
    assert len(messages[0]["P_DC_S1"]) == 11
    assert messages[0]["P_DC_S1"][:2] == [0.080000000000000002 * 671, 0.089999999999999997 * 671.20000000000005]

    assert messages[4]["device_id"] == "5"
    assert messages[4]["observation_time"] == ref_ts
    assert messages[4]["observation_time_device"] == ref_ts
    assert messages[4]["E_P_exp_interval"] == [4.4991666666666665, 5.5372222222222218, 3.589722222222222,
                                               2.3619444444444446, 2.3786111111111112, 2.3883333333333332,
                                               2.2461111111111109, 3.2947222222222221, 6.2255555555555553,
                                               5.0199999999999996, 1.8411111111111111]


def test_inverter_archive_parsing_full(inverter_archive_response_2, archive_parameters):
    """Tests parsing the archive files"""

    del archive_parameters["data points"]  # Query all data points

    api = fronius.FroniusSystemArchiveData(source_parameters=archive_parameters, executor_name="<test>")
    api.start()
    messages = list(api.fetch_data_bundle(raw_data=inverter_archive_response_2))
    api.stop()

    ref_ts = ["2022-12-05T15:45:00+01:00", "2022-12-05T15:50:00+01:00", "2022-12-05T15:55:00+01:00",
              "2022-12-05T16:00:00+01:00", "2022-12-05T16:05:00+01:00", "2022-12-05T16:10:00+01:00",
              "2022-12-05T16:15:00+01:00", "2022-12-05T16:20:00+01:00", "2022-12-05T16:25:00+01:00",
              "2022-12-05T16:30:00+01:00", "2022-12-05T16:35:00+01:00", "2022-12-05T16:40:00+01:00"]

    assert len(messages) == 5

    assert messages[0]["device_id"] == "1"
    assert messages[0]["observation_time"] == ref_ts
    assert messages[0]["observation_time_device"] == ref_ts

    assert messages[0]["I_L1"] == [0.08, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    assert messages[0]["I_L2"] == [0.09, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    assert messages[0]["I_L3"] == [0.07, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    assert messages[0]["I_DC_S2"] == [0, 0.001, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    assert messages[0]["U_DC_S1"] == [604.7, 523, 490, 494.3, 488.40000000000003, 472.1, 436.6, 388.5, 341.3, 294, 234,
                                      183.60000000000002]
    assert messages[0]["U_DC_S2"] == [185.9, 181.9, 169.70000000000002, 170.9, 169.5, 163.9, 152.8, 135.5,
                                      121.10000000000001, 112.30000000000001, 96.30000000000001, 75.4]

    assert messages[0]["device_temperature_1"] == [24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    assert messages[0]["U_L1N"] == [175.8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    assert messages[0]["U_L2N"] == [175.10000000000002, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    assert messages[0]["U_L3N"] == [175.60000000000002, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]


def test_inverter_archive_time_correction(inverter_archive_response_1, archive_parameters):
    """Tests parsing the archive files"""

    archive_parameters["correct device time"] = True

    api = fronius.FroniusSystemArchiveData(source_parameters=archive_parameters, executor_name="<test>")
    api.start()
    ts_now = datetime.datetime.now(tz=datetime.timezone.utc)
    messages = list(api.fetch_data_bundle(raw_data=inverter_archive_response_1))
    api.stop()

    ref_ts = ["2022-11-30T14:00:00+01:00", "2022-11-30T14:05:00+01:00", "2022-11-30T14:10:00+01:00",
              "2022-11-30T14:15:00+01:00", "2022-11-30T14:20:00+01:00", "2022-11-30T14:25:00+01:00",
              "2022-11-30T14:30:00+01:00", "2022-11-30T14:35:00+01:00", "2022-11-30T14:40:00+01:00",
              "2022-11-30T14:45:00+01:00", "2022-11-30T14:50:00+01:00"]

    assert len(messages) == 5

    assert messages[0]["device_id"] == "1"
    assert messages[0]["observation_time_device"] == ref_ts

    offset = ts_now - datetime.datetime.fromisoformat("2022-11-30T14:54:25+01:00")
    assert len(messages[0]["observation_time"]) == 11
    for ts_corr, ts_orig in zip(messages[0]["observation_time"], messages[0]["observation_time_device"]):
        ts_corr = datetime.datetime.fromisoformat(ts_corr)
        ts_orig = datetime.datetime.fromisoformat(ts_orig)

        assert ts_orig + offset - datetime.timedelta(seconds=0.5) <= ts_corr
        assert ts_corr <= ts_orig + offset + datetime.timedelta(seconds=0.5)


def test_inverter_archive_device_tags(inverter_archive_response_1, archive_parameters):
    """Tests the device-specific tag function"""

    archive_parameters["device tags"] = {
        "1": {"readable_name": "first"},
        "5": {"readable_name": "fifth"},
    }

    api = fronius.FroniusSystemArchiveData(source_parameters=archive_parameters, executor_name="<test>")
    api.start()
    messages = list(api.fetch_data_bundle(raw_data=inverter_archive_response_1))
    api.stop()

    assert len(messages) == 5
    assert messages[0]["device_id"] == "1"
    assert messages[0]["readable_name"] == "first"

    assert "readable_name" not in messages[1]
    assert "readable_name" not in messages[2]
    assert "readable_name" not in messages[3]

    assert messages[4]["device_id"] == "5"
    assert messages[4]["readable_name"] == "fifth"


@pytest.mark.xfail(strict=False)
def test_inverter_archive_fetch(archive_parameters):
    """Tests fetching an exemplary data logger"""

    del archive_parameters["data points"]  # Query all data points

    api = fronius.FroniusSystemArchiveData(source_parameters=archive_parameters, executor_name="<test>")
    api.start()
    ts_now = datetime.datetime.now(tz=datetime.timezone.utc)
    messages = list(api.fetch_data_bundle())
    api.stop()

    assert len(messages) > 0
    assert len(messages[0]["observation_time"]) > 0
    assert all(ts_now - datetime.timedelta(hours=1.5) <= datetime.datetime.fromisoformat(ts) <=
               ts_now + datetime.timedelta(hours=0.5) for ts in messages[0]["observation_time"])
    assert len(messages[0]["observation_time_device"]) > 0
    assert all(ts_now - datetime.timedelta(hours=1.5) <= datetime.datetime.fromisoformat(ts) <=
               ts_now + datetime.timedelta(hours=0.5) for ts in messages[0]["observation_time_device"])

    assert "E_P_exp_interval" in messages[0]
    assert "I_DC_S1" in messages[0]
    assert "U_DC_S1" in messages[0]
    assert "device_temperature_1" in messages[0]
    assert "U_L1N" in messages[0]
    assert "U_L2N" in messages[0]
    assert "U_L3N" in messages[0]
    assert "I_L1" in messages[0]
    assert "I_L2" in messages[0]
    assert "I_L3" in messages[0]
    assert "P_AC_avg" in messages[0]
    assert "P_DC_S1" in messages[0]
