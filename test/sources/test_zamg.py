import datetime
import json
import os

import pytest

import data_crawler.sources.zamg as zamg


@pytest.fixture()
def simplified_mea_response() -> dict:
    """Returns a simplified yr.no base response"""

    file = os.path.join(__file__, "../../../data/test/mea-zamg-reduced.json")
    file = os.path.abspath(file)

    with open(file, "r") as f:
        data = json.load(f)
    return data


@pytest.fixture()
def measurement_station_parameters() -> dict:
    """Returns an exemplary configuration of the station source"""

    return {
        "station id": "20209",
        "data points": ["DD", "FF", "GSX", "HSX", "P", "RF", "RR", "RRM", "TL", "TP"],
        "initial history": "48h"
    }


def test_measurement_station_parsing(measurement_station_parameters, simplified_mea_response):
    """Tests the parsing functions in detail"""

    api = zamg.MeasurementStationData(source_parameters=measurement_station_parameters, executor_name="<test>")
    response_data = api.fetch_data(raw_data=simplified_mea_response)

    assert response_data is not None
    assert response_data["observation_time"] == [
        "2022-08-10T12:00:00+00:00", "2022-08-10T12:10:00+00:00", "2022-08-10T12:20:00+00:00",
        "2022-08-10T12:30:00+00:00", "2022-08-10T12:40:00+00:00", "2022-08-10T12:50:00+00:00",
        "2022-08-10T13:00:00+00:00", "2022-08-10T13:10:00+00:00", "2022-08-10T13:20:00+00:00",
        "2022-08-10T13:30:00+00:00", "2022-08-10T13:40:00+00:00", "2022-08-10T13:50:00+00:00",
        "2022-08-10T14:00:00+00:00"
    ]

    assert response_data["longitude"] == 14.316667
    assert response_data["latitude"] == 46.619446

    assert response_data["wind_direction_10m"] == [94, 100, 81, 94, 82, 80, 101, 129, 140, 118, 94, 105, 118]
    assert response_data["wind_speed_10m"] == [3.6, 3.6, 3.8, 2.6, 3.1, 2.6, 2.8, 3.8, 3.4, 3.9, 2.9, 2.3, 3.6]

    assert response_data["air_temperature_2m"] == [26.6, 26.4, 26.5, 26, 26.3, 26.4, 26.7, 26.7, 26.8, 26.3, 26.5, 26.4,
                                                   26.5]
    assert response_data["relative_humidity_2m"] == [28, 29, 31, 31, 32, 32, 31, 30, 31, 30, 31, 30, 31]

    assert response_data["dew_point_temperature_2m"] == [6.7, 7, 8.1, 7.6, 8.1, 8.3, 8.1, 7.7, 8.2, 7.6, 7.9, 7.7, 7.9]

    assert response_data["precipitation_total_10min"] == [0.5] + [0] * 12
    assert response_data["precipitation_flag"] == [1] + [0] * 12
    assert response_data["global_horizontal_irradiation"] == [836, 823, 817, 811, 798, 792, 785, 773, 747, 722, 703,
                                                              690, 665]
    assert response_data["diffuse_irradiation"] == [None] * 13
    assert response_data["air_pressure"] == [None] * 13


def test_measurement_station_online(measurement_station_parameters):
    """Tests the online query against the real API endpoint"""

    del measurement_station_parameters["data points"]  # Fetch all
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    api = zamg.MeasurementStationData(source_parameters=measurement_station_parameters, executor_name="<test>")

    response_data = api.fetch_data()  # Initial response expect the data from yesterday
    assert response_data is not None

    response_length = len(response_data["observation_time"])
    assert response_length > 2

    first_ts = datetime.datetime.fromisoformat(response_data["observation_time"][0])
    assert first_ts < now - datetime.timedelta(hours=47)

    last_ts = datetime.datetime.fromisoformat(response_data["observation_time"][-1])
    assert last_ts > now - datetime.timedelta(hours=25)

    mandatory_measurements = [
        "air_temperature_2m", "air_pressure_at_sea_level", "dew_point_temperature_2m",
        "relative_humidity_2m", "wind_direction_10m", "wind_speed_10m", "precipitation_total_10min"
    ]
    for mea_name in mandatory_measurements:
        assert mea_name in response_data
        assert len(response_data[mea_name]) == response_length

    response_data = api.fetch_data()  # Likely no new measurements. Must not raise any exception, though
    assert response_data is not None
