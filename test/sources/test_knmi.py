"""
Tests the KNMI weather station parser
"""
import datetime
import json
import math
import os, pickle
from typing import Iterable

import pytest

import data_crawler.sources.knmi as knmi


@pytest.fixture()
def simplified_base_response() -> Iterable[bytes]:
    """Returns a simplified KNMI measurement station response with a signle element"""

    file = os.path.join(__file__, "../../../data/test/2024-06-17-test_knmi.nc")
    file = os.path.abspath(file)

    with open(file, "rb") as f:
        return [f.read()]


@pytest.fixture()
def weather_measurements_base_parameters() -> dict:
    """Returns a set of base parameters for a locationForecast"""
    source_parameters = {"api_key": os.environ.get("DATA_CRAWLER_KNMI_API_KEY", "---"),
                         "stations": ["06204",  # "K14-FA-1C"
                                      "06215",  # "VOORSCHOTEN AWS"
                                      "06216",  # "Hollandse Kust Zuid Alfa (HKZA)"
                                      "06225",  # "IJMUIDEN"
                                      "06235",  # "DE KOOY VK"
                                      "06242",  # "VLIELAND"
                                      "06248",  # "WIJDENES WP"
                                      "06249",  # "BERKHOUT AWS"
                                      "06258",  # "HOUTRIBDIJK WP"
                                      "06267"  # "STAVOREN AWS"
                                      ],
                         "initial_history": "30min"
                         }

    return source_parameters


def test_weather_station_parsing(simplified_base_response: bytes, weather_measurements_base_parameters: dict):
    """Tests the parsing and transformation mechanism with a static response"""

    api = knmi.WeatherStationsKNMI(source_parameters=weather_measurements_base_parameters, executor_name="<test>")
    response_data = api.fetch_data_bundle(raw_data=simplified_base_response)

    assert response_data is not None
    list_of_messages = [i for i in response_data]
    assert len(list(list_of_messages)) == len(weather_measurements_base_parameters['stations'])
    first_message: dict = list_of_messages[0]
    assert type(first_message) == dict
    assert first_message["observation_time"] == ['2024-06-17T14:20:00+00:00']
    assert first_message["location"] == 'NL-K14-FA-1C'
    assert type(first_message["longitude"]) == float
    assert type(first_message["latitude"]) == float
    assert type(first_message["wind_direction_10m"]) == list


def test_weather_station_online(weather_measurements_base_parameters: dict):
    """Tests fetching the online response for the KNMI weather station data"""

    api = knmi.WeatherStationsKNMI(source_parameters=weather_measurements_base_parameters, executor_name="<test>")
    response_data = api.fetch_data_bundle()
    response_data = list(response_data)

    station_ids = set(weather_measurements_base_parameters["stations"])

    t_now = datetime.datetime.now(tz=datetime.timezone.utc)
    for msg in response_data:
        # Check observation time
        assert isinstance(msg["observation_time"], list)
        assert len(msg["observation_time"]) >= 1
        obs_time = datetime.datetime.fromisoformat(msg["observation_time"][0])
        assert t_now - datetime.timedelta(hours=3) <= obs_time <= t_now + datetime.timedelta(minutes=2)

        # Check the device IDs
        assert isinstance(msg["device_id"], str)
        assert msg["device_id"] in station_ids

        # check some parameters that must always be present on the selected stations
        must_have_params = ["air_temperature_2m", "air_pressure_at_sea_level"]
        for param in must_have_params:
            assert param in msg
            assert isinstance(msg[param], list)
            assert len(msg[param]) >= 1


def test_weather_station_online_consecutive_fetch(weather_measurements_base_parameters: dict):
    """Tests two consecutive fetch operation and whether the time stamps are properly managed without duplicates"""

    api = knmi.WeatherStationsKNMI(source_parameters=weather_measurements_base_parameters, executor_name="<test>")

    t_now = datetime.datetime.now(tz=datetime.timezone.utc)
    first_response = list(api.fetch_data_bundle())
    second_response = list(api.fetch_data_bundle())

    assert len(first_response) == len(second_response) or len(second_response) == 0
    assert len(first_response) >= 1

    for first_msg in first_response:
        first_obs_times = [datetime.datetime.fromisoformat(t) for t in first_msg["observation_time"]]
        assert all(
            t_now - datetime.timedelta(hours=1) <= t <= t_now + datetime.timedelta(minutes=2)
            for t in first_obs_times
        )

    # second message set may be empty, but if it is not, the time stamps must be distinct.
    for first_msg, second_msg in zip(first_response, second_response):
        first_obs_times = [datetime.datetime.fromisoformat(t) for t in first_msg["observation_time"]]
        second_obs_times = [datetime.datetime.fromisoformat(t) for t in second_msg["observation_time"]]
        assert all(
            t_now - datetime.timedelta(hours=1) <= t <= t_now + datetime.timedelta(minutes=2)
            for t in second_obs_times
        )

        # The following asserts assume that there are rarely value updates. Please remove the condition, in case the
        # assumption does not hold in practice
        assert 0 <= len(second_obs_times) <= 1
        duplicated_values = set(first_obs_times).intersection(second_obs_times)
        assert duplicated_values == set()

