"""Tests the weatherbit.io data sources"""
import json
import os
import datetime

import pytest

import data_crawler.sources.weatherbit as weatherbit


@pytest.fixture()
def simplified_mea_response() -> dict:
    """Returns a simplified Weatherbit current weather base response"""

    file = os.path.join(__file__, "../../../data/test/weatherbit.io-current.json")
    file = os.path.abspath(file)

    with open(file, "r") as f:
        data = json.load(f)
    return data


@pytest.fixture()
def basic_weatherbit_config() -> dict:
    """Returns a basic weatherbit configuration including the API key"""

    return {
        "api key": os.environ.get("DATA_CRAWLER_WEATHERBIT_API_KEY", "---"),
        "latitude": 48.2687266,
        "longitude": 16.4268531,
        "cache": {"directory": ".cache-test-persistent"}  # Avoid too frequent calls that may be expensive
    }


def test_weatherbit_current_parsing(basic_weatherbit_config, simplified_mea_response):
    """Tests the parsing using a prefetched document"""

    api = weatherbit.CurrentWeather(source_parameters=basic_weatherbit_config, executor_name="<test>")
    message_data = api.fetch_data(raw_data=simplified_mea_response)

    assert message_data["observation_time"] == "2022-09-28T10:52:00+00:00"
    assert message_data["latitude"] == 46.6199
    assert message_data["longitude"] == 14.3165
    assert message_data["air_pressure"] == 948.5
    assert message_data["air_pressure_at_sea_level"] == 1001
    assert message_data["wind_speed_10m"] == 1.5
    assert message_data["wind_direction_10m"] == 40
    assert message_data["air_temperature_2m"] == 13
    assert message_data["apparent_temperature"] == 12
    assert message_data["relative_humidity_2m"] == 71
    assert message_data["dew_point_temperature_2m"] == 7.9
    assert message_data["cloud_area_fraction"] == 48
    assert message_data["visibility"] == 10
    assert message_data["precipitation_rate"] == 0
    assert message_data["snowfall_rate"] == 0
    assert message_data["uv_index"] == 3.31503
    assert message_data["air_quality_index_epa"] == 35
    assert message_data["global_horizontal_irradiation"] == 619.3


@pytest.mark.skipif("DATA_CRAWLER_WEATHERBIT_API_KEY" not in os.environ, reason="No API Key provided")
def test_weatherbit_current_online(basic_weatherbit_config):
    """Tests whether the source API correctly fetches online data"""

    api = weatherbit.CurrentWeather(source_parameters=basic_weatherbit_config, executor_name="<test>")
    message_data = api.fetch_data()

    assert message_data is not None
    assert datetime.datetime.fromisoformat(message_data["observation_time"]) > \
           datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(hours=3)


@pytest.fixture()
def simplified_hourly_fc_response() -> dict:
    """Returns a simplified Weatherbit hourly forecast response"""

    file = os.path.join(__file__, "../../../data/test/weatherbit.io-forecast-hourly-reduced.json")
    file = os.path.abspath(file)

    with open(file, "r") as f:
        data = json.load(f)
    return data


def test_weatherbit_hourly_fc_parsing(basic_weatherbit_config, simplified_hourly_fc_response):
    """Tests the parsing using a prefetched document"""

    api = weatherbit.HourlyForecasts(source_parameters=basic_weatherbit_config, executor_name="<test>")
    message_data = api.fetch_data(raw_data=simplified_hourly_fc_response)
    now = datetime.datetime.now(tz=datetime.timezone.utc)

    assert now - datetime.timedelta(seconds=2) <= datetime.datetime.fromisoformat(message_data["forecasting_time"])
    assert datetime.datetime.fromisoformat(message_data["forecasting_time"]) <= now

    assert message_data["observation_time"] == ["2022-09-29T14:00:00+00:00", "2022-09-29T15:00:00+00:00"]
    assert message_data["latitude"] == 48.2687
    assert message_data["longitude"] == 16.4269

    assert message_data["air_pressure"] == [985, 985.2]
    assert message_data["air_pressure_at_sea_level"] == [1004.1, 1004.3]
    assert message_data["wind_speed_10m"] == [2.06, 2.06]
    assert message_data["wind_speed_gust_10m"] == [4.63, 4.63]
    assert message_data["wind_direction_10m"] == [84, 88]
    assert message_data["air_temperature_2m"] == [15.2, 14.4]
    assert message_data["apparent_temperature"] == [15.2, 14.4]
    assert message_data["relative_humidity_2m"] == [56, 60]
    assert message_data["dew_point_temperature_2m"] == [6.5, 6.7]
    assert message_data["cloud_area_fraction"] == [75, 78]
    assert message_data["cloud_area_fraction_high"] == [100, 100]
    assert message_data["cloud_area_fraction_medium"] == [92, 89]
    assert message_data["cloud_area_fraction_low"] == [78, 69]
    assert message_data["visibility"] == [24.128, 24.128]
    assert message_data["precipitation_total_1h"] == [0, 0]
    assert message_data["snowfall_total_1h"] == [0, 0]
    assert message_data["precipitation_probability"] == [0, 0]
    assert message_data["uv_index"] == [0.7, 0.5]
    assert message_data["global_horizontal_irradiation"] == [120.278, 86.858]


@pytest.mark.skipif("DATA_CRAWLER_WEATHERBIT_API_KEY" not in os.environ, reason="No API Key provided")
def test_weatherbit_hourly_fc_online(basic_weatherbit_config):
    """Tests whether the source API correctly fetches online data"""

    api = weatherbit.HourlyForecasts(source_parameters=basic_weatherbit_config, executor_name="<test>")
    message_data = api.fetch_data()

    assert message_data is not None
    assert datetime.datetime.fromisoformat(message_data["forecasting_time"]) > \
           datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(hours=3)
