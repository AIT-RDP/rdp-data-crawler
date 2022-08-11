"""
Tests the yr.no forecasting parser
"""
import datetime
import json
import os

import pytest

import data_crawler.sources.yr_no as yr_no


@pytest.fixture()
def simplified_base_response() -> dict:
    """Returns a simplified yr.no base response"""

    file = os.path.join(__file__, "../../data/test/yr.no-reduced.json")
    file = os.path.abspath(file)

    with open(file, "r") as f:
        data = json.load(f)
    return data


@pytest.fixture()
def location_forecast_base_parameters() -> dict:
    """Returns a set of base parameters for a locationForecast"""

    return {
        "latitude": 48.2687266,
        "longitude": 16.4268531,
        "contact address": os.environ["DATA_CRAWLER_CONTACT"],
        "cache": {"directory": ".cache-test-persistent"}
    }


def test_location_forecast_parsing(simplified_base_response: dict, location_forecast_base_parameters: dict):
    """Tests the parsing and transformation mechanism with a static response"""

    api = yr_no.LocationForecast(source_parameters=location_forecast_base_parameters)
    response_data = api.fetch_data(raw_forecast=simplified_base_response)

    assert response_data is not None
    assert response_data["forecast_time"] == "2022-07-20T08:30:39+00:00"
    assert response_data["longitude"] == 16.4268
    assert response_data["latitude"] == 48.268
    assert response_data["altitude"] == 159

    assert response_data["observation_time"] == [
        "2022-07-20T09:00:00+00:00", "2022-07-20T13:00:00+00:00", "2022-07-20T14:00:00+00:00",
        "2022-07-22T18:00:00+00:00", "2022-07-22T19:00:00+00:00", "2022-07-29T06:00:00+00:00",
        "2022-07-29T12:00:00+00:00"
    ]

    assert response_data["air_temperature_2m"] == [31.0, 34.2, 34.1, 30.6, 28.4, 20.2, 20.6]
    assert response_data["air_pressure_at_sea_level"] == [1020.2, 1018.2, 1017.7, 1014.3, 1014.4, 1013.6, 1013.0]

    assert response_data["dew_point_temperature_2m"] == [11.0, 8.2, 8.2, 17.7, 16.5, 18.5, 18.2]
    assert response_data["relative_humidity_2m"] == [29.3, 20.4, 20.4, 46.2, 48.8, 91, 86.4]
    assert response_data["cloud_area_fraction"] == [0.0, 55.5, 75.8, 21.1, 21.9, 100.0, 100.0]
    assert response_data["cloud_area_fraction_high"] == [0.0, 55.5, 75.8, 10.9, 11.7, 100.0, 100.0]
    assert response_data["cloud_area_fraction_medium"] == [0.0, 0.0, 0.0, 12.5, 12.5, 60.2, 99.2]
    assert response_data["cloud_area_fraction_low"] == [0.0, 0.0, 0.0, 0.0, 0.0, 39.1, 92.2]

    assert response_data["wind_direction_10m"] == [142.4, 143.0, 140.8, 24.4, 50.3, 41.4, 6.9]
    assert response_data["wind_speed_10m"] == [3.5, 4.8, 5.0, 1.5, 1.8, 0.3, 3.5]

    assert response_data["fog_area_fraction"] == [0.0, 0.0, 0.0, 0.0, 0.0, None, None]
    assert response_data["uv_index"] == [6.2, 5.8, 4.1, 0.1, 0, None, None]
    assert response_data["precipitation_total_6h"] == [0.0, 0.0, 0.0, 0.0, None, 5.5, None]
    assert response_data["precipitation_total_1h"] == [0.0, 0.0, 0.0, 0.0, 0.1, None, None]


def test_location_forecast_online(location_forecast_base_parameters):
    """Queries the online forecast and does some basic integrity checks"""

    api = yr_no.LocationForecast(source_parameters=location_forecast_base_parameters)
    response_data = api.fetch_data()

    assert response_data is not None
    assert datetime.datetime.fromisoformat(response_data["forecast_time"]) > \
           datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)


def test_location_forecast_request_parameters(location_forecast_base_parameters):
    """Tests whether truncation is correctly performed"""

    api = yr_no.LocationForecast(source_parameters=location_forecast_base_parameters)
    assert "lat" in api.static_request_parameters
    assert api.static_request_parameters["lat"] == "48.2687"
    assert "lon" in api.static_request_parameters
    assert api.static_request_parameters["lon"] == "16.4269"