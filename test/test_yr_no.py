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
    assert response_data["latitude"] == 16.4268
    assert response_data["longitude"] == 48.268
    assert response_data["altitude"] == 159

    assert response_data["observation_time"] == [
        "2022-07-20T09:00:00+00:00", "2022-07-20T13:00:00+00:00", "2022-07-20T14:00:00+00:00",
        "2022-07-22T18:00:00+00:00", "2022-07-22T19:00:00+00:00", "2022-07-29T06:00:00+00:00",
        "2022-07-29T12:00:00+00:00"
    ]

    assert response_data["air_temperature_2m"] == [31.0, 34.2, 34.1, 30.6, 28.4, 20.2, 20.6]
    assert response_data["air_pressure_at_sea_level"] == [1020.2, 1018.2, 1017.7, 1014.3, 1014.4, 1013.6, 1013.0]


def test_location_forecast_online(location_forecast_base_parameters):
    """Queries the online forecast and does some basic integrity checks"""

    api = yr_no.LocationForecast(source_parameters=location_forecast_base_parameters)
    response_data = api.fetch_data()

    assert response_data is not None
    assert datetime.datetime.fromisoformat(response_data["forecast_time"]) > \
           datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)