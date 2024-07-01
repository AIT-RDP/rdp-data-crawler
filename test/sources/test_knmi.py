"""
Tests the KNMI weather station parser
"""
import datetime
import json
import os, pickle

import pytest

import data_crawler.sources.knmi as knmi

@pytest.fixture()
def simplified_base_response() -> bytes:
    """Returns a simplified yr.no base response"""

    file = os.path.join(__file__, "../../../data/test/2024-06-17-test_knmi.nc")
    file = os.path.abspath(file)

    with open(file, "rb") as f:
        return f.read()

@pytest.fixture()
def weather_measurements_base_parameters() -> dict:
    """Returns a set of base parameters for a locationForecast"""
    source_parameters = {"Authorization"    : os.environ.get("DATA_CRAWLER_KNMI_API_KEY", "---"),
                         "stations_to_save" : {"BERKHOUT AWS"                   : "06249", 
                                               "IJMUIDEN"                       : "06225", 
                                               "DE KOOY VK"                     : "06235", 
                                               "VLIELAND"                       : "06242", 
                                               "VOORSCHOTEN AWS"                : "06215", 
                                               "Hollandse Kust Zuid Alfa (HKZA)": "06216",
                                               "K14-FA-1C"                      : "06204",
                                               "WIJDENES WP"                    : "06248",
                                               "HOUTRIBDIJK WP"                 : "06258",
                                               "STAVOREN AWS"                   : "06267"},
                    }

    return source_parameters


def test_location_forecast_parsing(simplified_base_response: bytes, weather_measurements_base_parameters: dict):
    """Tests the parsing and transformation mechanism with a static response"""

    api = knmi.WeatherStationsKNMI(source_parameters=weather_measurements_base_parameters, executor_name="<test>")
    response_data = api.fetch_data_bundle(raw_data=simplified_base_response)

    assert response_data is not None
    list_of_messages = [i for i in response_data]
    assert len(list(list_of_messages)) == len(weather_measurements_base_parameters['stations_to_save'])
    first_message: dict = list_of_messages[0]
    assert type(first_message) == dict
    assert first_message["observation_time"] == ['2024-06-17T14:20:00+00:00']
    assert first_message["location"] == 'NL-K14-FA-1C'
    assert type(first_message["longitude"]) == float
    assert type(first_message["latitude"]) == float
    assert type(first_message["wind_direction_10m"]) == list
