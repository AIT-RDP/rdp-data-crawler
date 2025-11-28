"""
Tests the Open-Meteo Weather Forecast API parser
"""
import datetime
import json
import os

import pytest

import data_crawler.sources.open_meteo as open_meteo
import helpers


@pytest.fixture()
def simplified_base_response() -> dict:
    """Returns a simplified Open-Meteo forecast base response"""

    file = os.path.join(__file__, "../../../data/test/open-meteo-forecast-reduced.json")
    file = os.path.abspath(file)

    with open(file, "r") as f:
        data = json.load(f)
    return data


@pytest.fixture()
def forecast_base_parameters() -> dict:
    """Returns a set of base parameters for an Open-Meteo Forecast (Berlin)"""

    return {
        "latitude": 52.52,
        "longitude": 13.405,
        "cache": {"directory": ".cache-test-persistent"}
    }


@pytest.mark.parametrize("config_fkt", [helpers.direct_config, helpers.to_string_values])
def test_forecast_parsing(simplified_base_response: dict, forecast_base_parameters: dict, config_fkt):
    """Tests the parsing and transformation mechanism with a static response"""

    api = open_meteo.OpenMeteoForecast(source_parameters=config_fkt(forecast_base_parameters))
    response_data = api.fetch_data(raw_forecast=simplified_base_response)

    assert response_data is not None
    
    # Check metadata
    assert response_data["longitude"] == 13.419
    assert response_data["latitude"] == 52.52
    assert response_data["elevation"] == 38.0

    # Check time axis
    assert response_data["observation_time"] == [
        "2024-01-15T00:00:00",
        "2024-01-15T01:00:00",
        "2024-01-15T02:00:00",
        "2024-01-15T03:00:00",
        "2024-01-15T04:00:00",
        "2024-01-15T05:00:00",
        "2024-01-15T06:00:00"
    ]

    # Check temperature and humidity (aligned with weatherbit/yr.no naming)
    assert response_data["air_temperature_2m"] == [3.5, 3.1, 2.8, 2.5, 2.3, 2.0, 1.8]
    assert response_data["relative_humidity_2m"] == [85.0, 87.0, 89.0, 91.0, 92.0, 93.0, 94.0]
    assert response_data["dew_point_temperature_2m"] == [1.2, 1.0, 0.9, 0.8, 0.7, 0.6, 0.5]
    assert response_data["apparent_temperature"] == [0.5, 0.0, -0.3, -0.6, -0.8, -1.1, -1.3]
    
    # Check pressure
    assert response_data["air_pressure_at_sea_level"] == [1015.2, 1015.4, 1015.6, 1015.8, 1016.0, 1016.2, 1016.4]
    assert response_data["surface_pressure"] == [1011.5, 1011.7, 1011.9, 1012.1, 1012.3, 1012.5, 1012.7]

    # Check cloud cover
    assert response_data["cloud_area_fraction"] == [75.0, 80.0, 85.0, 90.0, 95.0, 100.0, 100.0]
    assert response_data["cloud_area_fraction_low"] == [20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0]
    assert response_data["cloud_area_fraction_medium"] == [30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0]
    assert response_data["cloud_area_fraction_high"] == [50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0]
    
    # Check visibility
    assert response_data["visibility"] == [24000, 22000, 20000, 18000, 16000, 14000, 12000]

    # Check 10m wind
    assert response_data["wind_speed_10m"] == [12.5, 11.8, 11.2, 10.5, 9.8, 9.2, 8.5]
    assert response_data["wind_direction_10m"] == [270.0, 275.0, 280.0, 285.0, 290.0, 295.0, 300.0]
    assert response_data["wind_gusts_10m"] == [22.5, 21.2, 20.0, 18.7, 17.5, 16.2, 15.0]
    
    # Check higher altitude winds (for wind energy applications)
    assert response_data["wind_speed_80m"] == [22.3, 21.1, 20.0, 18.8, 17.6, 16.5, 15.3]
    assert response_data["wind_direction_80m"] == [265.0, 270.0, 275.0, 280.0, 285.0, 290.0, 295.0]
    
    assert response_data["wind_speed_120m"] == [26.3, 25.1, 24.0, 22.8, 21.6, 20.5, 19.3]
    assert response_data["wind_direction_120m"] == [260.0, 265.0, 270.0, 275.0, 280.0, 285.0, 290.0]
    
    assert response_data["wind_speed_180m"] == [30.1, 28.5, 27.0, 25.4, 23.9, 22.3, 20.8]
    assert response_data["wind_direction_180m"] == [255.0, 260.0, 265.0, 270.0, 275.0, 280.0, 285.0]

    # Check precipitation
    assert response_data["precipitation_total"] == [0.0, 0.0, 0.1, 0.2, 0.3, 0.5, 0.8]
    assert response_data["precipitation_probability"] == [0, 5, 10, 20, 30, 45, 60]
    assert response_data["rain"] == [0.0, 0.0, 0.1, 0.2, 0.3, 0.5, 0.8]
    assert response_data["snowfall"] == [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    
    # Check weather code
    assert response_data["weather_code"] == [3, 3, 61, 61, 61, 63, 63]
    
    # Check radiation (shortwave mapped to global_horizontal_irradiation like weatherbit)
    assert response_data["global_horizontal_irradiation"] == [0.0, 0.0, 0.0, 0.0, 0.0, 5.0, 25.0]
    assert response_data["direct_radiation"] == [0.0, 0.0, 0.0, 0.0, 0.0, 2.0, 10.0]
    assert response_data["diffuse_radiation"] == [0.0, 0.0, 0.0, 0.0, 0.0, 3.0, 15.0]
    
    # Check UV index
    assert response_data["uv_index"] == [0.0, 0.0, 0.0, 0.0, 0.0, 0.1, 0.3]


def test_forecast_online(forecast_base_parameters):
    """Queries the online forecast and does some basic integrity checks"""

    api = open_meteo.OpenMeteoForecast(source_parameters=forecast_base_parameters)
    response_data = api.fetch_data()

    assert response_data is not None
    
    # Check that we got forecast_time
    assert "forecast_time" in response_data
    forecast_time = datetime.datetime.fromisoformat(response_data["forecast_time"])
    # Handle timezone-naive datetimes by making them UTC-aware
    if forecast_time.tzinfo is None:
        forecast_time = forecast_time.replace(tzinfo=datetime.timezone.utc)
    assert forecast_time > datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)
    
    # Check that wind data at various heights is present
    assert "wind_speed_10m" in response_data
    assert "wind_speed_80m" in response_data
    assert "wind_speed_120m" in response_data
    assert len(response_data["wind_speed_10m"]) > 0
    
    # Check that all wind speeds are reasonable (0-300 km/h)
    for speed in response_data["wind_speed_80m"]:
        if speed is not None:
            assert 0 <= speed <= 300, f"Wind speed {speed} is out of expected range"


@pytest.mark.parametrize("config_fkt", [helpers.direct_config, helpers.to_string_values])
def test_forecast_request_parameters(forecast_base_parameters, config_fkt):
    """Tests whether request parameters are correctly set"""

    api = open_meteo.OpenMeteoForecast(source_parameters=config_fkt(forecast_base_parameters))
    params = api.static_request_parameters
    
    assert "latitude" in params
    assert params["latitude"] == 52.52
    assert "longitude" in params
    assert params["longitude"] == 13.405
    assert "hourly" in params
    assert "wind_speed_10m" in params["hourly"]
    assert "wind_speed_80m" in params["hourly"]
    assert "wind_speed_120m" in params["hourly"]
    # Check that best_match is the default model
    assert params["models"] == "best_match"


def test_forecast_custom_variables():
    """Tests that custom hourly variables can be specified"""

    custom_params = {
        "latitude": 52.52,
        "longitude": 13.419,
        "hourly_variables": ["wind_speed_80m", "wind_speed_120m", "wind_direction_80m"],
        "cache": {"directory": ".cache-test-persistent"}
    }
    
    api = open_meteo.OpenMeteoForecast(source_parameters=custom_params)
    params = api.static_request_parameters
    
    assert params["hourly"] == "wind_speed_80m,wind_speed_120m,wind_direction_80m"


def test_forecast_optional_parameters():
    """Tests that optional parameters are correctly handled"""

    full_params = {
        "latitude": 52.0,
        "longitude": 13.0,
        "forecast_days": 10,
        "past_days": 2,
        "timezone": "Europe/Berlin",
        "elevation": 50.0,
        "models": "ecmwf_ifs04",
        "cache": {"directory": ".cache-test-persistent"}
    }
    
    api = open_meteo.OpenMeteoForecast(source_parameters=full_params)
    params = api.static_request_parameters
    
    assert params["forecast_days"] == 10
    assert params["past_days"] == 2
    assert params["timezone"] == "Europe/Berlin"
    assert params["elevation"] == 50.0
    assert params["models"] == "ecmwf_ifs04"


def test_forecast_wind_heights_online():
    """
    Tests querying wind at various heights from the online API
    
    This is useful for wind energy applications where hub heights vary.
    """
    
    minimal_params = {
        "latitude": 52.52,
        "longitude": 13.405,
        "hourly_variables": ["wind_speed_10m", "wind_speed_80m", "wind_speed_120m", "wind_speed_180m"],
        "forecast_days": 3,
        "cache": {"directory": ".cache-test-persistent"}
    }
    
    api = open_meteo.OpenMeteoForecast(source_parameters=minimal_params)
    response_data = api.fetch_data()
    
    assert response_data is not None
    assert "wind_speed_10m" in response_data
    assert "wind_speed_80m" in response_data
    assert "wind_speed_120m" in response_data
    assert "wind_speed_180m" in response_data
    
    # With 3 forecast days and hourly resolution, we should have ~72 data points
    assert len(response_data["wind_speed_80m"]) >= 48  # At least 2 days worth
    
    # Wind speed should generally increase with height
    # Check a few samples where this relationship holds
    for i in range(min(5, len(response_data["wind_speed_10m"]))):
        ws_10 = response_data["wind_speed_10m"][i]
        ws_80 = response_data["wind_speed_80m"][i]
        ws_120 = response_data["wind_speed_120m"][i]
        if ws_10 is not None and ws_80 is not None and ws_120 is not None:
            # Allow some tolerance - wind shear doesn't always hold perfectly
            assert ws_80 >= ws_10 * 0.8, f"Wind at 80m ({ws_80}) unexpectedly lower than 10m ({ws_10})"


def test_api_endpoint():
    """Tests that the correct API endpoint is used"""
    
    assert open_meteo.OpenMeteoForecast.API_ENDPOINT == "https://api.open-meteo.com/v1/forecast"
