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
    assert response_data["altitude"] == 38.0

    # Check time axis (with timezone information in ISO8601 format)
    assert response_data["observation_time"] == [
        "2024-01-15T00:00:00+00:00",
        "2024-01-15T01:00:00+00:00",
        "2024-01-15T02:00:00+00:00",
        "2024-01-15T03:00:00+00:00",
        "2024-01-15T04:00:00+00:00",
        "2024-01-15T05:00:00+00:00",
        "2024-01-15T06:00:00+00:00"
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

    # Check that we got forecast_time with timezone information
    assert "forecast_time" in response_data
    forecast_time = datetime.datetime.fromisoformat(response_data["forecast_time"])
    # Verify timezone is present
    assert forecast_time.tzinfo is not None, "forecast_time should include timezone information"
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
        "altitude": 50.0,
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


def test_forecast_minutely_15_parameters():
    """Tests that forecast_minutely_15 parameter is correctly set for 15-minute data"""

    # Test with 15-minute data enabled and custom forecast_minutely_15
    params_with_15min = {
        "latitude": 52.52,
        "longitude": 13.405,
        "enable_minutely_15": True,
        "minutely_15_variables": ["wind_speed_10m", "temperature_2m"],
        "forecast_minutely_15": 192,  # 48 hours = 192 timesteps
        "forecast_days": 10,  # Hourly data for 10 days
        "cache": {"directory": ".cache-test-persistent"}
    }

    api = open_meteo.OpenMeteoForecast(source_parameters=params_with_15min)
    params = api.static_request_parameters

    # Verify both forecast parameters are present and different
    assert "forecast_days" in params
    assert params["forecast_days"] == 10
    assert "forecast_minutely_15" in params
    assert params["forecast_minutely_15"] == 192
    assert "minutely_15" in params
    assert params["minutely_15"] == "wind_speed_10m,temperature_2m"

    # Test with 15-minute data enabled but using default forecast_minutely_15 (96 = 24 hours)
    params_default_15min = {
        "latitude": 52.52,
        "longitude": 13.405,
        "enable_minutely_15": True,
        "cache": {"directory": ".cache-test-persistent"}
    }

    api_default = open_meteo.OpenMeteoForecast(source_parameters=params_default_15min)
    params_default = api_default.static_request_parameters

    assert "forecast_minutely_15" in params_default
    assert params_default["forecast_minutely_15"] == 96  # Default is 24 hours

    # Test without 15-minute data - forecast_minutely_15 should not be present
    params_no_15min = {
        "latitude": 52.52,
        "longitude": 13.405,
        "enable_minutely_15": False,
        "cache": {"directory": ".cache-test-persistent"}
    }

    api_no_15min = open_meteo.OpenMeteoForecast(source_parameters=params_no_15min)
    params_no_15min_result = api_no_15min.static_request_parameters

    assert "forecast_minutely_15" not in params_no_15min_result
    assert "minutely_15" not in params_no_15min_result


def test_fetch_data_bundle_separate_messages(simplified_base_response, forecast_base_parameters):
    """Tests that fetch_data_bundle yields separate messages for hourly and 15-minute data"""

    # Configure with 15-minute data enabled
    params_with_15min = {
        **forecast_base_parameters,
        "enable_minutely_15": True,
        "hourly_variables": ["temperature_2m", "wind_speed_10m", "wind_speed_80m"],
        "minutely_15_variables": ["temperature_2m", "wind_speed_10m", "wind_speed_80m"]
    }

    # Add 15-minute data to test fixture
    test_response = simplified_base_response.copy()
    test_response["minutely_15"] = {
        "time": ["2024-01-15T00:00", "2024-01-15T00:15", "2024-01-15T00:30", "2024-01-15T00:45"],
        "temperature_2m": [3.5, 3.4, 3.3, 3.2],
        "wind_speed_10m": [12.5, 12.3, 12.1, 11.9],
        "wind_speed_80m": [22.3, 22.2, 22.0, 21.8]
    }

    api = open_meteo.OpenMeteoForecast(source_parameters=params_with_15min)
    messages = list(api.fetch_data_bundle(raw_forecast=test_response))

    # Should yield 2 messages: one hourly, one 15-minute
    assert len(messages) == 2, f"Expected 2 messages, got {len(messages)}"

    # Verify first message is hourly data
    hourly_msg = messages[0]
    assert hasattr(hourly_msg, 'payload'), "Message should have payload attribute"
    assert hasattr(hourly_msg, 'metadata'), "Message should have metadata attribute"
    assert "observation_time" in hourly_msg.payload
    assert len(hourly_msg.payload["observation_time"]) == 7  # From the test fixture
    assert "air_temperature_2m" in hourly_msg.payload
    assert "wind_speed_10m" in hourly_msg.payload
    assert "wind_speed_80m" in hourly_msg.payload
    # Should include common metadata
    assert "latitude" in hourly_msg.payload
    assert "longitude" in hourly_msg.payload
    assert hourly_msg.payload["latitude"] == 52.52

    # Verify second message is 15-minute data
    minutely_msg = messages[1]
    assert "observation_time" in minutely_msg.payload
    assert len(minutely_msg.payload["observation_time"]) == 4  # 15-minute intervals
    assert "air_temperature_2m" in minutely_msg.payload
    assert "wind_speed_10m" in minutely_msg.payload
    assert "wind_speed_80m" in minutely_msg.payload
    # Should include common metadata
    assert "latitude" in minutely_msg.payload
    assert "longitude" in minutely_msg.payload
    assert minutely_msg.payload["latitude"] == 52.52


def test_fetch_data_bundle_hourly_only(simplified_base_response, forecast_base_parameters):
    """Tests that fetch_data_bundle yields only hourly message when 15-minute data is disabled"""

    # Configure without 15-minute data
    params_hourly_only = {
        **forecast_base_parameters,
        "enable_minutely_15": False,
        "hourly_variables": ["temperature_2m", "wind_speed_10m"]
    }

    api = open_meteo.OpenMeteoForecast(source_parameters=params_hourly_only)
    messages = list(api.fetch_data_bundle(raw_forecast=simplified_base_response))

    # Should yield only 1 message (hourly)
    assert len(messages) == 1, f"Expected 1 message when 15-minute disabled, got {len(messages)}"

    hourly_msg = messages[0]
    assert "observation_time" in hourly_msg.payload
    assert len(hourly_msg.payload["observation_time"]) == 7
    assert "air_temperature_2m" in hourly_msg.payload


def test_transform_hourly_data(simplified_base_response):
    """Tests the new _transform_hourly_data static method"""

    hourly_vars = ["temperature_2m", "wind_speed_10m", "wind_speed_80m", "precipitation"]
    result = open_meteo.OpenMeteoForecast._transform_hourly_data(
        simplified_base_response,
        hourly_vars
    )

    # Check that hourly data was extracted
    assert "observation_time" in result
    assert len(result["observation_time"]) == 7
    assert "air_temperature_2m" in result
    assert result["air_temperature_2m"] == [3.5, 3.1, 2.8, 2.5, 2.3, 2.0, 1.8]
    assert "wind_speed_10m" in result
    assert "wind_speed_80m" in result
    assert "precipitation_total" in result

    # Should have forecast_time
    assert "forecast_time" in result
    assert result["forecast_time"] == "2024-01-15T00:00:00+00:00"


def test_transform_minutely_15_data():
    """Tests the new _transform_minutely_15_data static method"""

    # Create a test response with 15-minute data
    test_response = {
        "minutely_15": {
            "time": ["2024-01-15T00:00", "2024-01-15T00:15", "2024-01-15T00:30"],
            "temperature_2m": [10.5, 10.6, 10.7],
            "wind_speed_10m": [5.0, 5.2, 5.4]
        }
    }

    minutely_vars = ["temperature_2m", "wind_speed_10m"]
    result = open_meteo.OpenMeteoForecast._transform_minutely_15_data(
        test_response,
        minutely_vars
    )

    # Check that 15-minute data was extracted
    assert "observation_time" in result
    assert len(result["observation_time"]) == 3
    assert "air_temperature_2m" in result
    assert result["air_temperature_2m"] == [10.5, 10.6, 10.7]
    assert "wind_speed_10m" in result
    assert result["wind_speed_10m"] == [5.0, 5.2, 5.4]

    # Should have forecast_time
    assert "forecast_time" in result


def test_extract_common_metadata(simplified_base_response):
    """Tests the new _extract_common_metadata static method"""

    metadata = open_meteo.OpenMeteoForecast._extract_common_metadata(simplified_base_response)

    assert "latitude" in metadata
    assert "longitude" in metadata
    assert "altitude" in metadata
    assert metadata["latitude"] == 52.52
    assert metadata["longitude"] == 13.419
    assert metadata["altitude"] == 38.0


def test_backward_compatibility_combined_format(simplified_base_response, forecast_base_parameters):
    """Tests that the old fetch_data method still returns combined hourly and 15-minute data"""

    # Configure with 15-minute data enabled
    params_with_15min = {
        **forecast_base_parameters,
        "enable_minutely_15": True,
        "hourly_variables": ["temperature_2m", "wind_speed_10m"],
        "minutely_15_variables": ["temperature_2m", "wind_speed_10m"]
    }

    # Add 15-minute data to test fixture
    test_response = simplified_base_response.copy()
    test_response["minutely_15"] = {
        "time": ["2024-01-15T00:00", "2024-01-15T00:15"],
        "temperature_2m": [3.5, 3.4],
        "wind_speed_10m": [12.5, 12.3]
    }

    api = open_meteo.OpenMeteoForecast(source_parameters=params_with_15min)
    combined_data = api.fetch_data(raw_forecast=test_response)

    # Should have both hourly and 15-minute data in the same dict (with _15min suffix for 15-minute)
    assert "observation_time" in combined_data  # Hourly
    assert "observation_time_15min" in combined_data  # 15-minute
    assert len(combined_data["observation_time"]) == 7
    assert len(combined_data["observation_time_15min"]) == 2
    assert "air_temperature_2m" in combined_data  # Hourly temperature
    assert "air_temperature_2m_15min" in combined_data  # 15-minute temperature
