import datetime
import json
import os

import pytest
import requests.exceptions

import data_crawler.sources.zamg as zamg


@pytest.fixture()
def simplified_mea_response() -> dict:
    """Returns a simplified ZAMG measurement station base response"""

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
        "initial history": "96h",
        "timeout": "10s"
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


def test_measurement_station_drop_missing_observations(measurement_station_parameters, simplified_mea_response):
    """Tests the functionality that drops missing keys"""
    measurement_station_parameters["drop missing observations"] = True
    api = zamg.MeasurementStationData(source_parameters=measurement_station_parameters, executor_name="<test>")

    response_data = api.fetch_data(raw_data=simplified_mea_response)
    assert response_data is not None
    required_keys = [
        "observation_time", "longitude", "latitude", "wind_direction_10m", "wind_speed_10m", "air_temperature_2m",
        "relative_humidity_2m", "dew_point_temperature_2m", "precipitation_total_10min", "precipitation_flag",
        "global_horizontal_irradiation"
    ]
    for key in required_keys:
        assert key in response_data

    dropped_keys = ["diffuse_irradiation", "air_pressure"]
    for key in dropped_keys:
        assert key not in response_data


@pytest.mark.xfail(string=False, raises=(requests.exceptions.HTTPError, requests.exceptions.ConnectionError),
                   reason="ZAMG servers are notoriously unreliable")
@pytest.mark.xfail(string="No samples returned.", raises=RuntimeError,
                   reason="ZAMG servers are notoriously unreliable")
@pytest.mark.parametrize("endpoint,station_id", [
    ("climate", "20209"),
    ("tawes", "8989076")
])
def test_measurement_station_online(endpoint, station_id, measurement_station_parameters):
    """Tests the online query against the real API endpoint"""

    del measurement_station_parameters["data points"]  # Fetch all
    measurement_station_parameters["endpoint"] = endpoint
    measurement_station_parameters["station id"] = station_id

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    api = zamg.MeasurementStationData(source_parameters=measurement_station_parameters, executor_name="<test>")

    response_data = api.fetch_data()  # Initial response expect the data from yesterday
    assert response_data is not None

    response_length = len(response_data["observation_time"])

    if response_length == 0:
        # Most likely, ZAMG has some gaps in the data
        raise RuntimeError("No samples returned.")

    assert response_length > 2

    first_ts = datetime.datetime.fromisoformat(response_data["observation_time"][0])
    assert first_ts < now - datetime.timedelta(hours=95)

    last_ts = datetime.datetime.fromisoformat(response_data["observation_time"][-1])
    assert last_ts > now - datetime.timedelta(hours=94)  # Allow ZAMG to fail for some time

    mandatory_measurements = [
        "air_temperature_2m", "air_pressure_at_sea_level", "dew_point_temperature_2m",
        "relative_humidity_2m", "wind_direction_10m", "wind_speed_10m", "precipitation_total_10min"
    ]
    for mea_name in mandatory_measurements:
        assert mea_name in response_data
        assert len(response_data[mea_name]) == response_length

    response_data = api.fetch_data()  # Likely no new measurements. Must not raise any exception, though
    assert response_data is not None


@pytest.mark.xfail(string=False, raises=(requests.exceptions.HTTPError, requests.exceptions.ConnectionError),
                   reason="ZAMG servers are notoriously unreliable")
@pytest.mark.xfail(string="No history samples returned.", raises=RuntimeError,
                   reason="ZAMG servers are notoriously unreliable")
@pytest.mark.parametrize("endpoint,station_id", [
    ("climate", "20209"),
    ("tawes", "8989076")
])
def test_measurement_station_history_online(endpoint, station_id, measurement_station_parameters):
    """Tests the online query against the real API endpoint"""

    del measurement_station_parameters["data points"]  # Fetch all
    measurement_station_parameters["endpoint"] = endpoint
    measurement_station_parameters["station id"] = station_id

    api = zamg.MeasurementStationData(source_parameters=measurement_station_parameters, executor_name="<test>")

    utc = datetime.timezone.utc
    end_time = datetime.datetime.now(tz=utc) - datetime.timedelta(hours=48)
    start_time = end_time - datetime.timedelta(hours=24)

    filter_clauses = dict(start_time=start_time.isoformat(), end_time=end_time.isoformat())
    response_data = api.fetch_historic_data_bundle(filter_clauses)
    assert response_data is not None

    response_data = list(response_data)
    assert len(response_data) == 1

    response_data = response_data[0]
    response_length = len(response_data["observation_time"])

    if response_length == 0:
        # Most likely, ZAMG has some gaps in the data
        raise RuntimeError("No history samples returned.")

    assert response_length > 23

    first_ts = datetime.datetime.fromisoformat(response_data["observation_time"][0])
    assert start_time - datetime.timedelta(minutes=10) <= first_ts
    assert first_ts <= start_time + datetime.timedelta(minutes=10)

    last_ts = datetime.datetime.fromisoformat(response_data["observation_time"][-1])
    assert end_time - datetime.timedelta(minutes=10) <= last_ts
    assert last_ts <= end_time + datetime.timedelta(minutes=10)

    mandatory_measurements = [
        "air_temperature_2m", "air_pressure_at_sea_level", "dew_point_temperature_2m",
        "relative_humidity_2m", "wind_direction_10m", "wind_speed_10m", "precipitation_total_10min"
    ]
    for mea_name in mandatory_measurements:
        assert mea_name in response_data
        assert len(response_data[mea_name]) == response_length


@pytest.fixture()
def simplified_tawes_response() -> dict:
    """Returns a simplified ZAMG measurement station base response"""

    file = os.path.join(__file__, "../../../data/test/tawes-zamg-reduced.json")
    file = os.path.abspath(file)

    with open(file, "r") as f:
        data = json.load(f)
    return data


@pytest.fixture()
def tawes_station_parameters() -> dict:
    """Returns an exemplary configuration of the station source"""

    return {
        "station id": "8989076",
        "endpoint": "TAWES",
        "data points": ["DD", "FFAM", "GLOW", "P", "RFAM", "RR", "RRM", "TL", "TP", "SCHNEE"],
        "initial history": "48h",
        "timeout": "10s"
    }


def test_tawes_station_parsing_default_reduction(tawes_station_parameters, simplified_tawes_response):
    """Tests the parsing functions in detail using the default reduction and filtering techniques"""

    api = zamg.MeasurementStationData(source_parameters=tawes_station_parameters, executor_name="<test>")
    response_data = api.fetch_data(raw_data=simplified_tawes_response)

    assert response_data is not None
    assert response_data["observation_time"] == [
        "2022-11-14T15:30:00+00:00", "2022-11-14T15:40:00+00:00", "2022-11-14T15:50:00+00:00"
    ]

    assert response_data["longitude"] == 14.316666666666666
    assert response_data["latitude"] == 46.61944444444445

    assert response_data["wind_direction_10m"] == [198.0, 203.0, 193.0]

    assert response_data["air_temperature_2m"] == [9.5, 9.4, 9.3]

    assert response_data["dew_point_temperature_2m"] == [6.2, 6.3, 6.4]

    assert response_data["precipitation_total_10min"] == [0.0, 0.1, 0.0]
    assert response_data["precipitation_flag"] == [10.0, 10.0, 10.0]
    assert response_data["snow_depth"] == [10.0, 20.0, 30.0]

    assert response_data["air_pressure"] == [None] * 3


def test_tawes_station_parsing_full_message(tawes_station_parameters, simplified_tawes_response):
    """Tests the parsing functions without time stamp reduction"""

    tawes_station_parameters["drop excessive time stamps"] = False
    tawes_station_parameters["drop missing observations"] = False

    api = zamg.MeasurementStationData(source_parameters=tawes_station_parameters, executor_name="<test>")
    response_data = api.fetch_data(raw_data=simplified_tawes_response)

    assert response_data is not None
    assert response_data["observation_time"] == [
        "2022-11-14T15:30:00+00:00", "2022-11-14T15:40:00+00:00", "2022-11-14T15:50:00+00:00",
        "2022-11-14T16:00:00+00:00"
    ]

    assert response_data["longitude"] == 14.316666666666666
    assert response_data["latitude"] == 46.61944444444445

    assert response_data["wind_direction_10m"] == [198.0, 203.0, 193.0, None]

    assert response_data["air_temperature_2m"] == [9.5, 9.4, 9.3, None]

    assert response_data["dew_point_temperature_2m"] == [6.2, 6.3, 6.4, None]

    assert response_data["precipitation_total_10min"] == [0.0, 0.1, 0.0, None]
    assert response_data["precipitation_flag"] == [10.0, 10.0, 10.0, None]
    assert response_data["snow_depth"] == [10.0, 20.0, 30.0, None]

    assert response_data["air_pressure"] == [None] * 4


@pytest.fixture()
def nwp_parameters_minimal() -> dict:
    """Returns an exemplary configuration of a minimal NWP configuration"""

    return {
        "latitude": 48.2687266,
        "longitude": 16.4268531,
        "cache": {"directory": ".cache-test-persistent"},  # Avoid too frequent calls that may be expensive
        "timeout": "10s"
    }


@pytest.fixture()
def simplified_nwp_response() -> dict:
    """Returns a simplified Geosphere base response"""

    file = os.path.join(__file__, "../../../data/test/geosphere.at-forecast-nwp-v1-1h-2500m-reduced.json")
    file = os.path.abspath(file)

    with open(file, "r") as f:
        data = json.load(f)
    return data


@pytest.mark.xfail(string=False, raises=(requests.exceptions.HTTPError, requests.exceptions.ConnectionError),
                   reason="ZAMG servers are notoriously unreliable")
def test_nwp_online_call(nwp_parameters_minimal):
    """Test an online NWP (AROME) call"""

    api = zamg.NumericalWeatherPredictionData(source_parameters=nwp_parameters_minimal, executor_name="<test>")
    result = api.fetch_data()

    assert result is not None
    assert result["latitude"] == pytest.approx(48.2687266, abs=0.01)
    assert result["longitude"] == pytest.approx(16.4268531, abs=0.01)
    assert result["forecast_time"] is not None

    expected_lists = [
        "observation_time", "convective_available_potential_energy", "convective_inhibition", "air_temperature_2m",
        "air_temperature_min_2m", "air_temperature_max_2m", "relative_humidity_2m", "snow_surface_mass", "air_pressure",
        "wind_direction_10m", "wind_speed_10m", "global_horizontal_irradiation", "rainfall_total_1h"
    ]
    assert all(ex in result for ex in expected_lists)
    assert all(isinstance(result[ex], list) for ex in expected_lists)


def test_nwp_basic_parsing_and_computations(nwp_parameters_minimal, simplified_nwp_response):
    """Tests the parsing logic using the simplified response"""

    api = zamg.NumericalWeatherPredictionData(source_parameters=nwp_parameters_minimal, executor_name="<test>")
    result = api.fetch_data(raw_data=simplified_nwp_response)

    assert result is not None
    assert result["forecast_time"] == "2024-05-16T09:00+00:00"
    assert result["observation_time"] == [
        "2024-05-16T14:00:00+00:00",
        "2024-05-16T15:00:00+00:00",
        "2024-05-16T16:00:00+00:00",
        "2024-05-16T17:00:00+00:00",
        "2024-05-16T18:00:00+00:00",
        "2024-05-16T19:00:00+00:00",
        "2024-05-16T20:00:00+00:00",
        "2024-05-16T21:00:00+00:00"
    ]

    assert result["convective_available_potential_energy"] == [30, 13.5, 0.6, 0, 0.3, 0.1, 0, 0.1]
    assert result["convective_inhibition"] == [-0.6, -0.5, 0, 0, 0, 0, 0, 0]
    assert result["global_horizontal_irradiation"] == pytest.approx([
        195.981, 36.414, 21.275, 18.002, 0.408, 0, 0, None
    ], abs=1e-2)

    assert result["cloud_area_fraction"] == [100, 100, 90, 50, 100, 100, 100, 100]
    assert result["sunshine_fraction"] == pytest.approx([0, 13.7, 60.6, 65.7, 72.7, 80.6, 62.1, None], abs=1e-1)

    assert result["air_temperature_min_2m"] == [20.43, 21.85, 20.92, 20.35, 20.27, 20.83, 20.3, 20.07]
    assert result["air_temperature_max_2m"] == [22.06, 22.16, 21.94, 20.91, 20.98, 20.98, 20.86, 20.35]
    assert result["air_temperature_2m"] == [20.0, -0.2, 55.0, 20.8, 21, 20.8, 20.3, 20.4]

    assert result["_rainfall_mass_total"] == [0.034, 5.833, 11.821, 22.305, 22.305, 22.301, 22.305, 22.305]
    assert result["rainfall_total_1h"] == pytest.approx([
        5.809418, 5.988901, 10.636204, 0, 0, 0, 0, None
    ], abs=1e-5)

    assert result["_precipitation_mass_total"] == [0.0, 5.833, 11.821, 22.305, 22.305, 22.301, 22.305, 22.305]
    assert result["precipitation_total_1h"] == pytest.approx([
        5.843479, 5.988901, 10.636204, 0, 0, 0, 0, None
    ], abs=1e-5)

    assert result["relative_humidity_2m"] == [42.99, 42.46, 47.69, 44.52, 39.67, 39.19, 46.76, 41.74]

    assert result["wind_direction_10m"] == pytest.approx([
        138.08, 46.30, 200.43, 324.32, 116.10, 131.19, 142.65, 123.23
    ], abs=1e-2)
    assert result["wind_speed_10m"] == pytest.approx([
        6.59, 6.22, 5.44, 4.80, 5.46, 5.32, 4.78, 6.93
    ], abs=1e-2)

    assert result["wind_direction_gust_10m"] == pytest.approx([
        123.23, 142.65, 131.19, 116.10, 324.32, 200.43, 46.30, 138.08
    ], abs=1e-2)
    assert result["wind_speed_gust_10m"] == pytest.approx([
        6.93, 4.78, 5.32, 5.46, 4.80, 5.44, 6.22, 6.59
    ], abs=1e-2)

    assert result["air_pressure"] == pytest.approx([
        993.1615, 992.7688, 992.3291, 991.9656, 991.9598, 992.1767, 992.1181, 991.3325
    ], abs=1e-4)
    assert result["snowlimit"] == [2490.9, 2213.6, 2575.7, 2554.7, 2304.7, 2538.7, 2199.4, 2356.5]
    assert result["snow_surface_mass"] == [0, 0, 0, 0, 0, 0, 0, 1.2]


@pytest.fixture()
def ensemble_parameters_minimal() -> dict:
    """Returns an exemplary configuration of a minimal NWP configuration"""

    return {
        "latitude": 48.2687266,
        "longitude": 16.4268531,
        "endpoint": "ensemble",
        "cache": {"directory": ".cache-test-persistent"},  # Avoid too frequent calls that may be expensive
        "timeout": "10s"
    }


@pytest.fixture()
def simplified_ensemble_response() -> dict:
    """Returns a simplified Geosphere base response"""

    file = os.path.join(__file__, "../../../data/test/geosphere.at-forecast-ensemble-v1-1h-2500m-reduced.json")
    file = os.path.abspath(file)

    with open(file, "r") as f:
        data = json.load(f)
    return data


@pytest.mark.xfail(string=False, raises=(requests.exceptions.HTTPError, requests.exceptions.ConnectionError),
                   reason="ZAMG servers are notoriously unreliable")
def test_ensemble_online_call(ensemble_parameters_minimal):
    """Test an online Ensemble forecast call"""

    api = zamg.NumericalWeatherPredictionData(source_parameters=ensemble_parameters_minimal, executor_name="<test>")
    result = api.fetch_data()

    assert result is not None
    assert result["latitude"] == pytest.approx(48.2687266, abs=0.01)
    assert result["longitude"] == pytest.approx(16.4268531, abs=0.01)
    assert result["forecast_time"] is not None

    expected_lists = [
        "observation_time", "convective_available_potential_energy", "convective_available_potential_energy_p10",
        "convective_available_potential_energy_p90", "air_temperature_2m", "air_temperature_2m_p10",
        "air_temperature_2m_p90", "air_temperature_min_2m", "air_temperature_min_2m_p10", "air_temperature_min_2m_p90",
        "air_temperature_max_2m", "air_temperature_max_2m_p10", "air_temperature_max_2m_p90", "snow_surface_mass",
        "snow_surface_mass_p10", "snow_surface_mass_p90", "wind_direction_10m", "wind_direction_10m_p10",
        "wind_direction_10m_p90", "wind_speed_10m", "wind_speed_10m_p10", "wind_speed_10m_p90",
        "global_horizontal_irradiation", "global_horizontal_irradiation_p10", "global_horizontal_irradiation_p90",
        "rainfall_total_1h", "rainfall_total_1h_p10", "rainfall_total_1h_p90"
    ]
    assert all(ex in result for ex in expected_lists)
    assert all(isinstance(result[ex], list) for ex in expected_lists)


def test_ensemble_basic_parsing_and_computations(ensemble_parameters_minimal, simplified_ensemble_response):
    """Tests the parsing logic using the simplified response"""

    api = zamg.NumericalWeatherPredictionData(source_parameters=ensemble_parameters_minimal, executor_name="<test>")
    result = api.fetch_data(raw_data=simplified_ensemble_response)

    assert result is not None
    assert result["forecast_time"] == "2024-05-27T00:00+00:00"
    assert result["observation_time"] == [
        "2024-05-27T10:00:00+00:00", "2024-05-27T11:00:00+00:00",
        "2024-05-27T12:00:00+00:00", "2024-05-27T13:00:00+00:00"
    ]

    assert result["convective_available_potential_energy_p10"] == [167.1, 38.8, 11.9, 6.9]
    assert result["convective_available_potential_energy"] == [269.8, 205.2, 125, 57.3]
    assert result["convective_available_potential_energy_p90"] == [493.2, 339.2, 243.1, 299.2]

    assert result["global_horizontal_irradiation_p10"] == [781.7, 775.8, 740.5, 639.9]
    assert result["global_horizontal_irradiation"] == [840.2, 838.6, 827.6, 732.2]
    assert result["global_horizontal_irradiation_p90"] == [852.1, 883.7, 882.6, 798.5]

    assert result["air_temperature_min_2m_p10"] == [23.09, 24.34, 24.98, 23.59]
    assert result["air_temperature_min_2m"] == [23.94, 24.97, 25.58, 25.52]
    assert result["air_temperature_min_2m_p90"] == [24.52, 26.09, 26.56, 26.2]

    assert result["air_temperature_max_2m_p10"] == [24.64, 25.2, 25.39, 25.81]
    assert result["air_temperature_max_2m"] == [25.03, 25.69, 26.33, 26.45]
    assert result["air_temperature_max_2m_p90"] == [26.14, 27.39, 27.2, 26.86]

    assert result["rainfall_total_1h_p10"] == pytest.approx([0.100296, 0.0, 0.0, 0.0], abs=1e-5)
    assert result["rainfall_total_1h"] == pytest.approx([0.200592, 0.0, 0.0, 0.0], abs=1e-5)
    assert result["rainfall_total_1h_p90"] == pytest.approx([0.561832, 0.050173, 0.038130, 0.003010], abs=1e-5)

    assert result["precipitation_total_1h_p10"] == pytest.approx([0.0, 0.401196, 0.0, 0.0], abs=1e-5)
    assert result["precipitation_total_1h"] == pytest.approx([0.0, 0.501579, 0.0, 0.0], abs=1e-5)
    assert result["precipitation_total_1h_p90"] == pytest.approx([0.0, 0.602081, 0.0, 0.0], abs=1e-5)

    assert result["snow_surface_mass_p10"] == [0, 0.0, 0.2, 0]
    assert result["snow_surface_mass"] == [0, 0.0, 0.3, 0]
    assert result["snow_surface_mass_p90"] == [0, 0.0, 0.4, 0]

    assert result["snowlimit_p10"] == [2462, 2486, 2502, 2520]
    assert result["snowlimit"] == [2492, 2512, 2534, 2580]
    assert result["snowlimit_p90"] == [2530, 2562, 2566, 2644]

    assert result["sunshine_fraction_p10"] == pytest.approx([41.1, 0, 0, 0], abs=1e-1)
    assert result["sunshine_fraction"] == pytest.approx([85.5, 54.1, 6.3, 0.0], abs=1e-1)
    assert result["sunshine_fraction_p90"] == pytest.approx([95.3, 84.1, 57.3, 46.6], abs=1e-1)

    assert result["air_temperature_2m_p10"] == [25.0, 25.1,  25.3,  23.7]
    assert result["air_temperature_2m"] == [25, 25.7, 26,  25.8]
    assert result["air_temperature_2m_p90"] == [26.1, 26.8, 26.6, 26.4]

    assert result["cloud_area_fraction_p10"] == [20, 40, 50, 60]
    assert result["cloud_area_fraction"] == [60, 80, 90, 90]
    assert result["cloud_area_fraction_p90"] == [80, 90, 90, 90]

    assert result["wind_speed_10m_p10"] == pytest.approx([3.2, 4.0, 4.5, 4.3], abs=1e-1)
    assert result["wind_direction_10m_p10"] == pytest.approx([128.7, 131.0, 126.9, 134.1], abs=1e-1)

    assert result["wind_speed_10m"] == pytest.approx([3.7, 4.1, 4.3, 4.4], abs=1e-1)
    assert result["wind_direction_10m"] == pytest.approx([145.0, 140.9, 135.9, 161.6], abs=1e-1)

    assert result["wind_speed_10m_p90"] == pytest.approx([3.5, 4.3, 4.9, 5.8], abs=1e-1)
    assert result["wind_direction_10m_p90"] == pytest.approx([158.5, 157.0, 157.1, 208.8], abs=1e-1)
