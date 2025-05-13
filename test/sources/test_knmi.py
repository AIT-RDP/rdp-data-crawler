"""
Tests the KNMI weather station parser
"""
import datetime
import math
import os
from typing import Iterable

import numpy as np
import pytest

import data_crawler.sources.knmi as knmi


@pytest.fixture()
def simplified_base_response() -> Iterable[bytes]:
    """Returns a simplified KNMI measurement station response with a signle element"""

    file = os.path.join(__file__, "../../../data/test/2024-06-17-test_knmi.nc")
    file = os.path.abspath(file)

    with open(file, "rb") as f:
        return [f.read()]


_KNMI_MOCKUP_API_KEY = "---"


@pytest.fixture()
def weather_measurements_base_parameters() -> dict:
    """Returns a set of base parameters for a locationForecast"""
    source_parameters = {"api_key": os.environ.get("DATA_CRAWLER_KNMI_API_KEY", _KNMI_MOCKUP_API_KEY),
                         "stations": ["06204",  # "K14-FA-1C"
                                      6215,  # "VOORSCHOTEN AWS"
                                      "6216",  # "Hollandse Kust Zuid Alfa (HKZA)"
                                      "06225",  # "IJMUIDEN"
                                      "06235",  # "DE KOOY VK"
                                      "06242",  # "VLIELAND"
                                      "06248",  # "WIJDENES WP"
                                      "06249",  # "BERKHOUT AWS"
                                      "06258",  # "HOUTRIBDIJK WP"
                                      "06267"  # "STAVOREN AWS"
                                      ],
                         "initial_history": "30min",
                         "batch_size": "2",  # Try to trigger a two-stage response
                         }

    return source_parameters


def test_weather_station_parsing(simplified_base_response: Iterable[bytes], weather_measurements_base_parameters: dict):
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

    assert len(first_message["soil_temperature_5cm"]) == 1
    assert all(math.isnan(v) for v in first_message["soil_temperature_5cm"])


def test_weather_station_value_transformation_0(simplified_base_response: Iterable[bytes],
                                                weather_measurements_base_parameters: dict):
    """Tests the parsing and transformation mechanism with a static response"""

    api = knmi.WeatherStationsKNMI(source_parameters=weather_measurements_base_parameters, executor_name="<test>")
    response_data = list(api.fetch_data_bundle(raw_data=simplified_base_response))

    assert len(response_data) == 10

    message = response_data[0]  # Take the first message ordered by ID

    assert message["device_id"] == 6204
    assert message["observation_time"] == ["2024-06-17T14:20:00+00:00"]

    assert message["latitude"] == pytest.approx(53.2694, abs=1e-4)
    assert message["longitude"] == pytest.approx(3.6278, abs=1e-4)
    assert message["altitude"] == pytest.approx(41.8, abs=1e-1)

    assert message["rainfall_time_fraction_1h"] == pytest.approx([np.nan], abs=1e-2, nan_ok=True)  # D1H
    assert message["precipitation_time_fraction_gauge"] == pytest.approx([np.nan], abs=1e-1, nan_ok=True)  # dr
    assert message["precipitation_time_fraction_pws"] == pytest.approx([0.], abs=1e-2)  # pr
    assert message["precipitation_rate_pws"] == pytest.approx([0.], abs=1e-2)  # pg
    assert message["precipitation_rate_gauge"] == pytest.approx([np.nan], nan_ok=True)  # rg
    assert message["rainfall_total_24h"] == pytest.approx([np.nan], nan_ok=True)  # R24H
    assert message["rainfall_total_12h"] == pytest.approx([np.nan], nan_ok=True)  # R12H
    assert message["rainfall_total_6h"] == pytest.approx([np.nan], nan_ok=True)  # R6H
    assert message["rainfall_total_1h"] == pytest.approx([np.nan], nan_ok=True)  # R1H

    assert message["wind_direction_10m"] == pytest.approx([228.2], abs=1e-1)  # dd
    assert message["wind_direction_10m_min"] == pytest.approx([219.4], abs=1e-1)  # dn
    assert message["wind_direction_10m_std"] == pytest.approx([3.6], abs=1e-1)  # dsd
    assert message["wind_direction_10m_max"] == pytest.approx([236.3], abs=1e-1)  # dx
    assert message["wind_speed_10m_md"] == pytest.approx([7.68775], abs=1e-4)  # ff
    assert message["wind_speed_10m_sensor"] == pytest.approx([9.46], abs=1e-4)  # ffs
    assert message["wind_speed_10m_std"] == pytest.approx([0.41], abs=1e-4)  # fsd
    assert message["wind_speed_10m_1h_avg"] == pytest.approx([7.40738], abs=1e-4)  # Sav1H
    assert message["wind_speed_10m_1h_max"] == pytest.approx([7.68775], abs=1e-4)  # Sax1H
    assert message["wind_speed_10m_3h_max"] == pytest.approx([7.68775], abs=1e-4)  # Sax3H
    assert message["wind_speed_10m_6h_max"] == pytest.approx([7.68775], abs=1e-4)  # Sax6H
    assert message["wind_speed_gust_10m_full_average"] == pytest.approx([np.nan], nan_ok=True)  # fx
    assert message["wind_speed_gust_10m_sensor_full_average"] == pytest.approx([np.nan], nan_ok=True)  # fxs
    assert message["wind_speed_gust_10m_md"] == pytest.approx([9.0087], abs=1e-4)  # gff
    assert message["wind_speed_gust_10m_sensor"] == pytest.approx([10.660], abs=1e-4)  # gffs
    assert message["wind_speed_gust_10m_1h_max"] == pytest.approx([9.0087], abs=1e-4)  # Sx1H
    assert message["wind_speed_gust_10m_3h_max"] == pytest.approx([9.0087], abs=1e-4)  # Sx3H
    assert message["wind_speed_gust_10m_6h_max"] == pytest.approx([9.0087], abs=1e-4)  # Sx6H

    assert message["cloud_base_standard"] == pytest.approx([8493.222], abs=1e-2)  # h
    assert message["cloud_base_low_standard"] == pytest.approx([8493.222], abs=1e-2)  # h1
    assert message["cloud_base_medium_standard"] == pytest.approx([0.], abs=1e-2)  # h2
    assert message["cloud_base_high_standard"] == pytest.approx([0.], abs=1e-2)  # h3
    assert message["cloud_base_ceilometer"] == pytest.approx([8493.222], abs=1e-2)  # hc
    assert message["cloud_base_low_ceilometer"] == pytest.approx([8493.222], abs=1e-2)  # hc1
    assert message["cloud_base_medium_ceilometer"] == pytest.approx([0.], abs=1e-2)  # hc2
    assert message["cloud_base_high_ceilometer"] == pytest.approx([0.], abs=1e-2)  # hc3

    assert message["cloud_area_fraction_standard"] == pytest.approx([100 * 2. / 8], abs=1e-2)  # n
    assert message["cloud_area_fraction_low_standard"] == pytest.approx([100 * 2. / 8], abs=1e-2)  # n1
    assert message["cloud_area_fraction_medium_standard"] == pytest.approx([0.], abs=1e-2)  # n2
    assert message["cloud_area_fraction_high_standard"] == pytest.approx([0.], abs=1e-2)  # n3

    assert message["cloud_area_fraction_ceilometer"] == pytest.approx([100 * 2. / 8], abs=1e-2)  # nc
    assert message["cloud_area_fraction_low_ceilometer"] == pytest.approx([100 * 2. / 8], abs=1e-2)  # nc1
    assert message["cloud_area_fraction_medium_ceilometer"] == pytest.approx([0.], abs=1e-2)  # nc2
    assert message["cloud_area_fraction_high_ceilometer"] == pytest.approx([0.], abs=1e-2)  # nc3

    assert message["air_pressure_at_station_level"] == pytest.approx([1006.39], abs=1e-2)  # p0
    assert message["air_pressure_at_sea_level"] == pytest.approx([1011.39], abs=1e-2)  # pp
    assert message["air_pressure_at_sensor_level"] == pytest.approx([1008.1], abs=1e-2)  # ps

    assert message["global_horizontal_irradiation_1h_sum"] == pytest.approx([np.nan], nan_ok=True)  # Q1H
    assert message["global_horizontal_irradiation_24h_sum"] == pytest.approx([np.nan], nan_ok=True)  # Q24H
    assert message["global_horizontal_irradiation"] == pytest.approx([np.nan], nan_ok=True)  # qg
    assert message["global_horizontal_irradiation_min"] == pytest.approx([np.nan], nan_ok=True)  # qgn
    assert message["global_horizontal_irradiation_max"] == pytest.approx([np.nan], nan_ok=True)  # qgx
    assert message["sunshine_fraction"] == pytest.approx([np.nan], nan_ok=True)  # ss
    assert message["background_luminance"] == pytest.approx([np.nan], nan_ok=True)  # za

    assert message["squall_indicator"] == pytest.approx([0.], abs=1e-1)  # sq

    assert message["air_temperature_10min_avg"] == pytest.approx([np.nan], nan_ok=True)  # t10
    assert message["air_temperature_2m_10min_avg"] == pytest.approx([14.7], abs=1e-1)  # ta
    assert message["air_temperature_2m_10min_min"] == pytest.approx([14.6], abs=1e-1)  # tn
    assert message["air_temperature_2m_12h_min"] == pytest.approx([13.6], abs=1e-1)  # Tn12
    assert message["air_temperature_2m_14h_min"] == pytest.approx([13.6], abs=1e-1)  # Tn14
    assert message["air_temperature_2m_6h_min"] == pytest.approx([13.6], abs=1e-1)  # Tn6
    assert message["air_temperature_2m_10min_max"] == pytest.approx([14.7], abs=1e-1)  # tx
    assert message["air_temperature_2m_12h_max"] == pytest.approx([16.2], abs=1e-1)  # Tx12
    assert message["air_temperature_2m_24h_max"] == pytest.approx([16.2], abs=1e-1)  # Tx24
    assert message["air_temperature_2m_6h_max"] == pytest.approx([14.7], abs=1e-1)  # Tx6

    assert message["wet_bulb_temperature_2m"] == pytest.approx([12.5972], abs=1e-1)  # tb
    assert message["dew_point_temperature_2m_1min_avg"] == pytest.approx([11.3], abs=1e-1)  # td
    assert message["dew_point_temperature_2m_10min_avg"] == pytest.approx([np.nan], nan_ok=True)  # td10
    assert message["relative_humidity_2m_1min_avg"] == pytest.approx([79.], abs=1e-1)  # rh
    assert message["relative_humidity_2m_10min_avg"] == pytest.approx([np.nan], nan_ok=True)  # rh10

    assert message["soil_temperature_5cm"] == pytest.approx([np.nan], nan_ok=True)  # tb1
    assert message["soil_temperature_5cm_6h_min"] == pytest.approx([np.nan], nan_ok=True)  # Tb1n6
    assert message["soil_temperature_5cm_6h_max"] == pytest.approx([np.nan], nan_ok=True)  # Tb1x6
    assert message["soil_temperature_10cm"] == pytest.approx([np.nan], nan_ok=True)  # tb2
    assert message["soil_temperature_10cm_6h_min"] == pytest.approx([np.nan], nan_ok=True)  # Tb2n6
    assert message["soil_temperature_10cm_6h_max"] == pytest.approx([np.nan], nan_ok=True)  # Tb2x6
    assert message["soil_temperature_20cm"] == pytest.approx([np.nan], nan_ok=True)  # tb3
    assert message["soil_temperature_50cm"] == pytest.approx([np.nan], nan_ok=True)  # tb4
    assert message["soil_temperature_100cm"] == pytest.approx([np.nan], nan_ok=True)  # tb5

    assert message["grass_temperature_10cm_10min_avg"] == pytest.approx([np.nan], nan_ok=True)  # tg
    assert message["grass_temperature_10cm_10min_min"] == pytest.approx([np.nan], nan_ok=True)  # tgn
    assert message["grass_temperature_12h_min"] == pytest.approx([np.nan], nan_ok=True)  # Tgn12
    assert message["grass_temperature_14h_min"] == pytest.approx([np.nan], nan_ok=True)  # Tgn14
    assert message["grass_temperature_6h_min"] == pytest.approx([np.nan], nan_ok=True)  # Tgn6

    assert message["horizontal_visibility"] == pytest.approx([29.2], abs=1e-1)  # vv
    assert message["meteorological_optical_range"] == pytest.approx([29.2], abs=1e-1)  # zm

    assert message["past_weather_indicator"] == pytest.approx([0.], abs=1e-1)  # W10
    assert message["past_weather_indicator_10min"] == pytest.approx([0.], abs=1e-1)  # W10-10
    assert message["wawa_weather_code"] == pytest.approx([2.], abs=1e-1)  # ww
    assert message["wawa_weather_code_10min"] == pytest.approx([2.], abs=1e-1)  # ww-10


def test_weather_station_virtual_sensors_0(simplified_base_response: Iterable[bytes],
                                           weather_measurements_base_parameters: dict):
    """Tests the virtual sensor method that dynamically selects the measurements"""

    api = knmi.WeatherStationsKNMI(source_parameters=weather_measurements_base_parameters, executor_name="<test>")
    response_data = list(api.fetch_data_bundle(raw_data=simplified_base_response))

    assert len(response_data) == 10

    message = response_data[0]  # Take the first message ordered by ID

    assert message["device_id"] == 6204
    assert message["observation_time"] == ["2024-06-17T14:20:00+00:00"]

    assert message["precipitation_rate"] == pytest.approx([0.], abs=1e-2)
    assert message["precipitation_time_fraction"] == pytest.approx([0.], abs=1e-2)
    assert message["air_pressure"] == pytest.approx([1006.39], abs=1e-2)
    assert message["wind_speed_gust_10m"] == pytest.approx([9.0087], abs=1e-4)
    assert message["wind_speed_10m"] == pytest.approx([7.68775], abs=1e-4)
    assert message["cloud_base"] == pytest.approx([8493.222], abs=1e-3)
    assert message["cloud_base_low"] == pytest.approx([8493.222], abs=1e-3)
    assert message["cloud_base_medium"] == pytest.approx([0.], abs=1e-2)
    assert message["cloud_base_high"] == pytest.approx([0.], abs=1e-2)
    assert message["cloud_area_fraction"] == pytest.approx([100 * 2. / 8], abs=1e-2)
    assert message["cloud_area_fraction_low"] == pytest.approx([100 * 2. / 8], abs=1e-2)
    assert message["cloud_area_fraction_medium"] == pytest.approx([0.], abs=1e-2)
    assert message["cloud_area_fraction_high"] == pytest.approx([0.], abs=1e-2)
    assert message["relative_humidity_2m"] == pytest.approx([79.], abs=1e-1)
    assert message["air_temperature_2m"] == pytest.approx([14.7], abs=1e-2)
    assert message["dew_point_temperature_2m"] == pytest.approx([11.3], abs=1e-2)
    assert message["visibility"] == pytest.approx([29.2], abs=1e-2)


def test_weather_station_value_transformation_1(simplified_base_response: Iterable[bytes],
                                                weather_measurements_base_parameters: dict):
    """Tests the parsing and transformation mechanism with a static response"""

    api = knmi.WeatherStationsKNMI(source_parameters=weather_measurements_base_parameters, executor_name="<test>")
    response_data = list(api.fetch_data_bundle(raw_data=simplified_base_response))

    assert len(response_data) == 10

    message = response_data[1]  # Take the first message ordered by ID

    assert message["device_id"] == 6215
    assert message["observation_time"] == ["2024-06-17T14:20:00+00:00"]

    assert message["latitude"] == pytest.approx(52.1397, abs=1e-4)
    assert message["longitude"] == pytest.approx(4.4364, abs=1e-4)
    assert message["altitude"] == pytest.approx(-1.15, abs=1e-1)

    assert message["rainfall_time_fraction_1h"] == pytest.approx([0.], abs=1e-2)  # D1H
    assert message["precipitation_time_fraction_gauge"] == pytest.approx([0.], abs=1e-2)  # dr
    assert message["precipitation_rate_pws"] == pytest.approx([0.], abs=1e-2)  # pg
    assert message["precipitation_rate_gauge"] == pytest.approx([0.], abs=1e-2)  # rg
    assert message["precipitation_time_fraction_pws"] == pytest.approx([0.], abs=1e-2)  # pr
    assert message["rainfall_total_24h"] == pytest.approx([0.], abs=1e-2)  # R24H
    assert message["rainfall_total_12h"] == pytest.approx([0.], abs=1e-2)  # R12H
    assert message["rainfall_total_6h"] == pytest.approx([0.], abs=1e-2)  # R6H
    assert message["rainfall_total_1h"] == pytest.approx([0.], abs=1e-2)  # R1H

    assert message["wind_direction_10m"] == pytest.approx([258.1], abs=1e-1)  # dd
    assert message["wind_direction_10m_min"] == pytest.approx([199.7], abs=1e-1)  # dn
    assert message["wind_direction_10m_std"] == pytest.approx([24.2], abs=1e-1)  # dsd
    assert message["wind_direction_10m_max"] == pytest.approx([324.8], abs=1e-1)  # dx
    assert message["wind_speed_10m_md"] == pytest.approx([4.12], abs=1e-4)  # ff
    assert message["wind_speed_10m_sensor"] == pytest.approx([4.12], abs=1e-4)  # ffs
    assert message["wind_speed_10m_std"] == pytest.approx([0.9], abs=1e-4)  # fsd
    assert message["wind_speed_10m_1h_avg"] == pytest.approx([5.15683], abs=1e-4)  # Sav1H
    assert message["wind_speed_10m_1h_max"] == pytest.approx([6.33], abs=1e-4)  # Sax1H
    assert message["wind_speed_10m_3h_max"] == pytest.approx([6.66], abs=1e-4)  # Sax3H
    assert message["wind_speed_10m_6h_max"] == pytest.approx([6.66], abs=1e-4)  # Sax6H
    assert message["wind_speed_gust_10m_full_average"] == pytest.approx([6.16], abs=1e-4)  # fx
    assert message["wind_speed_gust_10m_sensor_full_average"] == pytest.approx([6.16], abs=1e-4)  # fxs
    assert message["wind_speed_gust_10m_md"] == pytest.approx([7.23], abs=1e-4)  # gff
    assert message["wind_speed_gust_10m_sensor"] == pytest.approx([7.23], abs=1e-4)  # gffs
    assert message["wind_speed_gust_10m_1h_max"] == pytest.approx([9.03], abs=1e-4)  # Sx1H
    assert message["wind_speed_gust_10m_3h_max"] == pytest.approx([9.57], abs=1e-4)  # Sx3H
    assert message["wind_speed_gust_10m_6h_max"] == pytest.approx([9.57], abs=1e-4)  # Sx6H

    assert message["cloud_base_standard"] == pytest.approx([0.], abs=1e-2)  # h
    assert message["cloud_base_low_standard"] == pytest.approx([0.], abs=1e-2)  # h1
    assert message["cloud_base_medium_standard"] == pytest.approx([0.], abs=1e-2)  # h2
    assert message["cloud_base_high_standard"] == pytest.approx([0.], abs=1e-2)  # h3
    assert message["cloud_base_ceilometer"] == pytest.approx([0.], abs=1e-2)  # hc
    assert message["cloud_base_low_ceilometer"] == pytest.approx([0.], abs=1e-2)  # hc1
    assert message["cloud_base_medium_ceilometer"] == pytest.approx([0.], abs=1e-2)  # hc2
    assert message["cloud_base_high_ceilometer"] == pytest.approx([0.], abs=1e-2)  # hc3

    assert message["cloud_area_fraction_standard"] == pytest.approx([0.], abs=1e-2)  # n
    assert message["cloud_area_fraction_low_standard"] == pytest.approx([0.], abs=1e-2)  # n1
    assert message["cloud_area_fraction_medium_standard"] == pytest.approx([0.], abs=1e-2)  # n2
    assert message["cloud_area_fraction_high_standard"] == pytest.approx([0.], abs=1e-2)  # n3

    assert message["cloud_area_fraction_ceilometer"] == pytest.approx([0.], abs=1e-2)  # nc
    assert message["cloud_area_fraction_low_ceilometer"] == pytest.approx([0.], abs=1e-2)  # nc1
    assert message["cloud_area_fraction_medium_ceilometer"] == pytest.approx([0.], abs=1e-2)  # nc2
    assert message["cloud_area_fraction_high_ceilometer"] == pytest.approx([0.], abs=1e-2)  # nc3

    assert message["air_pressure_at_station_level"] == pytest.approx([1012.49], abs=1e-2)  # p0
    assert message["air_pressure_at_sea_level"] == pytest.approx([1012.35], abs=1e-2)  # pp
    assert message["air_pressure_at_sensor_level"] == pytest.approx([1012.3], abs=1e-2)  # ps

    assert message["global_horizontal_irradiation_1h_sum"] == pytest.approx([816.5], abs=1e-1)  # Q1H
    assert message["global_horizontal_irradiation_24h_sum"] == pytest.approx([7843], abs=1e-1)  # Q24H
    assert message["global_horizontal_irradiation"] == pytest.approx([775.], abs=1e-1)  # qg
    assert message["global_horizontal_irradiation_min"] == pytest.approx([767.], abs=1e-1)  # qgn
    assert message["global_horizontal_irradiation_max"] == pytest.approx([785.], abs=1e-1)  # qgx
    assert message["sunshine_fraction"] == pytest.approx([100.0], abs=1e-1)  # ss
    assert message["background_luminance"] == pytest.approx([np.nan], nan_ok=True)  # za

    assert message["squall_indicator"] == pytest.approx([0.], abs=1e-1)  # sq

    assert message["air_temperature_10min_avg"] == pytest.approx([np.nan], nan_ok=True)  # t10
    assert message["air_temperature_2m_10min_avg"] == pytest.approx([19.4], abs=1e-1)  # ta
    assert message["air_temperature_2m_10min_min"] == pytest.approx([18.8], abs=1e-1)  # tn
    assert message["air_temperature_2m_12h_min"] == pytest.approx([12.2], abs=1e-1)  # Tn12
    assert message["air_temperature_2m_14h_min"] == pytest.approx([12.1], abs=1e-1)  # Tn14
    assert message["air_temperature_2m_6h_min"] == pytest.approx([16.6], abs=1e-1)  # Tn6
    assert message["air_temperature_2m_10min_max"] == pytest.approx([19.4], abs=1e-1)  # tx
    assert message["air_temperature_2m_12h_max"] == pytest.approx([19.4], abs=1e-1)  # Tx12
    assert message["air_temperature_2m_24h_max"] == pytest.approx([19.4], abs=1e-1)  # Tx24
    assert message["air_temperature_2m_6h_max"] == pytest.approx([19.4], abs=1e-1)  # Tx6

    assert message["wet_bulb_temperature_2m"] == pytest.approx([14.36], abs=1e-1)  # tb
    assert message["dew_point_temperature_2m_1min_avg"] == pytest.approx([11.1], abs=1e-1)  # td
    assert message["dew_point_temperature_2m_10min_avg"] == pytest.approx([np.nan], nan_ok=True)  # td10
    assert message["relative_humidity_2m_1min_avg"] == pytest.approx([58.], abs=1e-1)  # rh
    assert message["relative_humidity_2m_10min_avg"] == pytest.approx([np.nan], nan_ok=True)  # rh10

    assert message["soil_temperature_5cm"] == pytest.approx([np.nan], nan_ok=True)  # tb1
    assert message["soil_temperature_5cm_6h_min"] == pytest.approx([np.nan], nan_ok=True)  # Tb1n6
    assert message["soil_temperature_5cm_6h_max"] == pytest.approx([np.nan], nan_ok=True)  # Tb1x6
    assert message["soil_temperature_10cm"] == pytest.approx([np.nan], nan_ok=True)  # tb2
    assert message["soil_temperature_10cm_6h_min"] == pytest.approx([np.nan], nan_ok=True)  # Tb2n6
    assert message["soil_temperature_10cm_6h_max"] == pytest.approx([np.nan], nan_ok=True)  # Tb2x6
    assert message["soil_temperature_20cm"] == pytest.approx([np.nan], nan_ok=True)  # tb3
    assert message["soil_temperature_50cm"] == pytest.approx([np.nan], nan_ok=True)  # tb4
    assert message["soil_temperature_100cm"] == pytest.approx([np.nan], nan_ok=True)  # tb5

    assert message["grass_temperature_10cm_10min_avg"] == pytest.approx([22.3], abs=1e-1)  # tg
    assert message["grass_temperature_10cm_10min_min"] == pytest.approx([22.], abs=1e-1)  # tgn
    assert message["grass_temperature_12h_min"] == pytest.approx([9.4], abs=1e-1)  # Tgn12
    assert message["grass_temperature_14h_min"] == pytest.approx([9.4], abs=1e-1)  # Tgn14
    assert message["grass_temperature_6h_min"] == pytest.approx([20.2], abs=1e-1)  # Tgn6

    assert message["horizontal_visibility"] == pytest.approx([31.2], abs=1e-1)  # vv
    assert message["meteorological_optical_range"] == pytest.approx([31.2], abs=1e-1)  # zm

    assert message["past_weather_indicator"] == pytest.approx([0.], abs=1e-1)  # W10
    assert message["past_weather_indicator_10min"] == pytest.approx([0.], abs=1e-1)  # W10-10
    assert message["wawa_weather_code"] == pytest.approx([2.], abs=1e-1)  # ww
    assert message["wawa_weather_code_10min"] == pytest.approx([2.], abs=1e-1)  # ww-10


def test_weather_station_online(weather_measurements_base_parameters: dict):
    """Tests fetching the online response for the KNMI weather station data"""

    if weather_measurements_base_parameters["api_key"] == _KNMI_MOCKUP_API_KEY:
        raise KeyError(
            "The KNMI api_key is set to a mockup value but for the test, an actual one is needed. Consider setting the "
            "environment variable DATA_CRAWLER_KNMI_API_KEY"
        )

    api = knmi.WeatherStationsKNMI(source_parameters=weather_measurements_base_parameters, executor_name="<test>")
    response_data = api.fetch_data_bundle()
    response_data = list(response_data)

    station_ids = set(int(x) for x in weather_measurements_base_parameters["stations"])

    t_now = datetime.datetime.now(tz=datetime.timezone.utc)
    for msg in response_data:
        # Check observation time
        assert isinstance(msg["observation_time"], list)
        assert len(msg["observation_time"]) >= 1
        obs_time = datetime.datetime.fromisoformat(msg["observation_time"][0])
        assert t_now - datetime.timedelta(hours=3) <= obs_time <= t_now + datetime.timedelta(minutes=2)

        # Check the device IDs
        assert isinstance(msg["device_id"], int)
        assert msg["device_id"] in station_ids

        # check some parameters that must always be present on the selected stations
        must_have_params = ["air_temperature_2m", "air_pressure_at_sea_level"]
        for param in must_have_params:
            assert param in msg
            assert isinstance(msg[param], list)
            assert len(msg[param]) >= 1


@pytest.mark.xfail(raises=RuntimeWarning, reason="Some late value updates may be detected")
def test_weather_station_online_consecutive_fetch(weather_measurements_base_parameters: dict):
    """Tests two consecutive fetch operation and whether the time stamps are properly managed without duplicates"""

    if weather_measurements_base_parameters["api_key"] == _KNMI_MOCKUP_API_KEY:
        raise KeyError(
            "The KNMI api_key is set to a mockup value but for the test, an actual one is needed. Consider setting the "
            "environment variable DATA_CRAWLER_KNMI_API_KEY"
        )

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
        if duplicated_values != set():
            raise RuntimeWarning("Received duplicated values. This could be due to some one the fly updates but should "
                                 "not occur regularly.")


@pytest.fixture()
def weather_measurements_overrides() -> dict:
    """Returns a set of base parameters for a locationForecast"""
    source_parameters = {
        "api_key": os.environ.get("DATA_CRAWLER_KNMI_API_KEY", "---"),
        "stations": [
            {
                "id": "06204",
                "tags": {"location": "here", "my-id": "666"}
            }
        ],
        "initial_history": "30min",
        "batch_size": "2",  # Try to trigger a two-stage response
        "drop_missing_observations": True,  # Remove observations that are not present
    }

    return source_parameters


def test_weather_station_tag_overrides(simplified_base_response: Iterable[bytes], weather_measurements_overrides: dict):
    """Tests the parsing and transformation mechanism with a static response"""

    api = knmi.WeatherStationsKNMI(source_parameters=weather_measurements_overrides, executor_name="<test>")
    response_data = list(api.fetch_data_bundle(raw_data=simplified_base_response))

    assert len(response_data) == 1
    message = response_data[0]

    assert message["observation_time"] == ["2024-06-17T14:20:00+00:00"]
    assert message["location"] == "here"
    assert message["my-id"] == "666"


def test_weather_station_drop_missing(simplified_base_response: Iterable[bytes], weather_measurements_overrides: dict):
    """Tests whether missing observations are successfully dropped"""

    api = knmi.WeatherStationsKNMI(source_parameters=weather_measurements_overrides, executor_name="<test>")
    response_data = list(api.fetch_data_bundle(raw_data=simplified_base_response))

    assert len(response_data) == 1
    message = response_data[0]

    # Check whether all purely-nan based series have been dropped
    for val in message.values():
        if isinstance(val, list):
            assert all(not isinstance(v, float) for v in val) or not all(math.isnan(v) for v in val)
        else:
            assert val is not None


def test_duplicate_station_config(simplified_base_response: Iterable[bytes], weather_measurements_overrides: dict):
    """Tests whether an error is raised on duplicate station ids."""

    weather_measurements_overrides["stations"] = [
        {"id": "06204"},
        {"id": "6204"}  # Duplicate
    ]

    with pytest.raises(KeyError) as err_info:
        api = knmi.WeatherStationsKNMI(source_parameters=weather_measurements_overrides, executor_name="<test>")
        list(api.fetch_data_bundle(raw_data=simplified_base_response))

    assert "6204" in str(err_info.value)


def test_missing_id_in_config(simplified_base_response: Iterable[bytes], weather_measurements_overrides: dict):
    """Tests whether an error is raised on missing station ids."""

    weather_measurements_overrides["stations"] = [
        {"tags": {}},  # No ID.
        {"id": "6204"}
    ]

    with pytest.raises(KeyError) as err_info:
        api = knmi.WeatherStationsKNMI(source_parameters=weather_measurements_overrides, executor_name="<test>")
        list(api.fetch_data_bundle(raw_data=simplified_base_response))

    assert "id" in str(err_info.value)


def test_weather_station_online_invalid_api_key(weather_measurements_base_parameters: dict):
    """Tests the error message on having an invalid API key"""

    # Set the API key to a mockup value that should not be accepted
    weather_measurements_base_parameters["api_key"] = _KNMI_MOCKUP_API_KEY
    weather_measurements_base_parameters["expire"] = "0s"
    api = knmi.WeatherStationsKNMI(source_parameters=weather_measurements_base_parameters, executor_name="<test>")

    with pytest.raises(IOError) as err_info:
        list(api.fetch_data_bundle())

    assert "403 Client Error: Forbidden" in str(err_info.value)
