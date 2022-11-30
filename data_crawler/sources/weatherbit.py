"""
Implements the weatherbit.io forecasting sources

The API documentation can be found at https://www.weatherbit.io/api
"""
import datetime
import itertools
from typing import Dict, Any, Optional, List

import data_crawler.access.jsonpath as jx
import data_crawler.sources.abc.http_cache as http_cache


class CurrentWeather(http_cache.GenericHTTPSourceAPI):
    """
    Queries the current weather API documented at https://www.weatherbit.io/api/weather-current

    Note that weatherbit does not recommend to archive the data due to possible inconsistencies
    """

    # Maps the output names to the corresponding input as defined by weatherbit
    _output_mapping = {
        "latitude": "lat",
        "longitude": "lon",
        "air_pressure": "pres",
        "air_pressure_at_sea_level": "slp",
        "wind_speed_10m": "wind_spd",
        "wind_direction_10m": "wind_dir",
        "air_temperature_2m": "temp",
        "apparent_temperature": "app_temp",
        "relative_humidity_2m": "rh",
        "dew_point_temperature_2m": "dewpt",
        "cloud_area_fraction": "clouds",
        "visibility": "vis",
        "precipitation_rate": "precip",
        "snowfall_rate": "snow",
        "uv_index": "uv",
        "air_quality_index_epa": "aqi",
        "global_horizontal_irradiation": "solar_rad"
    }

    def __init__(self, source_parameters, executor_name, **kwargs):
        """
        Initializes the forecasting API but does not trigger any query

        :param source_parameters: The source parameters according to the configuration
        :param executor_name: The name of the executor for debugging purpose
        :param kwargs: Any extra arguments that will be sent to the super class
        """
        super(CurrentWeather, self).__init__(source_parameters=source_parameters, **kwargs)

        self._static_request_parameters = {
            "key": source_parameters["api key"],
            "lat": source_parameters["latitude"],
            "lon": source_parameters["longitude"],
            "units": "M",
        }

        self._extractors = self._get_extractors()

    @staticmethod
    def _get_extractors() -> List[jx.PathExtractor]:
        """Generates the list of extractors that transform the raw result"""

        ret = [
            jx.UnixTimeExtractor("observation_time", "data[*].ts", is_list=False)
        ]
        ret += [
            jx.PathExtractor(dst, f"data[*].{src}", is_list=False)
            for dst, src in CurrentWeather._output_mapping.items()
        ]
        return ret

    def fetch_data(self, raw_data: Optional[dict] = None) -> Dict[str, Any]:
        """
        Fetches the current observations and return the decoded result

        :param raw_data: A decoded JSON response to test the decoding behaviour of the data source
        :return: The decoded message
        """

        if raw_data is None:
            response = self.session.get("https://api.weatherbit.io/v2.0/current",
                                        params=self._static_request_parameters)
            response.raise_for_status()
            raw_data = response.json()

        redis_message = dict(itertools.chain(*[ext.extract_information(raw_data).items() for ext in self._extractors]))
        return redis_message


class HourlyForecasts(http_cache.GenericHTTPSourceAPI):
    """
    Fetches the hourly forecast source documented at https://www.weatherbit.io/api/weather-forecast-hourly
    """

    # Meta fields that do not depend on the time stamp
    _meta_output_mapping = {
        "latitude": "lat",
        "longitude": "lon",
    }

    # Maps the output names to the corresponding input as defined by weatherbit
    _series_output_mapping = {
        "air_pressure": "pres",
        "air_pressure_at_sea_level": "slp",
        "wind_speed_10m": "wind_spd",
        "wind_speed_gust_10m": "wind_gust_spd",
        "wind_direction_10m": "wind_dir",
        "air_temperature_2m": "temp",
        "apparent_temperature": "app_temp",
        "relative_humidity_2m": "rh",
        "dew_point_temperature_2m": "dewpt",
        "cloud_area_fraction": "clouds",
        "visibility": "vis",
        "precipitation_total_1h": "precip",
        "precipitation_probability": "pop",
        "snowfall_total_1h": "snow",
        "cloud_area_fraction_high": "clouds_hi",
        "cloud_area_fraction_medium": "clouds_mid",
        "cloud_area_fraction_low": "clouds_low",
        "uv_index": "uv",
        "global_horizontal_irradiation": "solar_rad"
    }

    def __init__(self, source_parameters, executor_name, **kwargs):
        """
        Initializes the forecasting API but does not trigger any query

        :param source_parameters: The source parameters according to the configuration
        :param executor_name: The name of the executor for debugging purpose
        :param kwargs: Any extra arguments that will be sent to the super class
        """
        super(HourlyForecasts, self).__init__(source_parameters=source_parameters, **kwargs)

        self._static_request_parameters = {
            "key": source_parameters["api key"],
            "lat": source_parameters["latitude"],
            "lon": source_parameters["longitude"],
            "hours": int(source_parameters.get("horizon hours", 240)),
            "units": "M",
        }

        self._extractors = self._get_extractors()

    @staticmethod
    def _get_extractors() -> List[jx.PathExtractor]:
        """Generates the list of extractors that transform the raw result"""

        ret = [
            jx.UnixTimeExtractor("observation_time", "data[*].ts", is_list=True)
        ]
        ret += [
            jx.PathExtractor(dst, f"{src}", is_list=False)
            for dst, src in HourlyForecasts._meta_output_mapping.items()
        ]
        ret += [
            jx.PathExtractor(dst, f"data[*].{src}", is_list=True)
            for dst, src in HourlyForecasts._series_output_mapping.items()
        ]
        return ret

    def fetch_data(self, raw_data: Optional[dict] = None) -> Dict[str, Any]:
        """
        Fetches the current forecast and return the decoded result

        :param raw_data: A decoded JSON response to test the decoding behaviour of the data source
        :return: The decoded message
        """

        if raw_data is None:
            response = self.session.get("https://api.weatherbit.io/v2.0/forecast/hourly",
                                        params=self._static_request_parameters)
            response.raise_for_status()
            raw_data = response.json()

        redis_message = dict(itertools.chain(*[ext.extract_information(raw_data).items() for ext in self._extractors]))

        # Unfortunately, no forecasting_time is returned. Hence, the query time is taken:
        redis_message["forecast_time"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        return redis_message
