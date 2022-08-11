"""
Implements the interface to the yr.no forecasting service
"""
import itertools
from typing import Dict, Any

import jsonpath_rw
import pandas as pd
import requests

import data_crawler.sources.abc.http_cache as http_cache


class _PathExtractor:
    """Helper class to define an extraction rule transforming the parsed response"""

    def __init__(self, target_key: str, src_path: str, is_list=True, dst_format=None):
        """
        Initializes the extractor

        :param target_key: The destination key of all extracted values
        :param src_path: The JSON Path expression extracting the information
        :param is_list: A Flag whether the result must be a list of individual values. In case no list is expected, the
            path must point to a single value.
        :param dst_format: A callable that transforms each value to a destination format. In case it is None, no
            transformation will be applied.
        """

        self._target_key = target_key
        self._src_expression = jsonpath_rw.parse(src_path)
        self._is_list = is_list
        self._dst_format = dst_format

    def _extract_raw_results(self, raw_data: dict) -> list:
        """
        Internal hook to extract the information without any postprocessing steps

        :param raw_data: The pile of input data to fetch the information from
        :return: A possibly empty list of extracted values
        """

        return [r.value for r in self._src_expression.find(raw_data)]

    def extract_information(self, raw_data: dict) -> Dict[str, Any]:
        """
        Extracts the specified information and returns it in the destination dictionary form

        :param raw_data: The input structure as nested dictionaries
        :return: The output structure as transformed dictionary
        """

        result = self._extract_raw_results(raw_data)

        if self._dst_format is not None:
            result = list(map(self._dst_format, result))

        if not self._is_list:
            if len(result) != 1:
                raise KeyError(f"The path expression {self._src_expression} does not yield exactly one element "
                               f"but {len(result)} ones")
            result = result[0]

        return {self._target_key: result}


class _DatetimePathExtractor(_PathExtractor):
    """A path extractor that converts each timestamp value to the ISO 8610 format"""

    def __init__(self, target_key: str, src_path: str, is_list=True):
        """
        Initializes the extractor

        :param target_key: The destination key of all extracted values
        :param src_path: The JSON Path expression extracting the information
        :param is_list: A Flag whether the result must be a list of individual values. In case no list is expected, the
            path must point to a single value.
        """
        super(_DatetimePathExtractor, self).__init__(target_key, src_path, is_list, self._to_iso)

    @staticmethod
    def _to_iso(x) -> str:
        """transforms the string representation to a common ISO 8610 format"""

        return pd.to_datetime(x).isoformat()


class _OptionalPathExtractor(_PathExtractor):
    """JSON Path extractor that allow to handle optional sub-paths"""

    def __init__(self, target_key: str, base_path: str, src_path: str, is_list=True, dst_format=None,
                 default_value=None):
        """
        Initializes the extractor

        In case src_path is found multiple times within base path, multiple values will be returned

        :param target_key: The destination key of all extracted values
        :param base_path: The JSON base path that defines the presence of each key
        :param src_path: The JSON sub path within the base path extracting the actual information
        :param is_list: A Flag whether the result must be a list of individual values. In case no list is expected, the
            path must point to a single value.
        :param dst_format: A callable that transforms each value to a destination format. In case it is None, no
            transformation will be applied.
        :param default_value: The default value to set in case the src_path is not present in the base_path
        """

        super(_OptionalPathExtractor, self).__init__(target_key, src_path, is_list=is_list, dst_format=dst_format)

        self._base_expression = jsonpath_rw.parse(base_path)
        self._default_value = default_value

    def _extract_raw_results(self, raw_data: dict) -> list:
        """Parses the base paths and tries to find the sub-paths within"""

        bases = [r.value for r in self._base_expression.find(raw_data)]
        result = []
        for base in bases:
            sub_res = super(_OptionalPathExtractor, self)._extract_raw_results(base)
            if len(sub_res) <= 0:
                sub_res = [self._default_value]
            result += sub_res
        return result


class LocationForecast(http_cache.GenericHTTPSourceAPI):
    """Queries the location forecast of yr.no"""

    def __init__(self, source_parameters, **kwargs):
        """
        Initializes the forecasting API but does not trigger any query

        :param source_parameters: The source parameters according to the configuration
        :param kwargs: Any extra arguments that will be sent to the super class
        """
        super(LocationForecast, self).__init__(source_parameters=source_parameters, **kwargs)

        self._static_request_parameters = {
            "lat": f"{source_parameters['latitude']:.4f}",
            "lon": f"{source_parameters['longitude']:.4f}"
        }
        if "altitude" in source_parameters:
            self._static_request_parameters["altitude"] = source_parameters["altitude"]

        contact = source_parameters["contact address"]
        self.session.headers["User-Agent"] = f"E3SchoolEMS {contact}"

    @property
    def static_request_parameters(self) -> Dict[str, str]:
        """Returns a copy of the static request parameters for testing purpose"""
        return self._static_request_parameters.copy()

    def fetch_data(self, raw_forecast=None) -> Dict[str, Any]:
        """
        Fetches the forecasting information and translates it into a common Redis-ready nomenclature

        :param raw_forecast: The raw forecast for testing purpose. It is not advised to use the parameter productively.
        :return: The dictionary of forecasts following the common nomenclature. See the AbstractSourceAPI class for more
            information on the expected output format.
        """

        if raw_forecast is None:
            response: requests.Response = self.session.get(
                "https://api.met.no/weatherapi/locationforecast/2.0/complete.json",
                params=self._static_request_parameters
            )
            response.raise_for_status()
            raw_forecast = response.json()

        redis_forecast = self._transform_to_redis_format(raw_forecast)
        return redis_forecast

    @staticmethod
    def _transform_to_redis_format(raw_forecast: dict) -> Dict[str, Any]:
        """
        Transforms the Yr.no forecast to the common Redis representation

        The function does some rudimentary checks but does not validate the schema entirely.
        """

        extractors = [
            # Meta-data
            _DatetimePathExtractor("forecast_time", "properties.meta.updated_at", is_list=False),
            _PathExtractor("longitude", "geometry.coordinates[0]", is_list=False),
            _PathExtractor("latitude", "geometry.coordinates[1]", is_list=False),
            _PathExtractor("altitude", "geometry.coordinates[2]", is_list=False),
            _DatetimePathExtractor("observation_time", "properties.timeseries[*].time"),

            # Air temperature and humidity
            _PathExtractor("air_temperature_2m", "properties.timeseries[*].data.instant.details.air_temperature"),
            _PathExtractor("air_pressure_at_sea_level",
                           "properties.timeseries[*].data.instant.details.air_pressure_at_sea_level"),
            _PathExtractor("dew_point_temperature_2m",
                           "properties.timeseries[*].data.instant.details.dew_point_temperature"),
            _PathExtractor("relative_humidity_2m", "properties.timeseries[*].data.instant.details.relative_humidity"),

            # Clouds
            _PathExtractor("cloud_area_fraction", "properties.timeseries[*].data.instant.details.cloud_area_fraction"),
            _PathExtractor("cloud_area_fraction_high",
                           "properties.timeseries[*].data.instant.details.cloud_area_fraction_high"),
            _PathExtractor("cloud_area_fraction_medium",
                           "properties.timeseries[*].data.instant.details.cloud_area_fraction_medium"),
            _PathExtractor("cloud_area_fraction_low",
                           "properties.timeseries[*].data.instant.details.cloud_area_fraction_low"),
            _OptionalPathExtractor("fog_area_fraction", "properties.timeseries[*]",
                                   "data.instant.details.fog_area_fraction"),
            _OptionalPathExtractor("uv_index", "properties.timeseries[*]",
                                   "data.instant.details.ultraviolet_index_clear_sky"),

            # Wind
            _PathExtractor("wind_direction_10m", "properties.timeseries[*].data.instant.details.wind_from_direction"),
            _PathExtractor("wind_speed_10m", "properties.timeseries[*].data.instant.details.wind_speed"),

            # Rain 6h and 1h ahead (Requires its own time axis)
            _OptionalPathExtractor("precipitation_total_6h", "properties.timeseries[*]",
                                   "data.next_6_hours.details.precipitation_amount"),
            _OptionalPathExtractor("precipitation_total_1h", "properties.timeseries[*]",
                                   "data.next_1_hours.details.precipitation_amount"),

        ]

        redis_forecast = dict(itertools.chain(*[ext.extract_information(raw_forecast).items() for ext in extractors]))
        return redis_forecast
