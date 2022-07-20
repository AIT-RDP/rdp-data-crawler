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

    def extract_information(self, raw_data: dict) -> Dict[str, Any]:
        """
        Extracts the specified information and returns it in the destination dictionary form

        :param raw_data: The input structure as nested dictionaries
        :return: The output structure as transformed dictionary
        """

        result = [r.value for r in self._src_expression.find(raw_data)]

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
            "lat": f"{source_parameters['latitude']:.4}",
            "lon": f"{source_parameters['longitude']:.4}"
        }
        if "altitude" in source_parameters:
            self._static_request_parameters["altitude"] = source_parameters["altitude"]

        contact = source_parameters["contact address"]
        self.session.headers["User-Agent"] = f"E3SchoolEMS {contact}"

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
            _DatetimePathExtractor("forecast_time", "properties.meta.updated_at", is_list=False),
            _PathExtractor("latitude", "geometry.coordinates[0]", is_list=False),
            _PathExtractor("longitude", "geometry.coordinates[1]", is_list=False),
            _PathExtractor("altitude", "geometry.coordinates[2]", is_list=False),
            _DatetimePathExtractor("observation_time", "properties.timeseries[*].time"),
            _PathExtractor("air_temperature_2m", "properties.timeseries[*].data.instant.details.air_temperature"),
            _PathExtractor("air_pressure_at_sea_level",
                           "properties.timeseries[*].data.instant.details.air_pressure_at_sea_level"),
        ]

        redis_forecast = dict(itertools.chain(*[ext.extract_information(raw_forecast).items() for ext in extractors]))
        return redis_forecast
