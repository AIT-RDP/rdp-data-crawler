"""
Implements the interface to the yr.no forecasting service
"""
import itertools
from typing import Dict, Any

import requests

import data_crawler.sources.abc.http_cache as http_cache
import data_crawler.access.jsonpath as jx


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
            "lat": f"{float(source_parameters['latitude']):.4f}",
            "lon": f"{float(source_parameters['longitude']):.4f}"
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
            jx.DatetimePathExtractor("forecast_time", "properties.meta.updated_at", is_list=False),
            jx.PathExtractor("longitude", "geometry.coordinates[0]", is_list=False),
            jx.PathExtractor("latitude", "geometry.coordinates[1]", is_list=False),
            jx.PathExtractor("altitude", "geometry.coordinates[2]", is_list=False),
            jx.DatetimePathExtractor("observation_time", "properties.timeseries[*].time"),

            # Air temperature and humidity
            jx.PathExtractor("air_temperature_2m", "properties.timeseries[*].data.instant.details.air_temperature"),
            jx.PathExtractor("air_pressure_at_sea_level",
                             "properties.timeseries[*].data.instant.details.air_pressure_at_sea_level"),
            jx.PathExtractor("dew_point_temperature_2m",
                             "properties.timeseries[*].data.instant.details.dew_point_temperature"),
            jx.PathExtractor("relative_humidity_2m", "properties.timeseries[*].data.instant.details.relative_humidity"),

            # Clouds
            jx.PathExtractor("cloud_area_fraction",
                             "properties.timeseries[*].data.instant.details.cloud_area_fraction"),
            jx.PathExtractor("cloud_area_fraction_high",
                             "properties.timeseries[*].data.instant.details.cloud_area_fraction_high"),
            jx.PathExtractor("cloud_area_fraction_medium",
                             "properties.timeseries[*].data.instant.details.cloud_area_fraction_medium"),
            jx.PathExtractor("cloud_area_fraction_low",
                             "properties.timeseries[*].data.instant.details.cloud_area_fraction_low"),
            jx.OptionalPathExtractor("fog_area_fraction", "properties.timeseries[*]",
                                     "data.instant.details.fog_area_fraction"),
            jx.OptionalPathExtractor("uv_index", "properties.timeseries[*]",
                                     "data.instant.details.ultraviolet_index_clear_sky"),

            # Wind
            jx.PathExtractor("wind_direction_10m", "properties.timeseries[*].data.instant.details.wind_from_direction"),
            jx.PathExtractor("wind_speed_10m", "properties.timeseries[*].data.instant.details.wind_speed"),

            # Rain 6h and 1h ahead (Requires its own time axis)
            jx.OptionalPathExtractor("precipitation_total_6h", "properties.timeseries[*]",
                                     "data.next_6_hours.details.precipitation_amount"),
            jx.OptionalPathExtractor("precipitation_total_1h", "properties.timeseries[*]",
                                     "data.next_1_hours.details.precipitation_amount"),

        ]

        redis_forecast = dict(itertools.chain(*[ext.extract_information(raw_forecast).items() for ext in extractors]))
        return redis_forecast
