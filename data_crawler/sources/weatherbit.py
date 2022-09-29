"""
Implements the weatherbit.io forecasting sources

The API documentation can be found at https://www.weatherbit.io/api
"""
import itertools
from typing import Dict, Any, Optional, List

import data_crawler.extractors.jsonpath as jx
import data_crawler.sources.abc.http_cache as http_cache

# Maps the output names to the corresponding input as defined by weatherbit
_output_mapping = {
    "latitude": "lat",
    "longitude": "lon",
    "air_pressure": "pres",
    "air_pressure_at_sea_level": "slp",
    "wind_speed_10m": "wind_spd",
    "wind_direction_10m": "wind_dir",
    "air_temperature_2m": "temp",
    "apparent_temperautre": "app_temp",
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


class CurrentWeather(http_cache.GenericHTTPSourceAPI):
    """
    Queries the current weather API documented at https://www.weatherbit.io/api/weather-current

    Note that weatherbit does not recommend to archive the data due to possible inconsistencies
    """

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
            for dst, src in _output_mapping.items()
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
