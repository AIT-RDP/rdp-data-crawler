"""
Implements supported ZAMG data sources
"""
import datetime
import itertools
import logging
from typing import Dict, Any, Optional, List

import data_crawler.extractors.jsonpath as jx
import data_crawler.sources.abc.http_cache as http_cache

import pandas as pd
import requests


class MeasurementStationData(http_cache.GenericHTTPSourceAPI):
    """
    Fetches the measurement station data provided in 10 minute resolution

    A description of the dataset can be found at https://data.hub.zamg.ac.at/dataset/klima-v1-10min . Be aware that the
    data is only available until midnight, the day before. Hence, the data source aligns all requests to a full day and
    only updates new days, if needed.
    """

    _parameter_mapping = {
        "TL": "air_temperature_2m",  # [degC]
        "TP": "dew_point_temperature_2m",  # [degC]
        "RF": "relative_humidity_2m",  # [%]

        "P": "air_pressure",  # [hPa]
        "P0": "air_pressure_at_sea_level",  # [hPa]

        "DD": "wind_direction_10m",  # [deg]
        "DDX": "wind_direction_gust_10m",  # [deg]
        "FF": "wind_speed_10m",  # [m/s]
        "FFX": "wind_speed_gust_10m",  # [m/s]

        "GSX": "global_horizontal_irradiation",  # [W/m^2]
        "HSX": "diffuse_irradiation",  # [W/m^2]

        "RR": "precipitation_total_10min",  # [mm]
        "RRM": "precipitation_flag",  # [1]

        "TB1": "soil_temperature_10_cm",  # [degC]
        "TB2": "soil_temperature_20_cm",  # [degC]
        "TB3": "soil_temperature_50_cm",  # [degC]
        "TS": "air_temperature_5cm",  # [degC]
    }

    def __init__(self, source_parameters, executor_name, **kwargs):
        """
        Initializes the forecasting API but does not trigger any query

        :param source_parameters: The source parameters according to the configuration
        :param executor_name: The name of the executor for debugging purpose
        :param kwargs: Any extra arguments that will be sent to the super class
        """
        super(MeasurementStationData, self).__init__(source_parameters=source_parameters, **kwargs)

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}.{executor_name}")

        data_points = list(source_parameters.get("data points", self._parameter_mapping.keys()))
        invalid_data_points = set(data_points).difference(self._parameter_mapping.keys())
        if len(invalid_data_points) > 0:
            raise ValueError(f"Invalid entries in the list of data points: {invalid_data_points}. Only "
                             f"{list(self._parameter_mapping.keys())} supported")

        self._static_request_parameters = {
            "station_ids": source_parameters["station id"],  # Visible from the web GUI
            "output_format": "geojson",  # Choose GeoJSON over CSV to have TZ-Aware timestamps
            "filename": "measurements",
            "parameters": data_points
        }

        self._extractors = self._compile_extractors(data_points)

        initial_history = pd.to_timedelta(source_parameters.get("initial history", "48h"))
        self._last_query_ts = self._align_ts_to_data(datetime.datetime.now(tz=datetime.timezone.utc) - initial_history)
        self._last_query_message = {}

    @staticmethod
    def _align_ts_to_data(ts: datetime.datetime) -> datetime.datetime:
        """Aligns the datetime to the data source boundaries (daily)"""
        ts = ts.astimezone(tz=datetime.timezone.utc)
        return datetime.datetime(ts.year, ts.month, ts.day, 0, 0, 0, tzinfo=datetime.timezone.utc)  # Floor

    @staticmethod
    def _compile_extractors(data_points: List[str]) -> List[jx.PathExtractor]:
        """Generates the list of extractors based on the selected data points"""

        # Static features:
        dst_extractors = [
            jx.PathExtractor("longitude", "features[*].geometry.coordinates[0]", is_list=False),
            jx.PathExtractor("latitude", "features[*].geometry.coordinates[1]", is_list=False),
            jx.DatetimePathExtractor("observation_time", "timestamps", is_list=True),
        ]

        # Dynamically selected features
        dst_extractors += [
            jx.PathExtractor(MeasurementStationData._parameter_mapping[data_point],
                             f"features[*].properties.parameters.{data_point}.data[*]", is_list=True)
            for data_point in data_points
        ]
        return dst_extractors

    def fetch_data(self, raw_data: Optional[dict] = None) -> Dict[str, Any]:
        """
        Fetches the latest available measurements and returns the result.

        :param raw_data: A geojson string to test the decoding functionality in detail
        :return: The conventional Redis message
        """

        if raw_data is None:
            raw_data = self._fetch_next_raw_result()

        if raw_data is None:
            self._logger.debug(f"No updated history, return the last message")
        else:
            self._last_query_message = self._decode_raw_message(raw_data)

        return self._last_query_message

    def _fetch_next_raw_result(self) -> Optional[str]:
        """Fetches the next period or returns None, in case no new data is available"""

        end_ts = self._align_ts_to_data(datetime.datetime.now(tz=datetime.timezone.utc))

        if self._last_query_ts >= end_ts:
            self._logger.warning(f"No updated data available. Last update: {self._last_query_ts.isoformat()}, Expected "
                                 f"data boundaries: {end_ts.isoformat()}")
            return None

        params = self._static_request_parameters.copy()
        params["start"] = self._last_query_ts.isoformat()
        params["end"] = end_ts.isoformat()

        response: requests.Response = self.session.get(
            "https://dataset.api.hub.zamg.ac.at/v1/station/historical/klima-v1-10min",
            params=params
        )
        response.raise_for_status()

        self._last_query_ts = end_ts
        return response.json()

    def _decode_raw_message(self, raw_data: dict) -> Dict[str, Any]:
        """Decodes the raw message into the common message format"""

        redis_forecast = dict(itertools.chain(*[ext.extract_information(raw_data).items() for ext in self._extractors]))
        return redis_forecast
