"""
Implements supported ZAMG data sources
"""
import dataclasses
import datetime
import itertools
import logging
from typing import Dict, Any, Optional, List

import data_crawler.access.jsonpath as jx
import data_crawler.sources.abc.http_cache as http_cache
import data_crawler.sources.abc.history as history

import pandas as pd
import requests


class MeasurementStationData(http_cache.GenericHTTPSourceAPI, history.AbstractTimedHistorySourceMixin):
    """
    Fetches the measurement station data provided in 10 minute resolution

    The API currently implements the climate and tawes endpoint which mostly differ by their delay and available data
    points. A description of the climate dataset can be found at https://data.hub.zamg.ac.at/dataset/klima-v1-10min .
    Be aware that the data is only available until midnight, the day before. Hence, the data source aligns all requests
    to a full day and only updates new days, if needed. The meta-fields can be accessed at
    https://dataset.api.hub.zamg.ac.at/v1/station/historical/klima-v1-10min/metadata .

    Similarly, the TAWES meta fields are accessible at
    https://dataset.api.hub.zamg.ac.at/v1/station/historical/tawes-v1-10min/metadata . Note that both TAWES and climate
    datasets use different station ids. One can use the meta-endpoint to extract the station ids as well.
    """

    @dataclasses.dataclass
    class _PRef:
        """Parameter reference to define some transformation rules"""
        name: str
        scaling: float = 1.0

    _parameter_mapping = {
        "climate": {  # Parameter mappings for the climate endpoint
            "TL": _PRef(name="air_temperature_2m"),  # [degC]
            "TP": _PRef(name="dew_point_temperature_2m"),  # [degC]
            "RF": _PRef(name="relative_humidity_2m"),  # [%]

            "P": _PRef(name="air_pressure"),  # [hPa]
            "P0": _PRef(name="air_pressure_at_sea_level"),  # [hPa]

            "DD": _PRef(name="wind_direction_10m"),  # [deg]
            "DDX": _PRef(name="wind_direction_gust_10m"),  # [deg]
            "FF": _PRef(name="wind_speed_10m"),  # [m/s]
            "FFX": _PRef(name="wind_speed_gust_10m"),  # [m/s]

            "GSX": _PRef(name="global_horizontal_irradiation"),  # [W/m^2]
            "HSX": _PRef(name="diffuse_irradiation"),  # [W/m^2]

            "RR": _PRef(name="precipitation_total_10min"),  # [mm]
            "RRM": _PRef(name="precipitation_flag"),  # [1]
            "SH": _PRef(name="snow_depth", scaling=10.0),  # [cm] converted to [mm]

            "TB1": _PRef(name="soil_temperature_10_cm"),  # [degC]
            "TB2": _PRef(name="soil_temperature_20_cm"),  # [degC]
            "TB3": _PRef(name="soil_temperature_50_cm"),  # [degC]
            "TS": _PRef(name="air_temperature_5cm"),  # [degC]
        },
        "tawes": {  # Parameter mapping for the tawes endpoint
            "TL": _PRef(name="air_temperature_2m"),  # [degC]
            "TP": _PRef(name="dew_point_temperature_2m"),  # [degC]
            "RFAM": _PRef(name="relative_humidity_2m"),  # [%]

            "P": _PRef(name="air_pressure"),  # [hPa]
            "PRED": _PRef(name="air_pressure_at_sea_level"),  # [hPa]

            "DD": _PRef(name="wind_direction_10m"),  # [deg]
            "DDX": _PRef(name="wind_direction_gust_10m"),  # [deg]
            "FFAM": _PRef(name="wind_speed_10m"),  # [m/s]
            "FFX": _PRef(name="wind_speed_gust_10m"),  # [m/s]

            "GLOW": _PRef(name="global_horizontal_irradiation"),  # [W/m^2]

            "RR": _PRef(name="precipitation_total_10min"),  # [mm]
            "RRM": _PRef(name="precipitation_flag"),  # [1]
            "SCHNEE": _PRef(name="snow_depth", scaling=10.0),  # [cm] converted to [mm]

            "TB1": _PRef(name="soil_temperature_10_cm"),  # [degC]
            "TB2": _PRef(name="soil_temperature_20_cm"),  # [degC]
            "TB3": _PRef(name="soil_temperature_50_cm"),  # [degC]
            "TS": _PRef(name="air_temperature_5cm"),  # [degC]
        }
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

        endpoint_name = source_parameters.get("endpoint", "climate")
        self._endpoint_url = self._resolve_endpoint(endpoint_name)
        self._local_mapping = self._parameter_mapping[endpoint_name.lower()]

        data_points = list(source_parameters.get("data points", self._local_mapping.keys()))
        invalid_data_points = set(data_points).difference(self._local_mapping.keys())
        if len(invalid_data_points) > 0:
            raise ValueError(f"Invalid entries in the list of data points: {invalid_data_points}. Only "
                             f"{list(self._local_mapping.keys())} are supported for {endpoint_name}")

        self._static_request_parameters = {
            "station_ids": source_parameters["station id"],  # Visible from the web GUI
            "output_format": "geojson",  # Choose GeoJSON over CSV to have TZ-Aware timestamps
            "filename": "measurements",
            "parameters": data_points
        }

        self._extractors = self._compile_extractors(data_points, self._local_mapping)

        initial_history = pd.to_timedelta(source_parameters.get("initial history", "48h"))
        self._last_query_ts = datetime.datetime.now(tz=datetime.timezone.utc) - initial_history

        self._drop_missing_observations = source_parameters.get("drop missing observations", False)
        self._drop_excessive_time_stamps = source_parameters.get("drop excessive time stamps", True)

    @staticmethod
    def _resolve_endpoint(endpoint_name: str) -> str:
        """Resolves the endpoint name into an URL"""

        endpoints = {
            "climate": "https://dataset.api.hub.zamg.ac.at/v1/station/historical/klima-v1-10min",
            "tawes": "https://dataset.api.hub.zamg.ac.at/v1/station/historical/tawes-v1-10min",
        }

        endpoint_name = endpoint_name.lower()
        if endpoint_name not in endpoints:
            raise ValueError(f"Unknown measurement station endpoint '{endpoint_name}', only "
                             f"{list(endpoints.keys())} supported.")
        return endpoints[endpoint_name]

    @staticmethod
    def _compile_extractors(data_points: List[str], local_mapping: Dict[str, _PRef]) -> List[jx.PathExtractor]:
        """Generates the list of extractors based on the selected data points"""

        # Static features:
        dst_extractors = [
            jx.PathExtractor("longitude", "features[*].geometry.coordinates[0]", is_list=False),
            jx.PathExtractor("latitude", "features[*].geometry.coordinates[1]", is_list=False),
            jx.PathExtractor("zamg_station_id", "features[*].properties.station", is_list=False),
            jx.DatetimePathExtractor("observation_time", "timestamps[*]", is_list=True),
        ]

        # Dynamically selected features
        for data_point in data_points:
            reference = local_mapping[data_point]
            dst_extractors.append(jx.PathExtractor(
                reference.name, f"features[*].properties.parameters.{data_point}.data[*]",
                # Use early binding for scale (sc), not default late binding taking the last iteration's value!
                is_list=True, dst_format=lambda x, sc=reference.scaling: (x * sc if x is not None else None),
                drop_missing=True
            ))
        return dst_extractors

    def fetch_data(self, raw_data: Optional[dict] = None) -> Dict[str, Any]:
        """
        Fetches the latest available measurements and returns the result.

        :param raw_data: A geojson string to test the decoding functionality in detail
        :return: The conventional Redis message
        """

        now_ts = datetime.datetime.now(tz=datetime.timezone.utc)
        decoded_message = self._fetch_message(self._last_query_ts, now_ts, raw_data)

        if len(decoded_message["observation_time"]) > 0:
            self._last_query_ts = datetime.datetime.fromisoformat(decoded_message["observation_time"][-1])
        else:
            self._logger.warning(f"No new data is available. The last observations are from {self._last_query_ts}.")

        return decoded_message

    def fetch_historic_data(self, start_time: datetime.datetime, end_time: datetime.datetime,
                            filter_clauses: Dict[str, Any], raw_data: Optional[dict] = None) -> Dict[str, Any]:
        """
        Fetches the historic data and outputs it in a single message.

        :param start_time: The interval start of the query period
        :param end_time: The interval end of the query period
        :param filter_clauses: any additional filter clauses. Will be gracefully ignored.
        :param raw_data: Optional raw data that may be passed on for testing the decoding capabilities
        :return: The resulting output message
        """

        if len(filter_clauses) > 0:
            raise ValueError(f"Unsupported filter clauses: {list(filter_clauses.keys())}")

        decoded_message = self._fetch_message(start_time, end_time, raw_data)
        return decoded_message

    def _fetch_message(self, start_time: datetime.datetime, end_time: datetime.datetime,
                       raw_data: Optional[dict] = None) -> Dict[str, Any]:
        """
        Fetches and decodes the message according to the source configuration but does not advance any internal state
        """

        if raw_data is None:
            raw_data = self._fetch_next_raw_result(start_time, end_time)

        decoded_message = self._decode_raw_message(raw_data)

        if self._drop_excessive_time_stamps:
            decoded_message = self._drop_leading_none_time_stamps(decoded_message)

        if self._drop_missing_observations:
            decoded_message = self._drop_all_none_observations(decoded_message)

        return decoded_message

    def _fetch_next_raw_result(self, start_time: datetime.datetime, end_time: datetime.datetime) -> dict:
        """Fetches the raw data of the next period"""

        end_time = end_time.astimezone(datetime.timezone.utc)
        start_time = start_time.astimezone(datetime.timezone.utc)

        params = self._static_request_parameters.copy()
        params["start"] = start_time.isoformat()
        params["end"] = end_time.isoformat()

        self._logger.debug(f"Try to fetch new readings starting from {start_time.isoformat()} to "
                           f"{end_time.isoformat()}")
        response: requests.Response = self.session.get(self._endpoint_url, params=params)
        response.raise_for_status()
        return response.json()

    def _decode_raw_message(self, raw_data: dict) -> Dict[str, Any]:
        """Decodes the raw message into the common message format"""

        redis_forecast = dict(itertools.chain(*[ext.extract_information(raw_data).items() for ext in self._extractors]))
        return redis_forecast

    def _drop_all_none_observations(self, decoded_message: dict) -> dict:
        """Drops all missing observations and returns the result"""

        dropped_keys = []
        all_ext_keys = {k.name for k in self._local_mapping.values()}
        for obs_key in all_ext_keys:
            if obs_key in decoded_message and all(map(lambda x: x is None, decoded_message[obs_key])):
                dropped_keys.append(obs_key)
                del decoded_message[obs_key]

        self._logger.debug(f"Dropped the empty keys {dropped_keys} on request.")
        return decoded_message

    def _drop_leading_none_time_stamps(self, decoded_message: dict) -> dict:
        """Drops all leading time instants that only have associated none values"""

        decoded_message = decoded_message.copy()

        all_ext_keys = {k.name for k in self._local_mapping.values() if k.name in decoded_message}
        dropped_ts = []
        while len(decoded_message["observation_time"]) > 0:
            if any(decoded_message[k][-1] is not None for k in all_ext_keys):
                break

            dropped_ts += [decoded_message["observation_time"][-1]]
            decoded_message["observation_time"] = decoded_message["observation_time"][:-1]

            for k in all_ext_keys:
                decoded_message[k] = decoded_message[k][:-1]

        self._logger.debug(f"Dropped {len(dropped_ts)} empty time instants: {dropped_ts}")
        return decoded_message
