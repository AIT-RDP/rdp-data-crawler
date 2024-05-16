"""
Implements supported ZAMG data sources
"""
import abc
import dataclasses
import datetime
import itertools
import logging
import math
from typing import Dict, Any, Optional, List

import data_crawler.access.jsonpath as jx
import data_crawler.sources.abc.http_cache as http_cache
import data_crawler.sources.abc.history as history

import pandas as pd
import requests


@dataclasses.dataclass
class _PRef:
    """Parameter reference to define some transformation rules"""
    name: str
    scaling: float = 1.0


@dataclasses.dataclass
class _EndpointConfig:
    """Groups the endpoint configuration of a single Geosphere endpoint"""

    parameter_mapping: Dict[str, _PRef]  # maps the Geosphere ID to the parameter reference description
    type: str  # Geosphere asset type (station, timeseries)
    mode: str  # Geosphere query mode (historical, current, forecast)
    resource_id: str  # The Geosphere resource ID (e.g., nwp-v1-1h-2500m)


class _AbstractGeosphereTimeSeriesAPI(http_cache.GenericHTTPSourceAPI, abc.ABC):
    """
    Implements some basic services to handle common Geosphere API arguments and to parse the geojson results
    """

    def __init__(self, source_parameters, executor_name, **kwargs):
        """
        Initializes the abstract Geosphere API that serves as a base for the particular implementations

        :param source_parameters: The source parameters according to the configuration
        :param executor_name: The name of the executor for debugging purpose
        :param kwargs: Any extra arguments that will be sent to the super class
        """
        super(_AbstractGeosphereTimeSeriesAPI, self).__init__(source_parameters=source_parameters, **kwargs)

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}.{executor_name}")

        endpoint_name = source_parameters.get("endpoint", list(self.endpoint_configs.keys())[0]).lower()
        if endpoint_name not in self.endpoint_configs:
            raise KeyError(f"Unsupported endpoint '{endpoint_name}'. Only {list(self.endpoint_configs.keys())} "
                           "are available.")

        self._local_mapping = self.endpoint_configs[endpoint_name].parameter_mapping
        data_points = list(source_parameters.get("data points", self._local_mapping.keys()))
        invalid_data_points = set(data_points).difference(self._local_mapping.keys())
        if len(invalid_data_points) > 0:
            raise ValueError(f"Invalid entries in the list of data points: {invalid_data_points}. Only "
                             f"{list(self._local_mapping.keys())} are supported for {endpoint_name}")

        self.static_request_parameters = {
            "parameters": data_points,
            "output_format": "geojson",  # Choose GeoJSON over CSV to have TZ-Aware timestamps
            "filename": "measurements",
        }
        self._endpoint_url = self._resolve_endpoint_url(self.endpoint_configs[endpoint_name])
        self._extractors = self._get_extractors(data_points)

        self._drop_missing_observations = source_parameters.get("drop missing observations", False)
        self._drop_excessive_time_stamps = source_parameters.get("drop excessive time stamps", True)

    @staticmethod
    def _resolve_endpoint_url(endpoint: _EndpointConfig) -> str:
        """Returns the base URL of the particular endpoint"""

        return f"https://dataset.api.hub.zamg.ac.at/v1/{endpoint.type}/{endpoint.mode}/{endpoint.resource_id}"

    @property
    def logger(self) -> logging.Logger:
        """Returns the API-specific logger of this object"""
        return self._logger

    @property
    @abc.abstractmethod
    def endpoint_configs(self) -> Dict[str, _EndpointConfig]:
        """
        Returns the endpoint configurations supported by the API

        For each supported endpoint, an own configuration must be returned. The first entry will be used as a default value.
        """
        return {}

    def _get_extractors(self, data_points: List[str]) -> List[jx.PathExtractor]:
        """
        Generates the list of default extractors and returns it

        The function may be overloaded to extend the list of extracted items.

        :param data_points: The list of requested data points
        """

        dst_extractors = [
            jx.PathExtractor("longitude", "features[*].geometry.coordinates[0]", is_list=False),
            jx.PathExtractor("latitude", "features[*].geometry.coordinates[1]", is_list=False),
            jx.DatetimePathExtractor("observation_time", "timestamps[*]", is_list=True),
        ]

        # Dynamically selected features
        for data_point in data_points:
            reference = self._local_mapping[data_point]
            dst_extractors.append(jx.PathExtractor(
                reference.name, f"features[*].properties.parameters.{data_point}.data[*]",
                # Use early binding for scale (sc), not default late binding taking the last iteration's value!
                is_list=True, dst_format=lambda x, sc=reference.scaling: (x * sc if x is not None else None),
                drop_missing=True
            ))
        return dst_extractors

    def _parse_raw_message(self, raw_data: dict) -> Dict[str, Any]:
        """
        Extracts the raw message content and returns the destination message ready to be returned by the API

        In case one needs to implement custom parsing logic and transformations in a derived class, this may be a good
        entry point.

        :param raw_data: The geojson-formatted content
        :return: The processed and filtered message
        """

        decoded_message = self._decode_raw_message(raw_data)

        if self._drop_excessive_time_stamps:
            decoded_message = self._drop_leading_none_time_stamps(decoded_message)

        if self._drop_missing_observations:
            decoded_message = self._drop_all_none_observations(decoded_message)

        return decoded_message

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

    def fetch_message(self, dynamic_request_parameters: Optional[dict] = None,
                      raw_data: Optional[dict] = None) -> dict:
        """
        Queries the remote endpoint using the additional dynamic parameters, if needed and returns the parsed message

        This function is intended to be used in derived classes but may not make sense outside the class hierarchy.

        :param dynamic_request_parameters: Any dynamic request parameters that are needed to get the results
        :param raw_data: The raw data that may be injected for testing purpose. In case None is given, the server will
            be queried as usual
        :return: The parsed message ready to be returned
        """

        if raw_data is None:
            raw_data = self._fetch_raw_data(dynamic_request_parameters)

        decoded_message = self._parse_raw_message(raw_data)
        return decoded_message

    def _fetch_raw_data(self, dynamic_request_parameters: Optional[dict] = None):
        """Fetches the raw data from the remote endpoint"""

        params = self.static_request_parameters.copy()
        if dynamic_request_parameters is not None:
            params.update(dynamic_request_parameters)

        response: requests.Response = self.session.get(self._endpoint_url, params=params)
        response.raise_for_status()
        return response.json()


class MeasurementStationData(_AbstractGeosphereTimeSeriesAPI, history.AbstractTimedHistorySourceMixin):
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
    _endpoint_configs = {
        "climate": _EndpointConfig(parameter_mapping={
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
        }, type="station", mode="historical", resource_id="klima-v1-10min"),

        "tawes": _EndpointConfig(parameter_mapping={
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
        }, type="station", mode="historical", resource_id="tawes-v1-10min")
    }

    def __init__(self, source_parameters, executor_name, **kwargs):
        """
        Initializes the forecasting API but does not trigger any query

        :param source_parameters: The source parameters according to the configuration
        :param executor_name: The name of the executor for debugging purpose
        :param kwargs: Any extra arguments that will be sent to the super class
        """
        super(MeasurementStationData, self).__init__(source_parameters=source_parameters, executor_name=executor_name,
                                                     **kwargs)
        self.static_request_parameters.update({
            "station_ids": source_parameters["station id"],  # Visible from the web GUI
        })

        initial_history = pd.to_timedelta(source_parameters.get("initial history", "48h"))
        self._last_query_ts = datetime.datetime.now(tz=datetime.timezone.utc) - initial_history

    @property
    def endpoint_configs(self) -> Dict[str, _EndpointConfig]:
        return self._endpoint_configs

    def _get_extractors(self, data_points: List[str]) -> List[jx.PathExtractor]:
        """Overloads the generic extractors by some custom ones"""

        dst_extractors = super()._get_extractors(data_points)
        dst_extractors += [
            jx.PathExtractor("zamg_station_id", "features[*].properties.station", is_list=False),
        ]
        return dst_extractors

    def fetch_data(self, raw_data: Optional[dict] = None) -> Dict[str, Any]:
        """
        Fetches the latest available measurements and returns the result.

        :param raw_data: A geojson string to test the decoding functionality in detail
        :return: The conventional Redis message
        """

        now_ts = datetime.datetime.now(tz=datetime.timezone.utc)
        decoded_message = self._fetch_measurement_message(self._last_query_ts, now_ts, raw_data)

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

        decoded_message = self._fetch_measurement_message(start_time, end_time, raw_data)
        return decoded_message

    def _fetch_measurement_message(self, start_time: datetime.datetime, end_time: datetime.datetime,
                                   raw_data: Optional[dict] = None) -> Dict[str, Any]:
        """
        Fetches and decodes the message according to the source configuration but does not advance any internal state
        """

        self.logger.debug(f"Try to fetch and process new readings starting from {start_time.isoformat()} to "
                          f"{end_time.isoformat()}")

        dynamic_request_parameters = {
            "start": start_time.isoformat(),
            "end": end_time.isoformat()
        }
        return self.fetch_message(dynamic_request_parameters, raw_data)


class NumericalWeatherPredictionData(_AbstractGeosphereTimeSeriesAPI):
    """
    Implements the AROME NWP data access.

    The API documentation could be found at https://dataset.api.hub.geosphere.at/v1/docs/user-guide/endpoints.html
    Meta-data is available at https://dataset.api.hub.geosphere.at/v1/timeseries/forecast/nwp-v1-1h-2500m/metadata
    """

    _endpoint_configs = {
        "nwp": _EndpointConfig(parameter_mapping={
            "cape": _PRef("convective_available_potential_energy"),  # [m2 s-2]
            "cin": _PRef("convective_inhibition"),  # [J kg-1]
            "grad": _PRef("_global_horizontal_irradiation_acc"),  # Seems to be an accumulated value. [Ws m-2]
            "mnt2m": _PRef("air_temperature_min_2m"),  # [degC]
            "mxt2m": _PRef("air_temperature_max_2m"),  # [degC]
            "rain_acc": _PRef("_rainfall_mass_total"),  # [kg m-2] Accumulated since start of the forecast
            "rh2m": _PRef("relative_humidity_2m"),  # [%]
            "rr_acc": _PRef("_precipitation_mass_total"),  # [kg m-2] Accumulated since start of the forecast
            "snow_acc": _PRef("snow_surface_mass"),  # [kg m-2] amount on the solid ground
            "snowlmt": _PRef("snowlimit"),  # [m above ground]
            "sp": _PRef("air_pressure", scaling=0.01),  # [Pa]
            "sundur_acc": _PRef("_sunshine_duration_total"),  # [s] sunshine duration since forecast start
            "t2m": _PRef("air_temperature_2m"),  # [degC]
            "tcc": _PRef("cloud_area_fraction", scaling=100),  # [1]
            "u10m": _PRef("_wind_speed_u_10m"),  # [m s-1] wind speed in eastward direction
            "v10m": _PRef("_wind_speed_v_10m"),  # [m s-1] wind speed in northward direction
            "ugust": _PRef("_wind_speed_gust_u_10m"),  # [m s-1]
            "vgust": _PRef("_wind_speed_gust_v_10m"),  # [m s-1]
        }, type="timeseries", mode="forecast", resource_id="nwp-v1-1h-2500m")
    }

    def __init__(self, source_parameters, executor_name, **kwargs):
        """
        Initializes the forecasting API but does not trigger any query

        :param source_parameters: The source parameters according to the configuration
        :param executor_name: The name of the executor for debugging purpose
        :param kwargs: Any extra arguments that will be sent to the super class
        """
        super(NumericalWeatherPredictionData, self).__init__(source_parameters=source_parameters,
                                                             executor_name=executor_name, **kwargs)

        self.static_request_parameters.update({
            "lat_lon": f"{source_parameters['latitude']},{source_parameters['longitude']}"
        })

    @property
    def endpoint_configs(self) -> Dict[str, _EndpointConfig]:
        return self._endpoint_configs

    def _get_extractors(self, data_points: List[str]) -> List[jx.PathExtractor]:
        """Overloads the generic extractors by some custom ones"""

        dst_extractors = super()._get_extractors(data_points)
        dst_extractors += [
            jx.PathExtractor("forecast_time", "reference_time", is_list=False),
        ]
        return dst_extractors

    def fetch_data(self, raw_data: Optional[dict] = None) -> Dict[str, Any]:
        """
        Fetches the latest available forecast and returns the result.

        :param raw_data: A geojson string to test the decoding functionality in detail
        :return: The conventional Redis message
        """

        out_message = self.fetch_message(raw_data=raw_data)
        out_message = self._extend_computed_outputs(out_message)
        return out_message

    def _extend_computed_outputs(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Generates the outputs that need further computation."""

        time = [datetime.datetime.fromisoformat(ts) for ts in message["observation_time"]]

        if "_wind_speed_u_10m" in message and "_wind_speed_v_10m" in message:
            message["wind_speed_10m"], message["wind_direction_10m"] = self._to_abs_dir(
                message["_wind_speed_u_10m"], message["_wind_speed_v_10m"]
            )

        if "_global_horizontal_irradiation_acc" in message:
            message["global_horizontal_irradiation"] = self._to_cnt_derivative(
                time, message["_global_horizontal_irradiation_acc"], "_global_horizontal_irradiation_acc"
            )

        if "_rainfall_mass_total" in message and "air_temperature_2m" in message:
            rain_mass_rate = self._to_cnt_derivative(
                time, message["_rainfall_mass_total"], "_rainfall_mass_total"
            )
            temperature = message["air_temperature_2m"]
            precipitation_1h = self._to_1h_precipitation_rain(rain_mass_rate, temperature)
            if precipitation_1h is not None:
                message["rainfall_total_1h"] = precipitation_1h

        return message

    def _to_1h_precipitation_rain(self, rain_mass_rate: List[float], temperature: List[float]) -> Optional[List[float]]:
        """Estimates the rain mass rate based on the temperature estimate and tabulated data"""

        try:
            # Optional dependencies!
            import pandas as pd
            import numpy as np
        except ModuleNotFoundError:
            return None

        rain_mass_rate = pd.Series(rain_mass_rate)
        temperature = pd.Series(temperature)

        # temperature [degC] -> density [kg/dm^3], taken from https://www.engineeringtoolbox.com/water-density-specific-weight-d_595.html
        density_at_saturation_pressure = pd.Series({
            0.1: 0.9998495,
            1: 0.9999017,
            4: 0.9999749,
            10: 0.9997000,
            15: 0.9991026,
            20: 0.9982067,
            25: 0.9970470,
            30: 0.9956488,
            35: 0.9940326,
            40: 0.9922152,
            45: 0.99021,
            50: 0.98804,
            55: 0.98569,
            60: 0.98320
        })

        temperature = temperature.clip(lower=density_at_saturation_pressure.index[0],
                                       upper=density_at_saturation_pressure.index[-1])
        density = np.interp(temperature, density_at_saturation_pressure.index, density_at_saturation_pressure)
        precipitation_1h = rain_mass_rate * 3600 / density

        # Convert to a list of values having None instead of NaN for unification
        precipitation_1h = list(x if not math.isnan(x) else None for x in precipitation_1h)
        return precipitation_1h

    @staticmethod
    def _to_abs_dir(u_values, v_values):
        """
        Converts the u/v representation to an abs, dir representation and returns it.
        :param u_values: wind in eastwards direction (coming from west)
        :param v_values: wind in northward direction (coming from south)
        :return: The absolute quantity and the direction in degrees (0° is north, 90° east, etc.)
        """

        direction = [
            math.atan2(u, v) * 180.0 / math.pi + 180
            for u, v in zip(u_values, v_values)
        ]

        absolute = [
            math.sqrt(x * x + y * y)
            for x, y in zip(u_values, v_values)
        ]
        return absolute, direction

    def _to_cnt_derivative(self, time: List[datetime.datetime], values: List[float],
                           measurement: str = "<unknown>") -> List[float]:
        """
        Computes the discrete derivative of the given counter.

        Since the first values may not be passed on to the forecast in case it is fetched later on, the counters may not
        start at zero. To avoid invalid first samples, the first element returned is always None. It is also assumed
        that the integral of values was performed using the units seconds abd that the counter always counts upwards.
        Since it is observed that some conters slightly count downwards, any negative derivative will be rounded to zero
        and added to the next number. to cancel out subsequent faults.
        """
        assert len(time) == len(values)

        ret: List[Optional[float]] = [None] if len(time) > 0 else []  # Return an empty array, if no elements are given
        carry = 0.0
        for i in range(1, len(time)):
            dt = (time[i] - time[i - 1]).total_seconds()
            dv = (values[i] - values[i - 1]) + carry

            if dv < 0.0:
                self.logger.warning(f"'{measurement}' returned by Geosphere counts downwards by {dv}. Round to 0.0.")
                carry = dv
                dv = 0.0
            else:
                carry = 0.0

            ret.append(dv / dt)
        return ret
