"""
Implements the interface to the Open-Meteo Weather Forecast API

See https://open-meteo.com/en/docs for the API documentation.
"""
import datetime
import itertools
from abc import ABC
from typing import Dict, Any, List, Optional, Iterable, Iterator
import json
import logging

import requests
import pydantic

import data_crawler.sources.abc.http_cache as http_cache
import data_crawler.sources.abc.abstract_source as abstract_source
import data_crawler.sources.abc.history as history
import data_crawler.access.jsonpath as jx
from data_crawler.sources.abc.message import Message, MessageData
from typing import Generator


class _OpenMeteoBase(http_cache.SyncHTTPMixin, abstract_source.AbstractMultiMessageSourceAPI, ABC):
    """
    Abstract base class for Open-Meteo sources

    The class holds common functionalities such as message parsing helpers to avoid duplicate code
    """

    def __init__(self, source_parameters: dict, executor_name: str = "-", **kwargs):
        """
        Initializes the Open-Meteo base class

        :param executor_name: The name of the executor for debugging purpose
        :param source_parameters: The source parameters according to the configuration.
        :param kwargs: Any extra arguments that will be sent to the super class
        """
        super(_OpenMeteoBase, self).__init__(
            source_parameters=source_parameters, executor_name=executor_name, **kwargs
        )

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}.{executor_name}")

    @staticmethod
    def _extract_common_metadata(raw_forecast: dict) -> Dict[str, Any]:
        """
        Extracts common metadata (lat, lon, altitude, generation time) from raw forecast

        :param raw_forecast: The raw JSON response from Open-Meteo API
        :return: Dictionary with common metadata fields
        """
        extractors = [
            jx.PathExtractor("longitude", "longitude", is_list=False),
            jx.PathExtractor("latitude", "latitude", is_list=False),
            jx.PathExtractor("altitude", "elevation", is_list=False, drop_missing=True),
            jx.PathExtractor("generationtime_ms", "generationtime_ms", is_list=False, drop_missing=True),
        ]

        return dict(itertools.chain(*[ext.extract_information(raw_forecast).items() for ext in extractors]))

    @staticmethod
    def _transform_hourly_data(raw_forecast: dict, hourly_variables: List[str]) -> Dict[str, Any]:
        """
        Transforms the Open-Meteo hourly forecast data to the common Redis representation

        :param raw_forecast: The raw JSON response from Open-Meteo API
        :param hourly_variables: List of hourly variables that were queried
        :return: Dictionary in the common Redis format with hourly data
        """
        if "hourly" not in raw_forecast:
            return {}

        variable_mapping = _OpenMeteoBase._get_variable_mapping()
        extractors = []

        # Time axis - use [*] to extract individual items from the array
        extractors.append(jx.DatetimePathExtractor("observation_time", "hourly.time[*]"))

        # Add extractors for each hourly variable that was queried
        for var in hourly_variables:
            internal_name = variable_mapping.get(var, var)
            src_path = f"hourly.{var}[*]"
            extractors.append(
                jx.PathExtractor(internal_name, src_path, is_list=True, drop_missing=True)
            )

        result = dict(itertools.chain(*[ext.extract_information(raw_forecast).items() for ext in extractors]))
        return result

    @staticmethod
    def _get_variable_mapping() -> Dict[str, str]:
        """
        Returns the mapping from Open-Meteo variable names to our internal nomenclature

        :return: Dictionary mapping Open-Meteo variable names to internal names
        """
        return {
            # Temperature
            "temperature_2m": "air_temperature_2m",
            "apparent_temperature": "apparent_temperature",
            # Humidity
            "relative_humidity_2m": "relative_humidity_2m",
            "dew_point_2m": "dew_point_temperature_2m",
            # Pressure
            "pressure_msl": "air_pressure_at_sea_level",
            "surface_pressure": "surface_pressure",
            # Clouds
            "cloud_cover": "cloud_area_fraction",
            "cloud_cover_low": "cloud_area_fraction_low",
            "cloud_cover_mid": "cloud_area_fraction_medium",
            "cloud_cover_high": "cloud_area_fraction_high",
            # Visibility
            "visibility": "visibility",
            # Wind 10m
            "wind_speed_10m": "wind_speed_10m",
            "wind_direction_10m": "wind_direction_10m",
            "wind_gusts_10m": "wind_gusts_10m",
            # Wind at higher altitudes (for wind energy)
            "wind_speed_80m": "wind_speed_80m",
            "wind_speed_100m": "wind_speed_100m",
            "wind_speed_120m": "wind_speed_120m",
            "wind_speed_180m": "wind_speed_180m",
            "wind_direction_80m": "wind_direction_80m",
            "wind_direction_120m": "wind_direction_120m",
            "wind_direction_100m": "wind_direction_100m",
            "wind_direction_180m": "wind_direction_180m",
            # Precipitation
            "precipitation": "precipitation_total",
            "precipitation_probability": "precipitation_probability",
            "rain": "rain",
            "snowfall": "snowfall",
            "weather_code": "weather_code",
            "showers": "precipitation_showers",
            "snow_depth": "snow_depth",
            # Radiation
            "shortwave_radiation": "global_horizontal_irradiation",
            "direct_radiation": "direct_radiation",
            "diffuse_radiation": "diffuse_radiation",
            "direct_normal_irradiance": "direct_normal_irradiance",
            # UV
            "uv_index": "uv_index",
        }


class OpenMeteoForecast(history.AbstractTimedMultiMessageHistorySourceMixin,
                        _OpenMeteoBase):
    """
    Queries the Open-Meteo Weather Forecast API
    
    The Open-Meteo API provides access to various weather models. Using `models=best_match` 
    (the default), it automatically selects the best available model for the given location.
    
    This implementation provides weather variables aligned with those used by weatherbit 
    and yr.no sources, plus wind data at various heights (10m, 80m, 120m, 180m) which is 
    useful for wind energy applications.
    """

    # Default hourly variables to query - aligned with weatherbit and yr.no
    DEFAULT_HOURLY_VARIABLES = [
        # Temperature and humidity
        "temperature_2m",
        "relative_humidity_2m",
        "dew_point_2m",
        "apparent_temperature",
        # Pressure
        "pressure_msl",
        "surface_pressure",
        # Clouds
        "cloud_cover",
        "cloud_cover_low",
        "cloud_cover_mid",
        "cloud_cover_high",
        # Visibility
        "visibility",
        # Wind at various heights
        "wind_speed_10m",
        "wind_speed_80m",
        "wind_speed_120m",
        "wind_speed_180m",
        "wind_direction_10m",
        "wind_direction_80m",
        "wind_direction_120m",
        "wind_direction_180m",
        "wind_gusts_10m",
        # Precipitation
        "precipitation",
        "precipitation_probability",
        "rain",
        "snowfall",
        "snow_depth",
        "weather_code",
        # Radiation
        "shortwave_radiation",
        "direct_radiation",
        "diffuse_radiation",
        # UV
        "uv_index",
    ]

    # Default 15-minute variables (subset of hourly variables that are available at 15-minute resolution)
    # Note: Not all hourly variables are available at 15-minute intervals (e.g., wind_speed_180m, precipitation_probability)
    DEFAULT_MINUTELY_15_VARIABLES = [
        # Temperature and humidity
        "temperature_2m",
        "relative_humidity_2m",
        "dew_point_2m",
        "apparent_temperature",
        # Pressure
        "pressure_msl",
        "surface_pressure",
        # Clouds
        "cloud_cover",
        # Visibility
        "visibility",
        # Wind (lower altitudes available at 15-min resolution)
        "wind_speed_10m",
        "wind_speed_80m",
        "wind_speed_120m",
        "wind_direction_10m",
        "wind_direction_80m",
        "wind_direction_120m",
        "wind_gusts_10m",
        # Precipitation
        "precipitation",
        "rain",
        "snowfall",
        "weather_code",
        # Radiation
        "shortwave_radiation",
        "direct_radiation",
        "diffuse_radiation",
        "direct_normal_irradiance"
    ]

    API_ENDPOINT = "https://api.open-meteo.com/v1/forecast"

    def __init__(self, source_parameters: dict, executor_name: str = "-", **kwargs):
        """
        Initializes the Open-Meteo forecasting API but does not trigger any query
        :param executor_name: The name of the executor for debugging purpose
        :param source_parameters: The source parameters according to the configuration.
            Required keys:
              - latitude: float, the latitude of the location
              - longitude: float, the longitude of the location
            Optional keys:
              - hourly_variables: list[str], the hourly variables to query (defaults to DEFAULT_HOURLY_VARIABLES)
              - enable_minutely_15: bool, whether to include 15-minute data (default False)
              - enable_hourly: bool, whether to include hourly data (default True)
              - minutely_15_variables: list[str], the 15-minute variables to query 
                  (defaults to DEFAULT_MINUTELY_15_VARIABLES if enable_minutely_15 is True)
              - forecast_days: int, number of forecast days for hourly data (1-16, default 7)
              - forecast_minutely_15: int, number of 15-minute timesteps for minutely data 
                  (default 96 = 24 hours). Only used if enable_minutely_15 is True.
                  Note: 15-minute data is limited to ~4 days (384 timesteps) by the API.
              - past_days: int, number of past days to include (0-92, default 0)
              - altitude: float, custom elevation in meters
              - models: str, the weather model to use (default "best_match")
                  Common options: "best_match", "ecmwf_ifs04", "gfs_seamless", "icon_seamless"
              - wind_speed_unit: str, the unit of the wind speed (default "ms" other option "kmh")
              - history_batch_size: int, number of days per historic data request (default 30)
        :param kwargs: Any extra arguments that will be sent to the super class
        """
        super(OpenMeteoForecast, self).__init__(
            source_parameters=source_parameters, executor_name=executor_name, **kwargs
        )

        self._latitude = float(source_parameters["latitude"])
        self._longitude = float(source_parameters["longitude"])

        # Optional parameters
        self._hourly_variables = source_parameters.get("hourly_variables", self.DEFAULT_HOURLY_VARIABLES)
        self._enable_minutely_15 = source_parameters.get("enable_minutely_15", False)
        self._enable_hourly = source_parameters.get("enable_hourly", True)
        self._minutely_15_variables = source_parameters.get("minutely_15_variables", self.DEFAULT_MINUTELY_15_VARIABLES)
        self._forecast_days = source_parameters.get("forecast_days", 7)
        self._forecast_minutely_15 = source_parameters.get("forecast_minutely_15", 96)  # 24 hours = 96 timesteps
        self._past_days = source_parameters.get("past_days", 0)
        self._altitude = source_parameters.get("altitude")
        self._models = source_parameters.get("models", "best_match")
        self._wind_speed_unit = source_parameters.get("wind_speed_unit", "ms")
        self._history_batch_size = int(source_parameters.get("history_batch_size", 30))

        if not self._enable_hourly and not self._enable_minutely_15:
            raise ValueError("At least one of 'enable_hourly' or 'enable_minutely_15' must be set to True.")

    @property
    def _static_request_parameters(self) -> Dict[str, Any]:
        """Returns a copy of the static request parameters for testing purpose"""
        params = {
            "latitude": self._latitude,
            "longitude": self._longitude,
            "timezone": "UTC",  # Only UTC is supported right now to avoid timezone conversion issues
            "models": self._models,
        }

        # Add hourly data if enabled
        if self._enable_hourly:
            params["hourly"] = ",".join(self._hourly_variables)

        # Add 15-minute data if enabled
        if self._enable_minutely_15:
            params["minutely_15"] = ",".join(self._minutely_15_variables)

        if self._altitude is not None:
            params["elevation"] = self._altitude
        if self._wind_speed_unit is not None:
            params["wind_speed_unit"] = self._wind_speed_unit

        self._logger.debug(f"Static_request_parameters: {json.dumps(params, indent=4)}")

        return params

    def get_forecast_request_parameters(self, start_time: Optional[datetime.datetime] = None,
                                        end_time: Optional[datetime.datetime] = None) -> Dict[str, Any]:
        """
        Returns the request parameters for the forecast API call, including optional start and stop times.

        In case the start_time and stop_time are provided, the data range will be limited accordingly. Otherwise, the
        configured horizon will be taken as reference and the starting point will be determined by the server.

        :param start_time: Optional start time for the forecast data. The start time must be time zone aware  and will
            be rounded to the next full day (00:00 UTC).
        :param end_time: Optional stop time for the forecast data. The stop time must be time zone aware and will be
            rounded to the previous full day (00:00 UTC).
        :return: Dictionary with request parameters
        """
        params = self._static_request_parameters.copy()
        if (start_time is None) != (end_time is None):
            raise ValueError("Both start_time and end_time must be provided together.")

        if start_time is not None and end_time is not None:
            params["start_date"] = self._to_utc_date_string(start_time)
            params["end_date"] = self._to_utc_date_string(end_time)
        else:
            # Use forecast_days and past_days if no explicit time range is given
            params["forecast_days"] = self._forecast_days
            params["past_days"] = self._past_days
            if self._enable_minutely_15:
                params["forecast_minutely_15"] = self._forecast_minutely_15
        return params

    @staticmethod
    def _to_utc_date_string(dt: datetime.datetime) -> str:
        """Converts the datetime to an UTC-aligned datetime string"""
        dt = dt.astimezone(datetime.timezone.utc)
        return dt.strftime("%Y-%m-%d")

    def fetch_data_bundle(self, raw_forecast: Optional[dict] = None) -> Generator[Message, None, None]:
        """
        Fetches the forecasting information and yields separate messages for hourly and 15-minute data

        This method queries the API once but publishes the hourly and 15-minute forecasts as separate
        Redis messages, allowing downstream consumers to subscribe to only the data resolution they need.

        :param raw_forecast: The raw forecast for testing purpose. It is not advised to use the parameter productively.
        :return: A generator yielding Message objects with hourly and (optionally) 15-minute forecast data
        """

        yield from self._fetch_data(raw_forecast)

    def fetch_timed_historic_data_bundle(
            self, start_time: datetime.datetime, end_time: datetime.datetime, filter_clauses: Dict[str, Any],
            raw_forecast: Optional[Iterable[dict]] = None
    ) -> Generator[MessageData, None, None]:
        """
        Fetches the historic remote data into a bundle of multiple messages.

        For each requested resolution (minutely, hourly), a dedicated message will be yielded. In case the time span
        exceeds the history_batch_size, multiple requests will be issued to cover the full range. The output will be
        divided into multiple messages as well.

        :param raw_forecast: An optional iterable of raw forecast dicts for testing purpose. If provided, one dict per batch
            will be used instead of querying the API.
        :param start_time: The beginning of the historic data range. Must be time-zone aware.
        :param end_time: The end of the historic data range. Must be time-zone aware.
        :param filter_clauses: The configuration stanza that specifies the amount of historic data to return.
        :return: The function will return a generator that yields one message at a time.
        """

        if raw_forecast is None:
            raw_forecast = itertools.repeat(None)
        raw_forecast_it = iter(raw_forecast)

        batch_size = datetime.timedelta(days=self._history_batch_size)
        batch_start = start_time
        while batch_start < end_time:
            batch_end = min(batch_start + batch_size, end_time)
            self._logger.debug(f"Fetching historic data batch from {batch_start.isoformat()} to "
                               f"{batch_end.isoformat()}")
            yield from self._fetch_data(start_time=batch_start, end_time=batch_end, raw_forecast=next(raw_forecast_it))
            batch_start = batch_end

    def _fetch_data(self, raw_forecast: Optional[dict] = None, start_time: Optional[datetime.datetime] = None,
                    end_time: Optional[datetime.datetime] = None) -> Generator[Message, None, None]:
        """
        Fetches the forecasting information and yields separate messages for hourly and 15-minute data

        :param raw_forecast: The raw forecast for testing purpose. It is not advised to use the parameter productively.
        :param start_time: Optional start time for the forecast data. The start time must be time zone aware  and will
            be rounded to the next full day (00:00 UTC).
        :param end_time: Optional stop time for the forecast data. The stop time must be time zone aware and will be
            rounded to the previous full day (00:00 UTC).
        :return: A generator yielding Message objects with hourly and (optionally) 15-minute forecast data
        """

        # Fetch raw data from API if not provided
        if raw_forecast is None:
            response: requests.Response = self.session.get(
                self.API_ENDPOINT,
                params=self.get_forecast_request_parameters(start_time, end_time)
            )
            response.raise_for_status()
            raw_forecast = response.json()

        # No forecast time is delivered by the server. Hence, the local time of retrieval will be taken.
        forecast_time = datetime.datetime.now(tz=datetime.timezone.utc)

        # Extract common metadata (shared by both messages)
        common_metadata = self._extract_common_metadata(raw_forecast)

        # Always yield hourly data message
        if self._enable_hourly and "hourly" in raw_forecast:
            hourly_data = self._transform_hourly_data(raw_forecast, self._hourly_variables, forecast_time)
            hourly_data.update(common_metadata)

            self._logger.debug(
                f"Yielding hourly forecast message with {len(hourly_data.get('observation_time', []))} timesteps")
            yield Message(
                payload=hourly_data,
                metadata={}  # can specify a certain stream name here.
            )

        # Yield 15-minute data message if enabled and available
        if self._enable_minutely_15 and "minutely_15" in raw_forecast:
            minutely_data = self._transform_minutely_15_data(raw_forecast, self._minutely_15_variables, forecast_time)
            minutely_data.update(common_metadata)

            self._logger.debug(
                f"Yielding 15-minute forecast message with {len(minutely_data.get('observation_time', []))} timesteps")
            yield Message(
                payload=minutely_data,
                metadata={}  # Can be configured to route to specific streams if needed
            )

    @staticmethod
    def _transform_hourly_data(raw_forecast: dict, hourly_variables: List[str],
                               forecast_time: Optional[datetime.datetime] = None) -> Dict[str, Any]:
        """
        Transforms the Open-Meteo hourly forecast data to the common Redis representation

        :param raw_forecast: The raw JSON response from Open-Meteo API
        :param hourly_variables: List of hourly variables that were queried
        :param forecast_time: An externally provided forecast time (optional) In case none is supplied, the first
            sample after past_days will be taken.
        :return: Dictionary in the common Redis format with hourly data
        """
        if "hourly" not in raw_forecast:
            return {}

        result = _OpenMeteoBase._transform_hourly_data(raw_forecast, hourly_variables)

        # Add a forecast_time based on the first observation time if available
        if "observation_time" in result and len(result["observation_time"]) > 0 and forecast_time is None:
            result["forecast_time"] = result["observation_time"][0]
        else:
            result["forecast_time"] = forecast_time.isoformat()

        return result

    @staticmethod
    def _transform_minutely_15_data(raw_forecast: dict, minutely_15_variables: List[str],
                                    forecast_time: Optional[datetime.datetime] = None) -> Dict[str, Any]:
        """
        Transforms the Open-Meteo 15-minute forecast data to the common Redis representation
        
        :param raw_forecast: The raw JSON response from Open-Meteo API
        :param minutely_15_variables: List of 15-minute variables that were queried
        :param forecast_time: An externally provided forecast time (optional) In case none is supplied, the first
            sample will be taken.
        :return: Dictionary in the common Redis format with 15-minute data
        """
        if "minutely_15" not in raw_forecast:
            return {}

        variable_mapping = OpenMeteoForecast._get_variable_mapping()
        extractors = []

        # Time axis for 15-minute data
        extractors.append(jx.DatetimePathExtractor("observation_time", "minutely_15.time[*]"))

        # Add extractors for each 15-minute variable
        for var in minutely_15_variables:
            internal_name = variable_mapping.get(var, var)
            src_path = f"minutely_15.{var}[*]"
            extractors.append(
                jx.PathExtractor(internal_name, src_path, is_list=True, drop_missing=True)
            )

        result = dict(itertools.chain(*[ext.extract_information(raw_forecast).items() for ext in extractors]))

        # Add a forecast_time based on the first observation time if available
        if "observation_time" in result and len(result["observation_time"]) > 0 and forecast_time is None:
            result["forecast_time"] = result["observation_time"][0]
        else:
            result["forecast_time"] = forecast_time.isoformat()

        return result


_HISTORY_DEFAULT_HOURLY_VARIABLES = [
    # Temperature and humidity
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    # Pressure
    "pressure_msl",
    "surface_pressure",
    # Clouds
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    # Wind at various heights
    "wind_speed_10m",
    "wind_speed_100m",

    "wind_direction_10m",
    "wind_direction_100m",
    "wind_gusts_10m",

    # Precipitation
    "precipitation",
    "rain",
    "snowfall",
    "snow_depth",
    "weather_code",
    # Radiation
    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation",
    "direct_normal_irradiance",
]


class OpenMeteoHistoryParameters(pydantic.BaseModel):
    """
    Pydantic model for Open-Meteo historical data source parameters

    Timing parameters are generally in days because the Open-Meteo API works with full days, only.
    """

    latitude: float = pydantic.Field(description="Latitude of the location")
    longitude: float = pydantic.Field(description="Longitude of the location")

    variables: List[str] = pydantic.Field(
        default=_HISTORY_DEFAULT_HOURLY_VARIABLES,
        description="List of hourly variables to query"
    )
    model: Optional[str] = pydantic.Field(
        description="Reanalysis model to use for historical data.",
        default="best_match"
    )

    lag_time: Optional[int] = pydantic.Field(
        description="Optional lag time in days to account for data availability delays",
        default=3, ge=3
    )
    initial_history: Optional[int] = pydantic.Field(
        description="Initial history duration in days to fetch data from the past",
        default=30, ge=1
    )
    batch_size: Optional[int] = pydantic.Field(
        description="Number of days per historic data request batch",
        default=30 * 6, ge=1
    )


class OpenMeteoHistory(history.AbstractTimedMultiMessageHistorySourceMixin,
                       _OpenMeteoBase):
    """
    Implements the Open-Meteo historical weather data source assessing the reanalysis data
    """

    API_ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"

    def __init__(self, source_parameters: dict | OpenMeteoHistoryParameters, executor_name: str = "-", **kwargs):
        """
        Initializes the Open-Meteo historical data source.

        :param source_parameters: The source parameters according to the configuration
        :param executor_name: The name of the executor for debugging purpose
        :param kwargs: Any extra arguments that will be sent to the super class
        """
        super().__init__(source_parameters=source_parameters, executor_name=executor_name, **kwargs)

        if isinstance(source_parameters, dict):
            source_parameters = OpenMeteoHistoryParameters.model_validate(source_parameters)

        if not isinstance(source_parameters, OpenMeteoHistoryParameters):
            raise TypeError("source_parameters must be a dict or OpenMeteoHistoryParameters instance")

        self._params = source_parameters
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}.{executor_name}")

        # Determine the aligned start time
        self._start_time = datetime.datetime.now(tz=datetime.timezone.utc)
        self._start_time = self._start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        self._start_time -= datetime.timedelta(days=self._params.initial_history + self._params.lag_time)

    def fetch_data_bundle(self) -> Generator[MessageData, None, None]:
        """Fetches the most recent remote data. In case no new data is available, no messages will be yielded."""

        end_time = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=self._params.lag_time)
        end_time = end_time.replace(hour=0, minute=0, second=0, microsecond=0)

        if self._start_time < end_time:
            yield from self._fetch_messages(self._start_time, end_time, raw_data=None)
            self._start_time = end_time

    def fetch_timed_historic_data_bundle(
            self, start_time: datetime.datetime, end_time: datetime.datetime,
            filter_clauses: Dict[str, Any], raw_data: Optional[Iterator[dict]] = None
    ) -> Generator[MessageData, None, None]:
        """
        Fetches the historic remote data into a bundle of multiple messages.
        :param start_time: The beginning of the historic data range. Must be time-zone aware.
        :param end_time: The end of the historic data range. Must be time-zone aware
        :param filter_clauses: Any additional filter clauses for the historic data request. Currently unused.
        :param raw_data: Optional iterator of raw data dicts for testing. If provided, will be used instead of API calls.
        :return: The function will return a generator that yields one message at a time.
        """
        yield from self._fetch_messages(start_time, end_time, raw_data=raw_data)

    def _fetch_messages(self, start_time: datetime.datetime, end_time: datetime.datetime,
                        raw_data: Optional[Iterator[dict]]) -> Generator[MessageData, None, None]:
        """
        Fetches the messages for the given time range

        :param start_time: The start time of the data range
        :param end_time: The end time of the data range
        :param raw_data: An optional iterator of raw data for testing purpose. If provided, one dict per batch will be
            used instead of querying the API.
        :return: A generator yielding MessageData objects with the historical data
        """

        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = end_time.replace(hour=0, minute=0, second=0, microsecond=0)

        if raw_data is None:
            raw_data = itertools.repeat(None)
        raw_data_it = iter(raw_data)

        batch_size = datetime.timedelta(days=self._params.batch_size)
        batch_start = start_time
        while batch_start < end_time:
            batch_end = min(batch_start + batch_size, end_time)
            self._logger.debug(f"Fetching historic data batch from {batch_start.isoformat()} to "
                               f"{batch_end.isoformat()}")

            # Fetch raw data from API if not provided
            message = self._fetch_single_batch(batch_start, batch_end, raw_data=next(raw_data_it))
            yield message
            batch_start = batch_end

    def _fetch_single_batch(self, start_time: datetime.datetime, end_time: datetime.datetime,
                            raw_data: Optional[dict]) -> MessageData:
        """
        Fetches a single batch of historical data

        :param start_time: The start time of the data range
        :param end_time: The end time of the data range
        :param raw_data: An optional raw data dict for testing purpose. If provided, it will be used instead of querying
            the API.
        :return: A MessageData object with the historical data
        """

        # Fetch raw data from API if not provided
        if raw_data is None:
            response: requests.Response = self.session.get(
                self.API_ENDPOINT,
                params={
                    "latitude": self._params.latitude,
                    "longitude": self._params.longitude,
                    "hourly": ",".join(self._params.variables),
                    "start_date": start_time.strftime("%Y-%m-%d"),
                    "end_date": end_time.strftime("%Y-%m-%d"),
                    "timezone": "GMT",
                    "models": self._params.model,
                    "wind_speed_unit": "ms"
                }
            )
            response.raise_for_status()
            raw_data = response.json()

        # Transform and return message
        data = self._transform_hourly_data(raw_data, self._params.variables)
        data.update(self._extract_common_metadata(raw_data))

        self._logger.debug(f"Fetched historic forecast message with {len(data.get('observation_time', []))} timesteps")
        return Message(
            payload=data,
            metadata={}
        )

## testing a = OpenMeteoForecast(source_parameters={"latitude": 52.593, "longitude": 4.752,
## testing                                          "enable_minutely_15": True, 
## testing                                          "minutely_15_variables": ["wind_speed_10m", "wind_speed_80m", "wind_speed_120m", "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", "wind_direction_120m", "wind_direction_180m", "precipitation", "weather_code", "shortwave_radiation", "direct_radiation", "diffuse_radiation"], "hourly_variables": ["wind_speed_10m", "wind_speed_80m", "wind_speed_120m", "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", "wind_direction_120m", "wind_direction_180m", "precipitation", "weather_code", "shortwave_radiation", "direct_radiation", "diffuse_radiation"]}) #, "wind_speed_unit": "kmh"})
## testing print(a._static_request_parameters)
## testing b = a.fetch_data_bundle()
## testing #print(json.dumps(b, indent=4))
## testing print('done')
