"""
Implements the interface to the Open-Meteo Weather Forecast API

See https://open-meteo.com/en/docs for the API documentation.
"""
import itertools
from typing import Dict, Any, List, Optional
import json
import logging

import requests

import data_crawler.sources.abc.http_cache as http_cache
import data_crawler.access.jsonpath as jx


class OpenMeteoForecast(http_cache.GenericHTTPSourceAPI):
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
              - minutely_15_variables: list[str], the 15-minute variables to query 
                  (defaults to DEFAULT_MINUTELY_15_VARIABLES if enable_minutely_15 is True)
              - forecast_days: int, number of forecast days for hourly data (1-16, default 7)
              - forecast_minutely_15: int, number of 15-minute timesteps for minutely data 
                  (default 96 = 24 hours). Only used if enable_minutely_15 is True.
                  Note: 15-minute data is limited to ~4 days (384 timesteps) by the API.
              - past_days: int, number of past days to include (0-92, default 0)
              - timezone: str, timezone for the response (default "UTC")
              - elevation: float, custom elevation in meters
              - models: str, the weather model to use (default "best_match")
                  Common options: "best_match", "ecmwf_ifs04", "gfs_seamless", "icon_seamless"
              - wind_speed_unit: str, the unit of the wind speed (default "ms" other option "kmh")
        :param kwargs: Any extra arguments that will be sent to the super class
        """
        super(OpenMeteoForecast, self).__init__(
            source_parameters=source_parameters, executor_name=executor_name, **kwargs
        )

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}.{executor_name}")

        self._latitude = float(source_parameters["latitude"])
        self._longitude = float(source_parameters["longitude"])
        
        # Optional parameters
        self._hourly_variables = source_parameters.get("hourly_variables", self.DEFAULT_HOURLY_VARIABLES)
        self._enable_minutely_15 = source_parameters.get("enable_minutely_15", False)
        self._minutely_15_variables = source_parameters.get("minutely_15_variables", self.DEFAULT_MINUTELY_15_VARIABLES)
        self._forecast_days = source_parameters.get("forecast_days", 7)
        self._forecast_minutely_15 = source_parameters.get("forecast_minutely_15", 96)  # 24 hours = 96 timesteps
        self._past_days = source_parameters.get("past_days", 0)
        self._timezone = source_parameters.get("timezone", "UTC")
        self._elevation = source_parameters.get("elevation")
        self._models = source_parameters.get("models", "best_match")
        self._wind_speed_unit = source_parameters.get("wind_speed_unit", "ms")


    @property
    def static_request_parameters(self) -> Dict[str, Any]:
        """Returns a copy of the static request parameters for testing purpose"""
        params = {
            "latitude": self._latitude,
            "longitude": self._longitude,
            "hourly": ",".join(self._hourly_variables),
            "forecast_days": self._forecast_days,
            "past_days": self._past_days,
            "timezone": self._timezone,
            "models": self._models,
        }
        
        # Add 15-minute data if enabled
        if self._enable_minutely_15:
            params["minutely_15"] = ",".join(self._minutely_15_variables)
            params["forecast_minutely_15"] = self._forecast_minutely_15
        
        if self._elevation is not None:
            params["elevation"] = self._elevation
        if self._wind_speed_unit is not None:
            params["wind_speed_unit"] = self._wind_speed_unit

        self._logger.debug(f"Static_request_parameters: {json.dumps(params, indent=4)}")

        return params

    def fetch_data(self, raw_forecast: Optional[dict] = None) -> Dict[str, Any]:
        """
        Fetches the forecasting information and translates it into a common Redis-ready nomenclature

        :param raw_forecast: The raw forecast for testing purpose. It is not advised to use the parameter productively.
        :return: The dictionary of forecasts following the common nomenclature. See the AbstractSourceAPI class for more
            information on the expected output format.
        """

        if raw_forecast is None:
            response: requests.Response = self.session.get(
                self.API_ENDPOINT,
                params=self.static_request_parameters
            )
            response.raise_for_status()
            raw_forecast = response.json()

        redis_forecast = self._transform_to_redis_format(
            raw_forecast, 
            self._hourly_variables,
            self._minutely_15_variables if self._enable_minutely_15 else None
        )
        return redis_forecast

    @staticmethod
    def _transform_to_redis_format(raw_forecast: dict, hourly_variables: List[str], 
                                   minutely_15_variables: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Transforms the Open-Meteo forecast to the common Redis representation

        The function does some rudimentary checks but does not validate the schema entirely.
        
        :param raw_forecast: The raw JSON response from Open-Meteo API
        :param hourly_variables: List of hourly variables that were queried
        :param minutely_15_variables: Optional list of 15-minute variables that were queried
        :return: Dictionary in the common Redis format
        """

        # Mapping from Open-Meteo variable names to our internal nomenclature
        # Aligned with weatherbit and yr.no naming conventions
        variable_mapping = {
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
            "wind_speed_120m": "wind_speed_120m",
            "wind_speed_180m": "wind_speed_180m",
            "wind_direction_80m": "wind_direction_80m",
            "wind_direction_120m": "wind_direction_120m",
            "wind_direction_180m": "wind_direction_180m",
            # Precipitation
            "precipitation": "precipitation_total",
            "precipitation_probability": "precipitation_probability",
            "rain": "rain",
            "snowfall": "snowfall",
            "weather_code": "weather_code",
            # Radiation
            "shortwave_radiation": "global_horizontal_irradiation",
            "direct_radiation": "direct_radiation",
            "diffuse_radiation": "diffuse_radiation",
            "direct_normal_irradiance": "direct_normal_irradiance",
            # UV
            "uv_index": "uv_index",
        }

        extractors = [
            # Meta-data (scalar values)
            jx.PathExtractor("longitude", "longitude", is_list=False),
            jx.PathExtractor("latitude", "latitude", is_list=False),
            jx.PathExtractor("elevation", "elevation", is_list=False, drop_missing=True),
        ]
        
        # Extract hourly data if available
        if "hourly" in raw_forecast:
            # Time axis - use [*] to extract individual items from the array
            extractors.append(jx.DatetimePathExtractor("observation_time", "hourly.time[*]"))
            
            # Add extractors for each hourly variable that was queried
            # Use [*] to extract individual items from each hourly array
            for var in hourly_variables:
                internal_name = variable_mapping.get(var, var)
                src_path = f"hourly.{var}[*]"
                extractors.append(
                    jx.PathExtractor(internal_name, src_path, is_list=True, drop_missing=True)
                )
        
        # Extract 15-minute data if available and requested
        if minutely_15_variables is not None and "minutely_15" in raw_forecast:
            # Time axis for 15-minute data - use a different key to distinguish from hourly
            extractors.append(jx.DatetimePathExtractor("observation_time_15min", "minutely_15.time[*]"))
            
            # Add extractors for each 15-minute variable
            # Append "_15min" suffix to distinguish from hourly data
            for var in minutely_15_variables:
                internal_name = variable_mapping.get(var, var)
                internal_name_15min = f"{internal_name}_15min"
                src_path = f"minutely_15.{var}[*]"
                extractors.append(
                    jx.PathExtractor(internal_name_15min, src_path, is_list=True, drop_missing=True)
                )

        # Add forecast generation time if available
        extractors.append(
            jx.PathExtractor("generationtime_ms", "generationtime_ms", is_list=False, drop_missing=True)
        )

        redis_forecast = dict(itertools.chain(*[ext.extract_information(raw_forecast).items() for ext in extractors]))
        
        # Add a forecast_time based on the first observation time if available
        # Prioritize 15-minute data if available, otherwise use hourly
        if "observation_time_15min" in redis_forecast and len(redis_forecast["observation_time_15min"]) > 0:
            redis_forecast["forecast_time"] = redis_forecast["observation_time_15min"][0]
        elif "observation_time" in redis_forecast and len(redis_forecast["observation_time"]) > 0:
            redis_forecast["forecast_time"] = redis_forecast["observation_time"][0]

        return redis_forecast

## testing a = OpenMeteoForecast(source_parameters={"latitude": 52.593, "longitude": 4.752, 
## testing                                          "enable_minutely_15": True, 
## testing                                          "minutely_15_variables": ["wind_speed_10m", "wind_speed_80m", "wind_speed_120m", "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", "wind_direction_120m", "wind_direction_180m", "precipitation", "weather_code", "shortwave_radiation", "direct_radiation", "diffuse_radiation"], "hourly_variables": ["wind_speed_10m", "wind_speed_80m", "wind_speed_120m", "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", "wind_direction_120m", "wind_direction_180m", "precipitation", "weather_code", "shortwave_radiation", "direct_radiation", "diffuse_radiation"]}) #, "wind_speed_unit": "kmh"})
## testing print(a.static_request_parameters)
## testing b = a.fetch_data()
## testing #print(json.dumps(b, indent=4))
## testing print('done')