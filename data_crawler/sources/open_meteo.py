"""
Implements the interface to the Open-Meteo Weather Forecast API

See https://open-meteo.com/en/docs for the API documentation.
"""
import itertools
from typing import Dict, Any, List, Optional

import requests, json

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

    API_ENDPOINT = "https://api.open-meteo.com/v1/forecast"

    def __init__(self, source_parameters: dict, **kwargs):
        """
        Initializes the Open-Meteo forecasting API but does not trigger any query

        :param source_parameters: The source parameters according to the configuration.
            Required keys:
              - latitude: float, the latitude of the location
              - longitude: float, the longitude of the location
            Optional keys:
              - hourly_variables: list[str], the hourly variables to query (defaults to DEFAULT_HOURLY_VARIABLES)
              - forecast_days: int, number of forecast days (1-16, default 7)
              - past_days: int, number of past days to include (0-92, default 0)
              - timezone: str, timezone for the response (default "UTC")
              - elevation: float, custom elevation in meters
              - models: str, the weather model to use (default "best_match")
                  Common options: "best_match", "ecmwf_ifs04", "gfs_seamless", "icon_seamless"
              - wind_speed_unit: str, the unit of the wind speed (default "ms" other option "kmh")
        :param kwargs: Any extra arguments that will be sent to the super class
        """
        super(OpenMeteoForecast, self).__init__(source_parameters=source_parameters, **kwargs)

        self._latitude = float(source_parameters["latitude"])
        self._longitude = float(source_parameters["longitude"])
        
        # Optional parameters
        self._hourly_variables = source_parameters.get("hourly_variables", self.DEFAULT_HOURLY_VARIABLES)
        self._forecast_days = source_parameters.get("forecast_days", 7)
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
        if self._elevation is not None:
            params["elevation"] = self._elevation
        if self._wind_speed_unit is not None:
            params["wind_speed_unit"] = self._wind_speed_unit

        print("static_request_parameters")
        print(json.dumps(params, indent=4))

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

        redis_forecast = self._transform_to_redis_format(raw_forecast, self._hourly_variables)
        return redis_forecast

    @staticmethod
    def _transform_to_redis_format(raw_forecast: dict, hourly_variables: List[str]) -> Dict[str, Any]:
        """
        Transforms the Open-Meteo forecast to the common Redis representation

        The function does some rudimentary checks but does not validate the schema entirely.
        
        :param raw_forecast: The raw JSON response from Open-Meteo API
        :param hourly_variables: List of hourly variables that were queried
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
            
            # Time axis - use [*] to extract individual items from the array
            jx.DatetimePathExtractor("observation_time", "hourly.time[*]"),
        ]

        # Add extractors for each hourly variable that was queried
        # Use [*] to extract individual items from each hourly array
        for var in hourly_variables:
            internal_name = variable_mapping.get(var, var)
            src_path = f"hourly.{var}[*]"
            extractors.append(
                jx.PathExtractor(internal_name, src_path, is_list=True, drop_missing=True)
            )

        # Add forecast generation time if available
        extractors.append(
            jx.PathExtractor("generationtime_ms", "generationtime_ms", is_list=False, drop_missing=True)
        )

        redis_forecast = dict(itertools.chain(*[ext.extract_information(raw_forecast).items() for ext in extractors]))
        
        # Add a forecast_time based on the first observation time if available
        if "observation_time" in redis_forecast and len(redis_forecast["observation_time"]) > 0:
            redis_forecast["forecast_time"] = redis_forecast["observation_time"][0]

        return redis_forecast

## a = OpenMeteoForecast(source_parameters={"latitude": 52.593, "longitude": 4.752}) #, "wind_speed_unit": "kmh"})
## print(a.static_request_parameters)
## b = a.fetch_data()
## #print(json.dumps(b, indent=4))
## print('done')