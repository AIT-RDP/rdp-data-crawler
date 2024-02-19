"""
Implements the day-ahead market prices from ENTSO-E Transparency Platform
"""

from enum import IntEnum
from typing import Any

import entsoe
import pandas as pd
import pydantic

import data_crawler.sources.abc.http_cache as http_cache


class Resolution(IntEnum):
    """
    Resolution for the day-ahead prices in minutes
    """

    MIN_15 = 15
    MIN_30 = 30
    MIN_60 = 60


class DayAheadPricesModel(pydantic.BaseModel):
    """
    Details how a specific day-ahead price should be queried
    """

    country_code: str = pydantic.Field(description="Country code (e.g. `AT`)")
    timezone: str = pydantic.Field(
        description="Timezone for the prices; this information is used to determine the beginning and ending of the "
                    "next day")
    resolution: Resolution = pydantic.Field(default=Resolution.MIN_60, description="Resolution of the prices")


class ENTSOEDATransparencyModel(pydantic.BaseModel):
    """
    Data model for the source parameters of the source
    """

    # documentation how to get the API-key:
    # https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_authentication_and_authorisation
    api_key: str = pydantic.Field(description="API-key for the ENTSO-E transparency platform")
    day_ahead_prices: list[DayAheadPricesModel] = pydantic.Field(
        default=[],
        description="List of day-ahead prices to be queried",
    )


class ENTSOEDATransparency(http_cache.GenericHTTPSourceAPI):
    """
    Implements the day-ahead market prices from ENTSO-E Transparency Platform.
    """

    def __init__(self, source_parameters: dict[str, Any], **kwargs):
        """
        :param source_parameters: The source parameters according to the configuration
        :param kwargs: Any extra arguments that will be sent to the super class
        """

        super().__init__(source_parameters=source_parameters, **kwargs)

        self._source_parameters: ENTSOEDATransparencyModel = ENTSOEDATransparencyModel(**source_parameters)

        self._client: entsoe.EntsoePandasClient = entsoe.EntsoePandasClient(api_key=self._source_parameters.api_key)

    def fetch_data(self) -> dict[str, Any]:
        """
        Retrieves the data from the ENTSO-E transparency platform.

        :param raw_data: The raw data for testing purpose
        :return: Retrieved data
        """

        observation_time: list[pd.Timestamp] = []
        day_ahead_prices: list[float] = []
        day_ahead_resolution: list[int] = []
        location: list[str] = []

        for day_ahead_country in self._source_parameters.day_ahead_prices:
            country_code = day_ahead_country.country_code
            timezone = day_ahead_country.timezone
            resolution = day_ahead_country.resolution

            now = pd.Timestamp.now(tz=timezone)
            next_day_begin = (now + pd.DateOffset(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            next_day_end = (
                    (now + pd.DateOffset(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
                    - pd.Timedelta(minutes=resolution.value)
            )

            prices = self._client.query_day_ahead_prices(
                country_code=country_code,
                start=next_day_begin,
                end=next_day_end,
                resolution=f"{resolution.value}min",
            )

            observation_time.extend(prices.index.to_list())
            day_ahead_prices.extend(prices.to_list())
            day_ahead_resolution.extend(len(prices) * [resolution.value])
            location.extend(len(prices) * [country_code])

        return {
            "observation_time": [t.isoformat() for t in observation_time],
            "day_ahead_prices": day_ahead_prices,
            "day_ahead_resolution": day_ahead_resolution,
            "location": location,
        }
