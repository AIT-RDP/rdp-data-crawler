"""
Implements the day-ahead market prices from ENTSO-E Transparency Platform
"""
import datetime
from enum import IntEnum
from typing import Any, Dict, Generator, Optional

import entsoe
import pandas as pd
import pydantic

import data_crawler.sources.abc.http_cache as http_cache
import data_crawler.sources.abc.history as history
import data_crawler.access.type_checks as type_checks
from data_crawler.sources.abc.message import MessageData


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
    initial_history: Optional[type_checks.TimedeltaType] = pydantic.Field(
        default=None,
        description="Optional past duration to backfill on the first poll. After the initial query, only the next "
                    "day-ahead block is fetched. Omit to keep tomorrow-only polling.",
    )
    day_ahead_prices: list[DayAheadPricesModel] = pydantic.Field(
        default=[],
        description="List of day-ahead prices to be queried",
    )


class ENTSOEDATransparency(http_cache.GenericHTTPSourceAPI, history.AbstractTimedMultiMessageHistorySourceMixin):
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

        self._initial_history_pending = self._source_parameters.initial_history is not None

    def fetch_data(self) -> dict[str, Any]:
        """
        Retrieves the data from the ENTSO-E transparency platform.

        :param raw_data: The raw data for testing purpose
        :return: Retrieved data
        """

        start_time = pd.Timestamp.now(tz=datetime.timezone.utc)
        return self._fetch_da_block(start_time)

    def fetch_data_bundle(self) -> Generator[MessageData, None, None]:
        """
        Fetches the remote data into a bundle of multiple messages.

        When `initial_history` is configured, the first call backfills one message per day in
        `[now - initial_history, now)` and then yields the current tomorrow block. Later calls yield only the
        tomorrow block.
        """

        # Same now for the initial history backfill and the live data to avoid duplicates responses
        now = pd.Timestamp.now(tz=datetime.timezone.utc)

        # Backfill the initial history if configured
        if self._initial_history_pending:
            initial_history = self._source_parameters.initial_history
            if initial_history is None: # This check is only there so the type checker accepts "now - initial_history"
                raise RuntimeError("initial_history backfill is pending but no duration is configured")
            yield from self.fetch_timed_historic_data_bundle(now - initial_history, now, {})
            self._initial_history_pending = False

        # Live data
        yield self._fetch_da_block(now)

    def fetch_timed_historic_data_bundle(self, start_time: datetime.datetime, end_time: datetime.datetime,
                                         filter_clauses: Dict[str, Any]) -> Generator[MessageData, None, None]:
        """
        Fetches the historic remote data into a bundle of multiple messages.

        For each aligned block of day-ahead prices, a separate message will be yielded. In case the start_time and
        end_time marks do not align with the block sizes, the next block boundaries will be taken.

        :param start_time: The beginning of the historic data range. Must be time-zone aware.
        :param end_time: The end of the historic data range. Must be time-zone aware.
        """

        block_start = start_time
        while block_start < end_time:
            message = self._fetch_da_block(block_start)
            yield message
            block_start += datetime.timedelta(days=1)

    def _fetch_da_block(self, start_time: datetime.datetime) -> dict[str, Any]:
        """
        Fetches a single block of day-ahead prices starting from the given time stamp.

        The start time will be aligned to the next block boundary at the next day, midnight.
        :param start_time: The start time for the block. Must be time-zone aware.
        :return: The retrieved data block as single message
        """

        if start_time.tzinfo is None or start_time.tzinfo.utcoffset(start_time) is None:
            raise ValueError(f"The start time {start_time.isoformat()} must be time-zone aware.")

        observation_time: list[pd.Timestamp] = []
        day_ahead_prices: list[float] = []
        day_ahead_resolution: list[int] = []
        location: list[str] = []

        for day_ahead_country in self._source_parameters.day_ahead_prices:
            country_code = day_ahead_country.country_code
            timezone = type_checks.to_timezone(day_ahead_country.timezone)
            resolution = day_ahead_country.resolution

            local_start = start_time.astimezone(timezone)
            next_day_begin = (local_start + pd.DateOffset(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            next_day_end = (
                    (local_start + pd.DateOffset(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
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
