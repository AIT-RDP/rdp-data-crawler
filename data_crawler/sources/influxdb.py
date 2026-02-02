"""
Implements a data source for InfluxDB in the data crawler framework.
"""
import datetime
import logging
from typing import Generator, Optional, Dict, Any

import pydantic
import influxdb_client

import data_crawler.sources.abc.abstract_source as abstract_source
import data_crawler.sources.abc.history as history
import data_crawler.access.type_checks as type_checks
from data_crawler.sources.abc.message import MessageData


class InfluxDBSourceConfiguration(pydantic.BaseModel):
    """
    Configuration parameters for the InfluxDB data source.
    """

    url: str = pydantic.Field(description="URL of the InfluxDB instance")
    token: str = pydantic.Field(description="Authentication token for InfluxDB")
    org: str = pydantic.Field(description="Organization name in InfluxDB")
    query_timeout: type_checks.TimedeltaType = pydantic.Field(
        default=datetime.timedelta(seconds=10),
        description="Timeout duration for InfluxDB queries"
    )
    query: str = pydantic.Field(description="Flux query to fetch the data")
    initial_history: type_checks.TimedeltaType = pydantic.Field(
        default=datetime.timedelta(hours=1),
        description="Initial history duration to fetch data from the past"
    )
    lag_time: type_checks.TimedeltaType = pydantic.Field(
        default=datetime.timedelta(minutes=0),
        description="Lag time to account for late arriving data"
    )
    batch_duration: Optional[type_checks.TimedeltaType] = pydantic.Field(
        default=None,
        description="If set, the source will fetch data in batches of the specified duration"
    )


class InfluxDBSource(abstract_source.AbstractMultiMessageSourceAPI,
                     history.AbstractTimedMultiMessageHistorySourceMixin):
    """
    Implements the InfluxDB data source.

    The data source queries the remote InfluxDB instance on request and returns the resulting message data. Per default,
    each table is returned in a separate message. In order to limit the returned data to new samples, only, start and
    stop time parameters `_start_time` and `_stop_time` are maintained in a rolling horizon fashion. The parameters are
    updated after each successful query by the latest timestamp found in the returned data.

    To keep the source simple, it was decided to not implement convenience features such as renaming of fields. A one
    to one mapping of returned columns and message fields is performed. Consider the flux function
    [`rename`](https://docs.influxdata.com/flux/v0/stdlib/universe/rename/) if you need to change the naming
    conventions. Similarly, consider the flux function [`set`](https://docs.influxdata.com/flux/v0/stdlib/universe/set/)
    to statically add fields to the returned data.
    """

    def __init__(self, source_parameters: dict | InfluxDBSourceConfiguration, executor_name: str, **kwargs):
        """
        Initializes the InfluxDB data source.
        :param source_parameters: The source parameters according to the configuration
        :param executor_name: The name of the executor for debugging purpose
        :param kwargs: Any extra arguments that will be sent to the super class
        """
        super(InfluxDBSource, self).__init__(executor_name=executor_name, **kwargs)

        if isinstance(source_parameters, dict):
            source_parameters = InfluxDBSourceConfiguration.model_validate(source_parameters)

        if not isinstance(source_parameters, InfluxDBSourceConfiguration):
            raise TypeError("source_parameters must be a dict or InfluxDBSourceConfiguration instance")

        self._config = source_parameters
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}.{executor_name}")

        self._start_time = datetime.datetime.now(tz=datetime.timezone.utc) - self._config.initial_history
        self._start_time -= self._config.lag_time

        self._client = None

    def start(self):
        """
        Starts the operation of the data source
        """
        super().start()
        assert self._client is None, "InfluxDB client is already initialized."

        self._client = influxdb_client.InfluxDBClient(
            url=self._config.url,
            token=self._config.token,
            org=self._config.org,
            timeout=self._config.query_timeout.total_seconds() * 1000  # milliseconds
        )
        if not self._client.ping():
            raise ConnectionError(f"Unable to connect to InfluxDB instance with the provided configuration. "
                                  f"(url: {self._config.url}), org: {self._config.org})")
        self._logger.debug(f"Connected to InfluxDB at {self._config.url} for org {self._config.org} successfully.")

    def stop(self):
        """
        Stops the source operation and frees allocated resources
        """
        super().stop()
        if self._client is not None:
            self._client.close()
            self._client = None

    def fetch_data_bundle(self) -> Generator[MessageData, None, None]:
        """
        Fetches the remote data into a bundle of multiple messages.

        The function queries the InfluxDB instance and returns the resulting data as a series of messages, one per table.

        :returns: The function will return a generator that yields one message at a time.
        """
        stop_time = datetime.datetime.now(tz=datetime.timezone.utc) - self._config.lag_time
        yield from self._fetch_remote_data(self._start_time, stop_time)
        self._start_time = stop_time

    def fetch_timed_historic_data_bundle(self, start_time: datetime.datetime, end_time: datetime.datetime,
                                         filter_clauses: Dict[str, Any]) -> Generator[MessageData, None, None]:
        """
        Fetches the historic remote data into a bundle of multiple messages.
        """
        yield from self._fetch_remote_data(start_time, end_time)

    def _fetch_remote_data(self, start_time: datetime.datetime,
                           stop_time: datetime.datetime) -> Generator[MessageData, None, None]:
        """
        Fetches the remote data from InfluxDB between the specified start and stop times.

        :param start_time: The start time for data fetching
        :param stop_time: The stop time for data fetching
        :returns: A generator yielding message data dictionaries
        """
        if self._client is None:
            raise RuntimeError("InfluxDB client is not initialized. Call start() before fetching data.")

        query_api = self._client.query_api()

        if self._config.batch_duration is None:
            yield from self._fetch_remote_data_batch(query_api, start_time, stop_time)
        else:
            batch_start = start_time
            batch_duration = self._config.batch_duration
            while batch_start < stop_time:
                batch_end = min(batch_start + batch_duration, stop_time)
                yield from self._fetch_remote_data_batch(query_api, batch_start, batch_end)
                batch_start = batch_end

    def _fetch_remote_data_batch(self, query_api: influxdb_client.client.query_api.QueryApi,
                                 start_time: datetime.datetime,
                                 stop_time: datetime.datetime) -> Generator[MessageData, None, None]:
        """Fetches the remote data from InfluxDB between the specified start and stop times in one single batch."""

        params = {
            "_start_time": start_time,
            "_stop_time": stop_time
        }
        self._logger.debug(f"Start querying InfluxDB from {start_time.isoformat()} to {stop_time.isoformat()}.")
        tables = query_api.query(self._config.query, params=params)

        total_tables, total_rows = 0, 0
        for table in tables:
            message, num_rows = self._transform_table_to_message(table)
            total_tables += 1
            total_rows += num_rows
            yield message

        self._logger.debug(f"Queried InfluxDB from {start_time.isoformat()} to {stop_time.isoformat()} "
                           f"successfully: {total_tables} tables with {total_rows} rows in total.")

    @staticmethod
    def _transform_table_to_message(table: influxdb_client.client.flux_table.FluxTable) -> tuple[MessageData, int]:
        """
        Transforms the flux table to a message data dictionary.

        Each column becomes a key in the dictionary and will be represented as a list of values.
        :param table: The flux table to transform
        :return: The message data dictionary
        """

        message: dict = {}
        for record in table.records:
            for key, value in record.values.items():
                if key not in message:
                    message[key] = []
                message[key].append(value)
        return message, len(table.records)
