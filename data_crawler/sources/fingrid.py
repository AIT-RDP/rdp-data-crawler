"""
Implements the interface to the Fingrid Open Data API

See https://data.fingrid.fi/en/instructions and
https://developer-data.fingrid.fi/api-details#api=avoindata-api&operation=GetDatasetData
"""
import datetime
import itertools
from typing import Any, Dict, Optional

import pandas as pd
import requests

import data_crawler.access.jsonpath as jx
import data_crawler.sources.abc.http_cache as http_cache


class DatasetData(http_cache.GenericHTTPSourceAPI):
    """Queries a single Fingrid Open Data dataset via GetDatasetData"""

    API_ENDPOINT = "https://data.fingrid.fi/api/datasets/{dataset_id}/data"

    def __init__(self, source_parameters, **kwargs):
        """
        Initializes the Fingrid API but does not trigger any query

        :param source_parameters: The source parameters according to the configuration
        :param kwargs: Any extra arguments that will be sent to the super class
        """
        super(DatasetData, self).__init__(source_parameters=source_parameters, **kwargs)

        self._dataset_id = int(source_parameters["dataset_id"])
        self._page_size = int(source_parameters.get("page_size", 20000))

        self.session.headers["x-api-key"] = source_parameters["api_key"]

        now = datetime.datetime.now(tz=datetime.timezone.utc)
        initial_history = source_parameters.get("initial_history")
        if initial_history:
            self._last_query_ts = now - pd.Timedelta(initial_history)
        else:
            self._last_query_ts = now

    @property
    def last_query_ts(self) -> datetime.datetime:
        """Returns the rolling query cursor for testing purpose"""
        return self._last_query_ts

    def fetch_data(self, raw_data: Optional[dict] = None) -> Dict[str, Any]:
        """
        Fetches the dataset observations and translates them into a common Redis-ready nomenclature

        :param raw_data: The raw API response for testing purpose. It is not advised to use the parameter productively.
        :return: The dictionary of observations following the common nomenclature. See the AbstractSourceAPI class for
            more information on the expected output format.
        """

        if raw_data is None:
            end_time = datetime.datetime.now(tz=datetime.timezone.utc)
            raw_data = self._fetch_raw_data(self._last_query_ts, end_time)
            self._last_query_ts = end_time

        redis_data = self._transform_to_redis_format(raw_data)
        redis_data["dataset_id"] = self._dataset_id
        return redis_data

    def _fetch_raw_data(self, start_time: datetime.datetime, end_time: datetime.datetime) -> dict:
        """
        Fetches all pages of dataset data for the given time window and concatenates the observation rows

        :param start_time: Inclusive start of the query window
        :param end_time: Inclusive end of the query window
        :return: A JSON-like dict with a combined ``data`` list
        """

        all_rows = []
        page = 1
        url = self.API_ENDPOINT.format(dataset_id=self._dataset_id)

        while True:
            response: requests.Response = self.session.get(
                url,
                params={
                    "startTime": self._to_rfc3339(start_time),
                    "endTime": self._to_rfc3339(end_time),
                    "format": "json",
                    "page": page,
                    "pageSize": self._page_size,
                    "sortBy": "startTime",
                    "sortOrder": "asc",
                },
            )
            response.raise_for_status()
            payload = response.json()
            all_rows.extend(payload.get("data") or [])

            next_page = (payload.get("pagination") or {}).get("nextPage")
            if next_page is None:
                break
            page = int(next_page)

        return {"data": all_rows}

    @staticmethod
    def _to_rfc3339(dt: datetime.datetime) -> str:
        """Formats a timezone-aware datetime as RFC3339 UTC"""
        return dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def _transform_to_redis_format(raw_data: dict) -> Dict[str, Any]:
        """
        Transforms the Fingrid timeseries response to the common Redis representation

        The function does some rudimentary checks but does not validate the schema entirely.
        """

        extractors = [
            jx.DatetimePathExtractor("observation_time", "data[*].startTime"),
            jx.DatetimePathExtractor("end_time", "data[*].endTime"),
            jx.PathExtractor("value", "data[*].value"),
        ]

        return dict(itertools.chain(*[ext.extract_information(raw_data).items() for ext in extractors]))
