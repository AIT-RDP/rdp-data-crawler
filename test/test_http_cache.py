"""
Tests the HTTP caching mechanism provided by the GenericHTTPSourceAPI class
"""
import os
import shutil
import time
from typing import Dict, Any

import pandas as pd
import requests

import data_crawler.sources.abc.http_cache as http_cache


class TimeSourceAPI(http_cache.GenericHTTPSourceAPI):
    """Just queries the current time of day from a status API"""

    def __init__(self, **kwargs):
        """
        Initializes the source API

        :param kwargs: Parameters passed on to the underlying GenericHTTPSourceAPI
        """

        super(TimeSourceAPI, self).__init__(**kwargs)
        if "DATA_CRAWLER_CONTACT" not in os.environ:
            raise KeyError("Please put your contact details in the environment variable DATA_CRAWLER_CONTACT.")
        self.session.headers["User-Agent"] = f"E3-SCHOOL_EMS_Test_Suite {os.environ['DATA_CRAWLER_CONTACT']}"

    def fetch_data(self) -> Dict[str, Any]:
        """Queries the status API and returns the result"""

        response: requests.Response = self.session.get("https://api.met.no/weatherapi/locationforecast/2.0/status.json")
        response.raise_for_status()

        status = response.json()
        return {
            "last update": pd.to_datetime(status["last_update"]),
            "header date": pd.to_datetime(response.headers["date"]),
            "header expires": pd.to_datetime(response.headers["expires"])
        }


def test_expiration_date():
    """Tests the caching mechanism using an expiration date"""

    api = TimeSourceAPI(source_parameters={
        "cache": {
            "directory": ".cache-test",
            "expire": "1s"
        },
    })

    call_0 = api.fetch_data()
    call_1 = api.fetch_data()
    time.sleep(2.0)
    call_2 = api.fetch_data()

    assert os.path.isdir(".cache-test")
    shutil.rmtree(".cache-test")

    assert call_0["header date"] == call_1["header date"]
    assert call_0["header date"] < call_2["header date"]

    assert call_0["last update"] == call_1["last update"]
    assert call_0["header expires"] == call_1["header expires"]


def test_expiration_default():
    """Tests the default expiration behaviour according to the external website"""

    api = TimeSourceAPI(source_parameters={
        "cache": {
            "directory": ".cache-test",
            "expire": "1s"
        },
    })

    call_0 = api.fetch_data()
    call_1 = api.fetch_data()

    assert os.path.isdir(".cache-test")
    shutil.rmtree(".cache-test")

    assert call_0["header date"] == call_1["header date"]
    assert call_0["last update"] == call_1["last update"]
    assert call_0["header expires"] == call_1["header expires"]
