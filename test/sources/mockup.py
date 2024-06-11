"""
Implements some mockup sources used for testing
"""
import threading
import datetime
from typing import Optional, Any, Dict

import data_crawler.sources.abc.abstract_source as abstract_sources
import data_crawler.sources.abc.active_source_sync as active_source_sync
import data_crawler.sources.abc.message as msg
import data_crawler.sources.abc.history as history


class MockupSourceAPI(abstract_sources.AbstractSourceAPI, history.AbstractTimedHistorySourceMixin):
    """Test API source that just counts function invocations"""

    def __init__(self, source_parameters, metadata: Optional[dict[str, Any]] = None, **kwargs):
        """Stores the configuration and initializes the object"""
        self.kwargs = kwargs

        self.config = source_parameters
        self.fetch_invocations = 0
        self.history_invocations = 0
        self.start_invocations = 0
        self.stop_invocations = 0

        self.fetch_ts = []

        self.enable_fetch = threading.Event()
        self.enable_fetch.set()
        self._metadata = metadata

    def start(self):
        """Counts the start and performs some basic checks"""
        assert self.start_invocations == self.stop_invocations

        if self.config.get("raise_on_startup", False):
            raise ValueError("Uups!")

        self.start_invocations += 1

    def fetch_data(self) -> msg.MessageData:
        """Generates some content and returns it"""

        self.fetch_ts.append(datetime.datetime.utcnow())
        self.fetch_invocations += 1

        assert self.start_invocations == self.stop_invocations + 1

        if self.config.get("no odd invocations", False) and self.fetch_invocations % 2 == 1:
            raise ValueError("That's odd.")

        self.enable_fetch.wait()

        payload = {
            "invocations": self.fetch_invocations,
            "data": "some-test-nonsense",
            "duplicate": "api-key"
        }
        if self._metadata is not None:
            return msg.Message(payload=payload, metadata=self._metadata)
        else:
            return payload

    def fetch_historic_data(self, start_time: datetime.datetime, end_time: datetime.datetime,
                            filter_clauses: Dict[str, Any]) -> Dict[str, Any]:
        """Generates some content and returns it"""

        self.history_invocations += 1

        assert self.start_invocations == self.stop_invocations + 1

        if self.config.get("no odd invocations", False) and self.history_invocations % 2 == 1:
            raise ValueError("That's odd.")

        return {
            "fetch_invocations": self.fetch_invocations,
            "history_invocations": self.history_invocations,
            "data": "another-test-nonsense",
            "duplicate": "same-api-key"
        }

    def stop(self):
        """Counts the stop and performs some basic checks"""
        assert self.start_invocations == self.stop_invocations + 1
        self.stop_invocations += 1
