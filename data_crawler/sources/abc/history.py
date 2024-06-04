"""
Implements the history mixins that enable a data source to fetch historic entries on request

The history function will most likely be called out of sync and in a one-shot basis. However, expect that they are
independent of the regular fetch calls. Nevertheless to ease thread safety, it is guaranteed that the history functions
will be called in the same thread as all the other functions.
"""
import abc
import datetime
from typing import Generator, Dict, Any

from data_crawler.sources.abc.message import MessageData


class AbstractMultiMessageHistorySourceMixin(abc.ABC):
    """
    The mixin adds the abstract functionality to query historic messages

    The mixin can safely assume that the fetch functions are called after initializing the base object.
    """

    @abc.abstractmethod
    def fetch_historic_data_bundle(self, filter_clauses: Dict[str, Any]) -> Generator[MessageData, None, None]:
        """
        Fetches the historic remote data into a bundle of multiple messages.

        The historic data must be filtered by the user-defined set of clauses (e.g. dict(start_date="2021-09-13",
        end_date="2023-09-13"). However, the naming convention of the filter expressions is up to the particular
        implementation.

        :param filter_clauses: The configuration stanza that specifies the amount of historic data to return.
        :returns: The function will return a generator that yields one message at a time. It is advised to keep the
            same message format as the original data source. In case the source supports multiple values at a time, pack
            all of them in a single message, is possible.
        """

        yield {}


class AbstractTimedMultiMessageHistorySourceMixin(AbstractMultiMessageHistorySourceMixin):
    """
    Implements the history mixin returning multiple messages generated from a range of historic time stamps.

    The class is mostly intended to unify the user interface by a common set of parameters
    """

    def fetch_historic_data_bundle(self, filter_clauses: Dict[str, Any]) -> Generator[MessageData, None, None]:
        """
        Fetches the historic remote data into a bundle of multiple messages.

        The historic data must be filtered by the user-defined set of clauses. The function expects at least a
        start_time and end_time key within the filter expressions.

        :param filter_clauses: The configuration stanza that specifies the amount of historic data to return.
        :returns: The function will return a generator that yields one message at a time. It is advised to keep the
            same message format as the original data source. In case the source supports multiple values at a time, pack
            all of them in a single message, is possible.
        """

        filter_clauses = filter_clauses.copy()  # Avoid breaking modifications

        if "start_time" not in filter_clauses or "end_time" not in filter_clauses:
            raise KeyError(f"The filter clauses require a 'start_time' and an 'end_time' key, but only keys "
                           f"{list(filter_clauses.keys())} are given.")

        import pandas as pd
        start_time = pd.to_datetime(filter_clauses["start_time"])
        if start_time.tz is None:
            raise ValueError(f"The start time {start_time.isoformat()} must be time-zone aware.")

        if filter_clauses["end_time"] == "now":  # magic word to trigger fetching the most recent interval.
            end_time = pd.Timestamp.now(tz=datetime.timezone.utc)
        else:
            end_time = pd.to_datetime(filter_clauses["end_time"])

        if end_time.tz is None:
            raise ValueError(f"The end time {end_time.isoformat()} must be time-zone aware.")

        if start_time > end_time:
            raise ValueError(f"The start time {start_time.isoformat()} must not be later than the end time "
                             f"{end_time.isoformat()}.")

        del filter_clauses["start_time"]
        del filter_clauses["end_time"]

        yield from self.fetch_timed_historic_data_bundle(start_time, end_time, filter_clauses)

    @abc.abstractmethod
    def fetch_timed_historic_data_bundle(self, start_time: datetime.datetime, end_time: datetime.datetime,
                                         filter_clauses: Dict[str, Any]) -> Generator[MessageData, None, None]:
        """
        Fetches a bundle of messages that can be filtered by a time interval.

        The time interval may either limit the number of messages or the corresponding content

        :param start_time: The first time stamp that marks the continuous history interval. Always inclusive.
        :param end_time: The last time stamp that marks the continuous history interval. Should be exclusive.
        :param filter_clauses: Any additional arguments passed on as filter expressions
        :return: Yields the fetched messages
        """

        yield {}


class AbstractTimedHistorySourceMixin(AbstractTimedMultiMessageHistorySourceMixin):
    """
    Implements the history mixin returning a single message generated from a range of historic time stamps.

    The class is mostly intended to unify the user interface by a common set of parameters
    """

    def fetch_timed_historic_data_bundle(self, start_time: datetime.datetime, end_time: datetime.datetime,
                                         filter_clauses: Dict[str, Any]) -> Generator[MessageData, None, None]:
        """
        Fetches the single message and returns it in an iterator

        :param start_time: The first time stamp that marks the continuous history interval. Always inclusive.
        :param end_time: The last time stamp that marks the continuous history interval. Should be exclusive.
        :param filter_clauses: Any additional arguments passed on as filter expressions
        :return: Yields the fetched message
        """

        yield self.fetch_historic_data(start_time, end_time, filter_clauses)

    @abc.abstractmethod
    def fetch_historic_data(self, start_time: datetime.datetime, end_time: datetime.datetime,
                            filter_clauses: Dict[str, Any]) -> MessageData:
        """
        Fetches the historic data and returns it in one single message

        Both time stamps will be time-zone aware and in (weakly) ascending order.

        :param start_time: The first time stamp that marks the continuous history interval. Always inclusive.
        :param end_time: The last time stamp that marks the continuous history interval. Should be exclusive.
        :param filter_clauses: Any additional arguments passed on as filter expressions
        :return: The historic data in one single message
        """
        return {}
