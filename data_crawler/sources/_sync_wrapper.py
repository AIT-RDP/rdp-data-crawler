"""
Implements some synchronous wrapper classes to couple the polling-based and the active API.
"""
import copy
import datetime
import logging
import math
import random
import threading
import time
import traceback
from typing import Generator, Iterable, Dict, Any

import pandas as pd
import prometheus_client as prom

import data_crawler.sources.abc.message as msg
import data_crawler.sources.abc.abstract_source as abstract_source
import data_crawler.sources.abc.active_source_sync as active_source_sync
import data_crawler.sources.abc.history as history
from data_crawler.sources.abc.message import MessageData


class _ExecutionTimer:
    """A helper class that computes the time until the next query should be performed"""

    def __init__(self, timer_config: dict, logger: logging.Logger):
        """
        Initializes the timer

        :param timer_config: The configuration snippet describing the timing behavior
        :param logger: A logger to pase some debug information to
        """

        self._logger = logger

        freq_name = timer_config["frequency"]
        self._timer_interval = pd.Timedelta(freq_name).total_seconds()

        # Offset from the start of the current year, UTC. The start of the current year was chosen to mitigate some
        # issues with leap seconds. Right now, leap-seconds on June, 30 are not encountered.
        offset_name = timer_config.get("offset", "0s")
        self._offset = pd.Timedelta(offset_name).total_seconds()

        # Configure time slots that may be used to shift the queries to avoid concurrent access to some device
        slot_id = timer_config.get("slot id", 0)
        slot_count = int(timer_config.get("slot count", 1))
        if slot_id is None or str(slot_id).lower() == "false" or int(slot_id) < 0:
            self._logger.info(f"The timer is disabled by slot '{slot_id}'. No query will be performed.")
            self._timer_interval = 3600.0 * 24 * 356 * 100  # Once every 100 Years
            slot_id = 0
        slot_id = int(slot_id)
        if slot_id >= slot_count:
            raise ValueError(f"Invalid slot id {slot_id} on {slot_count} slot(s) in total.")
        self._offset += (self._timer_interval / slot_count) * slot_id

        # The uniformly distributed random jitter to apply. (Symmetrically around the offset)
        self._jitter = pd.Timedelta(timer_config.get("jitter", "0s")).total_seconds()
        self._rnd = random.Random()

        # Configure immediate fetch operation
        self._force_initial = bool(timer_config.get("force initial", True))

        self._logger.debug(f"Set timer interval to {self._timer_interval}s ({freq_name}) aligning to an offset of "
                           f"{self._offset}s ({offset_name}) +/-{self._jitter}s.")

        self._next_tick_actual = 0.0  # Pre-reset default value to satisfy the linter
        self._next_tick_nominal = 0.0  # Pre-reset default value to satisfy the linter
        self.reset()

    def reset(self):
        """Clears the state and instructs the timer to fire immediately"""
        date_now = datetime.datetime.utcnow()
        base_date = datetime.datetime(date_now.year, 1, 1, 0, 0, 0, tzinfo=date_now.tzinfo)
        base_date += pd.Timedelta(seconds=self._offset - self._timer_interval)

        num_skip = (date_now - base_date).total_seconds() / self._timer_interval
        # Floor to immediately trigger a tick or ceil to wait for the next tick to appear:
        num_skip = math.floor(num_skip) if self._force_initial else math.ceil(num_skip)
        base_date += pd.Timedelta(seconds=num_skip * self._timer_interval)

        # Set the nominal and actual tick as needed. Don't forget to also jitter the first tick, as needed
        self._next_tick_nominal = base_date.timestamp()
        if self._force_initial:
            # Use the current time as base for jitter since the last sample may be likely overdue.
            self._next_tick_actual = date_now.timestamp() + self._rnd.uniform(0, self._jitter)
        else:
            # Just jitter the nominal base date. It should point to the next instance of time
            self._next_tick_actual = base_date.timestamp() + self._rnd.uniform(-self._jitter, self._jitter)

    def get_remaining_seconds(self) -> float:
        """
        Returns the number of seconds until the timer fires next

        The number may be negative in case it should already be fired
        """
        now = datetime.datetime.utcnow().timestamp()  # Unify with reset function.
        return self._next_tick_actual - now

    def operation_done(self):
        """Indicates that the operation was just completed and that the time can advance to the next step."""

        # How late we finished relative to the tick just executed. Positive means overdue.
        # Note: get_remaining_seconds returns negative value if the tick is due.
        overdue = -self.get_remaining_seconds()
        num_skip = 0
        if overdue > self._timer_interval + 2 * self._jitter:
            # Drop whole missed intervals.
            num_skip = math.floor(overdue / self._timer_interval)

        self._next_tick_nominal += self._timer_interval * (1 + num_skip)
        self._next_tick_actual = self._next_tick_nominal + self._rnd.uniform(-self._jitter, self._jitter)

        if num_skip:
            self._logger.warning(f"Skipped {num_skip} queries since the previous queries were too much delayed.")

    @property
    def max_permitted_cycle_time(self) -> datetime.timedelta:
        """Returns the maximum interval between two timer ticks. Note that this may not be the nominal time."""

        max_time = self._timer_interval + 2 * self._jitter
        return datetime.timedelta(seconds=max_time)


class SyncPollingExecutor(active_source_sync.AbstractSyncActiveSourceAPI,
                          history.AbstractMultiMessageHistorySourceMixin):
    """
    Wraps the abstract sources and provides an active interface for them

    In order to allow for smooth coexistence of the polling-based and active API, this wrapper is intended to be
    directly instantiated instead to calling the factory function. By that braking configuration changes can be avoided
    and the wrapper can be transparently inserted.
    """

    _prom_calls = prom.Counter("data_crawler_source_calls", labelnames=["source_name", "status"],
                               documentation="Number of calls to the data source")
    _prom_source_duration = prom.Summary("data_crawler_crawling_duration_seconds", labelnames=["source_name"],
                                         documentation="The duration of crawling a given source")
    _prom_call_latency = prom.Summary("data_crawler_scheduling_delay_seconds", labelnames=["source_name"],
                                      documentation="The delay of starting a data crawling job")

    def __init__(self, source_api: abstract_source.AbstractMultiMessageSourceAPI, executor_config: dict,
                 executor_name: str):
        """
        Initializes the polling-based wrapper using the given API

        :param source_api: The actual API to wrap
        :param executor_config: The whole executor configuration to fetch the polling configuration from.
        :param executor_name: The name of the executor for debugging purposes
        """
        super(SyncPollingExecutor, self).__init__()

        self._source_api = source_api
        self._termination_event = threading.Event()

        self._logger = logging.getLogger(__name__ + "." + self.__class__.__name__ + "." + executor_name)
        self._timer = _ExecutionTimer(executor_config["polling"], self._logger)

        self._activity_status = active_source_sync.ActivityStatus(None, None, self._timer.max_permitted_cycle_time)
        self._activity_status_lock = threading.Lock()

        self._source_name = executor_name

        self._prom_calls.labels(source_name=self._source_name, status="started")
        self._prom_calls.labels(source_name=self._source_name, status="success")
        self._prom_calls.labels(source_name=self._source_name, status="failed")
        self._prom_source_duration.labels(source_name=self._source_name)
        self._prom_call_latency.labels(source_name=self._source_name)

    @property
    def source_api(self) -> abstract_source.AbstractMultiMessageSourceAPI:
        """
        Returns the encapsulated source API

        The function is mainly intended for debugging purpose and should hardly be necessary.
        """

        return self._source_api

    @classmethod
    def create(cls, source_parameters: active_source_sync.SourceParameters,
               **kwargs) -> active_source_sync.AbstractSyncActiveSourceAPI:
        """Raises an error as the wrapper needs to be transparently inserted and may nto be dynamically instantiated"""
        raise NotImplementedError("The SyncPollingExecutor cannot be automatically instantiated.")

    def start(self):
        """Relays the start hook to the wrapped source"""
        self._source_api.start()

    def run(self) -> Generator[msg.MessageData, None, None]:
        """Executes the source until a termination request is issued"""

        self._timer.reset()  # Reset after startup to avoid initial deadline misses

        while True:
            timeout = self._timer.get_remaining_seconds()
            # Anchor the wall-clock deadline onto the monotonic clock used by Event.wait() so that a sub-millisecond
            # skew between the two clocks cannot be misread as an early wakeup in the sanity check below.
            deadline_timer = time.monotonic() + timeout
            if timeout > 0:
                term_flag = self._termination_event.wait(timeout=timeout)
            else:
                term_flag = self._termination_event.is_set()

            if term_flag:
                self._logger.debug("Shut down the API crawler")
                break

            self._log_start_of_cycle(deadline_timer)

            # Init success to False to ensure that the cycle is reported as failed in case the generator raises an exception
            success = False
            try:
                # The success status is returned by the generator and thus only available once it is exhausted.
                success = yield from self._fetch_once()
            finally:
                self._timer.operation_done()
            self._log_end_of_cycle(success)

    def _log_start_of_cycle(self, deadline_timer: float):
        """Logs the start of the cycle and performs some basic sanity checks"""

        ts_now = datetime.datetime.now(tz=datetime.timezone.utc)
        timeout = deadline_timer - time.monotonic()
        if timeout > 0.0:
            self._logger.error(f"The timer didn't awaited its timeout. {timeout} seconds left.")
            assert False, "The event didn't awaited its timeout."

        self._prom_calls.labels(source_name=self._source_name, status="started").inc(1)
        self._prom_call_latency.labels(source_name=self._source_name).observe(-timeout)

        with self._activity_status_lock:
            self._activity_status.last_wakeup = ts_now

    def _log_end_of_cycle(self, success: bool):
        """Logs the end of the cycle"""

        ts_now = datetime.datetime.now(tz=datetime.timezone.utc)

        status = "success" if success else "failed"
        self._prom_calls.labels(source_name=self._source_name, status=status).inc(1)

        with self._activity_status_lock:
            self._activity_status.last_cycle_complete = ts_now

    def _fetch_once(self) -> Generator[msg.MessageData, None, bool]:
        """
        Performs one fetch and insert operation and returns the success status of the operations

        The status is the return value of the generator. Hence, it needs to be picked up by a 'yield from' statement.
        Note that a partially completed cycle - some messages were already yielded before the error occurred - is
        reported as a failure as well.
        """

        try:
            with self._prom_source_duration.labels(source_name=self._source_name).time():
                message_gen = self._source_api.fetch_data_bundle()
                yield from message_gen
        except Exception as err:
            self._logger.error(f"(Partially) skip one sample due to a "
                               f" {type(err).__name__}: {err}\n{traceback.format_exc()}")
            return False

        return True

    def shutdown(self) -> None:
        """Sets the termination event to shut down the periodic execution"""
        assert not self._termination_event.is_set(), "The executor has already been stopped"
        self._termination_event.set()

    def stop(self) -> None:
        """Relays the stop event to the encapsulated data source"""
        self._source_api.stop()

    def get_activity_status(self) -> active_source_sync.ActivityStatus:
        """Returns the current activity status"""
        with self._activity_status_lock:
            return copy.copy(self._activity_status)

    def fetch_historic_data_bundle(self, filter_clauses: Dict[str, Any]) -> Generator[MessageData, None, None]:
        """Relays the history call to the encapsulated API"""

        if isinstance(self._source_api, history.AbstractMultiMessageHistorySourceMixin):
            yield from self._source_api.fetch_historic_data_bundle(filter_clauses)
        else:
            raise ValueError(f"The data source of '{self._source_name}' - {self._source_name.__class__} is not a "
                             f"history data source")

    @staticmethod
    def parameter_model() -> type[active_source_sync.SourceParameters]:
        return active_source_sync.SourceParameters  # Do not enforce a specific parameter model.
