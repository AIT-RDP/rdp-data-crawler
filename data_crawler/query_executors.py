"""
Implements the logic to periodically execute API calls in a dedicated context
"""
import datetime
import importlib
import inspect
import json
import logging
import math
import random
import string
import threading
import traceback
from typing import Optional, Dict, Any

import pandas as pd
import redis

import data_crawler.sources.abc.abstract_source as abstract_source


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

        self._next_tick_nominal += self._timer_interval
        self._next_tick_actual = self._next_tick_nominal + self._rnd.uniform(-self._jitter, self._jitter)

        remaining = self.get_remaining_seconds()
        if remaining > self._timer_interval + 2 * self._jitter:  # Skip some queries
            num_skip = math.floor(remaining / self._timer_interval)
            self._next_tick_actual += self._timer_interval * num_skip
            self._next_tick_nominal += self._timer_interval * num_skip
            self._logger.warning(f"Skipped {num_skip} queries since the previous queries were too much delayed.")


class _RedisDataSink:
    """Helper class that relays data to a redis stream according to the configuration"""

    def __init__(self, redis_config: dict, redis_pool: redis.ConnectionPool):
        """
        Initializes the data sink

        :param redis_config: The executor-specific redis configuration
        :param redis_pool:  The connection pool to use at the redis client
        """

        self._client = redis.Redis(connection_pool=redis_pool)
        self._stream_template = string.Template(redis_config["stream"])
        self._tags = redis_config.get("tags", {})

    def push_data(self, message: Dict[str, Any]):
        """
        Write the received message to the Redis stream

        :param message: The message as received by the source API
        """

        message = message.copy()  # To be on the safe side. Remove if it turns out to be a performance bottleneck
        message.update(self._tags)

        encoded_message = {key: json.dumps(val) for key, val in message.items()}
        self._client.xadd(self._stream_template.substitute(message), encoded_message)


class ThreadQueryExecutor:
    """
    Periodically executes the hosted query and pushes the results to the connected REDIS database

    The long-running query operations are decoupled by a dedicated thread.
    """

    def __init__(self, executor_config: dict, redis_pool: redis.ConnectionPool,
                 source_api: Optional[abstract_source.AbstractSourceAPI] = None, name: str = "<default>"):
        """
        Initializes the executor and the connected source API but does not start any operation

        :param executor_config: The executor-specific configuration stanza
        :param redis_pool: The redis connection pool to draw the managed connections from.
        :param source_api: The source API to use. In case a source is given, the corresponding section in the
            configuration file will be ignored. Otherwise, the configuration will be parsed and the source will be
            dynamically instantiated.
        :param name: The name of the executor for debugging purpose
        """

        self._config = executor_config

        self._thread = threading.Thread(target=self._run_timed_execution)
        self._termination_event = threading.Event()
        self._startup_event = threading.Event()  # Mostly used for testing. Triggered when startup completes.

        if source_api is None:
            source_api = self._resolve_source_api(self._config, name)
        self._source_api = source_api

        self._logger = logging.getLogger(__name__ + "." + self.__class__.__name__ + "." + name)
        self._timer = _ExecutionTimer(self._config["polling"], self._logger)
        self._data_sink = _RedisDataSink(self._config["redis"], redis_pool)

    @staticmethod
    def _resolve_source_api(executor_config: dict, executor_name: str) -> abstract_source.AbstractMultiMessageSourceAPI:
        """
        Tries to load ind instantiate the source API

        :param executor_config: The configuration of the entire executor
        :param executor_name: The executor's name for debugging purposes
        :return: The newly instantiated source API object
        """

        type_name = executor_config["type"]
        name_components = str(type_name).split(".")
        if len(name_components) < 2:
            raise KeyError(f"The API type configuration '{type_name}' is invalid. Cannot separate the package and "
                           f"class component separated by dots.")

        module_name = ".".join(name_components[:-1])
        api_module = importlib.import_module(module_name)
        assert api_module is not None

        api_class: type = getattr(api_module, name_components[-1])
        if not inspect.isclass(api_class):
            raise ModuleNotFoundError(f"The specified source API '{type_name}' ({api_class}) is not an class.")

        if not issubclass(api_class, abstract_source.AbstractMultiMessageSourceAPI):
            raise ModuleNotFoundError(f"The specified source API class '{type_name}' ({api_class}) is not an "
                                      f"AbstractMultiMessageSourceAPI.")

        api_object = api_class(source_parameters=executor_config["source parameter"], executor_name=executor_name)
        return api_object

    @property
    def source_api(self) -> abstract_source.AbstractSourceAPI:
        """
        Returns the Source API object

        The getter is mostly intended for testing purpose any may not be needed otherwise. It will raise an error in
        case the thread is already started.
        """

        if self._thread.is_alive():
            raise AttributeError("The source_api is accessed while the local executor is already started")
        return self._source_api

    def start(self):
        """
        Starts the executor operation in an independent thread.
        """

        assert not self._termination_event.is_set(), "The executor has already been stopped"
        self._thread.start()
        self._startup_event.wait()

    def stop(self):
        """
        Signals to stop the executor but does not wait until it is actually stopped.
        """
        assert not self._termination_event.is_set(), "The executor has already been stopped"
        self._termination_event.set()

    def join(self):
        """
        Waits until the executor is stopped
        """
        assert self._termination_event.is_set(), "The executor was not stopped before"
        self._thread.join()

        self._termination_event.clear()
        self._startup_event.clear()

    def _run_timed_execution(self):
        """Executes the queries until termination is signaled"""

        self._source_api.start()
        self._timer.reset()  # Reset after startup to avoid initial deadline misses
        self._startup_event.set()  # Release the main thread (mostly to test the timing)

        while True:
            timeout = self._timer.get_remaining_seconds()
            if timeout > 0:
                term_flag = self._termination_event.wait(timeout=timeout)
            else:
                term_flag = self._termination_event.is_set()

            if term_flag:
                self._logger.debug("Shut down the API crawler")
                break

            assert self._timer.get_remaining_seconds() <= 0.0, "The event didn't awaited its timeout."

            try:
                self._fetch_once()
            finally:
                self._timer.operation_done()

        self._source_api.stop()

    def _fetch_once(self):
        """Performs one fetch and insert operation"""

        try:
            for data in self._source_api.fetch_data_bundle():
                self._data_sink.push_data(data)
        except Exception as err:
            self._logger.error(f"Skip one sample due to a {type(err).__name__}: {err}\n{traceback.format_exc()}")
