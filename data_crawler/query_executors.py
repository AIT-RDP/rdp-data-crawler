"""
Implements the logic to periodically execute API calls in a dedicated context
"""
import abc
import copy
import dataclasses
import datetime
import fnmatch
import importlib
import inspect
import itertools
import json
import logging
import math
import random
import string
import threading
import traceback
from typing import Optional, Dict, Any, List, Iterable

import pandas as pd
import prometheus_client as prom
import redis

import data_crawler.sinks.abc.abstract_sink as abstract_sink
import data_crawler.sources.abc.abstract_source as abstract_source
import data_crawler.sources.abc.history as history_source
import data_crawler.sources.abc.message as msg

import data_crawler.access.storage as storage

logger = logging.getLogger(__name__)


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

    @property
    def max_permitted_cycle_time(self) -> datetime.timedelta:
        """Returns the maximum interval between two timer ticks. Note that this may not be the nominal time."""

        max_time = self._timer_interval + 2 * self._jitter
        return datetime.timedelta(seconds=max_time)


@dataclasses.dataclass()
class ActivityStatus:
    """Groups the execution activity status information for debugging and fault detection"""

    last_wakeup: Optional[datetime.datetime]
    last_cycle_complete: Optional[datetime.datetime]
    max_permitted_cycle_time: datetime.timedelta  # The maximum allowed time, not the measured one


class _QueryExecutorBase(abc.ABC):
    """
    Simple container class that holds the basic facilities of executing a data source.

    The class only does not implement the execution logic itself. This function is implementation specific and must be
    relayed to the child class.
    """

    def __init__(self, executor_config: dict, redis_pool: redis.ConnectionPool,
                 source_api: Optional[abstract_source.AbstractSourceAPI], name: str):
        """
        Initializes the executor and the connected source API but does not start any operation

        :param executor_config: The executor-specific configuration stanza
        :param redis_pool: The redis connection pool to draw the managed connections from.
        :param source_api: The source API to use. In case a source is given, the corresponding section in the
            configuration file will be ignored. Otherwise, the configuration will be parsed and the source will be
            dynamically instantiated.
        :param name: The name of the executor for debugging purpose
        """

        self._config = executor_config  # Expect protected scope. May be used by child classes as well
        self._source_name = name  # Protected scope

        persistent_store = storage.PersistentAPIStorage(redis_pool, name)

        if source_api is None:
            source_api = self._resolve_source_api(self._config, name, persistent_store)
        self._source_api = source_api  # Expect protected scope.

        self._logger = logging.getLogger(__name__ + "." + self.__class__.__name__ + "." + name)  # Protected scope
        self._data_sink = self._resolve_sink_api(executor_config, name, redis_pool)  # Protected scope

    @staticmethod
    def _load_api_class(type_name: str) -> type:
        """Dynamically resolves a dot-separated type and returns it"""

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

        return api_class

    @staticmethod
    def _resolve_source_api(
            executor_config: dict, executor_name: str,
            persistent_store: storage.PersistentAPIStorage
    ) -> abstract_source.AbstractMultiMessageSourceAPI:
        """
        Tries to load ind instantiate the source API

        :param executor_config: The configuration of the entire executor
        :param executor_name: The executor's name for debugging purposes
        :return: The newly instantiated source API object
        """

        type_name = executor_config["type"]
        api_class = _QueryExecutorBase._load_api_class(type_name)

        if not issubclass(api_class, abstract_source.AbstractMultiMessageSourceAPI):
            raise ModuleNotFoundError(f"The specified source API class '{type_name}' ({api_class}) is not an "
                                      f"AbstractMultiMessageSourceAPI.")

        api_object = api_class(source_parameters=executor_config["source parameter"], executor_name=executor_name,
                               persistent_store=persistent_store)
        return api_object

    @staticmethod
    def _resolve_sink_api(executor_config: dict, executor_name: str,
                          redis_pool: redis.ConnectionPool) -> abstract_sink.AbstractSinkAPI:
        """
        Tries to dynamically load the sink API and its configuration
        :param executor_config: The overall configuration of the executor
        :param executor_name: The name of the current executor that can be passed on to the sink for identification
        :param redis_pool: The common connection pool to the managed Redis database
        :return: The initialized sink
        """

        if "redis" in executor_config:
            # Legacy configuration syntax
            if "sink_type" in executor_config or "sink_parameters" in executor_config:
                raise ValueError("Cannot set 'sink_type' or 'sink_parameters' options when the legacy 'redis' "
                                 "configuration is supplied. Consider to only use the new syntax instead.")

            sink_type = "data_crawler.sinks.redis.RedisStream"
            sink_parameters = executor_config["redis"]

        else:
            # New, dynamic sink configuration syntax
            sink_type = executor_config["sink_type"]
            sink_parameters = executor_config["sink_parameters"]

        api_class = _QueryExecutorBase._load_api_class(sink_type)
        if not issubclass(api_class, abstract_sink.AbstractSinkAPI):
            raise ModuleNotFoundError(f"The specified source API class '{sink_type}' ({api_class}) is not an "
                                      f"AbstractSinkAPI.")

        config = api_class.parameter_model().model_validate(sink_parameters)
        api_object = api_class.create(config, executor_name=executor_name, redis_pool=redis_pool)
        return api_object

    @property
    def source_api(self) -> abstract_source.AbstractSourceAPI:
        """
        Returns the Source API object
        """

        return self._source_api

    def _push_messages(self, messages: Iterable[dict]):
        """
        Pushes all messages to the redis data sink.

        This function is assumed to be protected and may be used in child classes
        :param messages: The iterable of generator that yields the messages
        """

        meta_class = self._data_sink.metadata_model()

        for message in messages:
            if isinstance(message, msg.Message):
                self._data_sink.insert_data(message.payload, meta_class.model_validate(message.metadata, strict=True))
            else:
                self._data_sink.insert_data(message, meta_class())


class ThreadQueryExecutor(_QueryExecutorBase):
    """
    Periodically executes the hosted query and pushes the results to the connected REDIS database

    The long-running query operations are decoupled by a dedicated thread.
    """

    _prom_calls = prom.Counter("data_crawler_source_calls", labelnames=["source_name", "status"],
                               documentation="Number of calls to the data source")
    _prom_source_duration = prom.Summary("data_crawler_crawling_duration_seconds", labelnames=["source_name"],
                                         documentation="The duration of crawling a given source")
    _prom_call_latency = prom.Summary("data_crawler_scheduling_delay_seconds", labelnames=["source_name"],
                                      documentation="The delay of starting a data crawling job")

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
        super(ThreadQueryExecutor, self).__init__(executor_config, redis_pool, source_api, name)

        self._thread = threading.Thread(target=self._run_timed_execution)
        self._termination_event = threading.Event()
        self._startup_event = threading.Event()  # Mostly used for testing. Triggered when startup completes.

        self._timer = _ExecutionTimer(self._config["polling"], self._logger)

        self._activity_status: ActivityStatus = ActivityStatus(None, None, self._timer.max_permitted_cycle_time)
        self._activity_status_lock = threading.Lock()

        self._prom_calls.labels(source_name=name, status="started")
        self._prom_calls.labels(source_name=name, status="success")
        self._prom_calls.labels(source_name=name, status="failed")
        self._prom_source_duration.labels(source_name=name)
        self._prom_call_latency.labels(source_name=name)

    @property
    def source_api(self) -> abstract_source.AbstractSourceAPI:
        """
        Returns the Source API object

        The getter is mostly intended for testing purpose any may not be needed otherwise. It will raise an error in
        case the thread is already started.
        """

        if self._thread.is_alive():
            raise AttributeError("The source_api is accessed while the local executor is already started")
        return super(ThreadQueryExecutor, self).source_api

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

    def is_stop_triggered(self) -> bool:
        """
        Returns whether the stop procedure was already triggered
        """
        return self._termination_event.is_set()

    def join(self):
        """
        Waits until the executor is stopped
        """
        assert (
                self._termination_event.is_set() or
                (self._startup_event.is_set() and not self._thread.is_alive())  # Thread must have been started
        ), "The executor was not stopped before"
        self._thread.join()

        self._termination_event.clear()
        self._startup_event.clear()

    def _run_timed_execution(self):
        """Executes the queries until termination is signaled"""

        try:
            self._source_api.start()
            self._timer.reset()  # Reset after startup to avoid initial deadline misses
        finally:
            # Release the main thread (mostly to test the timing) However, to avoid deadlocks on crashed threads, always
            # release the startup flag, even if startup fails. Dead threads will be picked up by the supervisor anyway.
            self._startup_event.set()

        while True:
            timeout = self._timer.get_remaining_seconds()
            if timeout > 0:
                term_flag = self._termination_event.wait(timeout=timeout)
            else:
                term_flag = self._termination_event.is_set()

            if term_flag:
                self._logger.debug("Shut down the API crawler")
                break

            self._log_start_of_cycle()
            try:
                success = self._fetch_once()
            finally:
                self._timer.operation_done()
            self._log_end_of_cycle(success)

        self._source_api.stop()

    def _log_start_of_cycle(self):
        """Logs the start of the cycle and performs some basic sanity checks"""

        ts_now = datetime.datetime.now(tz=datetime.timezone.utc)
        timeout = self._timer.get_remaining_seconds()
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

    def _fetch_once(self) -> bool:
        """Performs one fetch and insert operation and returns the success status of the operations"""

        try:
            with self._prom_source_duration.labels(source_name=self._source_name).time():
                message_gen = self._source_api.fetch_data_bundle()
                self._push_messages(message_gen)
            success = True
        except Exception as err:
            self._logger.error(f"Skip one sample due to a {type(err).__name__}: {err}\n{traceback.format_exc()}")
            success = False
        return success

    def get_activity_status(self) -> ActivityStatus:
        """
        Returns a copy of the current activity status in a thread-save way
        :return: The current activity status
        """

        with self._activity_status_lock:
            return copy.copy(self._activity_status)

    def is_alive(self):
        """Returns whether the executer thread is currently alive (running or waiting)"""
        return self._thread.is_alive()


class QuerySupervisor:
    """
    Manages the collection of long-running query executors (and therefore data sources)

    The supervisor provides a unified interface to control the life cycle of the individual executors and to monitor
    their operation. In addition, it supports a restarting mechanism to gracefully restart failed executors.
    """

    _prom_source_status = prom.Gauge("data_crawler_source_status", labelnames=["source_name"],
                                     documentation="Status flag of the source. Ok, if zero.")
    _prom_source_restarted = prom.Counter("data_crawler_source_restarts", labelnames=["source_name"],
                                          documentation="Number of forced restarts due to crashed sources")
    _prom_supervisor_heartbeat = prom.Counter("data_crawler_supervisor_heartbeat",
                                              documentation="Number of heartbeat invocations")

    def __init__(self, source_config: Dict[str, dict], supervisor_config: dict, redis_pool: redis.ConnectionPool,
                 ext_sources: Optional[Dict[str, abstract_source.AbstractSourceAPI]] = None):
        """
        :param source_config: The dictionary of configured data source indexed by their unique name used for debugging
        :param supervisor_config: The configuration snippet of the supervisor
        :param redis_pool: The redis connection pool to draw the managed connections from
        :param ext_sources: Externally supplied data sources to test the supervisor
        """

        if ext_sources is None:
            ext_sources = {}

        self._dead_timeout = pd.to_timedelta(supervisor_config.get("dead timeout", "1 min"))

        self._redis_pool = redis_pool
        self._ext_sources = ext_sources
        self._source_config = source_config
        self._executors = self._create_executors(source_config, redis_pool, ext_sources)

        for name in self._executors.keys():
            self._prom_source_restarted.labels(source_name=name)
            self._prom_source_status.labels(source_name=name).set(4)

    @staticmethod
    def _create_executors(source_config: Dict[str, dict], redis_pool: redis.ConnectionPool,
                          ext_sources: Dict[str, abstract_source.AbstractSourceAPI]) -> Dict[str, ThreadQueryExecutor]:
        """Creates the collection of executors"""

        # preserve order, hence do not use sets here
        sources = list(source_config.keys()) + [src for src in ext_sources.keys() if src not in source_config]

        ret = {
            ex_name: ThreadQueryExecutor(source_config.get(ex_name, {}), redis_pool, ext_sources.get(ex_name, None),
                                         ex_name)
            for ex_name in sources
        }
        return ret

    def start(self):
        """Starts up all executors"""

        for ex in self._executors.values():
            ex.start()
        logger.debug(f"Started all {len(self._executors)} threads managed by the supervisor.")

    def stop(self):
        """Stops and joins all executor threads"""

        for ex in self._executors.values():
            ex.stop()

        for ex in self._executors.values():
            ex.join()
        logger.debug(f"Stopped all {len(self._executors)} threads managed by the supervisor.")

    def heartbeat(self) -> Dict[str, str]:
        """
        Performs the check and repair policy of the supervisor

        It is advised to regularly call the heartbeat function to be able to collect statistics and repair any failed
        source.

        :return: A dictionary of status messages per source
        """

        ts_now = datetime.datetime.now(tz=datetime.timezone.utc)
        ex_status = {}

        for ex_name, executor in self._executors.copy().items():
            is_alive = executor.is_alive()
            status = executor.get_activity_status()

            if not is_alive:
                logger.warning(f"Found source {ex_name} with the last wakeup at {status.last_wakeup} and last "
                               f"complete cycle {status.last_cycle_complete} to be dead. Restart the executor.")
                self._executors[ex_name].join()  # Make sure no dangling threads are left behind
                self._start_executor(ex_name)
                ex_status[ex_name] = "restarted"
                self._prom_source_status.labels(source_name=ex_name).set(3)
                self._prom_source_restarted.labels(source_name=ex_name).inc(1)

            elif (
                    status.last_wakeup is not None and
                    ts_now - status.last_wakeup > status.max_permitted_cycle_time + self._dead_timeout and
                    not self._executors[ex_name].is_stop_triggered()  # Avoid duplicate stop calls
            ):
                logger.warning(f"The source {ex_name} does not complete its cycle in time (last wakeup at "
                               f"{status.last_wakeup} and last complete cycle {status.last_cycle_complete}, max cycle "
                               f"time {status.max_permitted_cycle_time}). Issue an asynchronous stop signal.")
                self._executors[ex_name].stop()  # Don't wait for joins to not block the entire supervisor.
                ex_status[ex_name] = "stop-by-timeout"
                self._prom_source_status.labels(source_name=ex_name).set(1)

            elif (
                    status.last_wakeup is not None and
                    ts_now - status.last_wakeup > status.max_permitted_cycle_time + self._dead_timeout
            ):
                logger.debug(f"Source {ex_name} reached a timeout and is marked for restart but has not stopped yet.")
                ex_status[ex_name] = "blocking"
                self._prom_source_status.labels(source_name=ex_name).set(2)

            else:
                ex_status[ex_name] = "ok"
                self._prom_source_status.labels(source_name=ex_name).set(0)

        self._prom_supervisor_heartbeat.inc(1)
        return ex_status

    def _start_executor(self, ex_name):
        """Tries to (re-)start the given executor"""

        executor = ThreadQueryExecutor(self._source_config.get(ex_name, {}), self._redis_pool,
                                       self._ext_sources.get(ex_name, None), ex_name)
        executor.start()
        self._executors[ex_name] = executor

    @property
    def source_names(self) -> List[str]:
        """The list of managed sources. (Mostly for testing)"""
        return list(self._executors.keys())


class OneShotQueryExecutor(_QueryExecutorBase):
    """
    Implements a one-shot query execution without waiting for any timing interval.

    In contrast to the standard query execution, filtering is supported to specify the data to fetch.
    """

    def __init__(self, executor_config: dict, redis_pool: redis.ConnectionPool, name: str,
                 source_api: Optional[abstract_source.AbstractSourceAPI] = None):
        """
        Initializes the executor and the connected source API but does not start any operation

        :param executor_config: The executor-specific configuration stanza
        :param redis_pool: The redis connection pool to draw the managed connections from.
        :param name: The name of the executor for debugging purpose
        :param source_api: The source API to use. In case a source is given, the corresponding section in the
            configuration file will be ignored. Otherwise, the configuration will be parsed and the source will be
            dynamically instantiated.
        """
        super(OneShotQueryExecutor, self).__init__(executor_config, redis_pool, source_api, name)

        if not isinstance(self.source_api, history_source.AbstractMultiMessageHistorySourceMixin):
            raise ValueError(f"The given data source '{name}' is not a history data source")

    def execute_batch(self, filter_expressions: Iterable[Dict[str, Any]]):
        """
        Executes the batch of source invocations using the filter expressions.

        The resulting messages of each invocation are written to the Redis data sink.

        :param filter_expressions: The filter expressions. One dict per run.
        """

        assert isinstance(self._source_api, history_source.AbstractMultiMessageHistorySourceMixin)
        assert isinstance(self._source_api, abstract_source.AbstractMultiMessageSourceAPI)

        self._source_api.start()
        try:
            self._logger.debug(f"Start executing history batch on all {len(filter_expressions)} expressions")

            for filter_clauses in filter_expressions:
                message_gen = self._source_api.fetch_historic_data_bundle(filter_clauses)
                self._push_messages(message_gen)

            self._logger.debug(f"Finished history batch")
        finally:
            self._source_api.stop()


def execute_one_shot_batches(source_config: Dict[str, dict], redis_pool: redis.ConnectionPool,
                             filter_expressions: Iterable[Dict[str, Any]],
                             target_sources: Iterable[str], override_config: Optional[dict] = None,
                             debug_return=False) -> Dict[str, abstract_source.AbstractMultiMessageSourceAPI]:
    """
    Executes the listed sources and performs the query actions.

    :param source_config: The global configuration of all data sources. The configuration will be filtered by the
        target sources.
    :param redis_pool: The connection pool for IO
    :param filter_expressions: The filter expressions that are passed on to all listed target sources
    :param target_sources: The selected sources that will be executed. All sources must support the history API. Each
        entry is treated as a glob pattern that selects a subset of sources.
    :param override_config: Some optional config stanzas that will be applied to every executed source. The
        configuration wilkl be merged recursively.
    :param debug_return: Flag that indicates whether the sources should be collected and returned. Otherwise, an empty
        dict is returned for performance and resource constraints reasons.
    :return: The instantiated sources for debugging purpose. Most likely not used in production.
    """

    if override_config is None:
        override_config = {}

    selected_sources = set(itertools.chain(*[fnmatch.filter(source_config.keys(), pt) for pt in target_sources]))
    sources = {}

    for source_name in selected_sources:
        config = _merge_config(source_config[source_name], override_config)
        logger.debug(f"Start one-shot runs of {source_name} with "
                     f"{'original' if override_config == dict() else 'updated'} configuration {config}.")
        executor = OneShotQueryExecutor(config, redis_pool, source_name)
        executor.execute_batch(filter_expressions)

        if debug_return:
            sources[source_name] = executor.source_api

    return sources


def _merge_config(cnf_dst, cnf_new):
    """Recursively merges the base configuration cnf_dst and the cnf_new that overrides any existing config"""

    if isinstance(cnf_dst, dict) != isinstance(cnf_new, dict):
        raise ValueError(f"One of the configs to merge is not a dict: {cnf_dst}, {cnf_new}")

    if isinstance(cnf_dst, list) != isinstance(cnf_new, list):
        raise ValueError(f"One of the configs to merge is not a list: {cnf_dst}, {cnf_new}")

    cnf_dst = copy.copy(cnf_dst)
    if isinstance(cnf_dst, dict):  # Merge the dicts item-wise
        for new_key, new_value in cnf_new.items():
            cnf_dst[new_key] = _merge_config(cnf_dst.get(new_key, new_value), new_value)

    elif isinstance(cnf_dst, list):  # Merge the list item wise
        if len(cnf_dst) != len(cnf_new):
            raise KeyError(f"Cannot merge two lists with different length: {cnf_dst}, {cnf_new}")

        cnf_dst = [_merge_config(a, b) for a, b in zip(cnf_dst, cnf_new)]

    else:  # It's a leaf, just return the new item
        cnf_dst = cnf_new

    return cnf_dst
