"""
Implements the logic to periodically execute API calls in a dedicated context
"""
import abc
import copy
import fnmatch
import importlib
import inspect
import itertools
import logging
import threading
import traceback
from typing import Optional, Dict, Any, Iterable

import redis

import data_crawler.sinks.abc.abstract_sink as abstract_sink
import data_crawler.sources.abc.abstract_source as abstract_source
import data_crawler.sources.abc.active_source_sync as active_sync_source
import data_crawler.sources.abc.history as history_source
import data_crawler.sources.abc.message as msg
import data_crawler.sources._sync_wrapper as _sync_wrapper

import data_crawler.access.storage as storage

logger = logging.getLogger(__name__)


class _QueryExecutorBase(abc.ABC):
    """
    Simple container class that holds the basic facilities of executing a data source.

    The class only does not implement the execution logic itself. This function is implementation specific and must be
    relayed to the child class.
    """

    def __init__(self, executor_config: dict, redis_pool: redis.ConnectionPool,
                 source_api: Optional[
                     abstract_source.AbstractSourceAPI | active_sync_source.AbstractSyncActiveSourceAPI],
                 name: str):
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
            source_api = self._resolve_source_api(self._config, name, redis_pool, persistent_store)
        if isinstance(source_api, abstract_source.AbstractMultiMessageSourceAPI):
            source_api = _sync_wrapper.SyncPollingExecutor(source_api, executor_config, name)
        self._source_api = source_api  # Expect protected scope.

        self._logger = logging.getLogger(__name__ + "." + self.__class__.__name__ + "." + name)  # Protected scope
        self._data_sink = self._resolve_sink_api(executor_config, name, redis_pool)  # Protected scope

        self._dry_run = self._resolve_save_boolean(executor_config.get("dry_run", False))
        if self._dry_run:
            self._logger.info(f"DRY RUN is ON. Channel {name} will execute queries but does not forward anything.")

        self._initial = True # Flag to indicate if the initial execution of the sink has already been performed

    @staticmethod
    def _resolve_save_boolean(value: bool | int | str) -> bool:
        """Resolves the boolean configuration raising an Exception if it cannot be interpreted"""

        if isinstance(value, bool):
            return value

        value = str(value)

        true_values = ["true", "1"]
        false_values = ["false", "0"]

        if any(ref == value.lower() for ref in true_values):
            return True
        elif any(ref == value.lower() for ref in false_values):
            return False
        else:
            raise ValueError(f"Cannot interpret the boolean configuration '{value}'. Expect one of "
                             f"{true_values + false_values}.")

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
            executor_config: dict, executor_name: str, redis_pool: redis.ConnectionPool,
            persistent_store: storage.PersistentAPIStorage
    ) -> abstract_source.AbstractMultiMessageSourceAPI | active_sync_source.AbstractSyncActiveSourceAPI:
        """
        Tries to load ind instantiate the source API

        :param executor_config: The configuration of the entire executor
        :param executor_name: The executor's name for debugging purposes
        :return: The newly instantiated source API object
        """

        type_name = executor_config["type"]
        api_class = _QueryExecutorBase._load_api_class(type_name)

        if issubclass(api_class, abstract_source.AbstractMultiMessageSourceAPI):
            # Instantiate the legacy object that expects synchronous polling
            api_object = api_class(source_parameters=executor_config["source parameter"], executor_name=executor_name,
                                   persistent_store=persistent_store, redis_pool=redis_pool)
        elif issubclass(api_class, active_sync_source.AbstractSyncActiveSourceAPI):
            # Instantiate the new active interface
            api_parameters = api_class.parameter_model().model_validate(executor_config["source parameter"])
            api_object = api_class.create(api_parameters, executor_name=executor_name,
                                          persistent_store=persistent_store, redis_pool=redis_pool)
        else:
            # No appropriate class found
            raise ModuleNotFoundError(f"The specified source API class '{type_name}' ({api_class}) is not an "
                                      f"AbstractMultiMessageSourceAPI or AbstractSyncActiveSourceAPI.")

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
    def source_api(
            self
    ) -> active_sync_source.AbstractSyncActiveSourceAPI | abstract_source.AbstractMultiMessageSourceAPI:
        """
        Returns the Source API object.

        For compatibility reasons, any polling-based API will be unwrapped and directly returned.
        """
        if isinstance(self._source_api, _sync_wrapper.SyncPollingExecutor):
            return self._source_api.source_api
        else:
            return self._source_api

    def _push_messages(self, messages: Iterable[dict | msg.Message]):
        """
        Pushes all messages to the redis data sink.

        This function is assumed to be protected and may be used in child classes
        :param messages: The iterable of generator that yields the messages
        """

        meta_class = self._data_sink.metadata_model()

        for message in messages:
            if isinstance(message, msg.Message):
                meta_data = meta_class.model_validate(message.metadata, strict=True)
                meta_data.initial = self._initial

                if self._data_sink.include_metadata:
                    message.payload.update(meta_data.model_dump())

                self._dry_run or self._data_sink.insert_data(message.payload, meta_data)
            else:
                meta_data = meta_class(initial=self._initial)

                if self._data_sink.include_metadata:
                    message.update(meta_data.model_dump())

                self._dry_run or self._data_sink.insert_data(message, meta_data)

        self._initial = False # After the first execution, the initial flag is set to False

class ThreadQueryExecutor(_QueryExecutorBase):
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
        super(ThreadQueryExecutor, self).__init__(executor_config, redis_pool, source_api, name)

        self._thread = threading.Thread(target=self._run_api)
        self._termination_event = threading.Event()
        self._startup_event = threading.Event()  # Mostly used for testing. Triggered when startup completes.

    @property
    def source_api(
            self
    ) -> active_sync_source.AbstractSyncActiveSourceAPI | abstract_source.AbstractMultiMessageSourceAPI:
        """
        Returns the Source API object

        The getter is mostly intended for testing purpose any may not be needed otherwise.
        """
        return super(ThreadQueryExecutor, self).source_api

    def start(self):
        """
        Starts the executor operation in an independent thread.
        """

        assert not self._termination_event.is_set(), "The executor has already been stopped"
        self._thread.start()
        self._startup_event.wait()

    def shutdown(self):
        """
        Signals to shutdown the executor but does not wait until it is actually stopped.
        """
        assert not self._termination_event.is_set(), "The executor has already been stopped"
        self._termination_event.set()
        self._source_api.shutdown()

    def is_shutdown_triggered(self) -> bool:
        """
        Returns whether the shutdown procedure was already triggered
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

    def _run_api(self):
        """Executes the queries until the source terminates"""

        try:
            self._source_api.start()
        finally:
            # Release the main thread (mostly to test the timing) However, to avoid deadlocks on crashed threads, always
            # release the startup flag, even if startup fails. Dead threads will be picked up by the supervisor anyway.
            self._startup_event.set()

        try:
            for message in self._source_api.run():
                self._push_messages([message])
        except Exception as ex:
            self._logger.error(f"Received a '{ex.__class__}' error within the execution cycle. Exit the channel."
                               f"{traceback.format_exc()}")
        finally:
            self._source_api.stop()

    def get_activity_status(self) -> active_sync_source.ActivityStatus:
        """
        Returns a copy of the current activity status in a thread-save way
        :return: The current activity status
        """

        return self._source_api.get_activity_status()

    def is_alive(self):
        """Returns whether the executor thread is currently alive (running or waiting)"""
        return self._thread.is_alive()


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
        assert isinstance(self._source_api, active_sync_source.AbstractSyncActiveSourceAPI)

        filter_expressions = list(filter_expressions)

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
                             debug_return=False) -> Dict[str, active_sync_source.AbstractSyncActiveSourceAPI]:
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
