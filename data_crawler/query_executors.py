"""
Implements the logic to periodically execute API calls in a dedicated context
"""

import importlib
import inspect
import threading
from typing import Optional

import redis

import data_crawler.sources.abc.abstract_source as abstract_source


class ThreadQueryExecutor:
    """
    Periodically executes the hosted query and pushes the results to the connected REDIS database

    The long-running query operations are decoupled by a dedicated thread.
    """

    def __init__(self, executor_config: dict, redis_pool: redis.ConnectionPool,
                 source_api: Optional[abstract_source.AbstractSourceAPI] = None):
        """
        Initializes the executor and the connected source API but does not start any operation

        :param executor_config: The executor-specific configuration stanza
        :param redis_pool: The redis connection pool to draw the managed connections from.
        :param source_api: The source API to use. In case a source is given, the corresponding section in the
            configuration file will be ignored. Otherwise, the configuration will be parsed and the source will be
            dynamically instantiated.
        """

        self._config = executor_config
        self._redis_pool = redis_pool

        self._thread = threading.Thread(target=self._run_timed_execution)
        self._termination_event = threading.Event()

        if source_api is None:
            source_api = self._resolve_source_api(self._config)
        self._source_api = source_api

    @staticmethod
    def _resolve_source_api(executor_config: dict) -> abstract_source.AbstractSourceAPI:
        """
        Tries to load ind instantiate the source API

        :param executor_config: The configuration of the entire executor
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

        if not issubclass(api_class, abstract_source.AbstractSourceAPI):
            raise ModuleNotFoundError(f"The specified source API class '{type_name}' ({api_class}) is not an "
                                      f"AbstractSourceAPI.")

        api_object = api_class(source_parameters=executor_config["source parameter"])
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

    def _run_timed_execution(self):
        """Executes the queries until termination is signaled"""

        self._termination_event.wait()  # TODO: Do something useful
