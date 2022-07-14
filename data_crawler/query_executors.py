"""
Implements the logic to periodically execute API calls in a dedicated context
"""

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

        # TODO: Load and store the source API

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
