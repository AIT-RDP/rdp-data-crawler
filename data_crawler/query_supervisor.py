"""
Implements the query supervisor that manages the individual executors and restarts them, if needed
"""
import asyncio
import datetime
import itertools
import logging
from typing import Optional, Dict, Iterable, List

import pandas as pd
import prometheus_client as prom
import pyrdp_commons as commons
import redis

from data_crawler.query_executors import ThreadQueryExecutor
import data_crawler.sources.abc.abstract_source as abstract_source

logger = logging.getLogger(__name__)


class AsyncQuerySupervisor:
    """
    Manages the collection of long-running query executors (and therefore data sources)

    The supervisor provides a unified interface to control the life cycle of the individual executors and to monitor
    their operation. In addition, it supports a restarting mechanism to gracefully restart failed executors. To be able
    to efficiently watch for configuration changes or failed query executors, the supervisor is implemented as an
    asynchronous component.
    """

    _prom_source_status = prom.Gauge("data_crawler_source_status", labelnames=["source_name"],
                                     documentation="Status flag of the source. Ok, if zero.")
    _prom_source_restarted = prom.Counter("data_crawler_source_restarts", labelnames=["source_name"],
                                          documentation="Number of forced restarts due to crashed sources")
    _prom_supervisor_heartbeat = prom.Counter("data_crawler_supervisor_heartbeat",
                                              documentation="Number of heartbeat invocations")

    def __init__(self, source_config: commons.ConfigDict, supervisor_config: dict,
                 redis_pool: redis.ConnectionPool,
                 ext_sources: Optional[Dict[str, abstract_source.AbstractSourceAPI]] = None):
        """
        :param source_config: The configuration container of configured data source indexed by their unique name used
            for debugging. It is expected that the supervisor can watch for configuration changes at the configuration
            container. Hence, the previous dict-based interface is not sufficient anymore.
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
        self._config_observer: Optional[asyncio.Task] = None

        for name in self._executors.keys():
            self._prom_source_restarted.labels(source_name=name)
            self._prom_source_status.labels(source_name=name).set(4)

    @staticmethod
    def _create_executors(source_config: commons.ConfigDict, redis_pool: redis.ConnectionPool,
                          ext_sources: Dict[str, abstract_source.AbstractSourceAPI]) -> Dict[str, ThreadQueryExecutor]:
        """Creates the collection of executors"""

        # Only supply the build in containers to avoid concurrency issues when changing the content dynamically.
        source_config = source_config.to_built_in_container()

        # preserve order, hence do not use sets here
        sources = list(source_config.keys()) + [src for src in ext_sources.keys() if src not in source_config]

        ret = {
            ex_name: ThreadQueryExecutor(source_config.get(ex_name, {}), redis_pool, ext_sources.get(ex_name, None),
                                         ex_name)
            for ex_name in sources
        }
        return ret

    async def start(self):
        """Starts up all executors as well as the background task that watches the configuration"""

        assert self._config_observer is None, "the start() function must not me called beforehand"

        for ex in self._executors.values():
            ex.start()
        logger.debug(f"Started all {len(self._executors)} threads managed by the supervisor.")

        self._config_observer = asyncio.Task(self._watch_for_config_changes(), name="Configuration Change Observer")
        logger.debug(f"Start to listen for externally induced configuration changes")

    async def stop(self):
        """Stops and joins all executor threads"""

        # Stop watching for configuration changes and make sure the reloading logic is not operational anymore.
        assert self._config_observer is not None, "The start() function must be successfully called before"
        self._config_observer.cancel()
        try:
            await self._config_observer
        except asyncio.CancelledError:
            pass  # We cancelled the execution so this should be fine.
        logger.debug(f"Stopped listening for configuration changes")

        # Shutdown and join the executors
        for ex in self._executors.values():
            ex.shutdown()

        for ex in self._executors.values():
            ex.join()
        logger.debug(f"Stopped all {len(self._executors)} threads managed by the supervisor.")

    async def _watch_for_config_changes(self):
        """
        Watches for configuration changes and restart the appropriate sources

        The function will block as long as there may be new configuration changes. It can be safely cancelled to stop
        listening to new changes. Do not call this function more than once at it will overwrite the outdated listeners
        """
        current_config = self._source_config.to_built_in_container()  # Static image to detect any changes

        async def _handle_changes(event: commons.ChangeEvent):
            new_config = self._source_config.to_built_in_container()

            # Compute new and removed channels
            new_channels = set(new_config.keys()).difference(current_config.keys())
            removed_channels = set(current_config.keys()).difference(new_config.keys())

            # Filter the updated channels
            updated_channels = set(current_config.keys()).intersection(new_config.keys())
            updated_channels = set(filter(lambda chn: new_config[chn] != current_config[chn], updated_channels))

            # do the restarts
            await self._restart_executor_batch(new_channels, removed_channels, updated_channels)

        try:
            self._source_config.set_on_change(_handle_changes)
            await self._source_config.watch_and_fire()
        finally:
            self._source_config.set_on_change(None)

    async def _restart_executor_batch(self, new_channels: Iterable[str], removed_channels: Iterable[str],
                                      updated_channels: Iterable[str]):
        """
        Stops and starts a batch of executors
        :param new_channels: The new channels to just start
        :param removed_channels: The outdated channels to just stop
        :param updated_channels: The channels to do a full update cycle
        """

        for chn in itertools.chain(updated_channels, removed_channels):
            self._executors[chn].shutdown()

        for chn in itertools.chain(updated_channels, removed_channels):
            # Since the executor is still not asynchronous, we need to use the blocking join.
            self._executors[chn].join()

        for chn in itertools.chain(updated_channels, new_channels):
            self._start_executor(chn)

    async def heartbeat(self) -> Dict[str, str]:
        """
        Performs the check and repair policy of the supervisor

        It is advised to regularly call the heartbeat function to be able to collect statistics and repair any failed
        source. The function will return as soon as all check and repair operations are done. In order to support
        testing, a non-blocking design was chosen that returns the status of each executor for further assessment.

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
                    status.last_wakeup is not None and status.max_permitted_cycle_time is not None and
                    ts_now - status.last_wakeup > status.max_permitted_cycle_time + self._dead_timeout and
                    not self._executors[ex_name].is_shutdown_triggered()  # Avoid duplicate shutdown calls
            ):
                logger.warning(f"The source {ex_name} does not complete its cycle in time (last wakeup at "
                               f"{status.last_wakeup} and last complete cycle {status.last_cycle_complete}, max cycle "
                               f"time {status.max_permitted_cycle_time}). Issue an asynchronous stop signal.")
                self._executors[ex_name].shutdown()  # Don't wait for joins to not block the entire supervisor.
                ex_status[ex_name] = "stop-by-timeout"
                self._prom_source_status.labels(source_name=ex_name).set(1)

            elif (
                    status.last_wakeup is not None and status.max_permitted_cycle_time is not None and
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

    @property
    def executors(self) -> Dict[str, ThreadQueryExecutor]:
        """
        Returns the mapping of executor names and the corresponding executor for testing purpose.

        Do not fiddle around with the execuotrs manually as it could create some havoc.
        """
        return self._executors
