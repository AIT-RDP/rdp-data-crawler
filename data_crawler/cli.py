"""
Implements the main command line interface of the E3 data crawler
"""

import logging
import logging.config
import signal
import time
import warnings
from typing import Optional

import click
import prometheus_client as prom
import pyrdp_commons.cli as cli
import redis

import data_crawler.query_executors as query_executors

logger = logging.getLogger(__name__)


def main(argv=None, prog=None):
    """
    Parses the commandline arguments, reads the configuration and starts the main program flow

    :param argv: An optional argument vector that can be supplied for testing purposes
    :param prog: An optional program name. Otherwise the first element in the argument vector will be used
    """
    warnings.warn("The function main is deprecated and will be deleted soon. Please directly call periodic_operation() "
                  "or periodic_operation.main(arg, prog_name) if needed.", category=DeprecationWarning)

    periodic_operation.main(argv, prog_name=prog)


@click.command()
@click.option("-c", "--config_file", default="data_crawler.yaml",
              help="The main YAML configuration describing the data sources")
@click.option("--env", default=None, help="An environment file that specifies the variables to load")
def periodic_operation(config_file, env):
    """
    Polls the configured data items periodically

    The command reads the user-defined configuration and periodically queries the data from all configured data sources.
    In parallel, the data sources are supervised and restarted, if necessary.
    """

    logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s: %(message)s", level=logging.DEBUG)
    logger.debug("Parse main YAML configuration file '%s'", config_file)
    config = cli.setup_app(config_file, env)
    _startup_prometheus_client(config.get("prometheus client", {}))

    redis_pool = _load_redis_connection_pool(config)
    sup_config = config.get("supervision", {})
    supervisor = query_executors.QuerySupervisor(config["data sources"], sup_config, redis_pool)
    supervisor.start()

    logger.info(f"Startup of {len(supervisor.source_names)} source(s) complete, press Ctrl+C to exit the data crawler.")
    _heartbeat_until_termination_request(supervisor)

    logger.info(f"Begin to shutdown the data crawler.")
    supervisor.stop()
    logger.info("Bye!")


def _startup_prometheus_client(prometheus_config: Optional[dict] = None):
    """Starts a local webserver that exposes the internal metrics, if requested"""
    if prometheus_config is not None:
        port = prometheus_config.get("port", 8000)
        prom.start_http_server(port=port)
        logger.info(f"Started the prometheus server at http://localhost:{port}")


def _heartbeat_until_termination_request(supervisor: query_executors.QuerySupervisor):
    """Periodically triggers the heart beat until a termination request was received"""

    def _handler(signal_number, _frame):
        logger.debug(f"Received signal {signal_number}. Initiate shutdown.")
        raise KeyboardInterrupt("The end is near!")

    # Install the signal handlers
    signal_codes = ("SIGTERM", "SIGINT", "SIGBREAK", "SIGHUP")
    for code in signal_codes:
        # Some signals are not defined on Unix/Windows :-(
        signal_nr = getattr(signal, code, None)
        if signal_nr is not None:
            signal.signal(signal_nr, _handler)

    # Sleep until a KeyboardInterrupt it caught
    # Using an event rather than an exception would be nicer, but exit_event.wait() blocks the signal handler.
    try:
        while True:
            time.sleep(10)
            supervisor.heartbeat()
    except KeyboardInterrupt:
        pass


def _load_redis_connection_pool(config: dict) -> redis.ConnectionPool:
    """Parses the configuration and instantiates the Redis connection pool"""

    redis_config: dict = config["redis"]
    host = redis_config["host"]
    port = redis_config["port"]
    db = redis_config["db"]
    logger.debug(f"Configure redis connection to {host}:{port} using db {db}")

    pool = redis.ConnectionPool(host=host, port=port, db=db)
    client = redis.Redis(connection_pool=pool)
    client.ping()  # Will raise an exception in case a connection error occurs
    logger.debug(f"Redis connection to {host}:{port} using db {db} is alive.")

    return pool
