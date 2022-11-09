"""
Implements the main command line interface of the E3 data crawler
"""

import argparse
import logging
import os
import signal
import string
import threading
import time
from typing import Optional, List

import dotenv
import redis
import yaml

import data_crawler.query_executors as query_executors

logger = logging.getLogger(__name__)


def main(argv=None, prog=None):
    """
    Parses the commandline arguments, reads the configuration and starts the main program flow

    :param argv: An optional argument vector that can be supplied for testing purposes
    :param prog: An optional program name. Otherwise the first element in the argument vector will be used
    """

    logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s: %(message)s", level=logging.DEBUG)

    parser = argparse.ArgumentParser(prog=prog, description="Periodically fetches the data sources")
    parser.add_argument("--config_file", metavar="CONF", default="data_crawler.yaml",
                        help="The main YAML configuration describing the data sources")
    parser.add_argument("--env", metavar="ENV_FILE", default=None,
                        help="An environment file that specifies the variables to load")
    args = parser.parse_args(args=argv)

    load_env_file(args.env)
    logger.debug("Parse main YAML configuration file '%s'", args.config_file)
    config = load_config(args.config_file)

    redis_pool = _load_redis_connection_pool(config)
    executors = _startup_executors(config, redis_pool)

    logger.info(f"Startup of {len(executors)} source(s) complete, press Ctrl+C to exit the data crawler.")
    _wait_for_termination_request()

    logger.info(f"Begin to shutdown the data crawler.")
    _stop_executors(executors)
    logger.info("Bye!")


def _wait_for_termination_request():
    """suspends the main thread until a termination request was received"""

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
    except KeyboardInterrupt:
        pass


def _startup_executors(config: dict, redis_pool: redis.ConnectionPool) -> List[query_executors.ThreadQueryExecutor]:
    """Parses the system configuration and instantiates the query executors"""

    ret = []
    for exec_name, exec_config in config["data sources"].items():
        ret.append(query_executors.ThreadQueryExecutor(exec_config, redis_pool, name=exec_name))

    for exec in ret:
        exec.start()

    return ret


def _stop_executors(executors: List[query_executors.ThreadQueryExecutor]):
    """Stops all executors and waits until they are terminated"""

    for ex in executors:
        ex.stop()

    for ex in executors:
        ex.join()


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


def load_env_file(env_file: Optional[str]) -> None:
    """
    Populates the environment vairables by elements stored in the .env file

    :param env_file: The path of the .env file or None, in case nothing should be changed
    """

    if env_file is not None:
        logger.debug("Load environment file '%s'", env_file)
        dotenv.load_dotenv(env_file)


class _ContextLoader(yaml.Loader):
    """
    Implements a YAML loader that carries on a context dictionary to easily resolve nested objects
    """

    def __init__(self, context: dict, *args, **kwargs):
        """
        Initializes the loader with the given context

        :param context: The context object passed on to each individual constructor function
        :param args: The arguments passed on to the yaml loader
        :param kwargs: The keyword arguments directly passed on to the yaml loader
        """

        super(_ContextLoader, self).__init__(*args, **kwargs)
        self._context = context

    @property
    def context(self) -> dict:
        """Returns the shared context of the loader"""
        return self._context


def load_config(config_file: str) -> dict:
    """
    Parses the YAML configuration, preprocesses it and returns the resulting structure of dictionaries
    :param config_file: The path of the main configuration file
    :return: The obtained configuration as a structure of nested dicts as generated by the YAML loader
    """

    config_file = os.path.abspath(config_file)
    if not os.path.isfile(config_file):
        raise FileNotFoundError(f"The main configuration file '{config_file}' is not found")

    yaml.add_constructor("!env-template", _load_substitute_env)
    yaml.add_constructor("!table/csv", _load_table_csv)

    context = dict(base_dir=os.path.dirname(config_file))
    with open(config_file, "r") as f:
        config = yaml.load(f, Loader=lambda *arg, **kwargs: _ContextLoader(context, *arg, **kwargs))

    if not config.get("version", 1) == 1:
        raise SyntaxError("Invalid configuration version. Only version 1 is supported.")

    return config


def _load_table_csv(loader, node):
    """Loads a pandas table from a csv file."""
    import pandas as pd

    parameters = loader.construct_mapping(node)
    if "path" not in parameters:
        raise KeyError(f"The table/csv constructor requires a 'path' attribute but only "
                       "{list(parameters.keys())} are given.")

    filename = parameters["path"]
    if not os.path.isabs(filename):
        filename = os.path.join(loader.context["base_dir"], filename)
    table = pd.read_csv(filename, sep=parameters.get("sep", ";"))
    return table


def _load_substitute_env(loader, node):
    """Loads the YAML node by substituting environment variables using Python template syntax"""

    template_str = loader.construct_scalar(node)
    template = string.Template(template_str)
    return template.substitute(**os.environ)
