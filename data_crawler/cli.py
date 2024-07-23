"""
Implements the main command line interface of the E3 data crawler
"""
import asyncio
import dataclasses
import itertools
import logging
import logging.config
import signal
import time
import warnings
from typing import Optional, Iterable, List, Dict

import click
import prometheus_client as prom
import pyrdp_commons as commons
import pyrdp_commons.cli
import redis

import data_crawler.query_executors as query_executors
import data_crawler.query_supervisor

logger = logging.getLogger(__name__)


@dataclasses.dataclass()
class CommandContextInfo:
    """Implements some basic attributes that are passed on to subcommands"""

    config_file: str  # The path to the configuration file to parse
    env: Optional[str]  # An optional path to an environment file


def main(argv=None, prog=None):
    """
    Parses the commandline arguments, reads the configuration and starts the main program flow

    This function is deprecated. Consider directly invoking the commands.

    :param argv: An optional argument vector that can be supplied for testing purposes
    :param prog: An optional program name. Otherwise the first element in the argument vector will be used
    """
    warnings.warn("The function main is deprecated and will be deleted soon. Please directly call cli() "
                  "or cli.main(arg, prog_name) if needed.", category=DeprecationWarning)

    cli.main(argv, prog_name=prog)


@click.group(invoke_without_command=True)
@click.option("-c", "--config_file", default="data_crawler.yaml", envvar="DATA_CRAWLER_CONFIG",
              help="The main YAML configuration describing the data sources")
@click.option("--env", default=None, envvar="DATA_CRAWLER_ENV",
              help="An environment file that specifies the variables to load")
@click.pass_context
def cli(ctx: click.Context, config_file, env):
    """Loads the basic data crawler functionality"""

    ctx.obj = CommandContextInfo(config_file, env)

    if ctx.invoked_subcommand is None:
        warnings.warn("Directly calling the cli without and subcommand is deprecated and will be removed in future. "
                      "Call 'cli [OPTIONS] run' instead ", category=DeprecationWarning)
        ctx.invoke(run)  # Directly execute run per default to maintain compatibility


@cli.command("run")
@click.pass_context
def run(ctx):
    """
    Polls the configured data items periodically

    The command reads the user-defined configuration and periodically queries the data from all configured data sources.
    In parallel, the data sources are supervised and restarted, if necessary.
    """

    asyncio.run(_run_async(ctx.obj))


async def _run_async(context_info: CommandContextInfo):
    """Does the actual heavy lifting and runs the application in an asynchronous context"""

    config = await _setup_application(context_info)
    _startup_prometheus_client(config.get("prometheus client", {}))

    redis_pool = _load_redis_connection_pool(config)
    sup_config = config.get("supervision", {})
    supervisor = data_crawler.query_supervisor.AsyncQuerySupervisor(config["data sources"], sup_config, redis_pool)
    await supervisor.start()

    logger.info(f"Startup of {len(supervisor.source_names)} source(s) complete, press Ctrl+C to exit the data crawler.")
    await _heartbeat_until_termination_request(supervisor)

    logger.info(f"Begin to shutdown the data crawler.")
    await supervisor.stop()
    logger.info("Bye!")


async def _setup_application(context_info: CommandContextInfo) -> commons.ConfigDict:
    """
    Sets up the basic application and returns the global configuration

    :param context_info: The context information to read the global configuration from
    :return: The instantiated dynamic configuration of the program
    """

    logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s: %(message)s", level=logging.DEBUG)
    logger.debug("Parse main YAML configuration file '%s'", context_info.config_file)
    config = await pyrdp_commons.cli.async_setup_app(context_info.config_file, context_info.env,
                                                     supported_config_versions={1, 2}, dst_type="extended")
    if not isinstance(config, commons.ConfigDict):
        raise TypeError(f"The root configuration must be a dictionary, but {type(config)} given.")

    return config


@cli.command("fetch")
@click.option("-f", "--filter", "filter_expr", multiple=True, help="A filter expression as key=value pair")
@click.option("-o", "--override", multiple=True, help="Overrides the specified source config of all configured sources")
@click.argument("source_names", nargs=-1)
@click.pass_context
def fetch(ctx, filter_expr: Iterable[str], override: Iterable[str], source_names: Iterable[str]):
    """
    Executes the selected sources once and fetches the results in a one-shot action.

    The source names must match the corresponding names in the configuration file. Glob patterns are supported but most
    likely must be escaped to avoid shell expansion. In case no source is specified, the command will gracefully exit
    without executing a fetch operation.
    """

    asyncio.run(_fetch_async(ctx.obj, filter_expr, override, source_names))


async def _fetch_async(context_info: CommandContextInfo, filter_expr: Iterable[str], override: Iterable[str],
                       source_names: Iterable[str]):
    """Does the actual fetch operation in an asynchronous context"""

    config = await _setup_application(context_info)
    filter_configs = _parse_filter_expression(filter_expr)
    override_config = _parse_override_clauses(override)
    redis_pool = _load_redis_connection_pool(config)

    query_executors.execute_one_shot_batches(config["data sources"], redis_pool, filter_configs, source_names,
                                             override_config=override_config)


def _parse_filter_expression(filter_expr: Iterable[str]) -> List[Dict[str, str]]:
    """Parses the series of filter terms and does some basic count checking."""

    # Parse the input
    out_exp = {}
    for i, exp in enumerate(filter_expr):
        if "=" not in exp:
            raise ValueError(f"The {i + 1}th filter expression is not a key=value pair: '{exp}'")
        key, value = tuple(exp.split("=", maxsplit=1))
        out_exp[key] = out_exp.get(key, []) + [value]

    # Check the input
    length_values_ind = sorted(len(val) for val in out_exp.values())
    length_values = list(itertools.groupby(length_values_ind))
    if len(length_values) > 1:
        raise ValueError(f"Some filter keys are more often listed than others: "
                         f"{ {k: len(v) for k, v in out_exp.items()} }")

    # Transform the input to a record format
    if len(length_values) <= 0:
        return [{}]  # Output a single default configuration since there are no config items
    else:
        bucket_number = length_values[0][0]  # groupby returns (key, group) tuples
        return [
            {key: values[i] for key, values in out_exp.items()}
            for i in range(bucket_number)
        ]


def _parse_override_clauses(override_clauses: Iterable[str]) -> dict:
    """Parses the override syntax (dot-separated hierarchy) and returns the prototype dictionary"""

    ret_dict = {}
    for clause in override_clauses:
        rec_key, value = tuple(clause.split("=", maxsplit=1))
        level_names = rec_key.split(".")
        assert len(level_names) >= 1

        dst_dict = ret_dict
        for level in level_names[:-1]:
            dst_dict[level] = dst_dict.get(level, {})  # make sure there is an entry
            dst_dict = dst_dict[level]
        dst_dict[level_names[-1]] = value

    return ret_dict


def _startup_prometheus_client(prometheus_config: Optional[dict] = None):
    """Starts a local webserver that exposes the internal metrics, if requested"""
    if prometheus_config is not None:
        port = prometheus_config.get("port", 8000)
        prom.start_http_server(port=port)
        logger.info(f"Started the prometheus server at http://localhost:{port}")


class _ThreadSaveEvent(asyncio.Event):
    """Implements a thread-save version of the event facility"""

    def __init__(self):
        super().__init__()
        self._event_loop = asyncio.get_event_loop()

    def set(self):
        """Sets the event in a thread-save way"""
        self._event_loop.call_soon_threadsafe(super().set)


async def _heartbeat_until_termination_request(supervisor: data_crawler.query_supervisor.AsyncQuerySupervisor):
    """Periodically triggers the heart beat until a termination request was received"""

    termination_event = _ThreadSaveEvent()

    def _handler(signal_number, _frame):
        logger.debug(f"Received signal {signal_number}. Initiate shutdown.")
        termination_event.set()

    # Install the signal handlers
    signal_codes = ("SIGTERM", "SIGINT", "SIGBREAK", "SIGHUP")
    for code in signal_codes:
        # Some signals are not defined on Unix/Windows :-(
        signal_nr = getattr(signal, code, None)
        if signal_nr is not None:
            signal.signal(signal_nr, _handler)

    # Loop until the termination signal is received and the event is set
    pending = [asyncio.create_task(termination_event.wait())]
    while True:
        _, pending = await asyncio.wait(pending, timeout=10)
        if termination_event.is_set():
            break

        await supervisor.heartbeat()


def _load_redis_connection_pool(config: dict) -> redis.ConnectionPool:
    """Parses the configuration and instantiates the Redis connection pool"""

    redis_config: dict = config["redis"]
    host = redis_config["host"]
    port = redis_config["port"]
    db = redis_config["db"]
    password = redis_config.get("password", None)
    logger.debug(f"Configure redis connection to {host}:{port} using db {db} and "
                 f"{'a' if password is not None else 'no'} password")

    pool = redis.ConnectionPool(host=host, port=port, db=db, password=password)
    client = redis.Redis(connection_pool=pool)
    client.ping()  # Will raise an exception in case a connection error occurs
    logger.debug(f"Redis connection to {host}:{port} using db {db} is alive.")

    return pool


if __name__ == "__main__":
    main()
