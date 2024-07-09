"""
Configures unit tests globally
"""

import contextlib
import logging
import os
import sys
import warnings

import redis
import pytest

logger = logging.getLogger(__name__)


@pytest.fixture()
def redis_pool() -> redis.ConnectionPool:
    """Opens a Redis pool and tests the connection"""

    host = os.environ.get("DATA_CRAWLER_REDIS_HOST", "localhost")
    port = os.environ.get("DATA_CRAWLER_REDIS_PORT", "6379")
    db = os.environ.get("DATA_CRAWLER_REDIS_DB", "0")

    logger.debug(f"Initialize redis pool connecting to host={host}, port={port}, db={db}")

    pool = redis.ConnectionPool(host=host, port=port, db=db, decode_responses=True)
    client = redis.Redis(connection_pool=pool)

    client.ping()
    return pool


@contextlib.contextmanager
def _managed_redis_stream(redis_pool: redis.ConnectionPool, stream_name: str) -> str:
    """Returns a managed REDIS stream"""

    redis_client = redis.Redis(connection_pool=redis_pool)

    stream_content = redis_client.xrange(stream_name)  # Read to implicitly create the stream
    if len(stream_content) == 0:
        warnings.warn(f"There are already {len(stream_content)} items in the redis stream '{stream_name}'")

    try:
        yield stream_name
    finally:
        redis_client.xtrim(stream_name, maxlen=0)
        redis_client.delete(stream_name)  # Delete the stream again


@pytest.fixture()
def redis_stream_name(redis_pool) -> str:
    """Returns the name of a managed REDIS stream"""

    with _managed_redis_stream(redis_pool=redis_pool, stream_name="test.stream") as r:
        return r


@pytest.fixture()
def redis_stream_name_other(redis_pool) -> str:
    """Returns the name of another managed REDIS stream"""

    with _managed_redis_stream(redis_pool=redis_pool, stream_name="test.other_stream") as r:
        return r


@pytest.fixture()
def appended_test_path() -> str:
    """Appends the test directory to the system path and returns that path"""

    test_dir = os.path.abspath(os.path.join(__file__, ".."))
    sys.path.append(test_dir)

    yield test_dir

    assert sys.path[-1] == test_dir
    sys.path.pop(-1)
