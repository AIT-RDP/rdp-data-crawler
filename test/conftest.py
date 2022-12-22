"""
Configures unit tests globally
"""

import logging
import os
import sys

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


@pytest.fixture()
def appended_test_path() -> str:
    """Appends the test directory to the system path and returns that path"""

    test_dir = os.path.abspath(os.path.join(__file__, ".."))
    sys.path.append(test_dir)

    yield test_dir

    assert sys.path[-1] == test_dir
    sys.path.pop(-1)
