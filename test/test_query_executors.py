"""
Test the query executor services
"""

import logging
import os
from typing import Dict, Any

import pytest
import redis

import data_crawler.query_executors as query_executors
import data_crawler.sources.abc.abstract_source as abstract_sources

logger = logging.getLogger(__name__)


class MockupSourceAPI(abstract_sources.AbstractSourceAPI):
    """Test API source that just counts function invocations"""

    def __init__(self, source_parameters, **kwargs):
        """Stores the configuration and initializes the object"""
        self._config = source_parameters
        self.fetch_invocations = 0

    def fetch_data(self) -> Dict[str, Any]:
        """Generates some content and returns it"""

        self.fetch_invocations += 1
        return {
            "invocations": self.fetch_invocations
        }


@pytest.fixture()
def mockup_service_config():
    """Returns the configuration of a simple mockup service"""

    return {
        "type": "test.test_query_execution:MockupSourceAPI",
        "source parameter": {},
        "polling": {
            "frequency": "0.5s"
        },
        "redis tags": {},
    }


@pytest.fixture()
def redis_pool() -> redis.ConnectionPool:
    host = os.environ.get("DATA_CRAWLER_REDIS_HOST", "localhost")
    port = os.environ.get("DATA_CRAWLER_REDIS_PORT", "6379")
    db = os.environ.get("DATA_CRAWLER_REDIS_DB", "0")

    logger.debug(f"Initialize redis pool connecting to host={host}, port={port}, db={db}")

    return redis.ConnectionPool(host=host, port=port, db=db)


def test_thread_executor_lifecycle(mockup_service_config, redis_pool):
    """Tests the basic lifecycle of a threaded executor"""

    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool)

    with pytest.raises(AssertionError):
        executor.join()

    executor.start()
    executor.stop()
    with pytest.raises(AssertionError):
        executor.stop()

    executor.join()
