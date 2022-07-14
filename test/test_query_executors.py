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
        self.config = source_parameters
        self.fetch_invocations = 0

    def fetch_data(self) -> Dict[str, Any]:
        """Generates some content and returns it"""

        self.fetch_invocations += 1
        return {
            "invocations": self.fetch_invocations
        }


@pytest.fixture()
def mockup_service_config(appended_test_path):
    """Returns the configuration of a simple mockup service"""

    return {
        "type": "test_query_executors.MockupSourceAPI",
        "source parameter": {
            "key": "<keep it secret>"
        },
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


def test_thread_executor_api_instantiation(mockup_service_config, redis_pool):
    """Tests the API instantiation function using the mockup API"""

    executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool)
    assert isinstance(executor.source_api, MockupSourceAPI)

    api:MockupSourceAPI = executor.source_api
    assert "key" in api.config
    assert api.config["key"] == "<keep it secret>"


def test_thread_executor_invalid_api_name(mockup_service_config, redis_pool):
    """Tests an invalid API name"""

    mockup_service_config["type"] = "data_crawler.sources.nsa.Prism"
    with pytest.raises(ModuleNotFoundError, match="data_crawler\\.sources\\.nsa"):
        executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool)


def test_thread_executor_invalid_api_class(mockup_service_config, redis_pool):
    """Tests an invalid API class"""

    mockup_service_config["type"] = "threading.Thread"
    with pytest.raises(ModuleNotFoundError, match="Thread"):
        executor = query_executors.ThreadQueryExecutor(mockup_service_config, redis_pool)