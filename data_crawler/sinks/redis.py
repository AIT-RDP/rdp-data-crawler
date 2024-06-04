"""
Contains all redis-related sink configuration directives
"""
import json
import string
from typing import Any, Optional

import pydantic
import redis

import data_crawler.sinks.abc.abstract_sink as abstract_sink


class RedisStreamParameters(abstract_sink.SinkParameters):
    """Redis configuration parameters"""

    stream: Optional[str] = pydantic.Field(
        description="The default name of the Redis stream to push the data to. Template strings will be replaced by the"
                    "corresponding message fields. In case no stream is given, the corresponding metadata field must "
                    "be set.",
        default=None
    )
    tags: dict[str, Any] = pydantic.Field(description="Additional message fields that will be appended", default={})


class RedisStreamMetadata(abstract_sink.SinkMetadata):
    """Specifies the supported metadata fields"""

    stream: Optional[str] = pydantic.Field(
        description="Optional stream name that overrides the configuration default",
        default=None
    )


class RedisStream(abstract_sink.AbstractSinkAPI):
    """
    Implements the Redis data sink using a shared connection pool to save connections
    """

    def __init__(self, redis_config: RedisStreamParameters, redis_pool: redis.ConnectionPool):
        """Initializes the object"""
        self._client = redis.Redis(connection_pool=redis_pool)
        self._config = redis_config
        self._stream_template = string.Template(redis_config.stream) if redis_config.stream is not None else None

    @classmethod
    def create(cls, sink_parameters: RedisStreamParameters, **kwargs) -> abstract_sink.AbstractSinkAPI:
        """
        Factory function that creates a new sink

        :param sink_parameters: The configuration of the RedisStream
        :param kwargs: Any additional kwargs that will be gracefully ignored
        :return: The newly constructed data sink instance
        """
        assert "redis_pool" in kwargs, "Expect an externally supplied redis_pool"  # Avoid signature warnings
        redis_pool: redis.ConnectionPool = kwargs["redis_pool"]

        return RedisStream(sink_parameters, redis_pool)

    def insert_data(self, data: dict[str, Any], metadata: RedisStreamMetadata) -> None:
        """
        Pushes the data to the Redis stream

        Each message value will be encoded as JSON string to support handling of more complex data structures

        :param data: The actual message data to mush to the stream
        :param metadata: Any metadata that alters the behaviour of the function
        """

        stream_template = string.Template(metadata.stream) if metadata.stream is not None else self._stream_template
        if stream_template is None:
            raise ValueError("Neither the configuration nor the metadata contains a valid stream. Add a valid stream "
                             "configuration.")

        data = data.copy()  # To be on the safe side. Remove if it turns out to be a performance bottleneck
        data.update(self._config.tags)

        encoded_data = {key: json.dumps(val) for key, val in data.items()}
        self._client.xadd(stream_template.substitute(data), encoded_data)

    @staticmethod
    def parameter_model() -> type[abstract_sink.SinkParameters]:
        return RedisStreamParameters

    @staticmethod
    def metadata_model() -> type[abstract_sink.SinkMetadata]:
        return RedisStreamMetadata
