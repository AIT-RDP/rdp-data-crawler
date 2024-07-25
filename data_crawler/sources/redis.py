"""
Contains all redis-related source configuration directives
"""

import datetime
import json
import logging
import threading
from typing import Any, Generator

import pydantic
import redis


import data_crawler.sources.abc.active_source_sync as active_source_sync
from data_crawler.sources.abc import message as msg


class StreamParameters(pydantic.BaseModel):
    name: str = pydantic.Field(description="Name of the stream")
    metadata: dict[str, Any] = pydantic.Field(
        description="Metadata that will be provide additionally on each received message",
        default={},
    )


class RedisStreamParameters(active_source_sync.SourceParameters):
    """Redis configuration parameters"""

    streams: list[StreamParameters] = pydantic.Field(description="List of streams that are listened to.")
    group_name: str = pydantic.Field(
        description="Name of the consumer group. Clients with different consumer group names receive the same "
                    "messages, for clients with same consumer group names only one client will receive a new message "
                    "(load balancing)",
    )
    consumer_name: str = pydantic.Field(description="Name of the consumer. Use different consumer names.")
    block_time_ms: int = pydantic.Field(
        description="The client will make a blocking read and waits the selected time (ms) for new messages. "
                    "Afterwards it will check if a shutdown request was sent or if it should make another blocking "
                    "method call to read messages.",
        default=100,
    )


class RedisStream(active_source_sync.AbstractSyncActiveSourceAPI):
    """
    Implements the Redis data source using a shared connection pool to save connections
    """

    def __init__(self, redis_config: RedisStreamParameters, redis_pool: redis.ConnectionPool):
        """Initializes the object"""

        super().__init__()

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        if not redis_pool.get_encoder().decode_responses:
            raise RuntimeError("When creating the redis_pool, `decode_responses` is expected to be set to `True`")

        self._client: redis.Redis = redis.Redis(connection_pool=redis_pool)
        self._config: RedisStreamParameters = redis_config
        self._stream_parameters: dict[str, StreamParameters] = {stream.name: stream for stream in self._config.streams}
        self._shutdown_request: bool = False
        self._last_wakeup: datetime.datetime | None = None
        self._last_cycle_complete: datetime.datetime | None = None
        self._activity_status_lock: threading.Lock = threading.Lock()

    @classmethod
    def create(cls, source_parameters: RedisStreamParameters, **kwargs) -> "RedisStream":
        """
        Factory function that creates a new source

        :param source_parameters: The configuration of the RedisStream
        :param kwargs: A field `redis_pool` is mandatory which has a value an initialized Redis connection pool.
            Any additional kwargs that will be gracefully ignored
        :return: The newly constructed data source instance
        """

        if "redis_pool" not in kwargs:
            raise RuntimeError("Expect an externally supplied redis_pool")  # Avoid signature warnings
        redis_pool: redis.ConnectionPool = kwargs["redis_pool"]

        return cls(source_parameters, redis_pool)

    def start(self) -> None:
        """
        Starts the operation of the data source
        """

        self._init_redis_streams()

    def _init_redis_streams(self):
        """
        Initializes a Redis stream if it has not already been initialized
        """

        group_name = self._config.group_name

        for stream in self._config.streams:
            if not self._client.exists(stream.name):
                self._logger.info("Create Redis group %s and stream %s", group_name, stream.name)
                # Also consume messages before the group was created
                self._client.xgroup_create(name=stream.name, groupname=group_name, mkstream=True, id="0-0")
            else:
                self._logger.info("Try creating Redis group %s on existing stream %s", group_name,
                                  stream.name)
                try:
                    # We cannot easily determine whether a group is already existing. Hence, try to create it and
                    # re-raise the error in case it is not the expected one. (Thx to Denis and CLUE Data Sync.)
                    self._client.xgroup_create(name=stream.name, groupname=group_name, id="0-0")
                except redis.exceptions.ResponseError as e:
                    if "BUSYGROUP" not in str(e):
                        raise

    def run(self) -> Generator[msg.MessageData, None, None]:
        """
        Executes the Redis source and return messages as they arrive.

        The generator exits latest after the blocking time `block_time_ms`
        (plus the time it needs to process incoming messages when messages arrive at the end of the blocking time)
        as soon as a shutdown request is issued.
        """

        while not self._shutdown_request:
            data_multi_streams = self._client.xreadgroup(
                groupname=self._config.group_name,
                consumername=self._config.consumer_name,
                streams={stream.name: ">" for stream in self._config.streams},
                # for each stream read not more than 1 message at once
                count=1,
                # block for a certain time
                block=self._config.block_time_ms,
            )

            with self._activity_status_lock:
                self._last_wakeup = datetime.datetime.now(tz=datetime.timezone.utc)

            for stream_name, data_single_stream in data_multi_streams:
                _message_id = data_single_stream[0][0]

                # field values in Redis streams are expected to be formatted in JSON
                data = {k: json.loads(v) for k, v in data_single_stream[0][1].items()}

                yield msg.Message(payload=data, metadata=self._stream_parameters[stream_name].metadata)

            with self._activity_status_lock:
                self._last_cycle_complete = datetime.datetime.now(tz=datetime.timezone.utc)

        # reset shutdown request (because it will be fulfilled after this method returns)
        self._shutdown_request = False

    def shutdown(self) -> None:
        """
        Indicates that the run generator must stop
        """

        self._shutdown_request = True

    def get_activity_status(self) -> active_source_sync.ActivityStatus:
        """
        Returns the current activity status of the source

        :return: The current status information on the source activity
        """

        with self._activity_status_lock:
            return active_source_sync.ActivityStatus(
                last_wakeup=self._last_wakeup,
                last_cycle_complete=self._last_cycle_complete,
                max_permitted_cycle_time=datetime.timedelta(milliseconds=self._config.block_time_ms),
            )

    @staticmethod
    def parameter_model() -> type[RedisStreamParameters]:
        return RedisStreamParameters
