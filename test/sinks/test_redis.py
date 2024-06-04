"""
Tests the Redis data sink
"""

import pytest
import redis

import data_crawler.sinks.redis as redis_sink


def test_redis_stream_config_classes():
    """Tests the RedisStream sink configuration classes"""

    assert issubclass(redis_sink.RedisStream.parameter_model(), redis_sink.RedisStreamParameters)
    assert issubclass(redis_sink.RedisStream.metadata_model(), redis_sink.RedisStreamMetadata)


def test_redis_stream_default(redis_pool, redis_stream_name):
    """Tests the default functionality of the RedisStream sink"""

    config = redis_sink.RedisStreamParameters(stream=redis_stream_name)
    sink = redis_sink.RedisStream.create(config, redis_pool=redis_pool)
    sink.insert_data(dict(message="Stream it.", time="now"), redis_sink.RedisStreamMetadata())

    redis_client = redis.Redis(connection_pool=redis_pool)
    messages = redis_client.xrange(redis_stream_name)

    assert messages is not None
    assert len(messages) == 1
    assert messages[0][-1] == {"message": "\"Stream it.\"", "time": "\"now\""}


def test_redis_stream_tags(redis_pool, redis_stream_name):
    """Tests the tag configuration of the redis stream sink"""

    config = redis_sink.RedisStreamParameters(stream=redis_stream_name,
                                              tags=dict(device="doomsday", caption="press the red button"))
    sink = redis_sink.RedisStream.create(config, redis_pool=redis_pool)
    sink.insert_data(dict(time="now"), redis_sink.RedisStreamMetadata())

    redis_client = redis.Redis(connection_pool=redis_pool)
    messages = redis_client.xrange(redis_stream_name)

    assert messages is not None
    assert len(messages) == 1
    assert messages[0][-1] == {"time": "\"now\"", "device": "\"doomsday\"", "caption": "\"press the red button\""}


def test_redis_stream_meta_data_override(redis_pool, redis_stream_name):
    """Tests the override functionality by passing on the meta-data object"""

    config = redis_sink.RedisStreamParameters(stream="nowhere")
    sink = redis_sink.RedisStream.create(config, redis_pool=redis_pool)
    sink.insert_data(dict(time="tomorrow"), redis_sink.RedisStreamMetadata(stream=redis_stream_name))

    redis_client = redis.Redis(connection_pool=redis_pool)
    messages = redis_client.xrange(redis_stream_name)

    assert messages is not None
    assert len(messages) == 1
    assert messages[0][-1] == {"time": "\"tomorrow\""}


def test_redis_stream_missing_name(redis_pool):
    """Tests whether an error is raised at runtime if no stream is given."""

    config = redis_sink.RedisStreamParameters()
    sink = redis_sink.RedisStream.create(config, redis_pool=redis_pool)
    with pytest.raises(ValueError) as err_desc:
        sink.insert_data(dict(time="tomorrow"), redis_sink.RedisStreamMetadata())
    assert "stream" in str(err_desc.value)
