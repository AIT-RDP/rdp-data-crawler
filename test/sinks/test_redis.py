"""
Tests the Redis data sink
"""

import datetime
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


@pytest.mark.parametrize("data,expected", [
    # Test basic types
    ({"value": None}, {"value": "null"}),
    ({"value": "text"}, {"value": "\"text\""}),
    ({"value": 42}, {"value": "42"}),
    ({"value": 3.14}, {"value": "3.14"}),
    ({"value": True}, {"value": "true"}),
    ({"value": False}, {"value": "false"}),
    # Test list
    ({"value": [1, 2, 3]}, {"value": "[1, 2, 3]"}),
    # Test tuple conversion to list
    ({"value": (1, 2, 3)}, {"value": "[1, 2, 3]"}),
    # Test nested structures
    ({"value": {"nested": "data"}}, {"value": "{\"nested\": \"data\"}"}),
    ({"value": [{"a": 1}, {"b": 2}]}, {"value": "[{\"a\": 1}, {\"b\": 2}]"}),
])
def test_redis_stream_encoding_basic(redis_pool, redis_stream_name, data, expected):
    """Tests encoding of basic data types"""

    config = redis_sink.RedisStreamParameters(stream=redis_stream_name)
    sink = redis_sink.RedisStream.create(config, redis_pool=redis_pool)
    sink.insert_data(data, redis_sink.RedisStreamMetadata())

    redis_client = redis.Redis(connection_pool=redis_pool)
    messages = redis_client.xrange(redis_stream_name)

    assert messages is not None
    assert len(messages) == 1
    assert messages[0][-1] == expected


def test_redis_stream_encoding_datetime(redis_pool, redis_stream_name):
    """Tests encoding of datetime objects"""

    test_date = datetime.date(2024, 6, 15)
    test_datetime = datetime.datetime(2024, 6, 15, 14, 30, 45)

    data = {
        "date": test_date,
        "datetime": test_datetime
    }

    config = redis_sink.RedisStreamParameters(stream=redis_stream_name)
    sink = redis_sink.RedisStream.create(config, redis_pool=redis_pool)
    sink.insert_data(data, redis_sink.RedisStreamMetadata())

    redis_client = redis.Redis(connection_pool=redis_pool)
    messages = redis_client.xrange(redis_stream_name)

    assert messages is not None
    assert len(messages) == 1
    assert messages[0][-1]["date"] == "\"2024-06-15\""
    assert messages[0][-1]["datetime"] == "\"2024-06-15T14:30:45\""


def test_redis_stream_encoding_timedelta(redis_pool, redis_stream_name):
    """Tests encoding of timedelta objects"""

    delta = datetime.timedelta(days=1, hours=2, minutes=30, seconds=45)

    data = {"duration": delta}

    config = redis_sink.RedisStreamParameters(stream=redis_stream_name)
    sink = redis_sink.RedisStream.create(config, redis_pool=redis_pool)
    sink.insert_data(data, redis_sink.RedisStreamMetadata())

    redis_client = redis.Redis(connection_pool=redis_pool)
    messages = redis_client.xrange(redis_stream_name)

    assert messages is not None
    assert len(messages) == 1
    # 1 day + 2 hours + 30 minutes + 45 seconds = 95445 seconds
    assert messages[0][-1]["duration"] == "95445.0"


def test_redis_stream_encoding_nested_tuple(redis_pool, redis_stream_name):
    """Tests that nested tuples are correctly converted to lists"""

    data = {
        "nested": {
            "tuple_field": (1, 2, (3, 4)),
            "mixed": [(5, 6), {"inner": (7, 8)}]
        }
    }

    config = redis_sink.RedisStreamParameters(stream=redis_stream_name)
    sink = redis_sink.RedisStream.create(config, redis_pool=redis_pool)
    sink.insert_data(data, redis_sink.RedisStreamMetadata())

    redis_client = redis.Redis(connection_pool=redis_pool)
    messages = redis_client.xrange(redis_stream_name)

    assert messages is not None
    assert len(messages) == 1
    # Verify tuples are converted to lists in the JSON
    result = messages[0][-1]["nested"]
    assert "[1, 2, [3, 4]]" in result
    assert "[[5, 6]" in result
    assert "[7, 8]" in result


def test_redis_stream_encoding_complex_datetime_structures(redis_pool, redis_stream_name):
    """Tests datetime objects nested in complex structures"""

    data = {
        "timestamps": [
            datetime.datetime(2024, 1, 1, 12, 0, 0),
            datetime.datetime(2024, 1, 2, 12, 0, 0)
        ],
        "schedule": {
            "start": datetime.date(2024, 6, 1),
            "duration": datetime.timedelta(hours=3, minutes=15)
        }
    }

    config = redis_sink.RedisStreamParameters(stream=redis_stream_name)
    sink = redis_sink.RedisStream.create(config, redis_pool=redis_pool)
    sink.insert_data(data, redis_sink.RedisStreamMetadata())

    redis_client = redis.Redis(connection_pool=redis_pool)
    messages = redis_client.xrange(redis_stream_name)

    assert messages is not None
    assert len(messages) == 1
    result = messages[0][-1]
    assert "2024-01-01T12:00:00" in result["timestamps"]
    assert "2024-01-02T12:00:00" in result["timestamps"]
    assert "2024-06-01" in result["schedule"]
    assert "11700.0" in result["schedule"]  # 3*3600 + 15*60 = 11700 seconds


class NonSerializableClass:
    """A custom class for testing non-serializable types"""
    def __init__(self, value):
        self.value = value


def test_redis_stream_encoding_non_serializable(redis_pool, redis_stream_name):
    """Tests that non-serializable custom objects are handled gracefully (passed through)"""

    custom_obj = NonSerializableClass(42)
    data = {"custom": custom_obj}

    config = redis_sink.RedisStreamParameters(stream=redis_stream_name)
    sink = redis_sink.RedisStream.create(config, redis_pool=redis_pool)

    # This should raise a TypeError from json.dumps since custom objects aren't JSON serializable
    with pytest.raises(TypeError) as err_desc:
        sink.insert_data(data, redis_sink.RedisStreamMetadata())

    assert "not JSON serializable" in str(err_desc.value) or "Object of type" in str(err_desc.value)
