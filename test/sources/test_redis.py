"""
Tests the Redis data source
"""

import datetime

import redis

import data_crawler.sources.abc.active_source_sync as active_source_sync
import data_crawler.sources.redis as redis_source


def test_redis_stream_config_classes():
    """Tests the RedisStream sink configuration classes"""

    assert issubclass(redis_source.RedisStream.parameter_model(), redis_source.RedisStreamParameters)


def test_redis_stream_single_stream_single_message(redis_pool, redis_stream_name):
    """Tests a single message with a single stream with the RedisStream source"""

    config = redis_source.RedisStreamParameters(group_name="pytest", consumer_name="pytest",
                                                streams=[redis_source.StreamParameters(name=redis_stream_name)])
    source = redis_source.RedisStream.create(config, redis_pool=redis_pool)

    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.xadd(name=redis_stream_name, fields=dict(message='"Stream it."', time='"now"'))

    source.start()

    for idx, msg in enumerate(source.run()):
        assert idx == 0, "Only one message is expected"
        assert msg.payload == dict(message="Stream it.", time="now")

        source.shutdown()

    source.stop()


def test_redis_stream_single_stream_multiple_messages(redis_pool, redis_stream_name):
    """Tests multiple messages with a single stream with the RedisStream source"""

    config = redis_source.RedisStreamParameters(group_name="pytest", consumer_name="pytest",
                                                streams=[redis_source.StreamParameters(name=redis_stream_name)])
    source = redis_source.RedisStream.create(config, redis_pool=redis_pool)

    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.xadd(name=redis_stream_name, fields=dict(message='"Stream it."', time='"now"'))
    redis_client.xadd(name=redis_stream_name, fields=dict(message='"Happy Birthday!"', time='"not now :("'))

    source.start()

    for idx, msg in enumerate(source.run()):
        assert idx < 2

        msg_expected = dict(message="Stream it.", time="now") if idx == 0 else \
            dict(message="Happy Birthday!", time="not now :(")

        assert msg.payload == msg_expected

        if idx == 1:
            source.shutdown()

    # add messages on the fly
    redis_client.xadd(name=redis_stream_name, fields=dict(message='"You shall not pass!"', time='"tomorrow"'))

    last_idx: int | None = None

    for idx, msg in enumerate(source.run()):
        msg_expected = dict(message="You shall not pass!", time="tomorrow") if idx == 0 else \
            dict(message="My precious", time="a while ago")

        assert msg.payload == msg_expected

        match idx:
            case 0:
                redis_client.xadd(name=redis_stream_name, fields=dict(message='"My precious"', time='"a while ago"'))
            case 1:
                source.shutdown()
            case _other:
                assert False

        last_idx = idx

    source.stop()

    assert last_idx == 1


def test_redis_stream_multiple_streams_single_message(redis_pool, redis_stream_name, redis_stream_name_other):
    """Tests a single message with multiple streams with the RedisStream source"""

    config = redis_source.RedisStreamParameters(
        group_name="pytest", consumer_name="pytest",
        streams=[
            redis_source.StreamParameters(name=redis_stream_name),
            redis_source.StreamParameters(name=redis_stream_name_other),
        ]
    )
    source = redis_source.RedisStream.create(config, redis_pool=redis_pool)

    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.xadd(name=redis_stream_name, fields=dict(message='"Stream it."', time='"now"'))
    redis_client.xadd(name=redis_stream_name_other, fields=dict(message='"Happy Birthday!"', time='"not now :("'))

    source.start()

    payloads: list[dict[str, str]] = []
    last_idx: int | None = None

    for idx, msg in enumerate(source.run()):
        payloads.append(msg.payload)

        if idx == 1:
            source.shutdown()

        last_idx = idx

    source.stop()

    assert last_idx == 1
    assert len(payloads) == 2
    assert dict(message="Stream it.", time="now") in payloads
    assert dict(message="Happy Birthday!", time="not now :(") in payloads


def test_redis_stream_single_stream_two_consumers(redis_pool, redis_stream_name):
    """Tests if two sources can listen to the same stream when being in different groups"""

    config_1 = redis_source.RedisStreamParameters(group_name="pytest_1", consumer_name="pytest_1",
                                                  streams=[redis_source.StreamParameters(name=redis_stream_name)])
    source_1 = redis_source.RedisStream.create(config_1, redis_pool=redis_pool)

    config_2 = redis_source.RedisStreamParameters(group_name="pytest_2", consumer_name="pytest_2",
                                                  streams=[redis_source.StreamParameters(name=redis_stream_name)])
    source_2 = redis_source.RedisStream.create(config_2, redis_pool=redis_pool)

    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.xadd(name=redis_stream_name, fields=dict(message='"Stream it."', time='"now"'))

    source_1.start()
    source_2.start()

    for idx, msg in enumerate(source_1.run()):
        assert idx == 0, "Only one message is expected"
        assert msg.payload == dict(message="Stream it.", time="now")

        source_1.stop()
        source_1.shutdown()

    for idx, msg in enumerate(source_2.run()):
        assert idx == 0, "Only one message is expected"
        assert msg.payload == dict(message="Stream it.", time="now")

        source_2.shutdown()

    source_2.stop()


def test_redis_stream_metadata(redis_pool, redis_stream_name, redis_stream_name_other):
    """Test if metadata is successfully added to the generated message"""

    config = redis_source.RedisStreamParameters(
        group_name="pytest", consumer_name="pytest",
        streams=[
            redis_source.StreamParameters(
                name=redis_stream_name,
                metadata=dict(device="doomsday", caption="press the red button"),
            ),
            redis_source.StreamParameters(
                name=redis_stream_name_other,
                metadata=dict(device="bestday", caption="enjoy the sun"),
            ),
        ],
    )
    source = redis_source.RedisStream.create(config, redis_pool=redis_pool)

    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.xadd(name=redis_stream_name, fields=dict(message='"Stream it."', time='"now"'))
    redis_client.xadd(name=redis_stream_name_other, fields=dict(message='"Happy Birthday!"', time='"not now :("'))

    source.start()

    payloads: list[dict[str, str]] = []
    metadata: list[dict[str, str]] = []
    last_idx: int | None = None

    for idx, msg in enumerate(source.run()):
        payloads.append(msg.payload)
        metadata.append(msg.metadata)

        if idx == 1:
            source.shutdown()

        last_idx = idx

    source.stop()

    assert last_idx == 1
    assert len(payloads) == 2
    assert dict(message="Stream it.", time="now") in payloads
    assert dict(device="doomsday", caption="press the red button") in metadata
    assert dict(message="Happy Birthday!", time="not now :(") in payloads
    assert dict(device="bestday", caption="enjoy the sun") in metadata

    if dict(message="Stream it.", time="now") == payloads[0]:
        assert dict(device="doomsday", caption="press the red button") == metadata[0]
    else:
        assert dict(device="doomsday", caption="press the red button") == metadata[1]


def test_redis_stream_activity_status(redis_pool, redis_stream_name):
    """Test the activity statis of the Redis source"""

    config = redis_source.RedisStreamParameters(group_name="pytest", consumer_name="pytest",
                                                streams=[redis_source.StreamParameters(name=redis_stream_name)])
    source = redis_source.RedisStream.create(config, redis_pool=redis_pool)

    assert source.get_activity_status() == active_source_sync.ActivityStatus(
        last_wakeup=None,
        last_cycle_complete=None,
        max_permitted_cycle_time=datetime.timedelta(milliseconds=config.block_time_ms),
    )

    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.xadd(name=redis_stream_name, fields=dict(message='"Stream it."', time='"now"'))

    source.start()

    start_time = datetime.datetime.now(tz=datetime.timezone.utc)

    for idx, msg in enumerate(source.run()):
        assert idx == 0, "Only one message is expected"
        assert msg.payload == dict(message="Stream it.", time="now")

        source.shutdown()

    source.stop()
    stop_time = datetime.datetime.now(tz=datetime.timezone.utc)

    activity_status = source.get_activity_status()

    assert activity_status.max_permitted_cycle_time == datetime.timedelta(milliseconds=config.block_time_ms)
    assert start_time <= activity_status.last_wakeup <= stop_time
    assert start_time <= activity_status.last_cycle_complete <= stop_time
    assert activity_status.last_cycle_complete >= activity_status.last_wakeup
