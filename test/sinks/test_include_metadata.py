"""
Tests for the common sink include_metadata parameter and initial metadata field
"""
import pandas as pd
import pytest

import data_crawler.sinks.abc.abstract_sink as abstract_sink
import data_crawler.sinks.redis as redis_sink


def test_sink_parameters_include_metadata_default():
    """SinkParameters defaults include_metadata to False"""
    params = abstract_sink.SinkParameters()
    assert params.include_metadata is False


def test_sink_parameters_include_metadata_can_be_enabled():
    """SinkParameters accepts include_metadata=True"""
    params = abstract_sink.SinkParameters(include_metadata=True)
    assert params.include_metadata is True


def test_sink_metadata_initial_default():
    """SinkMetadata defaults initial to False"""
    metadata = abstract_sink.SinkMetadata()
    assert metadata.initial is False


def test_sink_metadata_initial_can_be_enabled():
    """SinkMetadata accepts initial=True"""
    metadata = abstract_sink.SinkMetadata(initial=True)
    assert metadata.initial is True


@pytest.mark.parametrize("include_metadata", [False, True])
@pytest.mark.parametrize("sink_type", ["redis", "websocket", "mqtt", "modbus", "opcua"])
def test_concrete_parameter_models_inherit_include_metadata(sink_type, include_metadata):
    """
    All concrete sink parameter models inherit include_metadata

    NOTE: Optional sinks (websocket, MQTT, Modbus, OPC UA) depend on Poetry
    extras that may not be installed. Skipping those cases when imports fail
    (via pytest.importorskip) lets Redis/abstract tests still run in a minimal
    environment instead of failing collection for the whole file.
    """
    if sink_type == "redis":
        params = redis_sink.RedisStreamParameters(
            include_metadata=include_metadata
        )
    elif sink_type == "websocket":
        websocket_sink = pytest.importorskip("data_crawler.sinks.websocket")
        params = websocket_sink.WebsocketParameters(
            url="ws://localhost:8080",
            include_metadata=include_metadata
        )
    elif sink_type == "mqtt":
        mqtt_sink = pytest.importorskip("data_crawler.sinks.mqtt")
        params = mqtt_sink.MqttSinkParameters(
            host="localhost",
            port=1883,
            topic="test/topic",
            include_metadata=include_metadata
        )
    elif sink_type == "modbus":
        modbus_shared = pytest.importorskip("data_crawler.shared.modbus")
        params = modbus_shared.ModbusParameters.model_validate({
            "address": "127.0.0.1",
            "port": 5020,
            "register spec": pd.DataFrame.from_dict({
                "Register_start": [100],
                "Register_type": ["i"],
                "Data_type": ["UINT16"],
                "Name": ["value"],
                "Unit": ["1"],
                "Scaling": [1.0],
            }),
            "include_metadata": include_metadata,
        })
    elif sink_type == "opcua":
        opcua_shared = pytest.importorskip("data_crawler.shared.opcua")
        params = opcua_shared.OPCUAParameters.model_validate({
            "endpoint": "opc.tcp://127.0.0.1:4840/",
            "register_spec": pd.DataFrame.from_dict({
                "address": ["ns=0;i=10000"],
                "name": ["value"],
            }),
            "include_metadata": include_metadata,
        })
    else:
        raise AssertionError(f"Unexpected sink_type: {sink_type}")

    assert params.include_metadata is include_metadata


@pytest.mark.parametrize("include_metadata", [False, True])
def test_redis_stream_include_metadata_wiring(redis_pool, redis_stream_name, include_metadata):
    """RedisStream.create wires include_metadata onto the sink instance"""
    config = redis_sink.RedisStreamParameters(stream=redis_stream_name, include_metadata=include_metadata)
    sink = redis_sink.RedisStream.create(config, redis_pool=redis_pool)
    assert sink.include_metadata is include_metadata
