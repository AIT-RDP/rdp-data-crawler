"""
Tests for shared sink metadata fields (initial, force_initial)
"""
import pytest

import data_crawler.sinks.abc.abstract_sink as abstract_sink


def test_sink_metadata_defaults():
    """SinkMetadata defaults initial and force_initial to False"""
    metadata = abstract_sink.SinkMetadata()
    assert metadata.initial is False
    assert metadata.force_initial is False


@pytest.mark.parametrize("field,value", [
    ("initial", True),
    ("force_initial", True),
])
def test_sink_metadata_fields_can_be_enabled(field, value):
    """SinkMetadata accepts enabling initial and force_initial"""
    metadata = abstract_sink.SinkMetadata(**{field: value})
    assert getattr(metadata, field) is value
