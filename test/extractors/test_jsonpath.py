"""
Tests the JSONPath extractors
"""
import datetime

import pytest

import data_crawler.extractors.jsonpath as jx


@pytest.fixture()
def json_data() -> dict:
    """Returns a nested dict-like test structure"""

    return {
        "type": "music/metal",
        "meta": {
            "query date": "2022-08-10T18:00:00+00:00",
            "query date unix": 1660154400
        },
        "samples": [
            {
                "time": "2022-08-10T12:00:00+00:00",
                "time_u": 1660132800,
                "song": "Metallica - Nothing Else Matters",
                "roommate complains": 2
            },
            {
                "time": "2022-08-10T13:00:00+00:00",
                "time_u": 1660136400,
                "song": "Nightwish - Nemo",
                "roommate complains": 1,
                "sing out loud": 1
            },
            {
                "time": "2022-08-10T14:00:00+00:00",
                "time_u": 1660140000,
                "song": "Epica - Sensorium",
                "roommate complains": 3
            }
        ]
    }


@pytest.mark.parametrize("path,is_list,reference_message", [
    ("type", False, "music/metal"),
    ("meta.'query date'", False, "2022-08-10T18:00:00+00:00"),
    ("samples[*].'roommate complains'", True, [2, 1, 3]),
    ("samples[*].song", True, ["Metallica - Nothing Else Matters", "Nightwish - Nemo", "Epica - Sensorium"])
])
def test_path_extractor(json_data, path: str, is_list: bool, reference_message):
    """Tests the generic path extractor on some feasible values"""

    extractor = jx.PathExtractor("test", path, is_list)

    result = extractor.extract_information(json_data)
    assert "test" in result
    assert result["test"] == reference_message


@pytest.mark.parametrize("path,is_list,reference_message", [
    ("meta.'query date'", False, "2022-08-10T18:00:00+00:00"),
    ("samples[*].time", True, ["2022-08-10T12:00:00+00:00", "2022-08-10T13:00:00+00:00", "2022-08-10T14:00:00+00:00"])
])
def test_datetime_path_extractor(json_data, path: str, is_list: bool, reference_message):
    """Tests the datetime path extractor on some feasible values"""

    extractor = jx.DatetimePathExtractor("test", path, is_list)

    result = extractor.extract_information(json_data)
    assert "test" in result
    assert result["test"] == reference_message


@pytest.mark.parametrize("path,is_list,reference_message", [
    ("meta.'query date unix'", False, "2022-08-10T18:00:00+00:00"),
    ("samples[*].time_u", True, ["2022-08-10T12:00:00+00:00", "2022-08-10T13:00:00+00:00", "2022-08-10T14:00:00+00:00"])
])
def test_unix_time_extractor(json_data, path: str, is_list: bool, reference_message):
    """Tests the unix time stamp extractor"""

    extractor = jx.UnixTimeExtractor("test", path, is_list)
    result = extractor.extract_information(json_data)
    assert "test" in result
    assert result["test"] == reference_message


def test_optional_path_extractor(json_data):
    """Tests the optional path extractor"""

    extractor = jx.OptionalPathExtractor("test", "samples[*]", "'sing out loud'", is_list=True, default_value=0)
    result = extractor.extract_information(json_data)
    assert "test" in result
    assert result["test"] == [0, 1, 0]
