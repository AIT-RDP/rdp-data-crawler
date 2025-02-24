"""
Tests the JSONPath extractors
"""
import datetime

import pytest

import data_crawler.access.jsonpath as jx


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


@pytest.mark.parametrize("in_data, target_type", [
    (12, float), (True, float), ("13", float), (14.5, float),
    (15, int), (16.1, int), ("17", int),
    (1, bool), (True, bool), (0, bool)
])
def test_path_extractor_target_type(in_data, target_type):
    """Tests whether the path extractor successfully converts the types to the target data type"""

    extractor = jx.PathExtractor("test", "d", is_list=False, target_type=target_type)

    result = extractor.extract_information({"d": in_data})
    result = result["test"]
    assert result == target_type(in_data)
    assert isinstance(result, target_type)


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


@pytest.mark.parametrize("in_data, target_type, use_default", [
    (12, float, True), (12, float, False),
    (16.1, int, True), (16.1, int, False),
    (1, bool, True), (1, bool, False)
])
def test_path_optional_path_extractor_target_type(in_data, target_type, use_default: bool):
    """Tests whether the optional path extractor successfully converts the types to the target data type"""

    extractor = jx.OptionalPathExtractor("test", "d", "e", is_list=False,
                                         default_value=in_data * use_default,
                                         target_type=target_type)

    if use_default:
        src_data = {"d": {}}
    else:
        src_data = {"d": {"e": in_data}}

    result = extractor.extract_information(src_data)
    result = result["test"]

    assert result == target_type(in_data)
    assert isinstance(result, target_type)
