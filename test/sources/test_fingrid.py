"""
Tests the Fingrid Open Data source
"""
import datetime
import json
import os

import pytest

import data_crawler.sources.fingrid as fingrid
from common import helpers


@pytest.fixture()
def simplified_dataset_response() -> dict:
    """Returns a simplified Fingrid GetDatasetData response"""

    file = os.path.join(__file__, "../../../data/test/fingrid-dataset-data-reduced.json")
    file = os.path.abspath(file)

    with open(file, "r") as f:
        data = json.load(f)
    return data


@pytest.fixture()
def fingrid_base_parameters() -> dict:
    """Returns a set of base parameters for DatasetData"""

    return {
        "api_key": os.environ.get("DATA_CRAWLER_FINGRID_API_KEY", "---"),
        "dataset_id": 245,
        "cache": {"directory": ".cache-test-persistent"}
    }


def _empty_page_response() -> dict:
    """Returns an empty paginated Fingrid response"""

    return {
        "data": [],
        "pagination": {
            "total": 0,
            "lastPage": 1,
            "prevPage": None,
            "nextPage": None,
            "perPage": 20000,
            "currentPage": 1,
            "from": 0,
            "to": 0,
        },
    }


def _mock_json_response(mocker, payload: dict):
    """Creates a mocked HTTP response returning the given JSON payload"""

    response = mocker.Mock()
    response.json.return_value = payload
    response.raise_for_status = mocker.Mock()
    return response


@pytest.mark.parametrize("config_fkt", [helpers.direct_config, helpers.to_string_values])
def test_dataset_data_parsing(simplified_dataset_response: dict, fingrid_base_parameters: dict, config_fkt):
    """Tests the parsing and transformation mechanism with a static response"""

    api = fingrid.DatasetData(source_parameters=config_fkt(fingrid_base_parameters))
    response_data = api.fetch_data(raw_data=simplified_dataset_response)

    assert response_data is not None
    assert response_data["dataset_id"] == 245
    assert response_data["observation_time"] == [
        "2023-06-28T12:15:00+00:00",
        "2023-06-28T12:30:00+00:00",
    ]
    assert response_data["end_time"] == [
        "2023-06-28T12:30:00+00:00",
        "2023-06-28T12:45:00+00:00",
    ]
    assert response_data["value"] == [186.8, 496.7]


@pytest.mark.parametrize("config_fkt", [helpers.direct_config, helpers.to_string_values])
def test_dataset_data_request_parameters(fingrid_base_parameters: dict, config_fkt, mocker):
    """Tests whether the HTTP request uses the expected URL, header, and query parameters"""

    api = fingrid.DatasetData(source_parameters=config_fkt(fingrid_base_parameters))
    mock_get = mocker.patch.object(
        api.session, "get", return_value=_mock_json_response(mocker, _empty_page_response())
    )

    api.fetch_data()

    mock_get.assert_called_once()
    args, kwargs = mock_get.call_args
    assert args[0] == "https://data.fingrid.fi/api/datasets/245/data"
    assert api.session.headers["x-api-key"] == fingrid_base_parameters["api_key"]

    params = kwargs["params"]
    assert params["format"] == "json"
    assert params["page"] == 1
    assert params["pageSize"] == 20000
    assert params["sortBy"] == "startTime"
    assert params["sortOrder"] == "asc"
    assert "startTime" in params
    assert "endTime" in params
    start_time = datetime.datetime.fromisoformat(params["startTime"].replace("Z", "+00:00"))
    end_time = datetime.datetime.fromisoformat(params["endTime"].replace("Z", "+00:00"))
    assert start_time <= end_time


@pytest.mark.parametrize("config_fkt", [helpers.direct_config, helpers.to_string_values])
def test_dataset_data_initial_history_omitted(fingrid_base_parameters: dict, config_fkt):
    """Tests that omitting initial_history starts the cursor at now"""

    before = datetime.datetime.now(tz=datetime.timezone.utc)
    api = fingrid.DatasetData(source_parameters=config_fkt(fingrid_base_parameters))
    after = datetime.datetime.now(tz=datetime.timezone.utc)

    assert before <= api.last_query_ts <= after


@pytest.mark.parametrize("config_fkt", [helpers.direct_config, helpers.to_string_values])
def test_dataset_data_initial_history_set(fingrid_base_parameters: dict, config_fkt):
    """Tests that initial_history backfills the first poll window"""

    parameters = {**fingrid_base_parameters, "initial_history": "24h"}
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    api = fingrid.DatasetData(source_parameters=config_fkt(parameters))

    assert api.last_query_ts <= now - datetime.timedelta(hours=23, minutes=59)
    assert api.last_query_ts >= now - datetime.timedelta(hours=24, minutes=1)


@pytest.mark.parametrize("config_fkt", [helpers.direct_config, helpers.to_string_values])
def test_dataset_data_cursor_advances(fingrid_base_parameters: dict, config_fkt, mocker):
    """Tests that the second poll only requests samples after the first poll window"""

    parameters = {**fingrid_base_parameters, "initial_history": "2h"}
    api = fingrid.DatasetData(source_parameters=config_fkt(parameters))
    mock_get = mocker.patch.object(
        api.session, "get", return_value=_mock_json_response(mocker, _empty_page_response())
    )

    api.fetch_data()
    first_end = mock_get.call_args.kwargs["params"]["endTime"]

    api.fetch_data()
    second_start = mock_get.call_args.kwargs["params"]["startTime"]

    assert mock_get.call_count == 2
    assert second_start == first_end


def test_dataset_data_pagination(fingrid_base_parameters: dict, mocker):
    """Tests that additional pages are fetched and concatenated"""

    page_1 = {
        "data": [
            {
                "datasetId": 245,
                "startTime": "2023-06-28T12:15:00.0000000+00:00",
                "endTime": "2023-06-28T12:30:00.0000000+00:00",
                "value": 186.8,
            }
        ],
        "pagination": {"nextPage": 2},
    }
    page_2 = {
        "data": [
            {
                "datasetId": 245,
                "startTime": "2023-06-28T12:30:00.0000000+00:00",
                "endTime": "2023-06-28T12:45:00.0000000+00:00",
                "value": 496.7,
            }
        ],
        "pagination": {"nextPage": None},
    }

    api = fingrid.DatasetData(source_parameters=fingrid_base_parameters)
    mock_get = mocker.patch.object(
        api.session,
        "get",
        side_effect=[
            _mock_json_response(mocker, page_1),
            _mock_json_response(mocker, page_2),
        ],
    )

    response_data = api.fetch_data()

    assert mock_get.call_count == 2
    assert mock_get.call_args_list[0].kwargs["params"]["page"] == 1
    assert mock_get.call_args_list[1].kwargs["params"]["page"] == 2
    assert response_data["value"] == [186.8, 496.7]
    assert response_data["observation_time"] == [
        "2023-06-28T12:15:00+00:00",
        "2023-06-28T12:30:00+00:00",
    ]


@pytest.mark.skipif("DATA_CRAWLER_FINGRID_API_KEY" not in os.environ, reason="No API Key provided")
def test_dataset_data_online(fingrid_base_parameters: dict):
    """Queries the online API and does some basic integrity checks"""

    parameters = {**fingrid_base_parameters, "initial_history": "2h"}
    api = fingrid.DatasetData(source_parameters=parameters)
    response_data = api.fetch_data()

    assert response_data is not None
    assert response_data["dataset_id"] == 245
    assert "observation_time" in response_data
    assert "value" in response_data
    assert len(response_data["observation_time"]) == len(response_data["value"])
    if response_data["observation_time"]:
        latest = datetime.datetime.fromisoformat(response_data["observation_time"][-1])
        assert latest > datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=2)
