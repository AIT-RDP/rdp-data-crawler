import numpy as np
import pandas as pd
import pytest

from data_crawler.sources import entsoe_da


@pytest.fixture(scope="module")
def day_ahead_prices() -> list[pd.Series]:
    return [
        pd.Series(
            data=50 * np.random.rand(24),
            index=pd.date_range(
                start=pd.Timestamp("2024-02-19T00:00:00", tz="Europe/Vienna"),
                freq="H",
                periods=24,
            ),
        ),
        pd.Series(
            data=50 * np.random.rand(96),
            index=pd.date_range(
                start=pd.Timestamp("2024-02-19T00:00:00", tz="Europe/Berlin"),
                freq="15min",
                periods=96,
            ),
        ),
    ]


@pytest.fixture(scope="module")
def historic_day_ahead_prices() -> list[pd.Series]:
    """Fixture providing historic day-ahead prices for multiple days"""
    return [
        # Day 1 - AT
        pd.Series(
            data=50 * np.random.rand(24),
            index=pd.date_range(
                start=pd.Timestamp("2024-02-20T00:00:00", tz="Europe/Vienna"),
                freq="H",
                periods=24,
            ),
        ),
        # Day 1 - DE_LU
        pd.Series(
            data=50 * np.random.rand(96),
            index=pd.date_range(
                start=pd.Timestamp("2024-02-20T00:00:00", tz="Europe/Berlin"),
                freq="15min",
                periods=96,
            ),
        ),
        # Day 2 - AT
        pd.Series(
            data=50 * np.random.rand(24),
            index=pd.date_range(
                start=pd.Timestamp("2024-02-21T00:00:00", tz="Europe/Vienna"),
                freq="H",
                periods=24,
            ),
        ),
        # Day 2 - DE_LU
        pd.Series(
            data=50 * np.random.rand(96),
            index=pd.date_range(
                start=pd.Timestamp("2024-02-21T00:00:00", tz="Europe/Berlin"),
                freq="15min",
                periods=96,
            ),
        ),
    ]


@pytest.fixture
def mock_entsoe_pandas_client(mocker, day_ahead_prices):
    mock = mocker.patch("entsoe.EntsoePandasClient")
    mock.return_value.query_day_ahead_prices.side_effect = day_ahead_prices

    return mock


@pytest.fixture
def mock_entsoe_pandas_client_history(mocker, historic_day_ahead_prices):
    """Mock for history fetch tests"""
    mock = mocker.patch("entsoe.EntsoePandasClient")
    mock.return_value.query_day_ahead_prices.side_effect = historic_day_ahead_prices
    return mock


@pytest.fixture
def mock_timestamp_now(mocker):
    mock = mocker.patch("pandas.Timestamp.now")
    mock.side_effect = [
        pd.Timestamp("2024-02-18T12:34:59.123", tz="Europe/Vienna"),
        pd.Timestamp("2024-02-18T12:35:12.543", tz="Europe/Berlin"),
    ]

    return mock


def test_entsoe_da(mock_entsoe_pandas_client, mock_timestamp_now, day_ahead_prices):
    source = entsoe_da.ENTSOEDATransparency(source_parameters=entsoe_da.ENTSOEDATransparencyModel(
        api_key="01d12ba5-5350-421f-b32e-19c2e65b9653",
        day_ahead_prices=[
            entsoe_da.DayAheadPricesModel(country_code="AT", timezone="Europe/Vienna"),
            entsoe_da.DayAheadPricesModel(
                country_code="DE_LU",
                timezone="Europe/Berlin",
                resolution=entsoe_da.Resolution.MIN_15,
            ),
        ]
    ).model_dump())

    response_data = source.fetch_data()

    # check of the returned object is as expected
    assert response_data["observation_time"] == [
        t.isoformat() for t in day_ahead_prices[0].index.to_list() + day_ahead_prices[1].index.to_list()
    ]
    assert np.allclose(response_data["day_ahead_prices"], day_ahead_prices[0].to_list() + day_ahead_prices[1].to_list())
    assert response_data["day_ahead_resolution"] == len(day_ahead_prices[0]) * [60] + len(day_ahead_prices[1]) * [15]
    assert response_data["location"] == len(day_ahead_prices[0]) * ["AT"] + len(day_ahead_prices[1]) * ["DE_LU"]

    # check if entsoe-py was called as expected
    args_list = mock_entsoe_pandas_client.return_value.query_day_ahead_prices.call_args_list

    for args, kwargs_expected in zip(args_list, [
        {
            "country_code": "AT",
            "resolution": "60min",
            "start": day_ahead_prices[0].index[0],
            "end": day_ahead_prices[0].index[-1],
        },
        {
            "country_code": "DE_LU",
            "resolution": "15min",
            "start": day_ahead_prices[1].index[0],
            "end": day_ahead_prices[1].index[-1],
        },
    ]):
        _, kwargs = args

        for key in kwargs_expected:
            assert kwargs[key] == kwargs_expected[key]


def test_fetch_timed_historic_data_bundle(mock_entsoe_pandas_client_history, historic_day_ahead_prices):
    """Test history fetch for multiple days"""
    source = entsoe_da.ENTSOEDATransparency(source_parameters=entsoe_da.ENTSOEDATransparencyModel(
        api_key="01d12ba5-5350-421f-b32e-19c2e65b9653",
        day_ahead_prices=[
            entsoe_da.DayAheadPricesModel(country_code="AT", timezone="Europe/Vienna"),
            entsoe_da.DayAheadPricesModel(
                country_code="DE_LU",
                timezone="Europe/Berlin",
                resolution=entsoe_da.Resolution.MIN_15,
            ),
        ]
    ).model_dump())

    start_time = pd.Timestamp("2024-02-19T10:00:00", tz="UTC")
    end_time = pd.Timestamp("2024-02-21T10:00:00", tz="UTC")

    messages = list(source.fetch_timed_historic_data_bundle(start_time, end_time, {}))

    # Should yield 2 messages (2 days)
    assert len(messages) == 2

    # Check first message structure
    assert "observation_time" in messages[0]
    assert "day_ahead_prices" in messages[0]
    assert "day_ahead_resolution" in messages[0]
    assert "location" in messages[0]

    # Verify data from first day
    assert len(messages[0]["observation_time"]) == 24 + 96  # AT hourly + DE_LU 15min
    assert len(messages[0]["day_ahead_prices"]) == 24 + 96
    assert messages[0]["day_ahead_resolution"] == 24 * [60] + 96 * [15]
    assert messages[0]["location"] == 24 * ["AT"] + 96 * ["DE_LU"]

    # Verify data from second day
    assert len(messages[1]["observation_time"]) == 24 + 96
    assert len(messages[1]["day_ahead_prices"]) == 24 + 96


def test_fetch_timed_historic_single_day(mock_entsoe_pandas_client_history, historic_day_ahead_prices):
    """Test history fetch for a single day"""
    source = entsoe_da.ENTSOEDATransparency(source_parameters=entsoe_da.ENTSOEDATransparencyModel(
        api_key="01d12ba5-5350-421f-b32e-19c2e65b9653",
        day_ahead_prices=[
            entsoe_da.DayAheadPricesModel(country_code="AT", timezone="Europe/Vienna"),
        ]
    ).model_dump())

    start_time = pd.Timestamp("2024-02-19T10:00:00", tz="UTC")
    end_time = pd.Timestamp("2024-02-20T10:00:00", tz="UTC")

    messages = list(source.fetch_timed_historic_data_bundle(start_time, end_time, {}))

    # Should yield 1 message (1 day)
    assert len(messages) == 1
    assert len(messages[0]["location"]) > 0


def test_fetch_da_block_timezone_aware_required():
    """Test that fetch_da_block raises error for naive datetime"""
    source = entsoe_da.ENTSOEDATransparency(source_parameters=entsoe_da.ENTSOEDATransparencyModel(
        api_key="01d12ba5-5350-421f-b32e-19c2e65b9653",
        day_ahead_prices=[
            entsoe_da.DayAheadPricesModel(country_code="AT", timezone="Europe/Vienna"),
        ]
    ).model_dump())

    naive_time = pd.Timestamp("2024-02-19T10:00:00")

    with pytest.raises(ValueError, match="must be time-zone aware"):
        source._fetch_da_block(naive_time)


def test_fetch_timed_historic_empty_range(mock_entsoe_pandas_client_history):
    """Test history fetch with start_time >= end_time"""
    source = entsoe_da.ENTSOEDATransparency(source_parameters=entsoe_da.ENTSOEDATransparencyModel(
        api_key="01d12ba5-5350-421f-b32e-19c2e65b9653",
        day_ahead_prices=[
            entsoe_da.DayAheadPricesModel(country_code="AT", timezone="Europe/Vienna"),
        ]
    ).model_dump())

    start_time = pd.Timestamp("2024-02-20T10:00:00", tz="UTC")
    end_time = pd.Timestamp("2024-02-19T10:00:00", tz="UTC")

    messages = list(source.fetch_timed_historic_data_bundle(start_time, end_time, {}))

    # Should yield no messages
    assert len(messages) == 0

