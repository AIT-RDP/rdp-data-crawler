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


@pytest.fixture
def mock_entsoe_pandas_client(mocker, day_ahead_prices):
    mock = mocker.patch("entsoe.EntsoePandasClient")
    mock.return_value.query_day_ahead_prices.side_effect = day_ahead_prices

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
