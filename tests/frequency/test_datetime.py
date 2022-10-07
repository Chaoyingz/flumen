from typing import List

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import pendulum
import pytest

from flumen.frequency.datetime import date_range
from flumen.models.frequency import Frequency


@pytest.mark.parametrize(
    "freq, start_datetime, end_datetime",
    [
        (
            Frequency.from_str("S"),
            "2020-01-01 00:00:00",
            "2020-01-02 00:00:05",
        ),
        (
            Frequency.from_str("1H"),
            "2020-01-01 00:00:00",
            "2020-02-01 00:00:00",
        ),
        (
            Frequency.from_str("1D"),
            "2020-01-01 00:00:00",
            "2020-02-01 00:00:00",
        ),
        (
            Frequency.from_str("1W"),
            "2020-01-01 00:00:00",
            "2020-02-01 00:00:00",
        ),
        (
            Frequency.from_str("1M"),
            "2020-01-01 00:00:00",
            "2022-02-01 00:00:00",
        ),
        (
            Frequency.from_str("1Q"),
            "2020-01-01 00:00:00",
            "2021-02-01 00:00:00",
        ),
        (
            Frequency.from_str("1Y"),
            "2020-01-01 00:00:00",
            "2022-02-01 00:00:00",
        ),
    ],
)
def test_pandas_date_range(
    freq: Frequency, start_datetime: str, end_datetime: str
) -> None:
    actual_date_range = date_range(
        freq, pendulum.parse(start_datetime), pendulum.parse(end_datetime)
    )
    expected_date_range = pd.date_range(
        start_datetime, end_datetime, freq=freq.raw_str, tz="UTC"
    )

    np.testing.assert_array_equal(actual_date_range, expected_date_range)


@pytest.mark.parametrize(
    "freq, start_datetime, end_datetime, holidays",
    [
        (
            Frequency.from_str("SSED"),
            "2020-01-01 00:00:00",
            "2020-01-10 00:00:00",
            ["2020-01-04", "2020-01-05"],
        ),
        (
            Frequency.from_str("HKEXD"),
            "2020-01-01 00:00:00",
            "2020-01-10 00:00:00",
            ["2020-01-04", "2020-01-05"],
        ),
    ],
)
def test_freq_greater_than_equal_to_1d_market_date_range(
    freq: Frequency, start_datetime: str, end_datetime: str, holidays: List[str]
) -> None:
    dts = date_range(freq, pendulum.parse(start_datetime), pendulum.parse(end_datetime))
    exchange = mcal.get_calendar(freq.unit.market_exchange)
    holiday_index = pd.DatetimeIndex(holidays, tz=exchange.tz).tz_convert("UTC")
    for dt in dts:
        assert dt not in holiday_index


def test_freq_less_than_1d_market_date_range() -> None:
    freq = Frequency.from_str("1SSET")
    start_datetime = pendulum.parse("2020-01-01 00:00:00")
    end_datetime = pendulum.parse("2020-01-02 00:00:00")
    dts = date_range(freq, start_datetime, end_datetime)
    assert len(dts) == 4 * 60
