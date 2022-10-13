import pandas as pd
import pendulum
import pytest

from flumen.models.frequency import Frequency
from flumen.models.timeseries import TimeSeries


def test_validate_datetime() -> None:
    with pytest.raises(ValueError) as excinfo:
        TimeSeries(
            entity="entity_id",
            field="field_id",
            freq=Frequency.from_str("D"),
            start_datetime=pendulum.parse("2020-01-03"),
            end_datetime=pendulum.parse("2020-01-01"),
            values=pd.Series(
                [],
                dtype="float32",
                index=pd.DatetimeIndex([], dtype="datetime64[ns, UTC]", freq="D"),
            ),
        )
    assert "start_datetime must be less than end_datetime" in str(excinfo.value)


def test_validate_value_type() -> None:
    with pytest.raises(ValueError) as excinfo:
        TimeSeries(
            entity="entity_id",
            field="field_id",
            freq=Frequency.from_str("D"),
            start_datetime=pendulum.parse("2020-01-01"),
            end_datetime=pendulum.parse("2020-01-03"),
            values=pd.Series(
                [1, 2, 3],
                index=pd.DatetimeIndex(
                    ["2020-01-01", "2020-01-02", "2020-01-03"],
                    dtype="datetime64[ns]",
                    freq="D",
                ),
            ),
        )
    assert "series index must be datetime with timezone" in str(excinfo.value)


def test_validate_values() -> None:
    with pytest.raises(ValueError) as excinfo:
        TimeSeries(
            entity="entity_id",
            field="field_id",
            freq=Frequency.from_str("D"),
            start_datetime=pendulum.parse("2020-01-01"),
            end_datetime=pendulum.parse("2020-01-03"),
            values=pd.Series(
                [1, 2, 3],
                index=pd.DatetimeIndex(
                    ["2022-01-01", "2022-01-02", "2022-01-03"],
                    dtype="datetime64[ns, UTC]",
                    freq="D",
                ),
            ),
        )
    assert "series index must match date_range" in str(excinfo.value)


def test_convert_timezone() -> None:
    ts = TimeSeries(
        entity="entity_id",
        field="field_id",
        freq=Frequency.from_str("D"),
        start_datetime=pendulum.parse("2020-01-01"),
        end_datetime=pendulum.parse("2020-01-03"),
        values=pd.Series(
            [1, 2, 3],
            index=pd.DatetimeIndex(
                ["2020-01-01", "2020-01-02", "2020-01-03"],
                dtype="datetime64[ns, US/Eastern]",
                freq="D",
            ),
        ),
    )
    assert str(ts.values.index.tz) == "UTC"
