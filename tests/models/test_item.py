import pandas as pd
import pendulum
import pytest

from flumen.models.frequency import Frequency
from flumen.models.item import Item


def test_validate_datetime() -> None:
    with pytest.raises(ValueError) as excinfo:
        Item(
            entity_id="entity_id",
            field_id="field_id",
            freq=Frequency.from_str("D"),
            start_datetime=pendulum.parse("2020-01-03"),
            end_datetime=pendulum.parse("2020-01-01"),
            series=pd.Series(
                [],
                dtype="float32",
                index=pd.DatetimeIndex([], dtype="datetime64[ns, UTC]", freq="D"),
            ),
        )
    assert "start_datetime must be less than end_datetime" in str(excinfo.value)


def test_validate_series_type() -> None:
    with pytest.raises(ValueError) as excinfo:
        Item(
            entity_id="entity_id",
            field_id="field_id",
            freq=Frequency.from_str("D"),
            start_datetime=pendulum.parse("2020-01-01"),
            end_datetime=pendulum.parse("2020-01-03"),
            series=pd.Series(
                [1, 2, 3],
                index=pd.DatetimeIndex(
                    ["2020-01-01", "2020-01-02", "2020-01-03"],
                    dtype="datetime64[ns]",
                    freq="D",
                ),
            ),
        )
    assert "series index must be datetime64[ns, UTC]" in str(excinfo.value)


def test_validate_series() -> None:
    with pytest.raises(ValueError) as excinfo:
        Item(
            entity_id="entity_id",
            field_id="field_id",
            freq=Frequency.from_str("D"),
            start_datetime=pendulum.parse("2020-01-01"),
            end_datetime=pendulum.parse("2020-01-03"),
            series=pd.Series(
                [1, 2, 3],
                index=pd.DatetimeIndex(
                    ["2022-01-01", "2022-01-02", "2022-01-03"],
                    dtype="datetime64[ns, UTC]",
                    freq="D",
                ),
            ),
        )
    assert "series index must match date_range" in str(excinfo.value)
