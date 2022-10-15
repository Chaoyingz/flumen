import pathlib

import pandas as pd
import pendulum
import pytest

from flumen.models.frequency import Frequency
from flumen.models.timeseries import TimeSeries
from flumen.store.timeseries import TimeSeriesStore


@pytest.fixture()
def ts() -> TimeSeries:
    return TimeSeries(
        entity="XSHG.600519",
        field="close",
        freq=Frequency.from_str("SSED"),
        start_datetime=pendulum.parse("2022-03-01"),
        end_datetime=pendulum.parse("2022-03-10"),
        values=pd.Series(
            [1858.48, 1794.43, 1779.18, 1753.2, 1707, 1780.5, 1800, 1844.88],
            dtype="float32",
            index=pd.DatetimeIndex(
                [
                    "2022-03-01",
                    "2022-03-02",
                    "2022-03-03",
                    "2022-03-04",
                    "2022-03-07",
                    "2022-03-08",
                    "2022-03-09",
                    "2022-03-10",
                ],
                dtype="datetime64[ns, Asia/Shanghai]",
            ),
        ),
    )


@pytest.fixture()
def store(tmp_path: pathlib.Path) -> TimeSeriesStore:
    return TimeSeriesStore(str(tmp_path))


async def test_insert_one(store: TimeSeriesStore, ts: TimeSeries) -> None:
    await store.insert_one(ts)
    print(ts)
