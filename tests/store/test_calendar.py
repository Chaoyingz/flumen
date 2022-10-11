import pathlib

import numpy as np
import pandas as pd
import pendulum
import pytest

from flumen.models.frequency import Frequency
from flumen.storage.index_array import DatetimeIndexArrayStorage
from flumen.store.calenadar import CalendarStore


@pytest.fixture()
def calendar_store(tmp_path: pathlib.Path) -> CalendarStore:
    return CalendarStore(tmp_path)


@pytest.fixture()
def freq_1d() -> Frequency:
    return Frequency.from_str("D")


@pytest.fixture()
async def calendar_store_created(
    freq_1d: Frequency, calendar_store: CalendarStore
) -> CalendarStore:
    await calendar_store.insert(
        freq_1d,
        pendulum.parse("2020-01-01"),
        pendulum.parse("2020-01-31"),
    )
    return calendar_store


async def test_load_exists_data(
    freq_1d: Frequency,
    calendar_store_created: CalendarStore,
) -> None:
    invalid_calendar_file = calendar_store_created._uri / (
        "invalid_calendar_file" + calendar_store_created.CALENDAR_STORAGE_EXTENSION
    )
    invalid_calendar_file.touch()
    data = calendar_store_created._load_exists_data()
    assert freq_1d in data
    assert data[freq_1d][:].index.dtype == DatetimeIndexArrayStorage.SERIES_DTYPE
    assert len(data[freq_1d]) == 31


async def test_insert_calendar(
    freq_1d: Frequency, calendar_store_created: CalendarStore
) -> None:
    actual_calendar = calendar_store_created._data[freq_1d][:].index
    expected_calendar = pd.date_range(
        "2020-01-01", "2020-01-31", freq=freq_1d.to_str(), tz="UTC"
    )
    np.testing.assert_array_equal(actual_calendar, expected_calendar)
    actual_storage = DatetimeIndexArrayStorage(
        calendar_store_created.get_freq_calendar_uri(freq_1d),
    )[:]
    pd.testing.assert_series_equal(
        calendar_store_created._data[freq_1d][:], actual_storage
    )


async def test_insert_calendar_twice(
    freq_1d: Frequency, calendar_store_created: CalendarStore
) -> None:
    with pytest.raises(ValueError):
        await calendar_store_created.insert(
            freq_1d,
            pendulum.parse("2020-01-01"),
            pendulum.parse("2020-01-31"),
        )


async def test_find_calendar(
    freq_1d: Frequency, calendar_store_created: CalendarStore
) -> None:
    actual_calendar = await calendar_store_created.find(
        freq_1d,
        pendulum.parse("2020-01-05"),
        pendulum.parse("2020-01-10"),
    )
    expected_calendar = pd.date_range(
        "2020-01-05", "2020-01-10", freq=freq_1d.to_str(), tz="UTC"
    )
    np.testing.assert_array_equal(actual_calendar.index, expected_calendar)
    assert actual_calendar.index.dtype == DatetimeIndexArrayStorage.SERIES_DTYPE
    assert actual_calendar[0] == 4
    assert actual_calendar[-1] == 9


async def test_find_calendar_not_created(
    freq_1d: Frequency, calendar_store: CalendarStore
) -> None:
    with pytest.raises(ValueError):
        await calendar_store.find(
            freq_1d,
            pendulum.parse("2020-01-05"),
            pendulum.parse("2020-01-10"),
        )


async def test_update_calendar(
    freq_1d: Frequency, calendar_store: CalendarStore
) -> None:
    await calendar_store.insert(
        freq_1d,
        pendulum.parse("2020-01-05"),
        pendulum.parse("2020-01-10"),
    )
    await calendar_store.update(
        freq_1d,
        end_datetime=pendulum.parse("2020-01-15"),
    )
    actual_calendar = calendar_store._data[freq_1d][:].index
    expected_calendar = pd.date_range(
        "2020-01-05", "2020-01-15", freq=freq_1d.to_str(), tz="UTC"
    )
    np.testing.assert_array_equal(actual_calendar, expected_calendar)


async def test_update_calendar_not_created(
    freq_1d: Frequency, calendar_store: CalendarStore
) -> None:
    with pytest.raises(ValueError):
        await calendar_store.update(
            freq_1d,
            end_datetime=pendulum.parse("2020-01-15"),
        )


async def test_update_calendar_upsert(
    freq_1d: Frequency, calendar_store: CalendarStore
) -> None:
    await calendar_store.update(
        freq_1d,
        start_datetime=pendulum.parse("2020-01-01"),
        end_datetime=pendulum.parse("2020-01-31"),
        upsert=True,
    )
    actual_calendar = calendar_store._data[freq_1d][:].index
    expected_calendar = pd.date_range(
        "2020-01-01", "2020-01-31", freq=freq_1d.to_str(), tz="UTC"
    )
    np.testing.assert_array_equal(actual_calendar, expected_calendar)


async def test_update_calendar_end_datetime_less_than_start_datetime(
    freq_1d: Frequency,
    calendar_store_created: CalendarStore,
) -> None:
    with pytest.raises(ValueError):
        await calendar_store_created.update(
            freq_1d,
            end_datetime=pendulum.parse("2020-01-01"),
        )


async def test_delete_calendar(
    freq_1d: Frequency, calendar_store_created: CalendarStore
) -> None:
    await calendar_store_created.delete(freq_1d)
    assert freq_1d not in calendar_store_created._data
    assert not calendar_store_created.get_freq_calendar_uri(freq_1d).exists()


async def test_delete_calendar_not_created(
    freq_1d: Frequency, calendar_store: CalendarStore
) -> None:
    with pytest.raises(ValueError):
        await calendar_store.delete(freq_1d)
