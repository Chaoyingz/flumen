import pathlib
from typing import List

import numpy as np
import pandas as pd
import pytest

from flumen.storage.index_array import DatetimeIndexArrayStorage, IndexArrayStorage


@pytest.fixture()
def array_storage(tmp_file: pathlib.Path) -> IndexArrayStorage:
    return IndexArrayStorage[str](tmp_file, np.dtype("datetime64[ns]"))


@pytest.fixture()
def array() -> np.array:
    return np.array(
        ["2022-01-01", "2022-01-02", "2022-01-03", "2022-01-04", "2022-01-05"],
        dtype="datetime64[ns]",
    )


def test_insert_array_t(array_storage: IndexArrayStorage, array: np.array) -> None:
    array_storage.insert("2022-01-01", array)

    pd.testing.assert_series_equal(
        array_storage[:], array_storage.get_data_series(array, array_storage._dtype)
    )

    array_storage.insert("2022-01-01", "2022-01-06")
    assert array_storage["2022-01-06"] == 0
    with pytest.raises(ValueError):
        array_storage.insert("2022-01-10", "2022-01-06")


def test_insert_array_int(array_storage: IndexArrayStorage, array: List[str]) -> None:
    array_storage.insert(0, array)
    pd.testing.assert_series_equal(
        array_storage[:], array_storage.get_data_series(array, array_storage._dtype)
    )
    array_storage.insert(0, "2022-01-06")
    assert array_storage["2022-01-06"] == 0


def test_len_array(array_storage: IndexArrayStorage, array: List[str]) -> None:
    assert len(array_storage) == 0
    array_storage.append(array)
    assert len(array_storage) == 5


def test_get_array_t(array_storage: IndexArrayStorage, array: List[str]) -> None:
    array_storage.append(array)
    pd.testing.assert_series_equal(
        array_storage[:], array_storage.get_data_series(array, array_storage._dtype)
    )
    pd.testing.assert_series_equal(
        array_storage["2022-01-02":"2022-01-04"],  # type: ignore
        array_storage.get_data_series(array[1:4], array_storage._dtype, 1),
    )


def test_get_array_int(array_storage: IndexArrayStorage, array: List[str]) -> None:
    array_storage.append(array)
    assert array_storage[1] == array[1]
    np.testing.assert_array_equal(array_storage[1:4], array[1:4])


def test_set_array_t(array_storage: IndexArrayStorage, array: List[str]) -> None:
    array_storage.append(array)
    array_storage["2022-01-01"] = "2022-01-06"
    assert array_storage["2022-01-06"] == 0
    array_storage["2022-01-01":"2022-01-06"] = "2022-01-07"  # type: ignore
    pd.testing.assert_series_equal(
        array_storage[:],
        array_storage.get_data_series(
            ["2022-01-07", "2022-01-07", "2022-01-07", "2022-01-07", "2022-01-07"],
            array_storage._dtype,
        ),
    )


def test_set_array_int(array_storage: IndexArrayStorage, array: List[str]) -> None:
    array_storage.append(array)
    array_storage[0] = "2022-01-06"
    assert array_storage["2022-01-06"] == 0
    array_storage[0:5] = "2022-01-07"
    pd.testing.assert_series_equal(
        array_storage[:],
        array_storage.get_data_series(
            ["2022-01-07", "2022-01-07", "2022-01-07", "2022-01-07", "2022-01-07"],
            array_storage._dtype,
        ),
    )


def test_del_array_t(array_storage: IndexArrayStorage, array: List[str]) -> None:
    array_storage.append(array)
    del array_storage["2022-01-01"]
    assert array_storage["2022-01-02"] == 0
    del array_storage["2022-01-02":"2022-01-05"]  # type: ignore
    assert len(array_storage) == 0


def test_del_array_int(array_storage: IndexArrayStorage, array: List[str]) -> None:
    array_storage.append(array)
    del array_storage[0]
    assert array_storage["2022-01-02"] == 0


def test_append_array(array_storage: IndexArrayStorage) -> None:
    array_storage.append(pd.to_datetime("2022-01-01"))
    array_storage.append(pd.to_datetime("2022-01-02"))
    pd.testing.assert_series_equal(
        array_storage[:],
        array_storage.get_data_series(
            ["2022-01-01", "2022-01-02"], array_storage._dtype
        ),
    )


def test_extend_array(array_storage: IndexArrayStorage, array: List[str]) -> None:
    array_storage.extend(array)
    pd.testing.assert_series_equal(
        array_storage[:], array_storage.get_data_series(array, array_storage._dtype)
    )


async def test_load_array(array_storage: IndexArrayStorage, array: List[str]) -> None:
    empty_array = np.array([], dtype=array_storage._dtype)
    array_storage._uri.unlink()
    loaded_array = array_storage.load()
    assert np.array_equal(empty_array, loaded_array)
    assert array_storage._values.empty

    array_storage.append(array)
    await array_storage.save()
    loaded_array = array_storage.load()
    assert np.array_equal(array, loaded_array)
    pd.testing.assert_series_equal(
        array_storage[:], array_storage.get_data_series(array, array_storage._dtype)
    )


async def test_reload_array(array_storage: IndexArrayStorage, array: List[str]) -> None:
    array_storage.append(array)
    await array_storage.save()
    array_storage.append("2022-01-06")
    array_storage.reload()
    pd.testing.assert_series_equal(
        array_storage[:], array_storage.get_data_series(array, array_storage._dtype)
    )


async def test_save_array(array_storage: IndexArrayStorage, array: List[str]) -> None:
    array_storage.append(array)
    await array_storage.save()
    loaded_array = array_storage.load()
    assert np.array_equal(array, loaded_array)


def test_datetime_index_array(tmp_file: pathlib.Path, array: List[str]) -> None:
    dt_index_array = DatetimeIndexArrayStorage(tmp_file)
    dt_index_array.append(array)
    pd.testing.assert_series_equal(
        dt_index_array[:],
        dt_index_array.get_data_series(array, dt_index_array.SERIES_DTYPE),
    )
    assert dt_index_array[:].index.dtype == dt_index_array.SERIES_DTYPE
