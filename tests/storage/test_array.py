import pathlib
from typing import List

import numpy as np
import pytest

from flumen.storage.array import ArrayStorage


@pytest.fixture()
def array_storage(tmp_file: pathlib.Path) -> ArrayStorage:
    return ArrayStorage[str](tmp_file, np.dtype("U10"))


@pytest.fixture()
def array() -> np.array:
    return np.array(
        ["2022-01-01", "2022-01-02", "2022-01-03", "2022-01-04", "2022-01-05"]
    )


def test_insert_array(array_storage: ArrayStorage, array: np.array) -> None:
    array_storage.insert(0, array)
    assert np.array_equal(array_storage[:], array)
    array_storage.insert(0, "2022-01-06")
    assert array_storage[0] == "2022-01-06"


def test_len_array(array_storage: ArrayStorage, array: List[str]) -> None:
    assert len(array_storage) == 0
    array_storage.append(array)
    assert len(array_storage) == 5


def test_get_array(array_storage: ArrayStorage, array: List[str]) -> None:
    array_storage.append(array)
    assert np.array_equal(array_storage[:], array)
    assert (array_storage[0:2] == np.array(["2022-01-01", "2022-01-02"])).all()


def test_set_array(array_storage: ArrayStorage, array: List[str]) -> None:
    array_storage.append(array)
    array_storage[0] = "2022-01-06"
    assert array_storage[0] == "2022-01-06"
    array_storage[0:2] = np.array(["2022-01-07", "2022-01-08"], array_storage._dtype)
    assert np.array_equal(
        array_storage[0:2], np.array(["2022-01-07", "2022-01-08"], array_storage._dtype)
    )


def test_del_array(array_storage: ArrayStorage, array: List[str]) -> None:
    array_storage.append(array)
    del array_storage[0]
    assert array_storage[0] == "2022-01-02"


def test_append_array(array_storage: ArrayStorage) -> None:
    array_storage.append("2022-01-01")
    array_storage.append("2022-01-02")
    assert np.array_equal(
        array_storage[:], np.array(["2022-01-01", "2022-01-02"], array_storage._dtype)
    )


def test_extend_array(array_storage: ArrayStorage, array: List[str]) -> None:
    array_storage.extend(array)
    assert np.array_equal(array_storage[:], array)


def test_load_array(array_storage: ArrayStorage, array: List[str]) -> None:
    empty_array = np.array([], dtype=array_storage._dtype)
    loaded_array = array_storage.load()
    assert np.array_equal(empty_array, loaded_array)

    array_storage.append(array)
    array_storage.save()
    loaded_array = array_storage.load()
    assert np.array_equal(array, loaded_array)


def test_reload_array(array_storage: ArrayStorage, array: List[str]) -> None:
    array_storage.append(array)
    array_storage.save()
    array_storage.append("2022-01-06")
    array_storage.reload()
    assert np.array_equal(array, array_storage[:])


def test_save_array(array_storage: ArrayStorage, array: List[str]) -> None:
    array_storage.append(array)
    array_storage.save()
    loaded_array = array_storage.load()
    assert np.array_equal(array, loaded_array)


def test_index_array(array_storage: ArrayStorage, array: List[str]) -> None:
    array_storage.append(array)
    assert array_storage.index("2022-01-03") == 2
