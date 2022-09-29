import pathlib

import numpy as np
import pytest

from flumen.storage.big_array import BigArrayStorage


@pytest.fixture()
def big_array_storage(tmp_file: pathlib.Path) -> BigArrayStorage:
    return BigArrayStorage(tmp_file, dtype=np.dtype("f"))


@pytest.fixture()
def array() -> np.array:
    return np.array([1.0, 2.0, 3.0, 4.0, 5.0], dtype=np.dtype("f"))


def test_insert_big_array(big_array_storage: BigArrayStorage, array: np.array) -> None:
    big_array_storage.insert(0, array)
    assert np.array_equal(big_array_storage[:], array)
    big_array_storage.insert(2, 6.0)
    assert big_array_storage[2] == 6.0
    assert np.array_equal(
        big_array_storage[:], np.array([1.0, 2.0, 6.0, 3.0, 4.0, 5.0])
    )


def test_get_big_array(big_array_storage: BigArrayStorage, array: np.array) -> None:
    big_array_storage.insert(0, array)
    assert np.array_equal(big_array_storage[:], array)
    assert np.array_equal(big_array_storage[0:2], np.array([1.0, 2.0]))
    assert np.array_equal(big_array_storage[-1], 5.0)


def test_set_big_array(big_array_storage: BigArrayStorage, array: np.array) -> None:
    big_array_storage.insert(0, array)
    big_array_storage[0] = 6.0
    assert big_array_storage[0] == 6.0
    big_array_storage[0:2] = np.array([7.0, 8.0], dtype=np.dtype("f"))
    assert np.array_equal(big_array_storage[0:2], np.array([7.0, 8.0]))


def test_del_big_array(big_array_storage: BigArrayStorage, array: np.array) -> None:
    big_array_storage.insert(0, array)
    del big_array_storage[0]
    assert np.array_equal(big_array_storage[:], np.array([2.0, 3.0, 4.0, 5.0]))
    del big_array_storage[0:2]
    assert np.array_equal(big_array_storage[:], np.array([4.0, 5.0]))


def test_len_big_array(big_array_storage: BigArrayStorage, array: np.array) -> None:
    big_array_storage.insert(0, array)
    assert len(big_array_storage) == 5
    del big_array_storage[0]
    assert len(big_array_storage) == 4


def test_append_big_array(big_array_storage: BigArrayStorage) -> None:
    big_array_storage.append(1.0)
    assert big_array_storage[0] == 1.0
    big_array_storage.append(2.0)
    assert big_array_storage[1] == 2.0
    assert np.array_equal(big_array_storage[:], np.array([1.0, 2.0]))


def test_extend_big_array(big_array_storage: BigArrayStorage, array: np.array) -> None:
    big_array_storage.extend(array)
    assert np.array_equal(big_array_storage[:], array)
    big_array_storage.extend(np.array([6.0, 7.0], dtype=np.dtype("f")))
    assert np.array_equal(
        big_array_storage[:], np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
    )


def test_get_slice_index(big_array_storage: BigArrayStorage, array: np.array) -> None:
    big_array_storage.extend(array)
    assert big_array_storage.get_slice_index(slice(0, 2)) == (0, 2)
    assert big_array_storage.get_slice_index(slice(None, 3)) == (0, 3)
    assert big_array_storage.get_slice_index(slice(2, None)) == (2, len(array))
    assert big_array_storage.get_slice_index(slice(None, None)) == (0, len(array))
    assert big_array_storage.get_slice_index(slice(0, -2)) == (0, len(array) - 2)
