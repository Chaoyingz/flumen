import pathlib

import numpy as np
import pytest

from flumen.store.field import FieldStore


@pytest.fixture()
def field_store(tmp_path: pathlib.Path) -> FieldStore:
    return FieldStore(tmp_path)


@pytest.fixture()
def field_values() -> np.array:
    return np.array([1858.48, 1794.43, 1779.18, 1753.2], dtype="float32")


@pytest.fixture()
async def field_store_created(
    field_store: FieldStore, field_values: np.array
) -> FieldStore:
    await field_store.insert("open", field_values)
    return field_store


async def test_insert_field(
    field_store_created: FieldStore, field_values: np.array
) -> None:
    actual_array = await field_store_created.find("open", 0, -1)
    np.testing.assert_array_equal(actual_array, field_values)


async def test_insert_field_exists(
    field_store_created: FieldStore, field_values: np.array
) -> None:
    with pytest.raises(ValueError):
        await field_store_created.insert("open", field_values)


async def test_find_field(
    field_store_created: FieldStore, field_values: np.array
) -> None:
    actual_array = await field_store_created.find("open", 0, -1)
    np.testing.assert_array_equal(actual_array, field_values)


async def test_find_field_not_exists(field_store_created: FieldStore) -> None:
    with pytest.raises(ValueError):
        await field_store_created.find("close", 0, -1)


async def test_update_field(
    field_store_created: FieldStore, field_values: np.array
) -> None:
    new_values = np.array([1800, 1844.88], dtype="float32")
    await field_store_created.update(
        "open", start_index=2, end_index=4, values=new_values
    )
    actual_array = await field_store_created.find("open", 0, -1)
    np.testing.assert_array_equal(
        actual_array, np.array([1858.48, 1794.43, 1800, 1844.88], dtype="float32")
    )


async def test_update_field_not_exists(
    field_store_created: FieldStore, field_values: np.array
) -> None:
    new_values = np.array([1800, 1844.88], dtype="float32")
    with pytest.raises(ValueError):
        await field_store_created.update(
            "close", start_index=2, end_index=4, values=new_values
        )


async def test_delete_field(
    field_store_created: FieldStore, field_values: np.array
) -> None:
    await field_store_created.delete("open")
    with pytest.raises(ValueError):
        await field_store_created.find("open", 0, -1)


async def test_delete_field_not_exists(
    field_store_created: FieldStore, field_values: np.array
) -> None:
    with pytest.raises(ValueError):
        await field_store_created.delete("close")
