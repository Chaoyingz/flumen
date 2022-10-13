import pathlib

import pendulum
import pytest

from flumen.store.entity import EntityStore


@pytest.fixture()
def entity_store(tmp_path: pathlib.Path) -> EntityStore:
    return EntityStore(tmp_path)


@pytest.fixture()
async def entity_store_created(entity_store: EntityStore) -> EntityStore:
    await entity_store.insert(
        "XSHG.600519",
        pendulum.parse("2020-01-01"),
        pendulum.parse("2020-01-31"),
    )
    return entity_store


async def test_load_exists_data(
    tmp_path: pathlib.Path, entity_store_created: EntityStore
) -> None:
    invalid_entity_file = tmp_path / (
        "invalid_entity_file" + entity_store_created.ENTITY_STORAGE_EXTENSION
    )
    invalid_entity_file.touch()
    data = entity_store_created._load_exists_data()
    assert "XSHG.600519" in data
    assert data["XSHG.600519"] == (
        pendulum.parse("2020-01-01"),
        pendulum.parse("2020-01-31"),
    )


async def test_insert_entity(entity_store: EntityStore) -> None:
    await entity_store.insert(
        "XSHG.600519",
        pendulum.parse("2020-01-01"),
        pendulum.parse("2020-01-31"),
    )
    assert entity_store._data["XSHG.600519"] == (
        pendulum.parse("2020-01-01"),
        pendulum.parse("2020-01-31"),
    )


async def test_insert_entity_exists(entity_store_created: EntityStore) -> None:
    with pytest.raises(ValueError):
        await entity_store_created.insert(
            "XSHG.600519",
            pendulum.parse("2020-01-01"),
            pendulum.parse("2020-01-31"),
        )


async def test_find_entity(entity_store_created: EntityStore) -> None:
    assert await entity_store_created.find("XSHG.600519") == (
        pendulum.parse("2020-01-01"),
        pendulum.parse("2020-01-31"),
    )


async def test_find_entity_not_exists(entity_store_created: EntityStore) -> None:
    with pytest.raises(ValueError):
        await entity_store_created.find("XSHG.6005191")


async def test_update_entity(entity_store_created: EntityStore) -> None:
    await entity_store_created.update(
        "XSHG.600519",
        start_datetime=pendulum.parse("2020-10-01"),
        end_datetime=pendulum.parse("2020-11-30"),
    )
    assert entity_store_created._data["XSHG.600519"] == (
        pendulum.parse("2020-10-01"),
        pendulum.parse("2020-11-30"),
    )


async def test_update_entity_not_exists(entity_store_created: EntityStore) -> None:
    with pytest.raises(ValueError):
        await entity_store_created.update(
            "XSHG.6005191",
            start_datetime=pendulum.parse("2020-10-01"),
            end_datetime=pendulum.parse("2020-11-30"),
        )


async def test_delete_entity(entity_store_created: EntityStore) -> None:
    await entity_store_created.delete("XSHG.600519")
    assert "XSHG.600519" not in entity_store_created._data


async def test_delete_entity_not_exists(entity_store_created: EntityStore) -> None:
    with pytest.raises(ValueError):
        await entity_store_created.delete("XSHG.6005191")
