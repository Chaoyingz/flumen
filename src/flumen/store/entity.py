import pathlib
from typing import Optional, Tuple

import pendulum

from flumen.storage.dict import DictStorage
from flumen.store.base import Store


class EntityStore(Store):
    ENTITY_STORAGE_EXTENSION = ".entity"

    def __init__(self, root_uri: pathlib.Path) -> None:
        self._uri = self.get_uri(root_uri)
        self._data = self._load_exists_data()

    def get_uri(self, root_uri: pathlib.Path) -> pathlib.Path:
        return root_uri / ("entities" + self.ENTITY_STORAGE_EXTENSION)

    def _load_exists_data(self) -> DictStorage:
        return DictStorage(self._uri)

    async def insert(
        self,
        entity: str,
        start_datetime: pendulum.DateTime,
        end_datetime: pendulum.DateTime,
    ) -> None:
        if entity in self._data:
            raise ValueError(f"Entity {entity} already exists")
        self._data[entity] = (start_datetime, end_datetime)
        await self._data.save()

    async def find(
        self,
        entity: str,
    ) -> Tuple[pendulum.DateTime, pendulum.DateTime]:
        if entity not in self._data:
            raise ValueError(f"Entity {entity} does not exist")
        return self._data[entity]

    async def update(
        self,
        entity: str,
        *,
        start_datetime: Optional[pendulum.DateTime] = None,
        end_datetime: Optional[pendulum.DateTime] = None,
    ) -> None:
        if entity not in self._data:
            raise ValueError(f"Entity {entity} does not exist")
        if start_datetime is not None:
            self._data[entity] = (start_datetime, self._data[entity][1])
        if end_datetime is not None:
            self._data[entity] = (self._data[entity][0], end_datetime)
        await self._data.save()

    async def delete(self, entity: str) -> None:
        if entity not in self._data:
            raise ValueError(f"Entity {entity} does not exist")
        del self._data[entity]
        await self._data.save()
