import pathlib

import numpy as np

from flumen.storage.big_array import BigArrayStorage
from flumen.store.base import Store


class FieldStore(Store):
    FIELD_STORAGE_EXTENSION = ".field"
    FIELD_DTYPE = np.dtype("float32")

    def __init__(self, root_uri: pathlib.Path) -> None:
        self._uri = self.get_uri(root_uri)

    def get_field_uri(self, field: str) -> pathlib.Path:
        return self._uri / f"{field}{self.FIELD_STORAGE_EXTENSION}"

    async def insert(self, field: str, values: np.array) -> None:
        uri = self.get_field_uri(field)
        if uri.exists():
            raise ValueError(f"Field {field} already exists")
        BigArrayStorage(uri, dtype=self.FIELD_DTYPE).extend(values)

    async def find(
        self,
        field: str,
        start_index: int,
        end_index: int,
    ) -> np.array:
        uri = self.get_field_uri(field)
        if not uri.exists():
            raise ValueError(f"Field {field} does not exist")
        return BigArrayStorage(uri, dtype=self.FIELD_DTYPE)[start_index:end_index]

    async def update(
        self,
        field: str,
        *,
        start_index: int,
        end_index: int,
        values: np.array,
    ) -> None:
        uri = self.get_field_uri(field)
        if not uri.exists():
            raise ValueError(f"Field {field} does not exist")
        BigArrayStorage(uri, dtype=self.FIELD_DTYPE)[start_index:end_index] = values

    async def delete(self, field: str) -> None:
        uri = self.get_field_uri(field)
        if not uri.exists():
            raise ValueError(f"Field {field} does not exist")
        del BigArrayStorage(uri, dtype=self.FIELD_DTYPE)[:]
