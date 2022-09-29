import pathlib
from collections.abc import MutableSequence
from typing import Generic, Iterable, TypeVar, Union, overload

import numpy as np

_T = TypeVar("_T")


class ArrayStorage(Generic[_T], MutableSequence):
    def __init__(self, uri: pathlib.Path, dtype: np.dtype) -> None:
        self._dtype = dtype
        self._uri = uri
        self._data = self.load()

    def insert(self, index: int, item: Union[_T, np.array]) -> None:
        self._data = np.insert(self._data, index, item)

    def __len__(self) -> int:
        return len(self._data)

    @overload
    def __getitem__(self, index: int) -> _T:
        ...

    @overload
    def __getitem__(self, index: slice) -> np.array:
        ...

    def __getitem__(self, index: Union[int, slice]) -> Union[_T, np.array]:
        return self._data[index]

    def __setitem__(self, index: Union[int, slice], item: Union[_T, np.array]) -> None:
        if isinstance(index, int):
            self._data[index] = item
        else:
            self._data[np.r_[index]] = item

    @overload
    def __delitem__(self, index: int) -> None:
        ...

    @overload
    def __delitem__(self, index: slice) -> None:
        ...

    def __delitem__(self, index: Union[int, slice]) -> None:
        self._data = np.delete(self._data, index)

    def append(self, item: _T) -> None:
        self._data = np.append(self._data, item)

    def extend(self, values: Iterable[_T]) -> None:
        self._data = np.concatenate([self._data, values])

    def load(self) -> np.array:
        if self._uri.exists():
            return np.fromfile(self._uri, dtype=self._dtype)
        else:
            self._uri.touch()
            return np.array([], dtype=self._dtype)

    def reload(self) -> None:
        self._data = self.load()

    def save(self) -> None:
        self._data.tofile(self._uri)
