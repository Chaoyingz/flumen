import pathlib
from collections.abc import MutableSequence
from typing import Generic, Iterable, TypeVar, Union, overload

import numpy as np
import pandas as pd

_T = TypeVar("_T")


class IndexArrayStorage(Generic[_T], MutableSequence):
    def __init__(
        self,
        uri: pathlib.Path,
        dtype: np.dtype,
    ) -> None:
        self._dtype = dtype
        self._uri = uri
        self._keys: np.array = self.load()
        self._values: pd.Series = self.get_data_series(self._keys, self._dtype)

    @staticmethod
    def get_data_series(
        array: np.array, dtype: str, index_offset: int = 0
    ) -> pd.Series:
        return pd.Series(
            np.arange(index_offset, len(array) + index_offset),
            index=pd.Index(array, dtype=dtype),
        )

    @staticmethod
    def is_integer_index(index: Union[int, slice, _T]) -> bool:
        return isinstance(index, int) or (
            isinstance(index, slice)
            and any([isinstance(index.start, int), isinstance(index.stop, int)])
        )

    def _set_data_series(self) -> None:
        self._values = self.get_data_series(self._keys, self._dtype)

    def insert(self, index: Union[int, _T], item: Union[_T, np.array]) -> None:
        if not self.is_integer_index(index):
            try:
                value_index = self._values[index]
            except KeyError:
                if len(self._values) == 0:
                    value_index = 0
                else:
                    raise ValueError(f"Index {index} not found")
        else:
            value_index = index
        self._keys = np.insert(self._keys, value_index, item)
        self._set_data_series()

    def __len__(self) -> int:
        return len(self._keys)

    @overload
    def __getitem__(self, index: int) -> _T:
        ...

    @overload
    def __getitem__(self, index: slice) -> np.array:
        ...

    @overload
    def __getitem__(self, index: _T) -> _T:
        ...

    def __getitem__(self, index: Union[int, slice, _T]) -> Union[int, _T, pd.Series]:
        if self.is_integer_index(index):
            return self._keys[index]
        return self._values[index]

    def __setitem__(
        self, index: Union[int, slice, _T], item: Union[int, np.array]
    ) -> None:
        if self.is_integer_index(index):
            self._keys[index] = item
        else:
            self._keys[self._values.sort_index().loc[index]] = item
        self._set_data_series()

    @overload
    def __delitem__(self, index: int) -> None:
        ...

    @overload
    def __delitem__(self, index: slice) -> None:
        ...

    @overload
    def __delitem__(self, index: _T) -> None:
        ...

    def __delitem__(self, index: Union[int, slice, _T]) -> None:
        if self.is_integer_index(index):
            self._keys = np.delete(self._keys, index)
        else:
            self._keys = np.delete(self._keys, self._values[index])
        self._set_data_series()

    def append(self, item: _T) -> None:
        self._keys = np.append(self._keys, np.array([item], dtype=self._dtype))
        self._set_data_series()

    def extend(self, values: Iterable[_T]) -> None:
        self._keys = np.concatenate([self._keys, values])
        self._set_data_series()

    def load(self) -> pd.Series:
        if self._uri.exists():
            return np.fromfile(self._uri, dtype=self._dtype)
        else:
            self._uri.touch()
            return np.array([], dtype=self._dtype)

    def reload(self) -> None:
        self._keys = self.load()
        self._set_data_series()

    async def save(self) -> None:
        if self._keys.size > 0:
            self._keys.tofile(self._uri)
        else:
            self._uri.unlink(missing_ok=True)


class DatetimeIndexArrayStorage(IndexArrayStorage):
    ARRAY_DTYPE = np.dtype("datetime64[ns]")
    SERIES_DTYPE = "datetime64[ns, UTC]"

    def __init__(
        self,
        uri: pathlib.Path,
    ) -> None:
        super().__init__(uri, self.ARRAY_DTYPE)
        self._values: pd.Series = self.get_data_series(self._keys, self.SERIES_DTYPE)

    def _set_data_series(self) -> None:
        self._values = self.get_data_series(self._keys, self.SERIES_DTYPE)
