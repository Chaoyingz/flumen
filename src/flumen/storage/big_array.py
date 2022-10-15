import pathlib
import struct
from typing import Generic, Iterable, MutableSequence, Tuple, TypeVar, Union, overload

import numpy as np

_T = TypeVar("_T")


class BigArrayStorage(Generic[_T], MutableSequence):
    def __init__(self, uri: pathlib.Path, dtype: np.dtype) -> None:
        self._uri = uri
        self._dtype = dtype
        self._size = self._dtype.itemsize

    def insert(self, index: int, item: Union[_T, np.array]) -> None:
        data: np.ndarray = self[:]
        data = np.insert(data, index, item)
        data.tofile(self._uri)

    @overload
    def __getitem__(self, index: int) -> _T:
        ...

    @overload
    def __getitem__(self, index: slice) -> MutableSequence[_T]:
        ...

    def __getitem__(self, index: Union[int, slice]) -> Union[_T, np.array]:
        with open(self._uri, "r+b") as f:
            if isinstance(index, int):
                if index < 0:
                    index = len(self) + index
                f.seek(index * self._size)
                return struct.unpack(self._dtype.kind, f.read(self._size))[0]
            else:
                start, stop = self.get_slice_index(index)
                f.seek(start * self._size)
                return np.frombuffer(
                    f.read((stop - start) * self._size), dtype=self._dtype
                )

    @overload
    def __setitem__(self, index: int, item: _T) -> None:
        ...

    @overload
    def __setitem__(self, index: slice, item: Iterable[_T]) -> None:
        ...

    def __setitem__(self, index: Union[int, slice], item: Union[_T, np.array]) -> None:
        data = np.array(self[:])
        data[index] = item
        data.tofile(self._uri)

    @overload
    def __delitem__(self, index: int) -> None:
        ...

    @overload
    def __delitem__(self, index: slice) -> None:
        ...

    def __delitem__(self, index: Union[int, slice]) -> None:
        data: np.ndarray = self[:]
        data = np.delete(data, index)
        if data.size == 0:
            self._uri.unlink()
        else:
            data.tofile(self._uri)

    def __len__(self) -> int:
        rv: int = self._uri.stat().st_size // self._size
        return rv

    def append(self, item: _T) -> None:
        with open(self._uri, "a+b") as f:
            f.write(struct.pack(self._dtype.kind, item))

    def extend(self, values: Iterable[_T]) -> None:
        with open(self._uri, "a+b") as f:
            np.array(values).tofile(f)

    def get_slice_index(self, index: slice) -> Tuple[int, int]:
        start, stop = index.start, index.stop
        if start is None:
            start = 0
        if stop is None:
            stop = len(self) + 1
        if stop < 0:
            stop = len(self) + 1 + stop
        return start, stop
