import pathlib
import pickle
from collections.abc import MutableMapping
from typing import Dict, Generic, Iterator, TypeVar

_KT = TypeVar("_KT")
_VT = TypeVar("_VT")


class DictStorage(Generic[_KT, _VT], MutableMapping):
    def __init__(
        self,
        uri: pathlib.Path,
    ) -> None:
        self._uri = uri
        self._data = self.load()

    def __setitem__(self, key: _KT, item: _VT) -> None:
        self._data[key] = item

    def __delitem__(self, key: _KT) -> None:
        del self._data[key]

    def __getitem__(self, key: _KT) -> _VT:
        if key in self._data:
            return self._data[key]
        raise KeyError(key)

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Iterator[_KT]:
        rv: Iterator[_KT] = iter(self._data)
        return rv

    def load(self) -> Dict[_KT, _VT]:
        rv: Dict[_KT, _VT] = dict()
        if self._uri.exists():
            with open(self._uri, "rb") as f:
                try:
                    obj = pickle.load(f)
                except EOFError:
                    ...
                else:
                    if isinstance(obj, dict):
                        rv = obj
        else:
            self._uri.touch()
        return rv

    def reload(self) -> None:
        self._data = self.load()

    async def save(self) -> None:
        with open(self._uri, "wb") as f:
            pickle.dump(self._data, f)
