import pathlib
from typing import Dict

import pytest

from flumen.storage.dict import DictStorage


@pytest.fixture()
def dict_storage(tmp_file: pathlib.Path) -> DictStorage:
    return DictStorage[str, tuple](tmp_file)


@pytest.fixture()
def dict_() -> Dict[str, tuple]:
    return {
        "600519.XSHG": ("2022-01-01", "2022-09-01"),
        "600036.XSHG": ("2022-01-01", "2022-09-01"),
        "600000.XSHG": ("2022-01-01", "2022-09-01"),
    }


def test_set_dict(dict_storage: DictStorage, dict_: Dict[str, tuple]) -> None:
    dict_storage.update(dict_)
    assert dict_storage["600519.XSHG"] == ("2022-01-01", "2022-09-01")
    assert dict_storage["600036.XSHG"] == ("2022-01-01", "2022-09-01")
    assert dict_storage["600000.XSHG"] == ("2022-01-01", "2022-09-01")


def test_del_dict(dict_storage: DictStorage, dict_: Dict[str, tuple]) -> None:
    dict_storage.update(dict_)
    del dict_storage["600519.XSHG"]
    assert "600519.XSHG" not in dict_storage
    assert dict_storage["600036.XSHG"] == ("2022-01-01", "2022-09-01")
    assert dict_storage["600000.XSHG"] == ("2022-01-01", "2022-09-01")


def test_get_dict(dict_storage: DictStorage, dict_: Dict[str, tuple]) -> None:
    dict_storage.update(dict_)
    assert dict_storage["600519.XSHG"] == ("2022-01-01", "2022-09-01")
    assert dict_storage["600036.XSHG"] == ("2022-01-01", "2022-09-01")
    assert dict_storage["600000.XSHG"] == ("2022-01-01", "2022-09-01")
    with pytest.raises(KeyError):
        _ = dict_storage["600001.XSHE"]


def test_len_dict(dict_storage: DictStorage, dict_: Dict[str, tuple]) -> None:
    assert len(dict_storage) == 0
    dict_storage.update(dict_)
    assert len(dict_storage) == 3


def test_iter_dict(dict_storage: DictStorage, dict_: Dict[str, tuple]) -> None:
    dict_storage.update(dict_)
    assert set(dict_storage) == set(dict_)


def test_load_dict(dict_storage: DictStorage, dict_: Dict[str, tuple]) -> None:
    dict_storage.update(dict_)
    dict_storage.save()
    assert dict_storage.load() == dict_


def test_reload_dict(dict_storage: DictStorage, dict_: Dict[str, tuple]) -> None:
    dict_storage.update(dict_)
    dict_storage.save()
    dict_storage["600519.XSHG"] = ("2022-01-01", "2022-09-02")
    dict_storage.reload()
    assert dict_storage["600519.XSHG"] == ("2022-01-01", "2022-09-01")


def test_save_dict(dict_storage: DictStorage, dict_: Dict[str, tuple]) -> None:
    dict_storage.update(dict_)
    dict_storage.save()
    assert dict_storage.load() == dict_
