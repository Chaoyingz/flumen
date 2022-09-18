import pathlib
import random
import string

import pytest


@pytest.fixture()
def random_str() -> str:
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for _ in range(8))


@pytest.fixture()
def tmp_file(tmp_path: pathlib.Path, random_str: str) -> pathlib.Path:
    file = tmp_path / random_str
    file.touch()
    return file
