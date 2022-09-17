import random
import string

import pytest


@pytest.fixture()
def random_str() -> str:
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for _ in range(8))
