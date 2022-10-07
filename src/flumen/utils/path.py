import pathlib


def ensure_dir_exists(path: pathlib.Path) -> pathlib.Path:
    if not path.exists():
        path.mkdir(parents=True)
    return path
