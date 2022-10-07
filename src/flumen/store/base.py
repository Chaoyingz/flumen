import pathlib

from flumen.utils.path import ensure_dir_exists


class Store:
    def get_internal_type(self) -> str:
        return self.__class__.__name__

    def get_uri(self, root_uri: pathlib.Path) -> pathlib.Path:
        internal_type = self.get_internal_type()
        return ensure_dir_exists(root_uri / internal_type)
