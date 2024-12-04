from contextlib import contextmanager
from functools import partial
import shutil
from pathlib import Path, PurePath
import abc
from typing import Any, Generator, TypedDict, Union
from pydantic import BaseModel
from typing_extensions import Self


class SerialisedBlobPath(TypedDict):
    kind: str
    payload: Any


class DoesNotExist(Exception):
    pass


class BlobPath(abc.ABC):
    """
    A path object which can be easily used across cloud storage backends
    This is a very restricted class, only functionality supported across backends is added here
    performance is not that big a concern right now


    This path is supposed to be easily serialisable and deserialisable
    Its required that this can be passed across networks, and the properties remain the SAME
    We do not rely on pickle for this
    """

    kind = "BlobPath"

    @contextmanager
    @abc.abstractmethod
    def open(self, mode: str = "r") -> Generator: ...

    @abc.abstractmethod
    def exists(self) -> bool: ...

    @abc.abstractmethod
    def delete(self) -> bool: ...

    @abc.abstractmethod
    def serialise(self) -> SerialisedBlobPath: ...

    @classmethod
    @abc.abstractmethod
    def deserialise(cls, data: SerialisedBlobPath) -> Self: ...

    @classmethod
    @abc.abstractmethod
    def create_default(cls, p: PurePath) -> Self: ...

    def cp(self, destination: Union[Path, "BlobPath"]) -> None:
        with self.open("rb") as fr:
            open_fn = get_open_fn(destination)
            with open_fn("wb") as fw:
                shutil.copyfileobj(fr, fw)


def get_open_fn(p: Union[BlobPath, Path]):
    return partial(open, p) if isinstance(p, Path) else p.open
