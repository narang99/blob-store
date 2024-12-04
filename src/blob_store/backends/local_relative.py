from contextlib import contextmanager
from pydantic import BaseModel
from pathlib import Path, PurePath
from typing_extensions import Generator, Self
from blob_store.implicit import get_implicit_var, prefix_var
from blob_store.core import BlobPath, SerialisedBlobPath


BASE_VAR = prefix_var("LOCAL_RELATIVE_BASE_DIR")


class Payload(BaseModel):
    relpath_parts: list[str]


class LocalRelativeBlobPath(BlobPath):
    kind = "blob-store-local-relative"

    def __init__(self, relpath: PurePath) -> None:
        self._relpath = relpath

    @contextmanager
    def open(self, mode: str = "r") -> Generator:
        p = self._p()
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, mode) as f:
            yield f

    def serialise(self) -> SerialisedBlobPath:
        p = Payload(relpath_parts=list(self._relpath.parts))
        return SerialisedBlobPath(kind=self.kind, payload=p.model_dump(mode="json"))

    def exists(self) -> bool:
        return (self._p()).exists()

    def delete(self) -> bool:
        if self.exists():
            self._p().unlink()
            return True
        else:
            return False

    @classmethod
    def deserialise(cls, data: SerialisedBlobPath) -> Self:
        p = Payload.model_validate(data["payload"])
        return cls(PurePath(*p.relpath_parts))

    @classmethod
    def create_default(cls, p: PurePath) -> Self:
        return cls(p)

    def __repr__(self) -> str:
        return (
            f"kind={self.kind} relative_path={self._relpath} ImplicitVars=[{BASE_VAR}]"
        )

    def _p(self) -> Path:
        return _get_implicit_base_path() / self._relpath


def _get_implicit_base_path() -> Path:
    base_path = Path(get_implicit_var(BASE_VAR))
    base_path.mkdir(exist_ok=True, parents=True)
    return base_path
