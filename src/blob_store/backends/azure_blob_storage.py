from contextlib import contextmanager
from pathlib import PurePath, Path
from uuid import uuid4
from pydantic import BaseModel
from typing import Generator
from typing_extensions import Self
from blob_store.implicit import get_implicit_var, prefix_var
from blob_store.core import BlobPath, SerialisedBlobPath
from blob_store.tmpdir import get_tmp_dir
from azure.storage.blob import BlobServiceClient


class Payload(BaseModel):
    storage_account: str
    container: str
    name: str


IMPLICIT_GEN_STORAGE_ACCOUNT = prefix_var("GEN_AZURE_BLOB_STORAGE_ACCOUNT")
IMPLICIT_GEN_CONTAINER = prefix_var("GEN_AZURE_BLOB_CONTAINER")


class AzureBlobPath(BlobPath):
    kind = "blob-store-azure"

    def __init__(self, storage_account: str, container: str, name: str) -> None:
        self._storage_account = storage_account
        self._container = container
        self._name = name

    def _get_service_client(self):
        account_url = f"https://{self._storage_account}.blob.core.windows.net"
        from azure.identity import DefaultAzureCredential
        default_credential = DefaultAzureCredential()
        return BlobServiceClient(account_url, credential=default_credential)

    def _get_blob_client(self):
        return self._get_service_client().get_blob_client(self._container, self._name)

    def exists(self) -> bool:
        return self._get_blob_client().exists()

    def delete(self) -> bool:
        if self.exists():
            self._get_blob_client().delete_blob()
            return True
        else:
            return False

    @contextmanager
    def open(self, mode: str = "r") -> Generator:
        tmp_path = get_tmp_dir() / str(uuid4())
        if "r" in mode:
            self._download(tmp_path)
        with open(tmp_path, mode) as f:
            yield f
        if "w" in mode:
            self._upload(tmp_path)
        tmp_path.unlink()

    def _upload(self, src: Path) -> None:
        with open(file=src, mode="rb") as f:
            self._get_blob_client().upload_blob(f)

    def _download(self, dest: Path) -> None:
        client = self._get_service_client().get_container_client(self._container)
        with open(dest, "wb") as f:
            client.download_blob(self._name).readinto(f)

    def serialise(self) -> SerialisedBlobPath:
        payload = Payload(
            storage_account=self._storage_account,
            container=self._container,
            name=self._name,
        )
        return SerialisedBlobPath(
            kind=self.kind, payload=payload.model_dump(mode="json")
        )

    @classmethod
    def deserialise(cls, data: SerialisedBlobPath) -> Self:
        p = Payload.model_validate(data["payload"])
        return cls(p.storage_account, p.container, p.name)

    @classmethod
    def create_default(cls, p: PurePath) -> Self:
        storage_account = get_implicit_var(IMPLICIT_GEN_STORAGE_ACCOUNT)
        container = get_implicit_var(IMPLICIT_GEN_CONTAINER)
        return cls(storage_account, container, str(p))

    def __repr__(self) -> str:
        return f"kind={self.kind} storage_account={self._storage_account} container={self._container} name={self._name}"
