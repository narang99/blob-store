from blob_store.core import BlobPath, SerialisedBlobPath
from blob_store.backends.local_relative import LocalRelativeBlobPath
from blob_store.backends.s3 import S3BlobPath
from blob_store.backends.azure_blob_storage import AzureBlobPath

KIND_BY_BACKEND = {
    S3BlobPath.kind: S3BlobPath,
    LocalRelativeBlobPath.kind: LocalRelativeBlobPath,
    AzureBlobPath.kind: AzureBlobPath,
}


def _get_backend(kind: str) -> BlobPath:
    if kind not in KIND_BY_BACKEND:
        raise Exception(
            "failed in deserialising SerialisedBlobPath, "
            + f"unknown kind={kind}\n"
            + f"allowed values={list(KIND_BY_BACKEND.keys())}"
        )
    return KIND_BY_BACKEND[kind]


def deserialise(payload: SerialisedBlobPath) -> BlobPath:
    backend = _get_backend(payload["kind"])
    return backend.deserialise(payload)
