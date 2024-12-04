from contextlib import contextmanager
from pathlib import PurePath, Path
from uuid import uuid4
from blob_store.core.interface import DoesNotExist
import boto3.session
import botocore.exceptions
from pydantic import BaseModel
from typing import Generator
from typing_extensions import Self
from blob_store.implicit import get_implicit_var, prefix_var
from blob_store.core import BlobPath, SerialisedBlobPath
from blob_store.tmpdir import get_tmp_dir


class Payload(BaseModel):
    bucket: str
    region: str
    object_key: str


IMPLICIT_GEN_BUCKET = prefix_var("GEN_S3_BUCKET")
IMPLICIT_GEN_REGION = prefix_var("GEN_S3_REGION")


class S3BlobPath(BlobPath):
    kind = "blob-store-aws"

    def __init__(self, bucket: str, region: str, object_key: str) -> None:
        self._bucket = bucket
        self._region = region
        self._object_key = object_key

    def exists(self) -> bool:
        client = self._s3_client()
        try:
            client.head_object(Bucket=self._bucket, Key=self._object_key)
            return True
        except botocore.exceptions.ClientError as ex:
            if "Error" not in ex.response or "Code" not in ex.response["Error"]:
                raise Exception(
                    "developer exception: unidentified boto3 ClientError response body for `exists`"
                    + f"response={ex.response}"
                ) from ex
            if ex.response["Error"]["Code"] == "404":
                return False
            else:
                raise ex

    def delete(self) -> bool:
        if self.exists():
            o = self._s3_resource().Object(self._bucket, self._object_key)
            o.delete()
            return True
        else:
            return False

    @contextmanager
    def open(self, mode: str = "r") -> Generator:
        tmp_path = get_tmp_dir() / str(uuid4())
        bucket = self._s3_resource().Bucket(self._bucket)
        if "r" in mode:
            self._download_object(tmp_path)
        with open(tmp_path, mode) as f:
            yield f
        if "w" in mode:
            bucket.upload_file(tmp_path, self._object_key)
        tmp_path.unlink()

    def _download_object(self, dest: Path) -> None:
        bucket = self._s3_resource().Bucket(self._bucket)
        try:
            bucket.download_file(self._object_key, dest)
        except botocore.exceptions.ClientError as ex:
            if "Error" not in ex.response or "Code" not in ex.response["Error"]:
                raise Exception(
                    "developer exception: unidentified boto3 ClientError response body for `exists`"
                    + f"response={ex.response}"
                ) from ex
            if ex.response["Error"]["Code"] == "404":
                raise DoesNotExist(f"s3-blob {self} does not exist") from ex
            else:
                raise ex

        

    def serialise(self) -> SerialisedBlobPath:
        payload=Payload(
            bucket=self._bucket, region=self._region, object_key=self._object_key
        )
        return SerialisedBlobPath(
            kind=self.kind,
            payload=payload.model_dump(mode="json")
        )

    @classmethod
    def deserialise(cls, data: SerialisedBlobPath) -> Self:
        p = Payload.model_validate(data["payload"])
        return cls(p.bucket, p.region, p.object_key)

    @classmethod
    def create_default(cls, p: PurePath) -> Self:
        bucket = get_implicit_var(IMPLICIT_GEN_BUCKET)
        region = get_implicit_var(IMPLICIT_GEN_REGION)
        return cls(bucket, region, str(p))

    def _s3_resource(self):
        return _session().resource("s3", region_name=self._region)

    def _s3_client(self):
        return _session().client("s3", region_name=self._region)

    def __repr__(self) -> str:
        return f"kind={self.kind} bucket={self._bucket} region={self._region} object_key={self._object_key}"


def _session():
    return boto3.session.Session()
