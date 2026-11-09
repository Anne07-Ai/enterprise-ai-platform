from contextlib import AbstractAsyncContextManager
from typing import Literal, Protocol, TypedDict

from botocore.config import Config

# Structural subset of the dynamically generated S3 client and body.
class _Readable(Protocol):
    async def read(self) -> bytes: ...

class _GetObject(TypedDict):
    Body: AbstractAsyncContextManager[_Readable]

class _S3Client(Protocol):
    async def put_object(
        self, *, Bucket: str, Key: str, Body: bytes, ContentType: str
    ) -> object: ...
    async def get_object(self, *, Bucket: str, Key: str) -> _GetObject: ...
    async def delete_object(self, *, Bucket: str, Key: str) -> object: ...

class Session:
    def __init__(self) -> None: ...
    def client(
        self,
        service_name: Literal["s3"],
        *,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str,
        config: Config,
    ) -> AbstractAsyncContextManager[_S3Client]: ...
