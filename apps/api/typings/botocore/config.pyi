from collections.abc import Mapping

# Constructor options used by DocumentStorage.
class Config:
    def __init__(self, *, s3: Mapping[str, str], signature_version: str) -> None: ...
