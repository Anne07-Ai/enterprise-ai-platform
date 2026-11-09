from numpy import float32
from numpy.typing import NDArray
from sqlalchemy.types import UserDefinedType

class Vector(UserDefinedType[NDArray[float32]]):
    def __init__(self, dim: int | None = None) -> None: ...
