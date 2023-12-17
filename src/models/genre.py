from typing import Annotated
from uuid import UUID

from annotated_types import MinLen, IsNotNan
from pydantic import BaseModel


class Genre(BaseModel):
    """Жанр фильма"""
    uuid: UUID
    """Идентификатор жанра (UUID)"""
    name: Annotated[str, IsNotNan, MinLen(1)]
    """Название жанра"""
