from typing import Annotated
from uuid import UUID

from annotated_types import MinLen, IsNotNan
from pydantic import BaseModel


class PersonFilms(BaseModel):
    """Фильм, в котором принимала участия указанная персона"""
    uuid: UUID
    """Идентификатор фильма (UUID)"""
    roles: list[str]
    """Список ролей персоны в фильме"""


class Person(BaseModel):
    """Персона"""
    uuid: UUID
    """Идентификатор персоны (UUID)"""
    full_name: Annotated[str, IsNotNan, MinLen(1)]
    """Полное имя персоны"""
    films: list[PersonFilms] | None = []
    """Фильмы, в которых принимала участия указанная персона"""
