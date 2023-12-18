from typing import Annotated
from uuid import UUID

from annotated_types import MinLen, IsNotNan, Ge
from pydantic import BaseModel


class Genre(BaseModel):
    """Жанр фильма"""
    uuid: UUID
    """Идентификатор жанра (UUID)"""
    name: Annotated[str, IsNotNan, MinLen(1)]
    """Название жанра"""


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


class Film(BaseModel):
    """Фильм"""
    uuid: UUID
    """Идентификатор фильма (UUID)"""
    title: Annotated[str, IsNotNan, MinLen(1)]
    """Название фильма"""
    imdb_rating: Annotated[float, Ge(0)] | None
    """Рейтинг фильма"""
    description: str | None
    """Описание фильма"""
    genre: list[Genre]
    """Жанры фильма"""
    actors: list[Person] | None
    """Актеры, учавствовавшие в фильме"""
    writers: list[Person] | None
    """Сценаристы фильма"""
    directors: list[Person] | None
    """Режиссеры фильма"""
