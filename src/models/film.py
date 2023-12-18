from typing import List, Annotated
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
    roles: List[str]
    """Список ролей персоны в фильме"""


class Person(BaseModel):
    """Персона"""
    uuid: UUID
    """Идентификатор персоны (UUID)"""
    full_name: Annotated[str, IsNotNan, MinLen(1)]
    """Полное имя персоны"""
    films: List[PersonFilms] | None = []
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
    genre: List[Genre]
    """Жанры фильма"""
    actors: List[Person] | None
    """Актеры, учавствовавшие в фильме"""
    writers: List[Person] | None
    """Сценаристы фильма"""
    directors: List[Person] | None
    """Режиссеры фильма"""
