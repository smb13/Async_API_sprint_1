from typing import List, Optional, Annotated
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
    films: Optional[List[PersonFilms]] = []
    """Фильмы, в которых принимала участия указанная персона"""


class Film(BaseModel):
    """Фильм"""
    uuid: UUID
    """Идентификатор фильма (UUID)"""
    title: Annotated[str, IsNotNan, MinLen(1)]
    """Название фильма"""
    imdb_rating: Optional[Annotated[float, Ge(0)]]
    """Рейтинг фильма"""
    description: Optional[str]
    """Описание фильма"""
    genre: List[Genre]
    """Жанры фильма"""
    actors: Optional[List[Person]]
    """Актеры, учавствовавшие в фильме"""
    writers: Optional[List[Person]]
    """Сценаристы фильма"""
    directors: Optional[List[Person]]
    """Режиссеры фильма"""
