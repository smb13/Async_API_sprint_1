from typing import Optional
from uuid import UUID

from pydantic import BaseModel
from pydantic_settings import SettingsConfigDict


class Genre(BaseModel):
    uuid: UUID
    name: str

    def to_elastic(self):
        return self.model_dump()


class _Genre(BaseModel):
    uuid: UUID
    name: str

    model_config = SettingsConfigDict(alias_generator=lambda fn: f"genre_{fn}")

    def to_elastic(self):
        return self.model_dump()


class _Films(BaseModel):
    uuid: UUID
    roles: str

    model_config = SettingsConfigDict(alias_generator=lambda fn: f"films_{fn}")

    def to_elastic(self):
        return self.model_dump()


class Person(BaseModel):
    uuid: UUID
    full_name: str
    films: list[_Films]

    def films_to_elastic(self):
        uuids = set(n.uuid for n in self.films)
        grouped = [{'uuid': i, 'roles': [n.roles for n in self.films if n.uuid == i]} for i in uuids]
        return grouped

    def to_elastic(self):
        return {
            'uuid': self.uuid,
            'full_name': self.full_name,
            'films': self.films_to_elastic()
        }


class _Person(BaseModel):
    uuid: UUID
    full_name: str
    role: str

    model_config = SettingsConfigDict(alias_generator=lambda fn: f"person_{fn}")

    def to_elastic(self):
        return self.model_dump(exclude={'role'})


class Movie(BaseModel):
    uuid: UUID
    rating: Optional[float]
    title: str
    description: str
    genres: list[_Genre]
    persons: list[_Person]

    def persons_by_role(self, role: str):
        return filter(lambda person: person.role == role, self.persons)

    def to_elastic(self):
        return {
            'uuid': self.uuid,
            'title': self.title.upper(),
            'imdb_rating': self.rating,
            'description': self.description,
            'genre': list(map(lambda genre: genre.to_elastic(), self.genres)),
            'directors': list(map(lambda person: person.to_elastic(), self.persons_by_role('director'))),
            'actors_names': list(map(lambda person: person.full_name, self.persons_by_role('actor'))),
            'writers_names': list(map(lambda person: person.full_name, self.persons_by_role('writer'))),
            'actors': list(map(lambda person: person.to_elastic(), self.persons_by_role('actor'))),
            'writers': list(map(lambda person: person.to_elastic(), self.persons_by_role('writer')))
        }
