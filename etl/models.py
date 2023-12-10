import uuid
from typing import Optional
from pydantic import BaseModel
from pydantic_settings import SettingsConfigDict


class Person(BaseModel):
    id: uuid.UUID
    name: str
    role: str

    model_config = SettingsConfigDict(alias_generator=lambda fn: f"person_{fn}")

    def to_elastic(self):
        return {'id': self.id, 'name': self.name}


class Movie(BaseModel):
    id: uuid.UUID
    rating: Optional[float]
    title: str
    description: str
    genres: list[str]
    persons: list[Person]

    def persons_by_role(self, role: str):
        return filter(lambda person: person.role == role, self.persons)

    def to_elastic(self):
        return {
            'id': self.id,
            'imdb_rating': self.rating,
            'genre': self.genres,
            'title': self.title.upper(),
            'description': self.description,
            'director': list(map(lambda person: person.name, self.persons_by_role('director'))),
            'actors_names': list(map(lambda person: person.name, self.persons_by_role('actor'))),
            'writers_names': list(map(lambda person: person.name, self.persons_by_role('writer'))),
            'actors': list(map(lambda person: person.to_elastic(), self.persons_by_role('actor'))),
            'writers': list(map(lambda person: person.to_elastic(), self.persons_by_role('writer')))
        }
