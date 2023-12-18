from http import HTTPStatus
from typing import Annotated
from uuid import UUID, uuid4

from annotated_types import Gt, Le
from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.params import Query
from pydantic import BaseModel, Field

from .films import Film
from services.film import FilmService, get_film_service
from services.person import PersonService, get_person_service

# Создаем объект router, в котором будут регистрироваться обработчики.
router = APIRouter()


class PersonFilms(BaseModel):
    """Фильмы персоны"""
    uuid: UUID = Field(
        ..., description='Идентификатор фильма', example=uuid4()
    )
    roles: list[str] = Field(
        ..., description='Список ролей персоны в фильме', example='["writer", "director"]'
    )


class Person(BaseModel):
    """Персона"""
    uuid: UUID = Field(
        ..., description="Идентификатор персоны", example=uuid4()
    )
    full_name: str = Field(
        ..., description="Полное имя персоны", example='Mark Hamill'
    )
    films: list[PersonFilms] = Field(
        [], description="Список uuid фильмов и ролей персоны",
        example='[{"uuid": "57beb3fd-b1c9-4f8a-9c06-2da13f95251c","roles":["actor"]}]'
    )


# Регистрируем обработчик для запроса данных о персоне.
@router.get('/{person_id}', response_model=Person,
            description='Получение информации о персоне', name='Получение информации о персоне')
async def person_details(
        person_id: UUID = Path(..., description='Идентификатор персоны',
                               example='bdf146ce-d0f4-44be-8bde-4834573e18a7'),
        person_service: PersonService = Depends(get_person_service)
) -> Person:
    person = await person_service.get_by_id(person_id)
    if not person:
        # Если персона не найдена, отдаём 404 статус
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='person not found')

    # Перекладываем данные из models.Person в Person.
    return Person(**person.model_dump())


@router.get('/{person_id}/film/', response_model=list[Film],
            description='Получение списка фильмов по персоне', name='Получение списка фильмов по персоне')
async def films_by_person(
        person_id: UUID = Path(..., description='Идентификатор персоны',
                               example='bdf146ce-d0f4-44be-8bde-4834573e18a7'),
        person_service: PersonService = Depends(get_person_service),
        film_service: FilmService = Depends(get_film_service)
) -> list[Film]:
    person = await person_service.get_by_id(person_id)
    if not person:
        # Если персона не найдена, отдаём 404 статус
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='person not found')
    films = [await film_service.get_by_id(film_id) for film_id in set(n.uuid for n in person.films)]
    if not films:
        # Если ни один фильм по персоне не найден, отдаём 404 статус
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='films for the person not found')

    # Перекладываем данные из models.Film в Film.
    return list(map(lambda film: Film(**film.model_dump()), films))


@router.get('/search/', response_model=list[Person],
            description='Поиск персоналий', name='Поиск персоналий')
async def persons_search(
        query: Annotated[str, Query(description='строка поиска', example='Clooney')] = None,
        page_size: Annotated[int, Query(description='Число элементов на странице'), Gt(0), Le(100)] = 50,
        page_number: Annotated[int, Query(description='Номер страницы '), Gt(0)] = 1,
        person_service: PersonService = Depends(get_person_service)
) -> list[Person]:
    persons = await person_service.get_persons(query=query, page=page_number, per_page=page_size)
    if not persons:
        # Если ни один фильм не найден, отдаём 404 статус
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Persons not found')

    # Перекладываем данные из models.Person в Person.
    return list(map(lambda person: Person(**person.model_dump()), persons))
