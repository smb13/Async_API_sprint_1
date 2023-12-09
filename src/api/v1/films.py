from http import HTTPStatus
from typing import List, Annotated
from uuid import UUID, uuid4

from annotated_types import Gt, Le
from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.params import Query
from pydantic import BaseModel, Field

from services.film import FilmService, get_film_service

# Создаем объект router, в котором будут регистрироваться обработчики.
router = APIRouter()


class Genre(BaseModel):
    """Жанр"""
    uuid: UUID = Field(
        ..., description="Идентификатор жанра", example=uuid4()
    )
    name: str = Field(..., description="Название жанра", example='Adventure')


class Person(BaseModel):
    """Персона"""
    uuid: UUID = Field(
        ..., description="Идентификатор персоны", example=uuid4()
    )
    full_name: str = Field(
        ..., description="Полное имя персоны", example='Mark Hamill'
    )


class Film(BaseModel):
    """Фильм в списке"""
    uuid: UUID = Field(
        ..., description='Идентификатор фильма', example=uuid4()
    )
    title: str = Field(
        ..., description='Название фильма', example='STAR WARS: EPISODE IV - A NEW HOPE'
    )
    imdb_rating: float = Field(
        ..., description='Рейтинг фильма', example=8.6
    )


class FilmDetails(Film):
    """Детальное представление фильма"""
    description: str = Field(
        ..., description='Описание фильма',
        example="The Imperial Forces, under orders from cruel Darth Vader, hold Princess Leia hostage in their efforts"
                "to quell the rebellion against the Galactic Empire. Luke Skywalker and Han Solo, captain of the"
                "Millennium Falcon, work together with the companionable droid duo R2-D2 and C-3PO to rescue the"
                "beautiful princess, help the Rebel Alliance and restore freedom and justice to the Galaxy."
    )
    genre: List[Genre] = Field(
        ..., description='Жанры',
        examples=[[
            {'id': '120a21cf-9097-479e-904a-13dd7198c1dd', 'name': 'Adventure'},
            {'id': '3d8d9bf5-0d90-4353-88ba-4ccc5d2c07ff', 'name': 'Action'},
            {'id': '6c162475-c7ed-4461-9184-001ef3d9f26e', 'name': 'Sci-Fi'},
            {'id': 'b92ef010-5e4c-4fd0-99d6-41b6456272cd', 'name': 'Fantasy'}
        ]]
    )
    actors: List[Person] = Field(
        ..., description='Актеры',
        examples=[[
            {'id': '26e83050-29ef-4163-a99d-b546cac208f8', 'name': 'Mark Hamill'},
            {'id': '5b4bf1bc-3397-4e83-9b17-8b10c6544ed1', 'name': 'Harrison Ford'},
            {'id': 'b5d2b63a-ed1f-4e46-8320-cf52a32be358', 'name': 'Carrie Fisher'},
            {'id': 'e039eedf-4daf-452a-bf92-a0085c68e156', 'name': 'Peter Cushing'}
        ]]
    )
    writers: List[Person] = Field(
        ..., description='Сценаристы',
        examples=[[{'id': 'a5a8f573-3cee-4ccc-8a2b-91cb9f55250a', 'name': 'George Lucas'}]]
    )
    directors: List[Person] = Field(
        ..., description='Режисеры',
        examples=[[{'id': 'a5a8f573-3cee-4ccc-8a2b-91cb9f55250a', 'name': 'George Lucas'}]]
    )


# Регистрируем обработчик для запроса данных о фильме.
@router.get('/{film_id}', response_model=FilmDetails,
            description='Получение информации о фильме', name='Получение информации о фильме')
async def film_details(
        film_id: UUID = Path(..., description='Идентификатор фильма',
                             example='3d825f60-9fff-4dfe-b294-1a45fa1e115d'),
        film_service: FilmService = Depends(get_film_service)
) -> FilmDetails:
    film = await film_service.get_by_id(film_id)
    if not film:
        # Если фильм не найден, отдаём 404 статус
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    # Перекладываем данные из models.Film в Film.
    return FilmDetails(**film.model_dump())


@router.get('/', response_model=List[Film],
            description='Получение информации о фильме', name='Получение информации о фильме')
async def films_list(
        genre: Annotated[str, Query(description='Фильтр по жанрам')] = None,
        sort: Annotated[str | None, Query(enum=['imdb_rating', '-imdb_rating'], description='Сортировка')] = None,
        page_size: Annotated[int, Query(description='Число элементов на странице'), Gt(0), Le(100)] = 50,
        page_number: Annotated[int, Query(description='Номер страницы '), Gt(0)] = 1,
        film_service: FilmService = Depends(get_film_service)
) -> List[Film]:
    films = await film_service.get_films(sort, genre, page_number, page_size)
    if not films:
        # Если ни один фильм не найден, отдаём 404 статус
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='films not found')

    # Перекладываем данные из models.Film в Film.
    return list(map(lambda film: Film(**film.model_dump()), films))
