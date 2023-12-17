from http import HTTPStatus
from typing import List, Annotated
from uuid import UUID, uuid4

from annotated_types import Gt, Le
from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.params import Query
from pydantic import BaseModel, Field

from services.genre import GenreService, get_genre_service

# Создаем объект router, в котором будут регистрироваться обработчики.
router = APIRouter()


class Genre(BaseModel):
    """Жанр"""
    uuid: UUID = Field(
        ..., description="Идентификатор жанра", example=uuid4()
    )
    name: str = Field(..., description="Название жанра", example='Adventure')


# Регистрируем обработчик для запроса данных о жанре.
@router.get('/{genre_id}', response_model=Genre,
            description='Получение информации о жанре', name='Получение информации о жанре')
async def genre_details(
        genre_id: UUID = Path(..., description='Идентификатор жанра',
                              example='6d141ad2-d407-4252-bda4-95590aaf062a'),
        genre_service: GenreService = Depends(get_genre_service)
) -> Genre:
    genre = await genre_service.get_by_id(genre_id)
    if not genre:
        # Если жанр не найден, отдаём 404 статус
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='genre not found')

    # Перекладываем данные из models.Genre в Genre.
    return Genre(**genre.model_dump())


@router.get('/', response_model=List[Genre],
            description='Получение списка жанров', name='Получение списка жанров')
async def genres_list(
        page_size: Annotated[int, Query(description='Число элементов на странице'), Gt(0), Le(100)] = 50,
        page_number: Annotated[int, Query(description='Номер страницы '), Gt(0)] = 1,
        genre_service: GenreService = Depends(get_genre_service)
) -> List[Genre]:
    genres = await genre_service.get_genres(page=page_number, per_page=page_size)
    if not genres:
        # Если ни один фильм не найден, отдаём 404 статус
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='genres not found')

    # Перекладываем данные из models.Genre в Genre.
    return list(map(lambda genre: Genre(**genre.model_dump()), genres))
