from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Path
from pydantic import BaseModel, Field, UUID4

from services.film import FilmService, get_film_service

# Создаем объект router, в котором будут регистрироваться обработчики.
router = APIRouter()


class Film(BaseModel):
    """
    Модель описывающая ответ API.
    """
    id: UUID4 = Field(..., description='Идентификатор фильма', examples=['3d825f60-9fff-4dfe-b294-1a45fa1e115d'])
    title: str = Field(..., description='Название фильма', examples=['Star Wars'])


# Регистрируем обработчик для запроса данных о фильме.
@router.get('/{film_id}', response_model=Film,
            description='Получение информации о фильме', name='Получение информации о фильме')
async def film_details(
        film_id: UUID4 = Path(..., description='Идентификатор фильма',
                              example='3d825f60-9fff-4dfe-b294-1a45fa1e115d'),
        film_service: FilmService = Depends(get_film_service)
) -> Film:
    film = await film_service.get_by_id(film_id)
    if not film:
        # Если фильм не найден, отдаём 404 статус
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    # Перекладываем данные из models.Film в Film.
    return Film(id=film.id, title=film.title)
