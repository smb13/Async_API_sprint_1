from contextlib import asynccontextmanager

from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from redis.asyncio import Redis

from api.v1 import films, genres, persons
from core.config import redis_settings, elastic_settings, project_settings
from db import elastic, redisdb as redis


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Создаем подключение к базам при старте сервера.
    redis.redis = Redis(host=redis_settings.host, port=redis_settings.port)
    elastic.es = AsyncElasticsearch(hosts=[f'http://{elastic_settings.host}:{elastic_settings.port}'])

    # Проверяем соединения с базами.
    await redis.redis.ping()
    await elastic.es.ping()

    yield

    # Отключаемся от баз при выключении сервера
    await redis.redis.close()
    await elastic.es.close()


app = FastAPI(
    # Название проекта, используемое в документации.
    title=project_settings.name,
    # Адрес документации (swagger).
    docs_url='/api/openapi',
    # Адрес документации (openapi).
    openapi_url='/api/openapi.json',
    # Оптимизация работы с JSON-сериализатором.
    default_response_class=ORJSONResponse,
    # Указываем функцию, обработки жизненного цикла приложения.
    lifespan=lifespan,
    # Описание сервиса
    description="API для получения информации о фильмах, жанрах и людях, участвовавших в их создании",
)


# Подключаем роутер к серверу с указанием префикса для API (/v1/films).
app.include_router(films.router, prefix='/api/v1/films', tags=['Films'])

# Подключаем роутер к серверу с указанием префикса для API (/v1/genres).
app.include_router(genres.router, prefix='/api/v1/genres', tags=['Genres'])

# Подключаем роутер к серверу с указанием префикса для API (/v1/persons).
app.include_router(persons.router, prefix='/api/v1/persons', tags=['Persons'])

if __name__ == '__main__':
    pass
