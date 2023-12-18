import orjson
from functools import lru_cache
from typing import List

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from pydantic import UUID4
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redisdb import get_redis
from models.genre import Genre

GENRE_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class GenreService:
    """
    GenreService содержит бизнес-логику по работе с жанрами.
    """

    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    # Get_by_id возвращает объект жанра. Он опционален, так как жанр может отсутствовать в базе
    async def get_by_id(self, genre_id: UUID4) -> Genre | None:
        # Пытаемся получить данные из кеша, потому что оно работает быстрее
        genre = await self._genre_from_cache(genre_id)
        if not genre:
            # Если жанра нет в кеше, то ищем его в Elasticsearch
            genre = await self._get_genre_from_elastic(genre_id)
            if not genre:
                # Если он отсутствует в Elasticsearch, значит, жанра вообще нет в базе
                return None
            # Сохраняем жанр в кеш
            await self._put_genre_to_cache(genre)

        return genre

    async def get_genres(
            self, *, page: int | None = 1, per_page: int | None = 1
    ) -> List[Genre]:
        # Пытаемся получить данные из кеша, потому что оно работает быстрее.
        genres = await self._genres_list_from_cache(page=page, per_page=per_page)
        if not genres:
            # Если жанров нет в кеше, то ищем их в Elasticsearch
            genres = await self._get_genres_list_from_elastic(page=page, per_page=per_page
                                                              )
            if not genres:
                # Если он отсутствует в Elasticsearch, значит, жанра вообще нет в базе
                return []
            # Сохраняем жанр в кеш
            await self._put_genres_list_to_cache(page=page, per_page=per_page,
                                                 genres=genres)

        return genres

    async def _get_genre_from_elastic(self, genre_id: UUID4) -> Genre | None:
        try:
            doc = await self.elastic.get(index='genres', id=genre_id)
        except NotFoundError:
            return None
        return Genre(**doc['_source'])

    async def _get_genres_list_from_elastic(
            self, *, page: int | None = 1, per_page: int | None = 1
    ) -> List[Genre] | None:
        # Проверка аргументов.
        if page <= 0:
            page = 1
        if per_page <= 0:
            per_page = 1
        try:
            doc = await self.elastic.search(
                index='genres',
                from_=(page - 1) * per_page,
                size=per_page
            )
        except NotFoundError:
            return None
        return list(map(lambda flm: Genre(**flm['_source']), doc['hits']['hits']))

    async def _genre_from_cache(self, genre_id: UUID4) -> Genre | None:
        # Пытаемся получить данные о жанре из кеша, используя команду get https://redis.io/commands/get/
        data = await self.redis.get(str(genre_id))
        if not data:
            return None

        genre = Genre.model_validate_json(data)
        return genre

    async def _genres_list_from_cache(self, **kwargs) -> List[Genre] | None:
        # Пытаемся получить данные о жанре из кеша, используя команду get https://redis.io/commands/get/
        data = await self.redis.get(orjson.dumps(kwargs, option=orjson.OPT_SORT_KEYS))
        if not data:
            return None

        return [Genre.model_validate_json(genre) for genre in orjson.loads(data)]

    async def _put_genre_to_cache(self, genre: Genre):
        # Сохраняем данные о жанре в кэше, указывая время жизни.
        await self.redis.set(str(genre.uuid), genre.model_dump_json(), GENRE_CACHE_EXPIRE_IN_SECONDS)

    async def _put_genres_list_to_cache(self, genres: List[Genre], **kwargs):
        await self.redis.set(
            orjson.dumps(kwargs, option=orjson.OPT_SORT_KEYS),
            orjson.dumps([ob.model_dump_json() for ob in genres]),
            GENRE_CACHE_EXPIRE_IN_SECONDS
        )
        return None


# Get_genre_service — это провайдер GenreService.
# Используем lru_cache-декоратор, чтобы создать объект сервиса в едином экземпляре (синглтон)
@lru_cache()
def get_genre_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(redis, elastic)
