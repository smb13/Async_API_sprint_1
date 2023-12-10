import orjson
from functools import lru_cache
from typing import Optional, List

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from pydantic import UUID4
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redisdb import get_redis
from models.film import Film

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class FilmService:
    """
    FilmService содержит бизнес-логику по работе с фильмами.
    """
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    # Get_by_id возвращает объект фильма. Он опционален, так как фильм может отсутствовать в базе
    async def get_by_id(self, film_id: UUID4) -> Optional[Film]:
        # Пытаемся получить данные из кеша, потому что оно работает быстрее
        film = await self._film_from_cache(film_id)
        if not film:
            # Если фильма нет в кеше, то ищем его в Elasticsearch
            film = await self._get_film_from_elastic(film_id)
            if not film:
                # Если он отсутствует в Elasticsearch, значит, фильма вообще нет в базе
                return None
            # Сохраняем фильм в кеш
            await self._put_film_to_cache(film)

        return film

    async def get_films(
            self, *, sort: str | None, genre: str | None = None,
            page: int | None = 1, per_page: int | None = 1, query: str | None = None
    ) -> List[Film]:
        # Пытаемся получить данные из кеша, потому что оно работает быстрее.
        films = await self._films_list_from_cache(sort=sort, genre=genre, page=page, per_page=per_page, query=query)
        if not films:
            # Если фильмов нет в кеше, то ищем их в Elasticsearch
            films = await self._get_films_list_from_elastic(
                sort=sort, genre=genre, page=page, per_page=per_page, film=query
            )
            if not films:
                # Если он отсутствует в Elasticsearch, значит, фильма вообще нет в базе
                return []
            # Сохраняем фильм в кеш
            await self._put_films_list_to_cache(sort=sort, genre=genre, page=page, per_page=per_page,
                                                query=query, films=films)

        return films

    async def _get_film_from_elastic(self, film_id: UUID4) -> Optional[Film]:
        try:
            doc = await self.elastic.get(index='movies', id=film_id)
        except NotFoundError:
            return None
        return Film(**doc['_source'])

    async def _get_films_list_from_elastic(
            self, *, sort: str | None, genre: str | None,
            page: int | None = 1, per_page: int | None = 1, film: str | None = None
    ) -> Optional[List[Film]]:
        # Проверка аргументов.
        if page <= 0:
            page = 1
        if per_page <= 0:
            per_page = 1
        try:
            must = []
            if genre:
                must.append({
                    "nested": {"path": "genre", "query": {"bool": {"must": [{"match": {"genre.name": genre}}]}}}
                })
            if film:
                must.append({"match": {"title": film}})
            doc = await self.elastic.search(
                index='movies',
                body={"query": {"bool": {"must": must}}},
                from_=(page - 1) * per_page,
                size=per_page,
                sort=(sort[1:]+":desc" if sort[0] == '-' else sort) if sort else None
            )
        except NotFoundError:
            return None
        return list(map(lambda flm: Film(**flm['_source']), doc['hits']['hits']))

    async def _film_from_cache(self, film_id: UUID4) -> Optional[Film]:
        # Пытаемся получить данные о фильме из кеша, используя команду get https://redis.io/commands/get/
        data = await self.redis.get(str(film_id))
        if not data:
            return None

        film = Film.model_validate_json(data)
        return film

    async def _films_list_from_cache(self, **kwargs) -> Optional[List[Film]]:
        # Пытаемся получить данные о фильме из кеша, используя команду get https://redis.io/commands/get/
        data = await self.redis.get(orjson.dumps(kwargs, option=orjson.OPT_SORT_KEYS))
        if not data:
            return None

        return [Film.model_validate_json(film) for film in orjson.loads(data)]

    async def _put_film_to_cache(self, film: Film):
        # Сохраняем данные о фильме в кэше, указывая время жизни.
        await self.redis.set(str(film.uuid), film.model_dump_json(), FILM_CACHE_EXPIRE_IN_SECONDS)

    async def _put_films_list_to_cache(self, films: List[Film], **kwargs):
        await self.redis.set(
            orjson.dumps(kwargs, option=orjson.OPT_SORT_KEYS),
            orjson.dumps([ob.model_dump_json() for ob in films]),
            FILM_CACHE_EXPIRE_IN_SECONDS
        )
        return None


# Get_film_service — это провайдер FilmService.
# Используем lru_cache-декоратор, чтобы создать объект сервиса в едином экземпляре (синглтон)
@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(redis, elastic)
