import orjson
from functools import lru_cache
from typing import Optional, List

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from pydantic import UUID4
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redisdb import get_redis
from models.person import Person

PERSON_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class PersonService:
    """
    PersonService содержит бизнес-логику по работе с персонами.
    """

    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    # Get_by_id возвращает объект персоны. Он опционален, так как персона может отсутствовать в базе
    async def get_by_id(self, person_id: UUID4) -> Optional[Person]:
        # Пытаемся получить данные из кеша, потому что оно работает быстрее
        person = await self._person_from_cache(person_id)
        if not person:
            # Если персоны нет в кеше, то ищем его в Elasticsearch
            person = await self._get_person_from_elastic(person_id)
            if not person:
                # Если он отсутствует в Elasticsearch, значит, персона вообще нет в базе
                return None
            # Сохраняем персону в кеш
            await self._put_person_to_cache(person)

        return person

    async def get_persons(
            self, *, page: int | None = 1,
            per_page: int | None = 1, query: str | None = None
    ) -> List[Person]:
        # Пытаемся получить данные из кеша, потому что оно работает быстрее.
        persons = await self._persons_list_from_cache(page=page, per_page=per_page, query=query)
        if not persons:
            # Если персон нет в кеше, то ищем их в Elasticsearch
            persons = await self._get_persons_list_from_elastic(
                page=page, per_page=per_page, person=query
            )
            if not persons:
                # Если он отсутствует в Elasticsearch, значит, персон вообще нет в базе
                return []
            # Сохраняем персоны в кеш
            await self._put_persons_list_to_cache(page=page, per_page=per_page,
                                                  query=query, persons=persons)

        return persons

    async def _get_person_from_elastic(self, person_id: UUID4) -> Optional[Person]:
        try:
            doc = await self.elastic.get(index='persons', id=person_id)
        except NotFoundError:
            return None
        return Person(**doc['_source'])

    async def _get_persons_list_from_elastic(
            self, *, page: int | None = 1, per_page: int | None = 1, person: str | None = None
    ) -> Optional[List[Person]]:
        # Проверка аргументов.
        if page <= 0:
            page = 1
        if per_page <= 0:
            per_page = 1
        try:
            must = []
            if person:
                must.append({"match": {"full_name": person}})
            doc = await self.elastic.search(
                index='persons',
                body={"query": {"bool": {"must": must}}},
                from_=(page - 1) * per_page,
                size=per_page,
            )
        except NotFoundError:
            return None
        return list(map(lambda flm: Person(**flm['_source']), doc['hits']['hits']))

    async def _person_from_cache(self, person_id: UUID4) -> Optional[Person]:
        # Пытаемся получить данные о персоне из кеша, используя команду get https://redis.io/commands/get/
        data = await self.redis.get(str(person_id))
        if not data:
            return None

        person = Person.model_validate_json(data)
        return person

    async def _persons_list_from_cache(self, **kwargs) -> Optional[List[Person]]:
        # Пытаемся получить данные о персоне из кеша, используя команду get https://redis.io/commands/get/
        data = await self.redis.get(orjson.dumps(kwargs, option=orjson.OPT_SORT_KEYS))
        if not data:
            return None

        return [Person.model_validate_json(person) for person in orjson.loads(data)]

    async def _put_person_to_cache(self, person: Person):
        # Сохраняем данные о персоне в кэше, указывая время жизни.
        await self.redis.set(str(person.uuid), person.model_dump_json(), PERSON_CACHE_EXPIRE_IN_SECONDS)

    async def _put_persons_list_to_cache(self, persons: List[Person], **kwargs):
        await self.redis.set(
            orjson.dumps(kwargs, option=orjson.OPT_SORT_KEYS),
            orjson.dumps([ob.model_dump_json() for ob in persons]),
            PERSON_CACHE_EXPIRE_IN_SECONDS
        )
        return None


# Get_person_service — это провайдер PersonService.
# Используем lru_cache-декоратор, чтобы создать объект сервиса в едином экземпляре (синглтон)
@lru_cache()
def get_person_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(redis, elastic)
