from collections.abc import Generator
from datetime import datetime
from functools import wraps
from typing import Annotated, Literal

import psycopg
from annotated_types import Gt
from elasticsearch import Elasticsearch
from psycopg import ServerCursor
from psycopg.rows import dict_row
from typing_extensions import Union, Type

from logger import logger
from models import Movie, Genre, Person
from state import State


def coroutine(func):
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn

    return inner


def get_class_by_index(index_name: str) -> Union[Type[Movie], Type[Genre]]:
    match index_name:
        case 'movies':
            return Movie
        case 'genres':
            return Genre
        case 'persons':
            return Person
        case _:
            raise RuntimeError('Unprocessable index_name.')


@coroutine
def fetch_changes(
        pg: psycopg.Connection,
        index_name: str,
        table_name: str,
        next_step: Generator,
        *,
        default_is_now: bool = False,
        bulk_size: Annotated[int, Gt(0)] = 100
) -> Generator[None, State, None]:
    """
    Получение идентификаторов всех записей, изменившихся после последней проверки.
    :param pg: Соединение с бд
    :param index_name: имя индекса
    :param table_name: Имя таблицы
    :param next_step: Генератор, используемый на следующем шагу обработки
    :param default_is_now: Флаг, показывающий, что если нет сохраненного состояния,
        что нужно использовать текущее время, а не минимально возможное.
    :param bulk_size: Максимальное число одновременно обрабатываемых записей
    :return: Генератор, принимающий на вход объект состояния для запуска выборки
    """
    while cur_state := (yield):
        with ServerCursor(pg, 'fetch_changes', row_factory=dict_row) as cur:

            last_updated = cur_state.get_state(index_name + '_' + table_name + '_last_updated') \
                           or (default_is_now and datetime.now()) or datetime.min

            fuzzy_mode = cur_state.get_state(index_name + '_' + table_name + '_fuzzy')

            logger.info(f"""Fetching changed from {table_name} table after {last_updated}""")

            cur.execute(
                f"""
                SELECT id, updated_at
                FROM {table_name}
                WHERE updated_at {">=" if fuzzy_mode else ">"} %s
                ORDER BY updated_at""",
                (last_updated,)
            )
            while results := cur.fetchmany(size=bulk_size):
                # Обработка полученных данных следующим генератором.
                next_step.send([uuid['id'] for uuid in results])

                # В данной точке, все данные из полученной пачки обработаны, можно менять состояние.
                cur_state.set_state(index_name + '_' + table_name + '_last_updated', str(results[-1]['updated_at']))

                # Но при этом, возможно, что есть еще записи с таким же значением поля updated_at,
                # которые еще не обработаны, поэтому состояние помечается как нечеткое.
                if not fuzzy_mode:
                    fuzzy_mode = True
                    cur_state.set_state(index_name + '_' + table_name + '_fuzzy', fuzzy_mode)
            # Тут все записи уже точно обработаны, поэтому состояние помечается как четкое.
            cur_state.set_state(index_name + '_' + table_name + '_fuzzy', False)


@coroutine
def fetch_film_works_ids(
        pg: psycopg.Connection,
        table_name: Literal['genre', 'person'],
        next_step: Generator,
        *,
        bulk_size: Annotated[int, Gt(0)] = 100
) -> Generator[datetime, State, None]:
    """
    Получение идентификаторов всех кинопроизведений, которые нужно обновить, в связи с изменениями в связанных таблицах.
    :param pg: Соединение с бд
    :param table_name: Имя связанной таблицы
    :param next_step: Генератор, используемый на следующем шагу обработки
    :param bulk_size: Максимальное число одновременно обрабатываемых записей
    :return: Генератор, принимающий на вход список идентификаторов записей в связанной таблице
    """
    while ids := (yield):
        with ServerCursor(pg, 'fetch_film_works_ids', row_factory=dict_row) as cur:
            logger.info("Fetching film_works ids by genres ids")

            cur.execute(
                f"""
                SELECT fw.id as id
                FROM film_work fw
                LEFT JOIN {table_name + '_film_work'} gfw ON gfw.film_work_id = fw.id
                LEFT JOIN {table_name} g ON g.id = gfw.{table_name}_id
                WHERE g.id IN (""" + ",".join(['%s' for _ in ids]) + """)
                """,
                ids
            )
            while results := cur.fetchmany(size=bulk_size):
                next_step.send([fw['id'] for fw in results])


@coroutine
def fetch_film_works(
        pg: psycopg.Connection,
        next_step: Generator,
        *,
        bulk_size: Annotated[int, Gt(0)] = 100
) -> Generator[None, dict, None]:
    """
    Получение всех данных о кинопроизведениях из всех связанных таблиц
    :param pg: Соединение с бд
    :param next_step: Генератор, используемый на следующем шагу обработки
    :param bulk_size: Максимальное число одновременно обрабатываемых записей
    :return: Генератор, принимающий на вход список идентификаторов кинопроизведений
    """
    with ServerCursor(pg, 'film_works_enricher', row_factory=dict_row) as cur:
        while ids := (yield):
            logger.info("Fetching film_works data")
            sql = """ 
                SELECT
                    fw.id as uuid,
                    fw.rating, 
                    fw.title, 
                    fw.description, 
                    fw.rating, 
                    fw.type, 
                    fw.created_at, 
                    fw.updated_at, 
                    COALESCE (
                       json_agg(
                           DISTINCT jsonb_build_object(
                               'person_role', pfw.role,
                               'person_uuid', p.id,
                               'person_full_name', p.full_name
                           )
                       ) FILTER (WHERE p.id is not null),
                       '[]'
                    ) as persons,
                    COALESCE (
                       json_agg(
                           DISTINCT jsonb_build_object(
                               'genre_uuid', g.id,
                               'genre_name', g.name
                           )
                       ) FILTER (WHERE g.id is not null),
                       '[]'
                    ) as genres
                FROM film_work fw
                LEFT JOIN person_film_work pfw ON pfw.film_work_id = fw.id
                LEFT JOIN person p ON p.id = pfw.person_id
                LEFT JOIN genre_film_work gfw ON gfw.film_work_id = fw.id
                LEFT JOIN genre g ON g.id = gfw.genre_id
                WHERE fw.id IN (""" + ",".join(['%s' for _ in ids]) + """)
                GROUP BY fw.id"""
            cur.execute(sql, ids)

            while results := cur.fetchmany(size=bulk_size):
                next_step.send(results)


@coroutine
def fetch_genres(
        pg: psycopg.Connection,
        next_step: Generator,
        *,
        bulk_size: Annotated[int, Gt(0)] = 100
) -> Generator[None, dict, None]:
    """
    Получение данных жанрах
    :param pg: Соединение с бд
    :param next_step: Генератор, используемый на следующем шагу обработки
    :param bulk_size: Максимальное число одновременно обрабатываемых записей
    :return: Генератор, принимающий на вход список идентификаторов жанров
    """
    with ServerCursor(pg, 'genres_enricher', row_factory=dict_row) as cur:
        while ids := (yield):
            logger.info("Fetching genres data")
            sql = """ 
                SELECT
                    g.id as uuid,
                    g.name as name                   
                FROM genre g
                WHERE g.id IN (""" + ",".join(['%s' for _ in ids]) + """)
                """
            cur.execute(sql, ids)

            while results := cur.fetchmany(size=bulk_size):
                next_step.send(results)


@coroutine
def fetch_persons(
        pg: psycopg.Connection,
        next_step: Generator,
        *,
        bulk_size: Annotated[int, Gt(0)] = 100
) -> Generator[None, dict, None]:
    """
    Получение данных жанрах
    :param pg: Соединение с бд
    :param next_step: Генератор, используемый на следующем шагу обработки
    :param bulk_size: Максимальное число одновременно обрабатываемых записей
    :return: Генератор, принимающий на вход список идентификаторов жанров
    """
    with ServerCursor(pg, 'persons_enricher', row_factory=dict_row) as cur:
        while ids := (yield):
            logger.info("Fetching persons data")
            sql = """ 
                SELECT
                    p.id as uuid,
                    p.full_name as full_name,
                    COALESCE (
                    	json_agg(
                    	DISTINCT jsonb_build_object(
                          	'films_uuid', fw.id,
                          	'films_roles', pfw.role
                    	)
                    ) FILTER (WHERE fw.id is not null),
                      '[]'
                    ) as films
                FROM content.person p
                LEFT JOIN content.person_film_work pfw ON pfw.person_id = p.id
                LEFT JOIN content.film_work fw ON fw.id = pfw.film_work_id
                WHERE p.id IN (""" + ",".join(['%s' for _ in ids]) + """)
                GROUP BY p.id
            """
            cur.execute(sql, ids)

            while results := cur.fetchmany(size=bulk_size):
                next_step.send(results)


@coroutine
def transform_data(index_name: str,
                   next_step: Generator
                   ) -> Generator[None, list[dict], None]:
    """
    Подготовка данных для обновления индекса в elasticsearch
    :param  index_name: имя индекса
    :param  next_step: Генератор, используемый на следующем шагу обработки
    :return: Генератор, принимающий на вход список данных кинопроизведений для обновления индекса elasticsearch
    """
    data_class = get_class_by_index(index_name)
    while data_dicts := (yield):
        batch = []
        for data_dict in data_dicts:
            data_object = data_class(**data_dict)
            logger.debug(data_object.model_dump())
            batch.append(data_object)
        next_step.send(batch)


@coroutine
def save_data(
        index_name: str,
        es: Elasticsearch
) -> Generator[None, list[Union[Movie, Genre]], None]:
    """
    Обновление индекса в elasticsearch
    :param  index_name: имя индекса
    :param  es: Соединение с elasticsearch
    :return: Генератор, принимающий на вход полностью подготовленный список
        кинопроизведений для обновления индекса elasticsearch
    """
    data_class = get_class_by_index(index_name)
    while items := (yield):
        logger.info(f'Received for saving {len(items)} {data_class.__name__}s')
        for item in items:
            es.index(index=index_name, id=item.uuid, document=item.to_elastic())
