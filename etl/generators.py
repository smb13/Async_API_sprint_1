from collections.abc import Generator
from datetime import datetime
from functools import wraps
from typing import Annotated, Literal

import psycopg
from annotated_types import Gt
from elasticsearch import Elasticsearch
from psycopg import ServerCursor
from psycopg.rows import dict_row

from logger import logger
from models import Movie
from settings import elastic_settings
from state import State


def coroutine(func):
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn

    return inner


@coroutine
def fetch_changes(
        pg: psycopg.Connection,
        table_name: str,
        next_step: Generator,
        *,
        default_is_now: bool = False,
        bulk_size: Annotated[int, Gt(0)] = 100
) -> Generator[None, State, None]:
    """
    Получение идентификаторов всех записей, изменившихся после последней проверки.
    :param pg: Соединение с бд
    :param table_name: Имя таблицы
    :param next_step: Генератор, используемый на следующем шагу обработки
    :param default_is_now: Флаг, показывающий, что если нет сохраненного состояния,
        что нужно использовать текущее время, а не минимально возможное.
    :param bulk_size: Максимальное число одновременно обрабатываемых записей
    :return: Генератор, принимающий на вход объект состояния для запуска выборки
    """
    while cur_state := (yield):
        with ServerCursor(pg, 'fetch_changes', row_factory=dict_row) as cur:

            last_updated = cur_state.get_state(table_name+'_last_updated') \
                           or (default_is_now and datetime.now()) or datetime.min

            fuzzy_mode = cur_state.get_state(table_name + '_fuzzy')

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
                next_step.send([fw['id'] for fw in results])

                # В данной точке, все данные из полученной пачки обработаны, можно менять состояние.
                cur_state.set_state(table_name+'_last_updated', str(results[-1]['updated_at']))

                # Но при этом, возможно, что есть еще записи с таким же значением поля updated_at,
                # которые еще не обработаны, поэтому состояние помечается как нечеткое.
                if not fuzzy_mode:
                    fuzzy_mode = True
                    cur_state.set_state(table_name + '_fuzzy', fuzzy_mode)
            # Тут все записи уже точно обработаны, поэтому состояние помечается как четкое.
            cur_state.set_state(table_name + '_fuzzy', False)


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
                LEFT JOIN {table_name+'_film_work'} gfw ON gfw.film_work_id = fw.id
                LEFT JOIN {table_name} g ON g.id = gfw.{table_name}_id
                WHERE g.id IN ("""+",".join(['%s' for _ in ids])+""")
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
    with ServerCursor(pg, 'enricher', row_factory=dict_row) as cur:
        while ids := (yield):
            logger.info("Fetching film_works data")
            sql = """ 
                SELECT
                    fw.id as id,
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
                               'person_id', p.id,
                               'person_name', p.full_name
                           )
                       ) FILTER (WHERE p.id is not null),
                       '[]'
                    ) as persons,
                    array_agg(DISTINCT g.name) as genres
                FROM film_work fw
                LEFT JOIN person_film_work pfw ON pfw.film_work_id = fw.id
                LEFT JOIN person p ON p.id = pfw.person_id
                LEFT JOIN genre_film_work gfw ON gfw.film_work_id = fw.id
                LEFT JOIN genre g ON g.id = gfw.genre_id
                WHERE fw.id IN ("""+",".join(['%s' for _ in ids])+""")
                GROUP BY fw.id"""
            cur.execute(sql, ids)

            while results := cur.fetchmany(size=bulk_size):
                next_step.send(results)


@coroutine
def transform_film_works(
        next_step: Generator
) -> Generator[None, list[dict], None]:
    """
    Подготовка данных для обновления индекса в elasticsearch
    :param next_step: Генератор, используемый на следующем шагу обработки
    :return: Генератор, принимающий на вход список данных кинопроизведений для обновления индекса elasticsearch
    """
    while movie_dicts := (yield):
        batch = []
        for movie_dict in movie_dicts:
            movie = Movie(**movie_dict)
            logger.debug(movie.model_dump())
            batch.append(movie)
        next_step.send(batch)


@coroutine
def save_movies(
        es: Elasticsearch
) -> Generator[None, list[Movie], None]:
    """
    Обновление индекса в elasticsearch
    :param es: Соединение с elasticsearch
    :return: Генератор, принимающий на вход полностью подготовленный список
        кинопроизведений для обновления индекса elasticsearch
    """
    while movies := (yield):
        logger.info(f'Received for saving {len(movies)} movies')
        for movie in movies:
            es.index(index=elastic_settings.index_name, id=movie.id, document=movie.to_elastic())
