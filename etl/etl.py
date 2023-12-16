from time import sleep

import psycopg
from elasticsearch import Elasticsearch

from generators import (fetch_changes, save_data, transform_data, fetch_film_works, fetch_film_works_ids,
                        fetch_genres, fetch_persons)
from logger import logger
from postgres import pg_connect, pg_reconnect
from elastic import elastic_init, elastic_connect, elastic_reconnect
from settings import elastic_settings, etl_settings
from state import JsonFileStorage, State


def process(pg: psycopg.Connection, es: Elasticsearch):
    # Паплайн обновления индекса movies
    movies_updater = save_data(index_name='movies', es=es)
    movies_transformer = transform_data(index_name='movies', next_step=movies_updater)
    movies_fetcher = fetch_film_works(pg=pg, next_step=movies_transformer)

    movies_film_work_syncer = fetch_changes(pg=pg, index_name='movies',
                                            table_name='film_work', next_step=movies_fetcher)

    # Первичная синхронизация идет по таблице film_work, поэтому для других таблиц указываем,
    # что нужно рассматривать только изменения, произведенные после текущего момента.
    movies_film_works_by_genre = fetch_film_works_ids(pg=pg, table_name='genre', next_step=movies_fetcher)
    movies_genre_syncer = fetch_changes(pg=pg, index_name='movies', table_name='genre',
                                        next_step=movies_film_works_by_genre, default_is_now=False)

    movies_film_works_by_person = fetch_film_works_ids(pg=pg, table_name='person', next_step=movies_fetcher)
    movies_person_syncer = fetch_changes(pg=pg, index_name='movies',
                                         table_name='person', next_step=movies_film_works_by_person,
                                         default_is_now=False)

    # Паплайн обновления индекса genres
    genres_updater = save_data(index_name='genres', es=es)
    genres_transformer = transform_data(index_name='genres', next_step=genres_updater)
    genres_fetcher = fetch_genres(pg=pg, next_step=genres_transformer)
    genres_genre_syncer = fetch_changes(pg=pg, index_name='genres',
                                        table_name='genre', next_step=genres_fetcher, default_is_now=False)

    # Паплайн обновления индекса persons
    persons_updater = save_data(index_name='persons', es=es)
    persons_transformer = transform_data(index_name='persons', next_step=persons_updater)
    persons_fetcher = fetch_persons(pg=pg, next_step=persons_transformer)
    persons_person_syncer = fetch_changes(pg=pg, index_name='persons',
                                          table_name='person', next_step=persons_fetcher, default_is_now=False)

    # Запускаем цикл проверки обновлений.
    logger.info('Starting ETL process for updates ...')
    while True:
        # Паплайн обновления индекса movies
        # Проверка обновлений в таблице кинопроизведений.
        movies_film_work_syncer.send(state)
        # Проверка обновлений в таблице жанров.
        movies_genre_syncer.send(state)
        # Проверка обновлений в таблице персон.
        movies_person_syncer.send(state)

        # Паплайн обновления индекса genres
        genres_genre_syncer.send(state)

        # Паплайн обновления индекса persons
        persons_person_syncer.send(state)

        # Приостановка.
        sleep(etl_settings.timeout)


if __name__ == '__main__':
    state = State(JsonFileStorage(file_path=etl_settings.state_file_path, logger=logger))

    with (pg_connect() as pg_conn, elastic_connect() as es_conn):
        elastic_init(es_conn)

        while True:
            try:
                process(pg_conn, es_conn)
            except (psycopg.OperationalError, psycopg.IntegrityError, psycopg.InternalError):
                pg_conn = pg_reconnect(pg_conn)
            except elastic_settings.get_backoff_settings().get('exception'):
                elastic_reconnect(es_conn)
            # В данном случае используется psycopg==3.1.9, так как требования использования 2ой версии не было. Для
            # 3ей версии в документации четко сказано: "In Psycopg 3, using with connection will close the connection
            # at the end of the with block, making handling the connection resources more familiar.", поэтому
            # обработка дополнительных исключений не требуется (также проверено по коду, что вызов закрытия соединения
            # вызывается из __exit__() при условии, что соединение не входит в пул соединений).
