from time import sleep

import psycopg
from elasticsearch import Elasticsearch

from generators import fetch_changes, save_movies, transform_film_works, fetch_film_works, fetch_film_works_ids
from logger import logger
from postgres import pg_connect, pg_reconnect
from elastic import elastic_init, elastic_connect, elastic_reconnect
from settings import elastic_settings, etl_settings
from state import JsonFileStorage, State


def process(pg: psycopg.Connection, es: Elasticsearch):
    updater = save_movies(es)
    transformer = transform_film_works(updater)
    fetcher = fetch_film_works(pg, transformer)

    film_work_syncer = fetch_changes(pg, 'film_work', fetcher)

    # Первичная синхронизация идет по таблице film_work, поэтому для других таблиц указываем,
    # что нужно рассматривать только изменения, произведенные после текущего момента.
    film_works_by_genre = fetch_film_works_ids(pg, 'genre', fetcher)
    genres_syncer = fetch_changes(pg, 'genre', film_works_by_genre, default_is_now=True)

    film_works_by_person = fetch_film_works_ids(pg, 'person', fetcher)
    person_syncer = fetch_changes(pg, 'person', film_works_by_person, default_is_now=True)

    # Запускаем цикл проверки обновлений.
    logger.info('Starting ETL process for updates ...')
    while True:
        # Проверка обновлений в таблице кинопроизведений.
        film_work_syncer.send(state)
        # Проверка обновлений в таблице жанров.
        genres_syncer.send(state)
        # Проверка обновлений в таблице персон.
        person_syncer.send(state)
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
