import backoff
from elasticsearch import Elasticsearch
from logger import logger
from settings import elastic_settings


@backoff.on_exception(**elastic_settings.get_backoff_settings())
def elastic_init(es: Elasticsearch):
    logger.info('Initializing ...')
    # Проверяем создан ли индекс в ElasticSearch, если нет, то создаем
    index_names = ('movies', 'genres', 'persons')
    for index_name in index_names:
        if not es.indices.exists(index=index_name):
            logger.info(f'Creating index {index_name} ...')
            es.indices.create(**elastic_settings.get_index_settings(index_name))


@backoff.on_exception(**elastic_settings.get_backoff_settings())
def elastic_connect() -> Elasticsearch:
    """
    Установка соединения с базой данных PostgreSQL с учетом отказоустойчивости (backoff)
    """
    return Elasticsearch(**elastic_settings.get_connection_info())


@backoff.on_exception(**elastic_settings.get_backoff_settings())
def elastic_reconnect(es: Elasticsearch) -> None:
    """
    Восстановление соединения с базой данных PostgreSQL с учетом отказоустойчивости (backoff)
    """
    es.info()
