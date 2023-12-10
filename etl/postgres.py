import backoff
from psycopg import Connection, connect
from settings import postgres_settings


@backoff.on_exception(**postgres_settings.get_backoff_settings())
def pg_connect() -> Connection:
    """
    Установка соединения с базой данных PostgreSQL с учетом отказоустойчивости (backoff)
    """
    return connect(**postgres_settings.get_connection_info())


@backoff.on_exception(**postgres_settings.get_backoff_settings())
def pg_reconnect(pg: Connection) -> Connection:
    """
    Восстановление соединения с базой данных PostgreSQL с учетом отказоустойчивости (backoff)
    """
    pg.close()
    return pg.connect(**postgres_settings.get_connection_info())
