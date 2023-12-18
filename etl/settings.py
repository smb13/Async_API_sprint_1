import logging

import backoff
import elastic_transport
import psycopg
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from psycopg.conninfo import make_conninfo


class ETLSettings(BaseSettings):
    timeout: int = Field(60)
    state_file_path: str = Field('storage.json')
    logger_name: str = Field('etl_application')
    logger_level: int | str = Field(logging.INFO)
    logger_file: str = Field('logs/etl_logs.log')
    logger_file_max_bytes: int = Field(20_000_000)
    logger_file_backup_count: int = Field(5)
    logger_formatter: str = Field('%(asctime)s %(levelname)-8s [%(filename)-16s:%(lineno)-5d] %(message)s')

    model_config = SettingsConfigDict(env_prefix='etl_', env_file='.env')


class PostgresSettings(BaseSettings):
    dbname: str = Field('movies_database')
    user: str = ...
    password: str = ...
    host: str = Field('localhost')
    port: int = Field(5432)
    pg_schema: str = Field('content', validation_alias='POSTGRES_SCHEMA')

    model_config = SettingsConfigDict(env_prefix='postgres_', env_file='.env')

    def get_dsn(self):
        return make_conninfo(**self.model_dump(exclude={'pg_schema'}))

    def get_connection_info(self):
        """
        Получение настроек соединения с базой Postgres.
        """
        return {'conninfo': self.get_dsn(), 'options': f"-c search_path={self.pg_schema},public"}

    @staticmethod
    def get_backoff_settings():
        """
        Получение настроек для backoff логики установки соединения с базой Postgres.

        В будущем можно будет добавить получения настроек из файла или из переменных окружения.
        """
        return {
            'wait_gen': backoff.expo,
            'exception': psycopg.Error,
            'logger': 'etl_application',
            'base': 2,
            'factor': 1,
            'max_value': 60
        }


class IndexSettings(BaseSettings):
    index_name: str
    index_mappings: dict


class ElasticSettings(BaseSettings):
    es_schema: str = Field('http')
    host: str = Field('localhost')
    port: int = Field(9200)
    indexes_settings: dict = Field({
        "refresh_interval": "1s",
        "analysis": {
            "filter": {
                "english_stop": {
                    "type": "stop",
                    "stopwords": "_english_"
                },
                "english_stemmer": {
                    "type": "stemmer",
                    "language": "english"
                },
                "english_possessive_stemmer": {
                    "type": "stemmer",
                    "language": "possessive_english"
                },
                "russian_stop": {
                    "type": "stop",
                    "stopwords": "_russian_"
                },
                "russian_stemmer": {
                    "type": "stemmer",
                    "language": "russian"
                }
            },
            "analyzer": {
                "ru_en": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop",
                        "english_stemmer",
                        "english_possessive_stemmer",
                        "russian_stop",
                        "russian_stemmer"
                    ]
                }
            }
        }
    })
    indexes_mappings: dict = Field(
        {"movies":
            {"dynamic": "strict",
             "properties": {
                 "uuid": {
                     "type": "keyword"
                 },
                 "imdb_rating": {
                     "type": "float"
                 },
                 "genre": {
                     "type": "nested",
                     "dynamic": "strict",
                     "properties": {
                         "uuid": {
                             "type": "keyword"
                         },
                         "name": {
                             "type": "text",
                             "analyzer": "ru_en"
                         }
                     }
                 },
                 "title": {
                     "type": "text",
                     "analyzer": "ru_en",
                     "fields": {
                         "raw": {
                             "type": "keyword"
                         }
                     }
                 },
                 "description": {
                     "type": "text",
                     "analyzer": "ru_en"
                 },
                 "directors": {
                     "type": "nested",
                     "dynamic": "strict",
                     "properties": {
                         "uuid": {
                             "type": "keyword"
                         },
                         "full_name": {
                             "type": "text",
                             "analyzer": "ru_en"
                         }
                     }
                 },
                 "actors_names": {
                     "type": "text",
                     "analyzer": "ru_en"
                 },
                 "writers_names": {
                     "type": "text",
                     "analyzer": "ru_en"
                 },
                 "actors": {
                     "type": "nested",
                     "dynamic": "strict",
                     "properties": {
                         "uuid": {
                             "type": "keyword"
                         },
                         "full_name": {
                             "type": "text",
                             "analyzer": "ru_en"
                         }
                     }
                 },
                 "writers": {
                     "type": "nested",
                     "dynamic": "strict",
                     "properties": {
                         "uuid": {
                             "type": "keyword"
                         },
                         "full_name": {
                             "type": "text",
                             "analyzer": "ru_en"
                         }
                     }
                 }
             }
             },
         "genres":
             {
                 "dynamic": "strict",
                 "properties": {
                     "uuid": {
                         "type": "keyword"
                     },
                     "name": {
                         "type": "text",
                         "analyzer": "ru_en"
                     }
                 }
             },
         "persons":
             {
                 "dynamic": "strict",
                 "properties": {
                     "uuid": {
                         "type": "keyword"
                     },
                     "full_name": {
                         "type": "text",
                         "analyzer": "ru_en"
                     },
                     "films": {
                         "type": "nested",
                         "dynamic": "strict",
                         "properties": {
                             "uuid": {
                                 "type": "keyword"
                             },
                             "roles": {
                                 "type": "text",
                                 "analyzer": "ru_en"
                             }
                         }
                     }
                 }
             }
         }

    )

    model_config = SettingsConfigDict(env_prefix='ELASTIC_', env_file='.env')

    def get_connection_info(self):
        return {
            'hosts': self.es_schema + '://' + self.host + ':' + str(self.port)
        }

    def get_index_settings(self, index_name: str):
        return {'index': index_name, 'mappings': self.indexes_mappings.get(index_name),
                'settings': self.indexes_settings}

    @staticmethod
    def get_backoff_settings():
        """
        Получение настроек для backoff логики установки соединения с базой Postgres.

        В будущем можно будет добавить получения настроек из файла или из переменных окружения.
        """
        return {
            'wait_gen': backoff.expo,
            'exception': elastic_transport.TransportError,
            'logger': 'etl_application',
            'base': 2,
            'factor': 1,
            'max_value': 60
        }


etl_settings = ETLSettings()
postgres_settings = PostgresSettings()
elastic_settings = ElasticSettings()
