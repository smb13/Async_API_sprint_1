from logging import config as logging_config

from core.logger import LOGGING
from pydantic import Field
from pydantic_settings import SettingsConfigDict, BaseSettings

# Применяем настройки логирования
logging_config.dictConfig(LOGGING)


# Название проекта. Используется в Swagger-документации
class ProjectSettings(BaseSettings):
    name: str = Field('movies')

    model_config = SettingsConfigDict(env_prefix='project_', env_file='.env')


# Класс настройки Redis
class RedisSettings(BaseSettings):
    host: str = Field('127.0.0.1')
    port: int = Field(6379)

    model_config = SettingsConfigDict(env_prefix='redis_', env_file='.env')


# Класс настройки Elasticsearch
class ElasticSettings(BaseSettings):
    host: str = Field('127.0.0.1')
    port: int = Field(9200)

    model_config = SettingsConfigDict(env_prefix='elastic_', env_file='.env')


# Класс настройки Elasticsearch
class GunicornSettings(BaseSettings):
    host: str = Field('0.0.0.0')
    port: int = Field(8000)
    workers: int = Field(2)
    loglevel: str = Field('debug')
    model_config = SettingsConfigDict(env_prefix='gunicorn_', env_file='.env')


redis_settings = RedisSettings()
elastic_settings = ElasticSettings()
project_settings = ProjectSettings()
gunicorn_settings = GunicornSettings()
