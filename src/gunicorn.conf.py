from core.config import gunicorn_settings
from core.logger import LOGGING

bind = f'{gunicorn_settings.host}:{gunicorn_settings.port}'
workers = gunicorn_settings.workers
logconfig_dict = LOGGING
loglevel = gunicorn_settings.loglevel
worker_class = 'uvicorn.workers.UvicornH11Worker'
