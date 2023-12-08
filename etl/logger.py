import sys
import logging
from logging.handlers import RotatingFileHandler
from settings import etl_settings

logger = logging.getLogger(etl_settings.logger_name)
logger.setLevel(logging.INFO)

bo_logger = logging.getLogger('backoff')

formatter = logging.Formatter(etl_settings.logger_formatter)

fh = RotatingFileHandler(
    etl_settings.logger_file,
    maxBytes=etl_settings.logger_file_max_bytes,
    backupCount=etl_settings.logger_file_backup_count)
fh.setFormatter(formatter)
logger.addHandler(fh)
bo_logger.addHandler(fh)

sys_err = logging.StreamHandler(sys.stderr)
sys_err.setFormatter(formatter)
logger.addHandler(sys_err)
bo_logger.addHandler(sys_err)
