import logging
from logging.handlers import RotatingFileHandler
from bitmex_watcher.settings import settings


def setup_custom_logger(name, log_level=settings.LOG_LEVEL):
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(module)s - %(message)s', level=logging.INFO)
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    if 0 < len(settings.LOG_FILE_NAME):
        rotating_handler = RotatingFileHandler(settings.LOG_FILE_NAME, maxBytes=10000000, backupCount=5)
        formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
        rotating_handler.setFormatter(formatter)
        logger.addHandler(rotating_handler)

    return logger
