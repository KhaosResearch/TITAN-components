import logging.config


def configure_logging():
    DEFAULT_LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "loggers": {"drama.titan": {"level": "DEBUG"},},
    }

    logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)


logger = logging.getLogger("drama.titan")
