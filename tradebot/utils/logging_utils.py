import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import date
import os


def set_logger(name: str) -> logging.Logger:
    """
    returns logging object with the given name

    name:
        logger name
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(module)s.%(funcName)s - %(levelname)s - %(message)s"
    )

    today = date.today().strftime("%Y-%m-%d")
    base_dir = f"logs/{today}"
    os.makedirs(base_dir, exist_ok=True)

    time_handler = TimedRotatingFileHandler(
        f"{base_dir}/{name}.log", when="midnight", interval=1, backupCount=0
    )
    time_handler.setLevel(logging.DEBUG)
    time_handler.setFormatter(formatter)

    main_handler = TimedRotatingFileHandler(
        f"{base_dir}/main.log", when="midnight", interval=1, backupCount=0
    )
    main_handler.setLevel(logging.INFO)
    main_handler.setFormatter(formatter)

    error_handler = TimedRotatingFileHandler(
        f"{base_dir}/error.log", when="midnight", interval=1, backupCount=0
    )
    error_handler.setLevel(logging.WARNING)
    error_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(formatter)

    logger.addHandler(time_handler)
    logger.addHandler(main_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)

    return logger
