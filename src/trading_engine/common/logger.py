from __future__ import annotations

import logging
from os import getenv

_LOGGING_CONFIGURED = False


def configure_logging(default_level: str = "INFO") -> None:
    """Configure global logging once for the whole process."""

    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    raw_level = getenv("LOG_LEVEL", default_level).upper()
    level = getattr(logging, raw_level, logging.INFO)

    log_file = getenv("LOG_FILE", "./trading_engine.log")

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        filename=log_file,
    )
    _LOGGING_CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
