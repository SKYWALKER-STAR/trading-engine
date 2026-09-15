from __future__ import annotations

import logging
from os import getenv

_LOGGING_CONFIGURED = False

_DEFAULT_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
_FORMAT_OVERRIDES = {
    "trading_engine.infra.kafka_event_bus": "%(asctime)s %(levelname)s [%(name)s] [kafka] %(message)s",
    "trading_engine.infra.redis_position_view_projector": "%(asctime)s %(levelname)s [%(name)s] [redis-view] %(message)s",
    "trading_engine.strategy.rules": "%(asctime)s %(levelname)s [%(name)s] [strategy] %(message)s",
    "trading_engine.app.position_engine_kafka": "%(asctime)s %(levelname)s [%(name)s] [kafka] %(message)s symbol=%(symbol)s action=%(action)s reason=%(reason)s",
    "trading_engine.app.run_strategy_engine_factor": "%(asctime)s %(levelname)s [%(name)s] [strategy_factor] %(message)s symbol=%(symbol)s direction=%(direction)s reasons=%(reasons)s",
}


def configure_logging(default_level: str = "INFO") -> None:
    """Configure global logging once for the whole process.

    The default format stays compatible with the current codebase, while allowing
    a few modules to override their format without changing call sites.
    """

    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    raw_level = getenv("LOG_LEVEL", default_level).upper()
    level = getattr(logging, raw_level, logging.INFO)
    log_file = getenv("LOG_FILE", "./trading_engine.log")
    log_format = getenv("LOG_FORMAT", _DEFAULT_FORMAT)
    log_to_console = getenv("LOG_TO_CONSOLE", "true").strip().lower() not in {"0", "false", "no"}

    root = logging.getLogger()
    root.setLevel(level)

    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()

    if log_to_console:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(level)
        stream_handler.setFormatter(logging.Formatter(log_format))
        root.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(log_format))
    root.addHandler(file_handler)

    for logger_name, formatter_template in _FORMAT_OVERRIDES.items():
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        logger.propagate = False
        logger.handlers.clear()

        if log_to_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)
            console_handler.setFormatter(logging.Formatter(formatter_template))
            logger.addHandler(console_handler)

        file_handler_for_logger = logging.FileHandler(log_file, encoding="utf-8")
        file_handler_for_logger.setLevel(level)
        file_handler_for_logger.setFormatter(logging.Formatter(formatter_template))
        logger.addHandler(file_handler_for_logger)

    _LOGGING_CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
