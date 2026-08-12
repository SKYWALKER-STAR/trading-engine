from __future__ import annotations

import argparse
import time

from trading_engine.common.logger import configure_logging, get_logger
from trading_engine.config.settings import PositionViewProjectorSettings
from trading_engine.infra.redis_position_view_projector import RedisPositionViewProjector


LOGGER = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Redis raw->view position projector")
    parser.add_argument("--once", action="store_true", help="Run a single projection and exit")
    parser.add_argument("--stream", action="store_true", help="Run continuously")
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=None,
        help="Projection interval in stream mode; defaults to POSITION_VIEW_POLL_INTERVAL_SECONDS",
    )
    return parser


def run(argv: list[str] | None = None) -> None:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args(argv)

    settings = PositionViewProjectorSettings.from_env()
    interval = settings.poll_interval_seconds if args.interval_seconds is None else args.interval_seconds

    projector = RedisPositionViewProjector(
        redis_url=settings.redis_url,
        key_prefix=settings.key_prefix,
        enable_detail_keys=settings.enable_detail_keys,
    )

    LOGGER.info(
        "Position view projector started",
        extra={
            "once": args.once,
            "stream": args.stream,
            "interval_seconds": interval,
            "key_prefix": settings.key_prefix,
            "enable_detail_keys": settings.enable_detail_keys,
        },
    )

    if args.once or not args.stream:
        projector.project_once()
        return

    while True:
        projector.project_once()
        time.sleep(interval)


if __name__ == "__main__":
    run()
