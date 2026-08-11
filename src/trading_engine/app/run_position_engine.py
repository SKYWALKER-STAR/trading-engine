from __future__ import annotations

from trading_engine.app.position_engine_kafka import build_position_engine_consumer
from trading_engine.common.logger import configure_logging, get_logger
from trading_engine.config.settings import PositionEngineSettings
from trading_engine.infra.redis_position_repository import RedisPositionRepository


LOGGER = get_logger(__name__)


def run() -> None:
    configure_logging()
    settings = PositionEngineSettings.from_env()
    repository = RedisPositionRepository.from_env()
    consumer = build_position_engine_consumer(repository=repository, settings=settings)

    LOGGER.info(
        "Position engine runner started",
        extra={
            "consumer_group": settings.consumer_group,
            "signal_topic": settings.signal_topic,
            "order_update_topic": settings.order_update_topic,
            "position_state_topic": settings.position_state_topic,
            "trade_action_topic": settings.trade_action_topic,
        },
    )
    consumer.consume_forever((settings.signal_topic, settings.order_update_topic))


if __name__ == "__main__":
    run()