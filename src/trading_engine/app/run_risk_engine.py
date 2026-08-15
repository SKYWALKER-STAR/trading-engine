from __future__ import annotations

import argparse

from trading_engine.app.risk_engine_kafka import build_risk_engine_consumer
from trading_engine.common.logger import configure_logging, get_logger
from trading_engine.config.settings import RiskEngineSettings


LOGGER = get_logger(__name__)


def run(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the risk engine")
    parser.parse_args(argv)

    configure_logging()
    settings = RiskEngineSettings.from_env()
    consumer = build_risk_engine_consumer(settings=settings)

    LOGGER.info(
        "Risk engine runner started",
        extra={
            "consumer_group": settings.consumer_group,
            "signal_topic": settings.signal_topic,
            "position_state_topic": settings.position_state_topic,
            "risk_decision_topic": settings.risk_decision_topic,
            "require_position_snapshot": settings.require_position_snapshot,
            "default_open_quantity": settings.default_open_quantity,
        },
    )
    consumer.consume_forever((settings.signal_topic, settings.position_state_topic))


if __name__ == "__main__":
    run()
