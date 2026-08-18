from __future__ import annotations

import argparse

from trading_engine.app.binance_user_data_stream import BinanceUserDataStreamProcessor
from trading_engine.common.logger import configure_logging, get_logger
from trading_engine.config.settings import BinanceUserDataStreamSettings
from trading_engine.infra.binance_futures_user_data_stream import BinanceFuturesUserDataStream
from trading_engine.infra.kafka_event_bus import KafkaEventPublisher


LOGGER = get_logger(__name__)


def run(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the Binance Futures user data stream adapter")
    parser.parse_args(argv)

    configure_logging()
    settings = BinanceUserDataStreamSettings.from_env()
    if not settings.api_key:
        raise ValueError("BINANCE_API_KEY is required for the Binance user data stream")

    publisher = KafkaEventPublisher.from_env()
    processor = BinanceUserDataStreamProcessor(publisher, settings)
    stream = BinanceFuturesUserDataStream(
        rest_api_url=settings.rest_api_url,
        websocket_stream_url=settings.websocket_stream_url,
        api_key=settings.api_key,
        keepalive_seconds=settings.listen_key_keepalive_seconds,
        reconnect_initial_seconds=settings.reconnect_initial_seconds,
        reconnect_max_seconds=settings.reconnect_max_seconds,
        request_timeout_seconds=settings.request_timeout_seconds,
    )
    LOGGER.info(
        "Binance Futures user data stream adapter started",
        extra={"order_update_topic": settings.order_update_topic},
    )
    stream.run_forever(processor.handle_message)


if __name__ == "__main__":
    run()
