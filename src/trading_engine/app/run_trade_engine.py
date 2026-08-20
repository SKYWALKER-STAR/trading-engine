from __future__ import annotations

import argparse

from trading_engine.app.trade_engine_kafka import build_trade_engine_consumer
from trading_engine.common.logger import configure_logging, get_logger
from trading_engine.config.settings import TradeEngineSettings
from trading_engine.infra.binance_futures_ws_gateway import BinanceFuturesWsGateway
from trading_engine.trade.gateway import TradeExecutionGateway


LOGGER = get_logger(__name__)


def run(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the trade engine")
    parser.parse_args(argv)

    configure_logging()
    settings = TradeEngineSettings.from_env()
    gateway = _build_gateway(settings)
    consumer = build_trade_engine_consumer(settings=settings, gateway=gateway)

    LOGGER.info(
        "Trade engine runner started",
        extra={
            "consumer_group": settings.consumer_group,
            "trade_action_topic": settings.trade_action_topic,
            "order_update_topic": settings.order_update_topic,
            "exchange": settings.exchange,
            "order_account_id": settings.order_account_id,
            "request_timeout_seconds": settings.request_timeout_seconds,
            "binance_ws_api_url": settings.binance_ws_api_url,
        },
    )
    consumer.consume_forever((settings.trade_action_topic,))


def _build_gateway(settings: TradeEngineSettings) -> TradeExecutionGateway:
    if settings.exchange != "binance":
        raise ValueError(f"Unsupported trade exchange: {settings.exchange}")

    if not settings.binance_api_key or not settings.binance_api_secret:
        raise ValueError("BINANCE_API_KEY and BINANCE_API_SECRET are required for trade engine")

    return BinanceFuturesWsGateway(
        endpoint=settings.binance_ws_api_url,
        api_key=settings.binance_api_key,
        api_secret=settings.binance_api_secret,
        order_type=settings.binance_order_type,
        recv_window=settings.binance_recv_window,
        timeout_seconds=settings.request_timeout_seconds,
    )


if __name__ == "__main__":
    run()
