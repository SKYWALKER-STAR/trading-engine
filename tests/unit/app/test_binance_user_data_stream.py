from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from trading_engine.app.binance_user_data_stream import (
    BinanceUserDataStreamProcessor,
    parse_order_trade_update,
)
from trading_engine.config.settings import BinanceUserDataStreamSettings
from trading_engine.contracts.messages import EngineEvent, EngineEventType


class _FakePublisher:
    def __init__(self) -> None:
        self.published: list[tuple[str, EngineEvent[Any], str]] = []

    def publish(self, topic: str, event: EngineEvent[Any], key: str) -> None:
        self.published.append((topic, event, key))


def _order_trade_update() -> dict[str, Any]:
    return {
        "e": "ORDER_TRADE_UPDATE",
        "E": 1_723_629_914_973,
        "o": {
            "s": "BTCUSDT",
            "c": "client-order-1",
            "S": "BUY",
            "o": "LIMIT",
            "f": "GTC",
            "q": "0.100",
            "x": "TRADE",
            "X": "PARTIALLY_FILLED",
            "i": 888,
            "l": "0.020",
            "z": "0.050",
            "t": 999,
            "T": 1_723_629_914_900,
            "ps": "BOTH",
            "R": False,
        },
    }


def test_parse_order_trade_update_keeps_incremental_and_cumulative_quantities() -> None:
    payload = parse_order_trade_update(_order_trade_update())

    assert payload is not None
    assert payload.symbol == "BTCUSDT"
    assert payload.order_id == "888"
    assert payload.status == "partially_filled"
    assert payload.last_filled_quantity == 0.02
    assert payload.cumulative_filled_quantity == 0.05
    assert payload.filled_quantity == 0.05
    assert payload.original_quantity == 0.1
    assert payload.trade_id == "999"
    assert payload.execution_type == "TRADE"
    assert payload.updated_at == datetime.fromtimestamp(1_723_629_914_900 / 1000, tz=UTC)


def test_parse_order_trade_update_ignores_other_user_events() -> None:
    assert parse_order_trade_update({"e": "ACCOUNT_UPDATE"}) is None


def test_processor_publishes_order_update_to_kafka() -> None:
    publisher = _FakePublisher()
    settings = BinanceUserDataStreamSettings(order_update_topic="orders.v1")
    processor = BinanceUserDataStreamProcessor(publisher, settings)

    processor.handle_message(_order_trade_update())

    assert len(publisher.published) == 1
    topic, event, key = publisher.published[0]
    assert topic == "orders.v1"
    assert key == "BTCUSDT"
    assert event.event_type is EngineEventType.ORDER_UPDATE_RECEIVED
    assert event.correlation_id == "client-order-1"
    assert event.causation_id == "999"
