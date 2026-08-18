from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from trading_engine.common.logger import get_logger
from trading_engine.config.settings import BinanceUserDataStreamSettings
from trading_engine.contracts.messages import (
    EngineEventType,
    OrderUpdatePayload,
    build_event,
)
from trading_engine.infra.kafka_event_bus import KafkaEventPublisher


LOGGER = get_logger(__name__)


class BinanceUserDataStreamProcessor:
    def __init__(
        self,
        publisher: KafkaEventPublisher,
        settings: BinanceUserDataStreamSettings,
        *,
        producer_name: str = "binance-user-data-stream",
    ) -> None:
        self._publisher = publisher
        self._settings = settings
        self._producer_name = producer_name

    def handle_message(self, message: dict[str, Any]) -> None:
        payload = parse_order_trade_update(message)
        if payload is None:
            event_type = message.get("e")
            if event_type == "listenKeyExpired":
                raise RuntimeError("Binance user data stream listen key expired")
            LOGGER.debug(
                "Ignoring Binance user data event",
                extra={"binance_event_type": event_type},
            )
            return

        event = build_event(
            EngineEventType.ORDER_UPDATE_RECEIVED,
            payload,
            producer=self._producer_name,
            occurred_at=payload.updated_at,
            correlation_id=payload.client_order_id or payload.order_id,
            causation_id=payload.trade_id,
        )
        self._publisher.publish(
            self._settings.order_update_topic,
            event,
            key=payload.symbol,
        )


def parse_order_trade_update(message: dict[str, Any]) -> OrderUpdatePayload | None:
    if message.get("e") != "ORDER_TRADE_UPDATE":
        return None
    order = message.get("o")
    if not isinstance(order, dict):
        raise ValueError("ORDER_TRADE_UPDATE is missing the order payload")

    event_time = _milliseconds_to_datetime(message.get("E"))
    trade_time = _milliseconds_to_datetime(order.get("T"))
    updated_at = trade_time or event_time or datetime.now(UTC)
    cumulative = _optional_float(order.get("z"))
    last_fill = _optional_float(order.get("l"))
    trade_id = _optional_identifier(order.get("t"))
    if trade_id == "0":
        trade_id = None

    return OrderUpdatePayload(
        symbol=str(order["s"]),
        order_id=_optional_identifier(order.get("i")),
        status=_normalize_order_status(order["X"]),
        updated_at=updated_at,
        # Backward-compatible alias. PositionManager still consumes this field in phase one.
        filled_quantity=cumulative,
        last_filled_quantity=last_fill,
        cumulative_filled_quantity=cumulative,
        original_quantity=_optional_float(order.get("q")),
        trade_id=trade_id,
        execution_type=None if order.get("x") is None else str(order["x"]),
        side=None if order.get("S") is None else str(order["S"]),
        position_side=None if order.get("ps") is None else str(order["ps"]),
        reduce_only=None if order.get("R") is None else bool(order["R"]),
        client_order_id=None if order.get("c") is None else str(order["c"]),
        event_time=event_time,
        trade_time=trade_time,
        metadata={
            "exchange": "binance",
            "exchange_order_status": str(order["X"]),
            "order_type": str(order.get("o", "")),
            "time_in_force": str(order.get("f", "")),
        },
    )


def _milliseconds_to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(float(value) / 1000.0, tz=UTC)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _optional_identifier(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _normalize_order_status(value: Any) -> str:
    status = str(value).strip().upper()
    if status == "EXPIRED":
        return "canceled"
    if status == "EXPIRED_IN_MATCH":
        return "rejected"
    return status.lower()
