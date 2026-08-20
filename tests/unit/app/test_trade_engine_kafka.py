from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from platform import processor
from typing import Any

import pytest

from trading_engine.app.trade_engine_kafka import TradeEngineMessageProcessor
from trading_engine.config.settings import TradeEngineSettings
from trading_engine.contracts.messages import (
    EngineEvent,
    EngineEventType,
    TradeActionPayload,
    build_event,
)
from trading_engine.trade.models import (
    TrackedOrder,
    TradeExecutionResult,
    TradeExecutionStatus,
    TradeOrderRequest,
)
from trading_engine.app.trade_engine_kafka import _to_trade_order_request


class _FakePublisher:
    def __init__(self) -> None:
        self.published: list[tuple[str, EngineEvent[Any], str]] = []

    def publish(self, topic: str, event: EngineEvent[Any], key: str) -> None:
        self.published.append((topic, event, key))


class _FakeGateway:
    def __init__(self, result: TradeExecutionResult) -> None:
        self._result = result
        self.requests: list[TradeOrderRequest] = []

    def submit_order(self, request: TradeOrderRequest) -> TradeExecutionResult:
        self.requests.append(request)
        return self._result


class _FailingGateway:
    def submit_order(self, request: TradeOrderRequest) -> TradeExecutionResult:
        raise TimeoutError(f"submission timed out for {request.client_order_id}")


class _FakeOrderRepository:
    def __init__(self) -> None:
        self.orders: dict[str, TrackedOrder] = {}
        self.saved: list[TrackedOrder] = []

    def save(self, order: TrackedOrder) -> None:
        self.orders[order.client_order_id] = order
        self.saved.append(order)

    def bind_order_id(
        self,
        *,
        exchange: str,
        account_id: str,
        client_order_id: str,
        symbol: str,
        order_id: str,
    ) -> TrackedOrder:
        order = replace(self.orders[client_order_id], order_id=order_id)
        self.orders[client_order_id] = order
        return order


def _trade_action_event(quantity: float | None = 0.2) -> EngineEvent[TradeActionPayload]:
    now = datetime(2026, 8, 16, 9, 0, tzinfo=UTC)
    payload = TradeActionPayload(
        symbol="BTCUSDT",
        action="open_long",
        side="BUY",
        requested_at=now,
        quantity=quantity,
        state="open_long",
        metadata={"source": "position-engine"},
    )
    return build_event(
        EngineEventType.TRADE_ACTION_REQUESTED,
        payload,
        producer="position-engine",
        occurred_at=now,
        correlation_id="corr-1",
    )


def test_trade_engine_publishes_new_and_filled_updates() -> None:
    now = datetime(2026, 8, 16, 9, 0, 1, tzinfo=UTC)
    publisher = _FakePublisher()
    settings = TradeEngineSettings()
    gateway = _FakeGateway(
        TradeExecutionResult(
            symbol="BTCUSDT",
            status=TradeExecutionStatus.FILLED,
            updated_at=now,
            order_id="ord-1",
            filled_quantity=0.2,
            metadata={"exchange": "binance"},
        )
    )
    processor = TradeEngineMessageProcessor(publisher=publisher, settings=settings, gateway=gateway)

    processor.handle_trade_action(_trade_action_event())

    assert len(gateway.requests) == 1
    assert gateway.requests[0].quantity == 0.2
    assert len(publisher.published) == 2
    assert publisher.published[0][1].payload.status == "new"
    assert publisher.published[1][1].payload.status == "filled"


def test_trade_engine_generates_and_preserves_deterministic_client_order_id() -> None:
    now = datetime(2026, 8, 16, 9, 0, 1, tzinfo=UTC)
    publisher = _FakePublisher()
    order_repository = _FakeOrderRepository()
    gateway = _FakeGateway(
        TradeExecutionResult(
            symbol="BTCUSDT",
            status=TradeExecutionStatus.NEW,
            updated_at=now,
            order_id="ord-client-1",
        )
    )
    processor = TradeEngineMessageProcessor(
        publisher=publisher,
        settings=TradeEngineSettings(),
        gateway=gateway,
        order_repository=order_repository,
    )
    event = _trade_action_event()

    processor.handle_trade_action(event)

    request = gateway.requests[0]
    expected = _to_trade_order_request(event.payload, event, TradeEngineSettings())
    assert request.client_order_id is not None
    assert request.client_order_id == expected.client_order_id
    assert request.client_order_id.startswith("te-")
    assert len(request.client_order_id) == 35
    assert request.metadata["newClientOrderId"] == request.client_order_id
    assert publisher.published[0][1].payload.client_order_id == request.client_order_id
    assert [order.status.value for order in order_repository.saved] == [
        "pending_submit",
        "new",
    ]
    saved_order = order_repository.orders[request.client_order_id]
    assert saved_order.order_id == "ord-client-1"
    assert saved_order.client_order_id == request.client_order_id


def test_trade_engine_persists_unknown_when_submission_raises() -> None:
    order_repository = _FakeOrderRepository()
    processor = TradeEngineMessageProcessor(
        publisher=_FakePublisher(),
        settings=TradeEngineSettings(),
        gateway=_FailingGateway(),
        order_repository=order_repository,
    )

    with pytest.raises(TimeoutError):
        processor.handle_trade_action(_trade_action_event())

    assert [order.status.value for order in order_repository.saved] == [
        "pending_submit",
        "unknown",
    ]
    assert order_repository.saved[-1].metadata["submission_error"] == "TimeoutError"


def test_trade_engine_publishes_rejected_update_only() -> None:
    now = datetime(2026, 8, 16, 9, 0, 2, tzinfo=UTC)
    publisher = _FakePublisher()
    settings = TradeEngineSettings()
    gateway = _FakeGateway(
        TradeExecutionResult(
            symbol="BTCUSDT",
            status=TradeExecutionStatus.REJECTED,
            updated_at=now,
            metadata={"exchange": "binance", "error": "insufficient_margin"},
        )
    )
    processor = TradeEngineMessageProcessor(publisher=publisher, settings=settings, gateway=gateway)

    processor.handle_trade_action(_trade_action_event())

    assert len(publisher.published) == 1
    topic, event, key = publisher.published[0]
    assert topic == settings.order_update_topic
    assert key == "BTCUSDT"
    assert event.payload.status == "rejected"


def test_trade_engine_uses_approved_quantity_metadata_when_payload_quantity_missing() -> None:
    now = datetime(2026, 8, 16, 9, 0, 3, tzinfo=UTC)
    publisher = _FakePublisher()
    settings = TradeEngineSettings()
    gateway = _FakeGateway(
        TradeExecutionResult(
            symbol="BTCUSDT",
            status=TradeExecutionStatus.NEW,
            updated_at=now,
            order_id="ord-2",
        )
    )
    processor = TradeEngineMessageProcessor(publisher=publisher, settings=settings, gateway=gateway)
    event = _trade_action_event(quantity=None)
    event.payload.metadata["approved_quantity"] = 0.15

    processor.handle_trade_action(event)

    assert len(gateway.requests) == 1
    assert gateway.requests[0].quantity == 0.15
    assert len(publisher.published) == 1
    assert publisher.published[0][1].payload.status == "new"


def test_trade_engine_sets_reduce_only_and_position_side_metadata() -> None:
    now = datetime(2026, 8, 16, 9, 0, 4, tzinfo=UTC)
    publisher = _FakePublisher()
    settings = TradeEngineSettings(binance_position_side="SHORT")
    gateway = _FakeGateway(
        TradeExecutionResult(
            symbol="BTCUSDT",
            status=TradeExecutionStatus.NEW,
            updated_at=now,
            order_id="ord-3",
        )
    )
    processor = TradeEngineMessageProcessor(publisher=publisher, settings=settings, gateway=gateway)
    event = _trade_action_event(quantity=0.12)
    event.payload.metadata["risk_action"] = "reduce_only"

    processor.handle_trade_action(event)

    assert len(gateway.requests) == 1
    assert gateway.requests[0].metadata["reduceOnly"] == "true"
    assert gateway.requests[0].metadata["positionSide"] == "SHORT"


def test_trade_engine_rejects_limit_without_price_and_time_in_force() -> None:
    now = datetime(2026, 8, 16, 9, 0, 5, tzinfo=UTC)
    publisher = _FakePublisher()
    settings = TradeEngineSettings(binance_order_type="LIMIT")
    gateway = _FakeGateway(
        TradeExecutionResult(
            symbol="BTCUSDT",
            status=TradeExecutionStatus.NEW,
            updated_at=now,
            order_id="ord-4",
        )
    )
    processor = TradeEngineMessageProcessor(publisher=publisher, settings=settings, gateway=gateway)

    try:
        processor.handle_trade_action(_trade_action_event(quantity=0.2))
        assert False, "Expected ValueError for incomplete LIMIT order metadata"
    except ValueError as exc:
        assert "LIMIT order requires" in str(exc)

def test_trade_engine_deal_position_manager_action() -> None:
    now = datetime(2026, 8, 16, 9, 0, 6, tzinfo=UTC)
    publisher = _FakePublisher()
    settings = TradeEngineSettings(binance_order_type="LIMIT")
    gateway = _FakeGateway(
        TradeExecutionResult(
            symbol="BTCUSDT",
            status=TradeExecutionStatus.NEW,
            updated_at=now,
            order_id="ord-4",
        )
    )
    processor = TradeEngineMessageProcessor(publisher=publisher, settings=settings, gateway=gateway)
    contract_event = build_event(
    EngineEventType.TRADE_ACTION_REQUESTED,
        TradeActionPayload(
            symbol="TESTEVENT",
            action="test action",
            side="test side",
            requested_at=now,
            quantity=0.1,
            state="test state",
            metadata={"test": "metadata","price": "100.0", "timeInForce": "GTC"},
        ),
        producer="test_producer",
        occurred_at=now,
    )
    request = _to_trade_order_request(contract_event.payload, contract_event, settings)
    processor.handle_trade_action(contract_event)

    assert request.quantity == 0.1
