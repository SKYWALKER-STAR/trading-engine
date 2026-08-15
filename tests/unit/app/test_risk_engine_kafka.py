from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from trading_engine.app.risk_engine_kafka import RiskEngineMessageProcessor
from trading_engine.config.settings import RiskEngineSettings
from trading_engine.contracts.messages import (
    EngineEvent,
    EngineEventType,
    PositionStatePayload,
    PositionStateSnapshot,
    StrategySignalPayload,
)


class _FakePublisher:
    def __init__(self) -> None:
        self.published: list[tuple[str, EngineEvent[Any], str]] = []

    def publish(self, topic: str, event: EngineEvent[Any], key: str) -> None:
        self.published.append((topic, event, key))


def _signal_event(direction: str = "long") -> EngineEvent[StrategySignalPayload]:
    signal = StrategySignalPayload(
        strategy_name="factor_score",
        symbol="BTCUSDT",
        direction=direction,
        score=88.2,
        confidence=0.71,
        timestamp=datetime(2026, 8, 11, 10, 0, tzinfo=UTC),
        metadata={"interval": "1m"},
    )
    return EngineEvent(
        event_id="sig-1",
        event_type=EngineEventType.STRATEGY_SIGNAL_GENERATED,
        schema_version=1,
        occurred_at=signal.timestamp,
        producer="strategy-engine",
        correlation_id="corr-1",
        causation_id=None,
        payload=signal,
    )


def _position_event(direction: str, lifecycle: str, quantity: float) -> EngineEvent[PositionStatePayload]:
    now = datetime(2026, 8, 11, 10, 1, tzinfo=UTC)
    payload = PositionStatePayload(
        previous=PositionStateSnapshot(
            symbol="BTCUSDT",
            direction="flat",
            lifecycle="flat",
            quantity=0.0,
            active_order_id=None,
            updated_at=now,
        ),
        current=PositionStateSnapshot(
            symbol="BTCUSDT",
            direction=direction,
            lifecycle=lifecycle,
            quantity=quantity,
            active_order_id=None,
            updated_at=now,
        ),
        reason="test_state_update",
        occurred_at=now,
    )
    return EngineEvent(
        event_id="pos-1",
        event_type=EngineEventType.POSITION_STATE_CHANGED,
        schema_version=1,
        occurred_at=now,
        producer="position-engine",
        correlation_id="corr-1",
        causation_id=None,
        payload=payload,
    )


def test_risk_engine_publishes_reduce_only_when_short_position_receives_long_signal() -> None:
    publisher = _FakePublisher()
    settings = RiskEngineSettings(default_open_quantity=0.25)
    processor = RiskEngineMessageProcessor(publisher=publisher, settings=settings)

    processor.handle_position_state(_position_event(direction="short", lifecycle="short", quantity=0.15))
    processor.handle_strategy_signal(_signal_event(direction="long"))

    assert len(publisher.published) == 1
    topic, event, key = publisher.published[0]
    assert topic == settings.risk_decision_topic
    assert key == "BTCUSDT"
    assert event.event_type is EngineEventType.RISK_DECISION_MADE
    assert event.payload.action.value == "reduce_only"
    assert event.payload.metadata["approved_quantity"] == 0.15


def test_risk_engine_rejects_signal_when_position_snapshot_is_required_and_missing() -> None:
    publisher = _FakePublisher()
    settings = RiskEngineSettings(require_position_snapshot=True)
    processor = RiskEngineMessageProcessor(publisher=publisher, settings=settings)

    processor.handle_strategy_signal(_signal_event(direction="short"))

    assert len(publisher.published) == 1
    _, event, _ = publisher.published[0]
    assert event.payload.action.value == "reject"
    assert event.payload.reason == "position_snapshot_missing"
