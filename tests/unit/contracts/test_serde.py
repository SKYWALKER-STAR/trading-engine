from __future__ import annotations

from datetime import UTC, datetime

from trading_engine.contracts.messages import (
    EngineEventType,
    PositionStatePayload,
    PositionStateSnapshot,
    TradeActionFailedPayload,
    StrategySignalPayload,
    build_event,
)
from trading_engine.contracts.serde import decode_event, encode_event


def test_strategy_signal_event_round_trip() -> None:
    payload = StrategySignalPayload(
        strategy_name="factor_score",
        symbol="BTCUSDT",
        direction="long",
        score=88.2,
        confidence=0.71,
        timestamp=datetime(2026, 8, 11, 10, 0, tzinfo=UTC),
        metadata={"interval": "1m"},
    )
    event = build_event(
        EngineEventType.STRATEGY_SIGNAL_GENERATED,
        payload,
        producer="strategy-engine",
        occurred_at=payload.timestamp,
    )

    encoded = encode_event(event)
    decoded = decode_event(encoded)

    assert decoded.event_type is EngineEventType.STRATEGY_SIGNAL_GENERATED
    assert decoded.producer == "strategy-engine"
    assert decoded.payload == payload


def test_position_state_event_round_trip() -> None:
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
            direction="long",
            lifecycle="long",
            quantity=0.25,
            active_order_id="ord-1",
            updated_at=now,
            metadata={"source": "risk-approved"},
        ),
        reason="order_filled_open_long",
        occurred_at=now,
    )
    event = build_event(
        EngineEventType.POSITION_STATE_CHANGED,
        payload,
        producer="position-engine",
        occurred_at=now,
        correlation_id="corr-1",
        causation_id="cause-1",
    )

    decoded = decode_event(encode_event(event))

    assert decoded.correlation_id == "corr-1"
    assert decoded.causation_id == "cause-1"
    assert decoded.payload == payload


def test_trade_action_failed_event_round_trip() -> None:
    now = datetime(2026, 8, 11, 10, 2, tzinfo=UTC)
    payload = TradeActionFailedPayload(
        symbol="BTCUSDT",
        status="rejected",
        reason="order_rejected",
        failed_at=now,
        order_id="ord-2",
        state="opening_long",
        metadata={"exchange_reason": "insufficient_margin"},
    )
    event = build_event(
        EngineEventType.TRADE_ACTION_FAILED,
        payload,
        producer="position-engine",
        occurred_at=now,
        correlation_id="corr-2",
    )

    decoded = decode_event(encode_event(event))

    assert decoded.event_type is EngineEventType.TRADE_ACTION_FAILED
    assert decoded.payload == payload