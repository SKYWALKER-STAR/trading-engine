from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from trading_engine.app.position_engine_kafka import PositionEngineMessageProcessor
from trading_engine.contracts.messages import (
    EngineEventType,
    OrderUpdatePayload,
    PositionSignalCommand,
    RiskAction,
    RiskDecisionPayload,
    SignalDirection,
    StrategySignalPayload,
    build_event,
)
from trading_engine.position.manager import PositionManager
from trading_engine.position.models import PositionState


@dataclass
class InMemoryPositionRepository:
    state: dict[str, PositionState]

    def get(self, symbol: str) -> PositionState | None:
        return self.state.get(symbol)

    def save(self, state: PositionState) -> None:
        self.state[state.symbol] = state


def test_risk_approved_signal_drives_position_manager() -> None:
    now = datetime.now(UTC)
    repository = InMemoryPositionRepository(state={})
    manager = PositionManager(repository=repository)
    processor = PositionEngineMessageProcessor(manager)
    event = build_event(
        EngineEventType.RISK_DECISION_MADE,
        RiskDecisionPayload(
            symbol="BTCUSDT",
            action=RiskAction.APPROVE,
            approved_signal=StrategySignalPayload(
                strategy_name="factor_score",
                symbol="BTCUSDT",
                direction="long",
                score=90.0,
                confidence=0.85,
                timestamp=now,
            ),
            reason="allow_open_long",
            decided_at=now,
        ),
        producer="risk-engine",
        occurred_at=now,
        correlation_id="corr-1",
    )

    processor.handle_risk_decision(event)

    saved_state = repository.state["BTCUSDT"]
    assert saved_state.lifecycle.value == "open_long"


def test_risk_reject_does_not_drive_position_manager() -> None:
    now = datetime.now(UTC)
    repository = InMemoryPositionRepository(state={})
    manager = PositionManager(repository=repository)
    processor = PositionEngineMessageProcessor(manager)
    event = build_event(
        EngineEventType.RISK_DECISION_MADE,
        RiskDecisionPayload(
            symbol="BTCUSDT",
            action=RiskAction.REJECT,
            approved_signal=None,
            reason="already_long",
            decided_at=now,
        ),
        producer="risk-engine",
        occurred_at=now,
        correlation_id="corr-1",
    )

    processor.handle_risk_decision(event)

    assert repository.state == {}


def test_order_update_event_advances_position_state() -> None:
    now = datetime.now(UTC)
    repository = InMemoryPositionRepository(state={})
    manager = PositionManager(repository=repository)
    processor = PositionEngineMessageProcessor(manager)
    manager.handle_signal(
        PositionSignalCommand(
            strategy_name="factor_score",
            symbol="BTCUSDT",
            direction=SignalDirection.LONG,
            score=90.0,
            confidence=0.85,
            timestamp=now,
        )
    )
    event = build_event(
        EngineEventType.ORDER_UPDATE_RECEIVED,
        OrderUpdatePayload(
            symbol="BTCUSDT",
            order_id="ord-1",
            status="new",
            updated_at=now + timedelta(seconds=1),
        ),
        producer="trade-engine",
        occurred_at=now + timedelta(seconds=1),
    )

    processor.handle_order_update(event)

    saved_state = repository.state["BTCUSDT"]
    assert saved_state.lifecycle.value == "opening_long"