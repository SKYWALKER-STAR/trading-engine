from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from trading_engine.app.position_engine_kafka import PositionEngineMessageProcessor
from trading_engine.contracts.messages import (
    EngineEventType,
    OrderUpdatePayload,
    PositionSignalCommand,
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


def test_strategy_signal_drives_position_manager() -> None:
    now = datetime.now(UTC)
    repository = InMemoryPositionRepository(state={})
    manager = PositionManager(repository=repository)
    processor = PositionEngineMessageProcessor(manager)
    event = build_event(
        EngineEventType.STRATEGY_SIGNAL_GENERATED,
        StrategySignalPayload(
            strategy_name="factor_score",
            symbol="BTCUSDT",
            direction="long",
            score=90.0,
            confidence=0.85,
            timestamp=now,
        ),
        producer="strategy-engine",
        occurred_at=now,
        correlation_id="corr-1",
    )

    processor.handle_strategy_signal(event)

    saved_state = repository.state["BTCUSDT"]
    assert saved_state.lifecycle.value == "open_long"


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