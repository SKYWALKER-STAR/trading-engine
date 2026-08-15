from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from trading_engine.contracts.messages import PositionSignalCommand, SignalDirection
from trading_engine.position.manager import PositionManager
from trading_engine.position.models import (
    OrderUpdateStatus,
    PositionDirection,
    PositionLifecycle,
    PositionOrderEvent,
    PositionState,
    TradeActionFailed,
    TradeActionType,
)


@dataclass
class InMemoryPositionRepository:
    state: dict[str, PositionState]

    def get(self, symbol: str) -> PositionState | None:
        return self.state.get(symbol)

    def save(self, state: PositionState) -> None:
        self.state[state.symbol] = state


def build_signal(direction: SignalDirection, now: datetime) -> PositionSignalCommand:
    return PositionSignalCommand(
        strategy_name="factor_score",
        symbol="BTCUSDT",
        direction=direction,
        score=0.8,
        confidence=0.8,
        timestamp=now,
    )


def test_flat_signal_long_generates_open_long_action() -> None:
    now = datetime.now(UTC)
    manager = PositionManager(repository=InMemoryPositionRepository(state={}))

    decision = manager.handle_signal(build_signal(SignalDirection.LONG, now))

    assert decision.trade_action is not None
    assert decision.trade_action.action_type is TradeActionType.OPEN_LONG
    assert decision.state.direction is PositionDirection.FLAT
    assert decision.state.lifecycle is PositionLifecycle.OPEN_LONG


def test_new_order_moves_open_long_to_opening_long() -> None:
    now = datetime.now(UTC)
    repository = InMemoryPositionRepository(state={})
    manager = PositionManager(repository=repository)
    manager.handle_signal(build_signal(SignalDirection.LONG, now))

    decision = manager.handle_order_event(
        PositionOrderEvent(
            symbol="BTCUSDT",
            status=OrderUpdateStatus.NEW,
            updated_at=now + timedelta(seconds=1),
            order_id="ord-1",
        )
    )

    assert decision.trade_action is None
    assert decision.state.direction is PositionDirection.FLAT
    assert decision.state.lifecycle is PositionLifecycle.OPENING_LONG
    assert decision.state.active_order_id == "ord-1"


def test_filled_order_moves_opening_long_to_long() -> None:
    now = datetime.now(UTC)
    repository = InMemoryPositionRepository(state={})
    manager = PositionManager(repository=repository)
    manager.handle_signal(build_signal(SignalDirection.LONG, now))
    manager.handle_order_event(
        PositionOrderEvent(
            symbol="BTCUSDT",
            status=OrderUpdateStatus.NEW,
            updated_at=now + timedelta(seconds=1),
            order_id="ord-1",
        )
    )

    decision = manager.handle_order_event(
        PositionOrderEvent(
            symbol="BTCUSDT",
            status=OrderUpdateStatus.FILLED,
            updated_at=now + timedelta(seconds=2),
            order_id="ord-1",
            filled_quantity=0.25,
        )
    )

    assert decision.trade_action is None
    assert decision.state.direction is PositionDirection.LONG
    assert decision.state.lifecycle is PositionLifecycle.LONG
    assert decision.state.quantity == 0.25


def test_long_signal_flat_generates_close_long_action() -> None:
    now = datetime.now(UTC)
    repository = InMemoryPositionRepository(
        state={
            "BTCUSDT": PositionState(
                symbol="BTCUSDT",
                direction=PositionDirection.LONG,
                lifecycle=PositionLifecycle.LONG,
                quantity=0.5,
                updated_at=now,
            )
        }
    )
    manager = PositionManager(repository=repository)

    decision = manager.handle_signal(build_signal(SignalDirection.FLAT, now + timedelta(seconds=1)))

    assert decision.trade_action is not None
    assert decision.trade_action.action_type is TradeActionType.CLOSE_LONG
    assert decision.state.direction is PositionDirection.LONG
    assert decision.state.lifecycle is PositionLifecycle.CLOSE_LONG


def test_partially_filled_order_keeps_opening_state_and_updates_quantity() -> None:
    now = datetime.now(UTC)
    repository = InMemoryPositionRepository(state={})
    manager = PositionManager(repository=repository)
    manager.handle_signal(build_signal(SignalDirection.LONG, now))
    manager.handle_order_event(
        PositionOrderEvent(
            symbol="BTCUSDT",
            status=OrderUpdateStatus.NEW,
            updated_at=now + timedelta(seconds=1),
            order_id="ord-1",
        )
    )

    decision = manager.handle_order_event(
        PositionOrderEvent(
            symbol="BTCUSDT",
            status=OrderUpdateStatus.PARTIALLY_FILLED,
            updated_at=now + timedelta(seconds=2),
            order_id="ord-1",
            filled_quantity=0.10,
        )
    )

    assert decision.state.direction is PositionDirection.LONG
    assert decision.state.lifecycle is PositionLifecycle.OPENING_LONG
    assert decision.state.quantity == 0.10


def test_rejected_order_rolls_back_and_emits_failed_event() -> None:
    now = datetime.now(UTC)
    repository = InMemoryPositionRepository(state={})
    manager = PositionManager(repository=repository)
    manager.handle_signal(build_signal(SignalDirection.LONG, now))
    manager.handle_order_event(
        PositionOrderEvent(
            symbol="BTCUSDT",
            status=OrderUpdateStatus.NEW,
            updated_at=now + timedelta(seconds=1),
            order_id="ord-1",
        )
    )

    decision = manager.handle_order_event(
        PositionOrderEvent(
            symbol="BTCUSDT",
            status=OrderUpdateStatus.REJECTED,
            updated_at=now + timedelta(seconds=2),
            order_id="ord-1",
        )
    )

    assert decision.state.lifecycle is PositionLifecycle.FLAT
    failed_events = [event for event in decision.events if isinstance(event, TradeActionFailed)]
    assert len(failed_events) == 1
    assert failed_events[0].status == "rejected"


def test_recover_stale_transition_rolls_back_on_timeout() -> None:
    now = datetime.now(UTC)
    repository = InMemoryPositionRepository(state={})
    manager = PositionManager(repository=repository)
    manager.handle_signal(build_signal(SignalDirection.LONG, now))
    manager.handle_order_event(
        PositionOrderEvent(
            symbol="BTCUSDT",
            status=OrderUpdateStatus.NEW,
            updated_at=now + timedelta(seconds=1),
            order_id="ord-timeout",
        )
    )

    decision = manager.recover_stale_transition(
        symbol="BTCUSDT",
        now=now + timedelta(seconds=40),
        timeout_seconds=30.0,
    )

    assert decision is not None
    assert decision.state.lifecycle is PositionLifecycle.FLAT
    failed_events = [event for event in decision.events if isinstance(event, TradeActionFailed)]
    assert len(failed_events) == 1
    assert failed_events[0].status == "timed_out"