from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from trading_engine.contracts.messages import PositionSignalCommand, SignalDirection


class PositionDirection(str, Enum):
    FLAT = "flat"
    LONG = "long"
    SHORT = "short"


class PositionLifecycle(str, Enum):
    FLAT = "flat"
    OPEN_LONG = "open_long"
    OPENING_LONG = "opening_long"
    LONG = "long"
    CLOSE_LONG = "close_long"
    CLOSING_LONG = "closing_long"
    OPEN_SHORT = "open_short"
    OPENING_SHORT = "opening_short"
    SHORT = "short"
    CLOSE_SHORT = "close_short"
    CLOSING_SHORT = "closing_short"


class TradeActionType(str, Enum):
    OPEN_LONG = "open_long"
    CLOSE_LONG = "close_long"
    OPEN_SHORT = "open_short"
    CLOSE_SHORT = "close_short"


class OrderUpdateStatus(str, Enum):
    NEW = "new"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class PositionState:
    symbol: str
    direction: PositionDirection
    lifecycle: PositionLifecycle
    quantity: float = 0.0
    active_order_id: str | None = None
    updated_at: datetime | None = None
    metadata: dict[str, str | float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TradeAction:
    symbol: str
    action_type: TradeActionType
    side: str
    created_at: datetime
    quantity: float | None = None
    signal: PositionSignalCommand | None = None
    metadata: dict[str, str | float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PositionOrderEvent:
    symbol: str
    status: OrderUpdateStatus
    updated_at: datetime
    order_id: str | None = None
    filled_quantity: float | None = None
    metadata: dict[str, str | float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PositionStateChanged:
    previous: PositionState
    current: PositionState
    occurred_at: datetime
    reason: str


@dataclass(frozen=True, slots=True)
class TradeActionCreated:
    action: TradeAction
    state: PositionState
    occurred_at: datetime


@dataclass(frozen=True, slots=True)
class TradeActionFailed:
    symbol: str
    status: str
    reason: str
    occurred_at: datetime
    order_id: str | None = None
    state: PositionState | None = None
    metadata: dict[str, str | float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PositionDecision:
    state: PositionState
    trade_action: TradeAction | None = None
    events: tuple[PositionStateChanged | TradeActionCreated | TradeActionFailed, ...] = ()


def make_flat_position(symbol: str, now: datetime) -> PositionState:
    return PositionState(
        symbol=symbol,
        direction=PositionDirection.FLAT,
        lifecycle=PositionLifecycle.FLAT,
        updated_at=now,
    )


def to_position_direction(signal_direction: SignalDirection) -> PositionDirection:
    if signal_direction is SignalDirection.LONG:
        return PositionDirection.LONG
    if signal_direction is SignalDirection.SHORT:
        return PositionDirection.SHORT
    return PositionDirection.FLAT