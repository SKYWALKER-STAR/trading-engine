from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class TradeExecutionStatus(str, Enum):
    NEW = "new"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"


class TrackedOrderStatus(str, Enum):
    """Persistent lifecycle of one logical exchange order."""

    PENDING_SUBMIT = "pending_submit"
    UNKNOWN = "unknown"
    NEW = "new"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"

    @property
    def is_terminal(self) -> bool:
        return self in {
            TrackedOrderStatus.FILLED,
            TrackedOrderStatus.CANCELED,
            TrackedOrderStatus.REJECTED,
            TrackedOrderStatus.EXPIRED,
        }


@dataclass(frozen=True, slots=True)
class TrackedOrder:
    """Persistent identity and execution progress for one logical order."""

    exchange: str
    account_id: str
    symbol: str
    client_order_id: str
    side: str
    order_type: str
    original_quantity: float
    status: TrackedOrderStatus
    created_at: datetime
    updated_at: datetime
    order_id: str | None = None
    position_side: str | None = None
    reduce_only: bool = False
    cumulative_filled_quantity: float = 0.0
    last_trade_id: str | None = None
    metadata: dict[str, str | float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TradeOrderRequest:
    symbol: str
    side: str
    quantity: float
    order_type: str
    requested_at: datetime
    correlation_id: str
    causation_id: str
    client_order_id: str | None = None
    metadata: dict[str, str | float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TradeExecutionResult:
    symbol: str
    status: TradeExecutionStatus
    updated_at: datetime
    order_id: str | None = None
    client_order_id: str | None = None
    filled_quantity: float | None = None
    metadata: dict[str, str | float] = field(default_factory=dict)
