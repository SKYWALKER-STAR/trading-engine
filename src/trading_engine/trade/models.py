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
