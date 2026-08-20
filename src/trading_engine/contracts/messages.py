from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Generic, TypeVar
from uuid import uuid4


class EngineEventType(str, Enum):
    STRATEGY_SIGNAL_GENERATED = "strategy.signal.generated"
    RISK_DECISION_MADE = "risk.decision.made"
    POSITION_STATE_CHANGED = "position.state.changed"
    TRADE_ACTION_REQUESTED = "trade.action.requested"
    TRADE_ACTION_FAILED = "trade.action.failed"
    ORDER_UPDATE_RECEIVED = "trade.order.update.received"


class TopicNames:
    STRATEGY_SIGNAL_GENERATED = "strategy.signal.generated.v1"
    RISK_DECISION_MADE = "risk.decision.made.v1"
    POSITION_STATE_CHANGED = "position.state.changed.v1"
    TRADE_ACTION_REQUESTED = "trade.action.requested.v1"
    TRADE_ACTION_FAILED = "trade.action.failed.v1"
    ORDER_UPDATE_RECEIVED = "trade.order.update.received.v1"


class RiskAction(str, Enum):
    APPROVE = "approve"
    REJECT = "reject"
    REDUCE_ONLY = "reduce_only"
    
class SignalDirection(str, Enum):
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


@dataclass(frozen=True, slots=True)
class StrategySignalPayload:
    strategy_name: str
    symbol: str
    direction: str
    score: float
    confidence: float
    timestamp: datetime
    metadata: dict[str, str | float] = field(default_factory=dict)

@dataclass(frozen=True, slots=True)
class PositionSignalCommand:
    strategy_name: str
    symbol: str
    direction: SignalDirection
    score: float
    confidence: float
    timestamp: datetime
    metadata: dict[str, str | float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RiskDecisionPayload:
    symbol: str
    action: RiskAction
    approved_signal: StrategySignalPayload | None
    reason: str
    decided_at: datetime
    metadata: dict[str, str | float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TradeActionPayload:
    symbol: str
    action: str
    side: str
    requested_at: datetime
    quantity: float | None = None
    state: str | None = None
    metadata: dict[str, str | float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TradeActionFailedPayload:
    symbol: str
    status: str
    reason: str
    failed_at: datetime
    order_id: str | None = None
    state: str | None = None
    metadata: dict[str, str | float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class OrderUpdatePayload:
    symbol: str
    order_id: str | None
    status: str
    updated_at: datetime
    filled_quantity: float | None = None
    last_filled_quantity: float | None = None
    cumulative_filled_quantity: float | None = None
    original_quantity: float | None = None
    trade_id: str | None = None
    execution_type: str | None = None
    side: str | None = None
    position_side: str | None = None
    reduce_only: bool | None = None
    client_order_id: str | None = None
    event_time: datetime | None = None
    trade_time: datetime | None = None
    metadata: dict[str, str | float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PositionStateSnapshot:
    symbol: str
    direction: str
    lifecycle: str
    quantity: float
    active_order_id: str | None
    updated_at: datetime | None
    active_client_order_id: str | None = None
    last_order_id: str | None = None
    last_client_order_id: str | None = None
    metadata: dict[str, str | float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PositionStatePayload:
    previous: PositionStateSnapshot
    current: PositionStateSnapshot
    reason: str
    occurred_at: datetime


PayloadT = TypeVar("PayloadT")


@dataclass(frozen=True, slots=True)
class EngineEvent(Generic[PayloadT]):
    event_id: str
    event_type: EngineEventType
    schema_version: int
    occurred_at: datetime
    producer: str
    correlation_id: str
    causation_id: str | None
    payload: PayloadT

    def to_dict(self) -> dict[str, Any]:
        payload_value = self.payload
        if is_dataclass(payload_value):
            payload_dict = _serialize_value(asdict(payload_value))
        else:
            payload_dict = _serialize_value(payload_value)

        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "schema_version": self.schema_version,
            "occurred_at": self.occurred_at.isoformat(),
            "producer": self.producer,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "payload": payload_dict,
        }


def build_event(
    event_type: EngineEventType,
    payload: PayloadT,
    *,
    producer: str,
    occurred_at: datetime,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    schema_version: int = 1,
) -> EngineEvent[PayloadT]:
    generated_event_id = str(uuid4())
    return EngineEvent(
        event_id=generated_event_id,
        event_type=event_type,
        schema_version=schema_version,
        occurred_at=occurred_at,
        producer=producer,
        correlation_id=correlation_id or generated_event_id,
        causation_id=causation_id,
        payload=payload,
    )


def _serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {key: _serialize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    if isinstance(value, tuple):
        return [_serialize_value(item) for item in value]
    return value
