from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from trading_engine.contracts.messages import (
    EngineEvent,
    EngineEventType,
    OrderUpdatePayload,
    PositionStatePayload,
    PositionStateSnapshot,
    RiskAction,
    RiskDecisionPayload,
    StrategySignalPayload,
    TradeActionFailedPayload,
    TradeActionPayload,
)


def encode_event(event: EngineEvent[Any]) -> bytes:
    return json.dumps(event.to_dict(), ensure_ascii=True).encode("utf-8")


def decode_event(raw: bytes | str) -> EngineEvent[Any]:
    if isinstance(raw, bytes):
        payload = json.loads(raw.decode("utf-8"))
    else:
        payload = json.loads(raw)

    event_type = EngineEventType(payload["event_type"])
    decoded_payload = _decode_payload(event_type, payload["payload"])
    return EngineEvent(
        event_id=str(payload["event_id"]),
        event_type=event_type,
        schema_version=int(payload["schema_version"]),
        occurred_at=datetime.fromisoformat(payload["occurred_at"]),
        producer=str(payload["producer"]),
        correlation_id=str(payload["correlation_id"]),
        causation_id=None if payload["causation_id"] is None else str(payload["causation_id"]),
        payload=decoded_payload,
    )


def _decode_payload(event_type: EngineEventType, payload: dict[str, Any]) -> Any:
    if event_type is EngineEventType.STRATEGY_SIGNAL_GENERATED:
        return StrategySignalPayload(
            strategy_name=str(payload["strategy_name"]),
            symbol=str(payload["symbol"]),
            direction=str(payload["direction"]),
            score=float(payload["score"]),
            confidence=float(payload["confidence"]),
            timestamp=datetime.fromisoformat(payload["timestamp"]),
            metadata={key: value for key, value in payload.get("metadata", {}).items()},
        )

    if event_type is EngineEventType.RISK_DECISION_MADE:
        signal_payload = payload.get("approved_signal")
        approved_signal = None
        if signal_payload is not None:
            approved_signal = StrategySignalPayload(
                strategy_name=str(signal_payload["strategy_name"]),
                symbol=str(signal_payload["symbol"]),
                direction=str(signal_payload["direction"]),
                score=float(signal_payload["score"]),
                confidence=float(signal_payload["confidence"]),
                timestamp=datetime.fromisoformat(signal_payload["timestamp"]),
                metadata={key: value for key, value in signal_payload.get("metadata", {}).items()},
            )
        return RiskDecisionPayload(
            symbol=str(payload["symbol"]),
            action=RiskAction(payload["action"]),
            approved_signal=approved_signal,
            reason=str(payload["reason"]),
            decided_at=datetime.fromisoformat(payload["decided_at"]),
            metadata={key: value for key, value in payload.get("metadata", {}).items()},
        )

    if event_type is EngineEventType.TRADE_ACTION_REQUESTED:
        return TradeActionPayload(
            symbol=str(payload["symbol"]),
            action=str(payload["action"]),
            side=str(payload["side"]),
            requested_at=datetime.fromisoformat(payload["requested_at"]),
            quantity=None if payload.get("quantity") is None else float(payload["quantity"]),
            state=None if payload.get("state") is None else str(payload["state"]),
            metadata={key: value for key, value in payload.get("metadata", {}).items()},
        )

    if event_type is EngineEventType.TRADE_ACTION_FAILED:
        return TradeActionFailedPayload(
            symbol=str(payload["symbol"]),
            status=str(payload["status"]),
            reason=str(payload["reason"]),
            failed_at=datetime.fromisoformat(payload["failed_at"]),
            order_id=None if payload.get("order_id") is None else str(payload["order_id"]),
            state=None if payload.get("state") is None else str(payload["state"]),
            metadata={key: value for key, value in payload.get("metadata", {}).items()},
        )

    if event_type is EngineEventType.ORDER_UPDATE_RECEIVED:
        return OrderUpdatePayload(
            symbol=str(payload["symbol"]),
            order_id=None if payload.get("order_id") is None else str(payload["order_id"]),
            status=str(payload["status"]),
            updated_at=datetime.fromisoformat(payload["updated_at"]),
            filled_quantity=None if payload.get("filled_quantity") is None else float(payload["filled_quantity"]),
            last_filled_quantity=(
                None
                if payload.get("last_filled_quantity") is None
                else float(payload["last_filled_quantity"])
            ),
            cumulative_filled_quantity=(
                None
                if payload.get("cumulative_filled_quantity") is None
                else float(payload["cumulative_filled_quantity"])
            ),
            original_quantity=(
                None
                if payload.get("original_quantity") is None
                else float(payload["original_quantity"])
            ),
            trade_id=None if payload.get("trade_id") is None else str(payload["trade_id"]),
            execution_type=(
                None if payload.get("execution_type") is None else str(payload["execution_type"])
            ),
            side=None if payload.get("side") is None else str(payload["side"]),
            position_side=(
                None if payload.get("position_side") is None else str(payload["position_side"])
            ),
            reduce_only=None if payload.get("reduce_only") is None else bool(payload["reduce_only"]),
            client_order_id=(
                None if payload.get("client_order_id") is None else str(payload["client_order_id"])
            ),
            event_time=(
                None if payload.get("event_time") is None else datetime.fromisoformat(payload["event_time"])
            ),
            trade_time=(
                None if payload.get("trade_time") is None else datetime.fromisoformat(payload["trade_time"])
            ),
            metadata={key: value for key, value in payload.get("metadata", {}).items()},
        )

    if event_type is EngineEventType.POSITION_STATE_CHANGED:
        return PositionStatePayload(
            previous=_decode_position_state_snapshot(payload["previous"]),
            current=_decode_position_state_snapshot(payload["current"]),
            reason=str(payload["reason"]),
            occurred_at=datetime.fromisoformat(payload["occurred_at"]),
        )

    raise ValueError(f"Unsupported event type: {event_type.value}")


def _decode_position_state_snapshot(payload: dict[str, Any]) -> PositionStateSnapshot:
    updated_at_raw = payload.get("updated_at")
    updated_at = None if updated_at_raw is None else datetime.fromisoformat(updated_at_raw)
    return PositionStateSnapshot(
        symbol=str(payload["symbol"]),
        direction=str(payload["direction"]),
        lifecycle=str(payload["lifecycle"]),
        quantity=float(payload["quantity"]),
        active_order_id=None if payload.get("active_order_id") is None else str(payload["active_order_id"]),
        updated_at=updated_at,
        active_client_order_id=(
            None
            if payload.get("active_client_order_id") is None
            else str(payload["active_client_order_id"])
        ),
        last_order_id=(
            None if payload.get("last_order_id") is None else str(payload["last_order_id"])
        ),
        last_client_order_id=(
            None
            if payload.get("last_client_order_id") is None
            else str(payload["last_client_order_id"])
        ),
        metadata={key: value for key, value in payload.get("metadata", {}).items()},
    )
