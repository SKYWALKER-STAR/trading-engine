from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast

from trading_engine.common.logger import get_logger
from trading_engine.config.settings import RiskEngineSettings
from trading_engine.contracts.messages import (
    EngineEvent,
    EngineEventType,
    PositionStatePayload,
    PositionStateSnapshot,
    RiskAction,
    RiskDecisionPayload,
    SignalDirection,
    StrategySignalPayload,
    build_event,
)
from trading_engine.infra.kafka_event_bus import KafkaEventConsumer, KafkaEventPublisher


LOGGER = get_logger(__name__)


class RiskEngineMessageProcessor:
    """Consumes strategy and position events, then publishes risk decisions."""

    def __init__(
        self,
        publisher: KafkaEventPublisher,
        settings: RiskEngineSettings,
        *,
        producer_name: str = "risk-engine",
    ) -> None:
        self._publisher = publisher
        self._settings = settings
        self._producer_name = producer_name
        self._positions: dict[str, PositionStateSnapshot] = {}

    def handle_position_state(self, event: EngineEvent[Any]) -> None:
        if event.event_type is not EngineEventType.POSITION_STATE_CHANGED:
            raise ValueError(f"Unexpected event type: {event.event_type.value}")

        payload = cast(PositionStatePayload, event.payload)
        self._positions[payload.current.symbol] = payload.current
        LOGGER.debug(
            "Updated local risk position snapshot",
            extra={
                "symbol": payload.current.symbol,
                "direction": payload.current.direction,
                "lifecycle": payload.current.lifecycle,
                "quantity": payload.current.quantity,
            },
        )

    def handle_strategy_signal(self, event: EngineEvent[Any]) -> None:
        if event.event_type is not EngineEventType.STRATEGY_SIGNAL_GENERATED:
            raise ValueError(f"Unexpected event type: {event.event_type.value}")

        payload = cast(StrategySignalPayload, event.payload)
        position = self._positions.get(payload.symbol)
        decision = self._decide(payload, position)

        risk_event = build_event(
            EngineEventType.RISK_DECISION_MADE,
            decision,
            producer=self._producer_name,
            occurred_at=decision.decided_at,
            correlation_id=event.correlation_id,
            causation_id=event.event_id,
        )
        self._publisher.publish(self._settings.risk_decision_topic, risk_event, key=payload.symbol)

    def _decide(
        self,
        signal: StrategySignalPayload,
        position: PositionStateSnapshot | None,
    ) -> RiskDecisionPayload:
        now = datetime.now(UTC)
        open_quantity = float(self._settings.default_open_quantity)

        if position is None:
            if self._settings.require_position_snapshot:
                return RiskDecisionPayload(
                    symbol=signal.symbol,
                    action=RiskAction.REJECT,
                    approved_signal=None,
                    reason="position_snapshot_missing",
                    decided_at=now,
                    metadata={"signal_direction": signal.direction},
                )
            approved_signal = self._signal_with_risk_metadata(signal, quantity=open_quantity)
            return RiskDecisionPayload(
                symbol=signal.symbol,
                action=RiskAction.APPROVE,
                approved_signal=approved_signal,
                reason="position_snapshot_missing_assumed_flat",
                decided_at=now,
                metadata={"approved_quantity": open_quantity},
            )

        lifecycle = position.lifecycle
        if lifecycle not in ("flat", "long", "short"):
            return RiskDecisionPayload(
                symbol=signal.symbol,
                action=RiskAction.REJECT,
                approved_signal=None,
                reason="position_transition_in_progress",
                decided_at=now,
                metadata={"position_lifecycle": lifecycle},
            )

        direction = SignalDirection(signal.direction)

        if direction is SignalDirection.FLAT:
            if position.direction == "flat":
                return RiskDecisionPayload(
                    symbol=signal.symbol,
                    action=RiskAction.REJECT,
                    approved_signal=None,
                    reason="already_flat",
                    decided_at=now,
                    metadata={"position_direction": position.direction},
                )
            close_quantity = position.quantity if position.quantity > 0 else open_quantity
            approved_signal = self._signal_with_risk_metadata(signal, quantity=close_quantity)
            return RiskDecisionPayload(
                symbol=signal.symbol,
                action=RiskAction.APPROVE,
                approved_signal=approved_signal,
                reason="allow_flatten_position",
                decided_at=now,
                metadata={"approved_quantity": close_quantity},
            )

        if direction is SignalDirection.LONG:
            if position.direction == "long":
                return RiskDecisionPayload(
                    symbol=signal.symbol,
                    action=RiskAction.REJECT,
                    approved_signal=None,
                    reason="already_long",
                    decided_at=now,
                    metadata={"position_direction": position.direction},
                )
            if position.direction == "short":
                reduce_quantity = position.quantity if position.quantity > 0 else open_quantity
                approved_signal = self._signal_with_risk_metadata(signal, quantity=reduce_quantity)
                return RiskDecisionPayload(
                    symbol=signal.symbol,
                    action=RiskAction.REDUCE_ONLY,
                    approved_signal=approved_signal,
                    reason="must_close_short_before_long",
                    decided_at=now,
                    metadata={"approved_quantity": reduce_quantity},
                )
            approved_signal = self._signal_with_risk_metadata(signal, quantity=open_quantity)
            return RiskDecisionPayload(
                symbol=signal.symbol,
                action=RiskAction.APPROVE,
                approved_signal=approved_signal,
                reason="allow_open_long",
                decided_at=now,
                metadata={"approved_quantity": open_quantity},
            )

        if position.direction == "short":
            return RiskDecisionPayload(
                symbol=signal.symbol,
                action=RiskAction.REJECT,
                approved_signal=None,
                reason="already_short",
                decided_at=now,
                metadata={"position_direction": position.direction},
            )
        if position.direction == "long":
            reduce_quantity = position.quantity if position.quantity > 0 else open_quantity
            approved_signal = self._signal_with_risk_metadata(signal, quantity=reduce_quantity)
            return RiskDecisionPayload(
                symbol=signal.symbol,
                action=RiskAction.REDUCE_ONLY,
                approved_signal=approved_signal,
                reason="must_close_long_before_short",
                decided_at=now,
                metadata={"approved_quantity": reduce_quantity},
            )

        approved_signal = self._signal_with_risk_metadata(signal, quantity=open_quantity)
        return RiskDecisionPayload(
            symbol=signal.symbol,
            action=RiskAction.APPROVE,
            approved_signal=approved_signal,
            reason="allow_open_short",
            decided_at=now,
            metadata={"approved_quantity": open_quantity},
        )

    @staticmethod
    def _signal_with_risk_metadata(signal: StrategySignalPayload, *, quantity: float) -> StrategySignalPayload:
        enriched_metadata = dict(signal.metadata)
        enriched_metadata["approved_quantity"] = quantity
        return replace(signal, metadata=enriched_metadata)


def build_risk_engine_consumer(
    settings: RiskEngineSettings,
    *,
    producer_name: str = "risk-engine",
) -> KafkaEventConsumer:
    publisher = KafkaEventPublisher.from_env()
    processor = RiskEngineMessageProcessor(
        publisher=publisher,
        settings=settings,
        producer_name=producer_name,
    )

    consumer = KafkaEventConsumer.from_env(group_id=settings.consumer_group)
    consumer.subscribe(settings.signal_topic, processor.handle_strategy_signal)
    consumer.subscribe(settings.position_state_topic, processor.handle_position_state)
    return consumer
