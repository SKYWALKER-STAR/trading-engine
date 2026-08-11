from __future__ import annotations

from typing import Any, cast

from trading_engine.common.logger import get_logger
from trading_engine.config.settings import PositionEngineSettings
from trading_engine.contracts.messages import (
    EngineEvent,
    EngineEventType,
    OrderUpdatePayload,
    PositionStatePayload,
    PositionSignalCommand,
    PositionStateSnapshot,
    SignalDirection,
    StrategySignalPayload,
    TopicNames,
    TradeActionPayload,
    build_event,
)
from trading_engine.infra.bus.base import EventBus
from trading_engine.infra.kafka_event_bus import KafkaEventConsumer, KafkaEventPublisher
from trading_engine.position.manager import PositionManager
from trading_engine.position.models import (
    OrderUpdateStatus,
    PositionOrderEvent,
    PositionStateChanged,
    TradeActionCreated,
)
from trading_engine.position.repository import PositionRepository


LOGGER = get_logger(__name__)


class PositionKafkaEventBus(EventBus):
    """Bridges position domain events to versioned Kafka contracts."""

    def __init__(
        self,
        publisher: KafkaEventPublisher,
        settings: PositionEngineSettings,
        producer_name: str = "position-engine",
    ) -> None:
        self._publisher = publisher
        self._settings = settings
        self._producer_name = producer_name

    def publish(self, topic: str, event: Any) -> None:
        if isinstance(event, PositionStateChanged):
            contract_event = build_event(
                EngineEventType.POSITION_STATE_CHANGED,
                PositionStatePayload(
                    previous=_to_state_snapshot(event.previous),
                    current=_to_state_snapshot(event.current),
                    reason=event.reason,
                    occurred_at=event.occurred_at,
                ),
                producer=self._producer_name,
                occurred_at=event.occurred_at,
            )
            self._publisher.publish(self._settings.position_state_topic, contract_event, key=event.current.symbol)
            return

        if isinstance(event, TradeActionCreated):
            contract_event = build_event(
                EngineEventType.TRADE_ACTION_REQUESTED,
                TradeActionPayload(
                    symbol=event.action.symbol,
                    action=event.action.action_type.value,
                    side=event.action.side,
                    requested_at=event.occurred_at,
                    quantity=event.action.quantity,
                    state=event.state.lifecycle.value,
                    metadata=dict(event.action.metadata),
                ),
                producer=self._producer_name,
                occurred_at=event.occurred_at,
            )
            self._publisher.publish(self._settings.trade_action_topic, contract_event, key=event.action.symbol)
            return

        raise ValueError(f"Unsupported position event for Kafka publishing: {type(event)!r}")

    def subscribe(self, topic: str, handler: Any) -> None:
        raise NotImplementedError("PositionKafkaEventBus is publish-only; use KafkaEventConsumer for subscriptions")


class PositionEngineMessageProcessor:
    """Consumes Kafka contracts and delegates to PositionManager."""

    def __init__(self, manager: PositionManager) -> None:
        self._manager = manager

    def handle_strategy_signal(self, event: EngineEvent[Any]) -> None:
        if event.event_type is not EngineEventType.STRATEGY_SIGNAL_GENERATED:
            raise ValueError(f"Unexpected event type: {event.event_type.value}")

        payload = cast(StrategySignalPayload, event.payload)
        self._manager.handle_signal(_to_position_signal_command(payload))

    def handle_order_update(self, event: EngineEvent[Any]) -> None:
        if event.event_type is not EngineEventType.ORDER_UPDATE_RECEIVED:
            raise ValueError(f"Unexpected event type: {event.event_type.value}")

        payload = cast(OrderUpdatePayload, event.payload)
        self._manager.handle_order_event(
            PositionOrderEvent(
                symbol=payload.symbol,
                status=OrderUpdateStatus(payload.status),
                updated_at=payload.updated_at,
                order_id=payload.order_id,
                filled_quantity=payload.filled_quantity,
                metadata=dict(payload.metadata),
            )
        )


def build_position_engine_consumer(
    repository: PositionRepository,
    settings: PositionEngineSettings,
    *,
    producer_name: str = "position-engine",
) -> KafkaEventConsumer:
    publisher = KafkaEventPublisher.from_env()
    manager = PositionManager(
        repository=repository,
        publisher=PositionKafkaEventBus(
            publisher=publisher,
            settings=settings,
            producer_name=producer_name,
        ),
        state_topic=TopicNames.POSITION_STATE_CHANGED,
        action_topic=TopicNames.TRADE_ACTION_REQUESTED,
    )
    processor = PositionEngineMessageProcessor(manager)
    consumer = KafkaEventConsumer.from_env(group_id=settings.consumer_group)
    consumer.subscribe(settings.signal_topic, processor.handle_strategy_signal)
    consumer.subscribe(settings.order_update_topic, processor.handle_order_update)
    return consumer


def _to_position_signal_command(
    signal_payload: StrategySignalPayload | PositionSignalCommand,
) -> PositionSignalCommand:
    if isinstance(signal_payload, PositionSignalCommand):
        return signal_payload

    return PositionSignalCommand(
        strategy_name=signal_payload.strategy_name,
        symbol=signal_payload.symbol,
        direction=SignalDirection(signal_payload.direction),
        score=signal_payload.score,
        confidence=signal_payload.confidence,
        timestamp=signal_payload.timestamp,
        metadata=dict(signal_payload.metadata),
    )


def _to_state_snapshot(state: Any) -> PositionStateSnapshot:
    return PositionStateSnapshot(
        symbol=state.symbol,
        direction=state.direction.value,
        lifecycle=state.lifecycle.value,
        quantity=state.quantity,
        active_order_id=state.active_order_id,
        updated_at=state.updated_at,
        metadata=dict(state.metadata),
    )