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
    RiskAction,
    RiskDecisionPayload,
    SignalDirection,
    StrategySignalPayload,
    TradeActionFailedPayload,
    TradeActionPayload,
    build_event,
)
from trading_engine.debug.dashboard import PositionDebugStore
from trading_engine.infra.bus.base import EventBus
from trading_engine.infra.kafka_event_bus import KafkaEventConsumer, KafkaEventPublisher
from trading_engine.position.manager import PositionManager
from trading_engine.position.models import (
    OrderUpdateStatus,
    PositionOrderEvent,
    PositionStateChanged,
    TradeActionCreated,
    TradeActionFailed,
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

        if isinstance(event, TradeActionFailed):
            contract_event = build_event(
                EngineEventType.TRADE_ACTION_FAILED,
                TradeActionFailedPayload(
                    symbol=event.symbol,
                    status=event.status,
                    reason=event.reason,
                    failed_at=event.occurred_at,
                    order_id=event.order_id,
                    state=None if event.state is None else event.state.lifecycle.value,
                    metadata=dict(event.metadata),
                ),
                producer=self._producer_name,
                occurred_at=event.occurred_at,
            )
            self._publisher.publish(self._settings.trade_action_failed_topic, contract_event, key=event.symbol)
            return

        raise ValueError(f"Unsupported position event for Kafka publishing: {type(event)!r}")

    def subscribe(self, topic: str, handler: Any) -> None:
        raise NotImplementedError("PositionKafkaEventBus is publish-only; use KafkaEventConsumer for subscriptions")


class PositionEngineMessageProcessor:
    """Consumes Kafka contracts and delegates to PositionManager."""

    def __init__(self, manager: PositionManager, *, order_update_timeout_seconds: float = 30.0,
                 recover_on_timeout: bool = True) -> None:
        self._manager = manager
        self._order_update_timeout_seconds = order_update_timeout_seconds
        self._recover_on_timeout = recover_on_timeout

    def handle_risk_decision(self, event: EngineEvent[Any]) -> None:
        if event.event_type is not EngineEventType.RISK_DECISION_MADE:
            raise ValueError(f"Unexpected event type: {event.event_type.value}")

        payload = cast(RiskDecisionPayload, event.payload)
        recovered = self._manager.recover_stale_transition(
            payload.symbol,
            payload.decided_at,
            self._order_update_timeout_seconds,
        ) if self._recover_on_timeout else None
        if recovered is not None:
            LOGGER.warning(
                "Recovered stale position transition before applying new risk decision",
                extra={
                    "symbol": payload.symbol,
                    "action": payload.action.value,
                    "reason": "order_timeout",
                },
            )

        if payload.action is RiskAction.REJECT:
            LOGGER.info(
                "Risk decision rejected; position state unchanged",
                extra={
                    "symbol": payload.symbol,
                    "action": payload.action.value,
                    "reason": payload.reason,
                },
            )
            return

        approved_signal = payload.approved_signal
        if approved_signal is None:
            LOGGER.warning(
                "Risk decision accepted without approved signal; ignoring",
                extra={
                    "symbol": payload.symbol,
                    "action": payload.action.value,
                    "reason": payload.reason,
                },
            )
            return

        LOGGER.info(
            "Handling risk-approved signal",
            extra={
                "symbol": payload.symbol,
                "action": payload.action.value,
                "reason": payload.reason,
            },
        )
        self._manager.handle_signal(
            _to_position_signal_command(
                approved_signal,
                risk_action=payload.action.value,
                risk_reason=payload.reason,
                risk_decided_at=payload.decided_at.isoformat(),
            )
        )

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
                client_order_id=payload.client_order_id,
                cumulative_filled_quote=payload.cumulative_filled_quote,
                last_filled_price=payload.last_filled_price,
                filled_quantity=payload.filled_quantity,
                last_filled_quantity=payload.last_filled_quantity,
                cumulative_filled_quantity=payload.cumulative_filled_quantity,
                trade_id=payload.trade_id,
                metadata=dict(payload.metadata),
            )
        )


def build_position_engine_consumer(
    repository: PositionRepository,
    settings: PositionEngineSettings,
    *,
    producer_name: str = "position-engine",
    debug_store: PositionDebugStore | None = None,
) -> KafkaEventConsumer:
    from trading_engine.app.position_workflow import PositionWorkflow
    from trading_engine.infra.redis_position_repository import RedisPositionRepository
    from trading_engine.infra.redis_workflow import RedisWorkflowStore

    if not isinstance(repository, RedisPositionRepository):
        raise TypeError("Durable position runtime requires RedisPositionRepository")
    store = RedisWorkflowStore(repository._get_client(), repository._key_prefix, repository._account_id)
    if not store.client.exists(f"{store.base}:initialized"):
        raise RuntimeError("Execution state is not initialized; run position-state-migrate before starting position")
    processor = PositionWorkflow(store, settings)
    consumer = KafkaEventConsumer.from_env(group_id=settings.consumer_group, manual_commit=True)
    consumer.subscribe(settings.risk_decision_topic, processor.handle)
    consumer.subscribe(settings.order_update_topic, processor.handle)
    return consumer


def _to_position_signal_command(
    signal_payload: StrategySignalPayload | PositionSignalCommand,
    *,
    risk_action: str | None = None,
    risk_reason: str | None = None,
    risk_decided_at: str | None = None,
) -> PositionSignalCommand:
    if isinstance(signal_payload, PositionSignalCommand):
        return signal_payload

    metadata = dict(signal_payload.metadata)
    if risk_action is not None:
        metadata["risk_action"] = risk_action
    if risk_reason is not None:
        metadata["risk_reason"] = risk_reason
    if risk_decided_at is not None:
        metadata["risk_decided_at"] = risk_decided_at

    return PositionSignalCommand(
        strategy_name=signal_payload.strategy_name,
        symbol=signal_payload.symbol,
        direction=SignalDirection(signal_payload.direction),
        score=signal_payload.score,
        confidence=signal_payload.confidence,
        timestamp=signal_payload.timestamp,
        metadata=metadata,
    )


def _to_state_snapshot(state: Any) -> PositionStateSnapshot:
    return PositionStateSnapshot(
        symbol=state.symbol,
        direction=state.direction.value,
        lifecycle=state.lifecycle.value,
        quantity=state.quantity,
        entry_avg_price=state.entry_avg_price,
        cost_complete=state.cost_complete,
        opened_at=state.opened_at,
        active_order_id=state.active_order_id,
        active_client_order_id=state.active_client_order_id,
        last_order_id=state.last_order_id,
        last_client_order_id=state.last_client_order_id,
        updated_at=state.updated_at,
        metadata=dict(state.metadata),
    )
