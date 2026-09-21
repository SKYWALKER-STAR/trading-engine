from __future__ import annotations

import hashlib
from dataclasses import replace
from typing import Any

from trading_engine.app.position_engine_kafka import PositionEngineMessageProcessor, PositionKafkaEventBus
from trading_engine.config.settings import PositionEngineSettings
from trading_engine.contracts.messages import EngineEvent, EngineEventType
from trading_engine.infra.redis_position_repository import RedisPositionRepository
from trading_engine.infra.redis_workflow import RedisWorkflowStore, WorkflowChange
from trading_engine.position.manager import PositionManager
from trading_engine.position.models import PositionState


class PlannedPositionRepository:
    """Private snapshot used for one optimistic transaction; no external writes."""

    def __init__(self, state: PositionState | None) -> None:
        self.state = state

    def get(self, symbol: str) -> PositionState | None:
        return self.state

    def save(self, state: PositionState) -> None:
        self.state = state


class BufferedPublisher:
    def __init__(self) -> None:
        self.events: list[tuple[str, EngineEvent[Any], str]] = []

    def publish(self, topic: str, event: EngineEvent[Any], key: str) -> None:
        self.events.append((topic, event, key))


class PositionWorkflow:
    def __init__(self, store: RedisWorkflowStore, settings: PositionEngineSettings) -> None:
        self.store = store
        self.settings = settings

    def handle(self, event: EngineEvent[Any]) -> None:
        if event.event_type not in (EngineEventType.RISK_DECISION_MADE, EngineEventType.ORDER_UPDATE_RECEIVED):
            raise ValueError("Unexpected position input")
        symbol = event.payload.symbol.upper()

        def decide(raw: dict[str, Any] | None) -> WorkflowChange:
            state = None if raw is None else RedisPositionRepository.decode(raw)
            if event.event_type is EngineEventType.ORDER_UPDATE_RECEIVED:
                update = event.payload
                # Only known active orders can change execution state. Old terminal
                # updates and manual/external orders do not stop Kafka consumption.
                matches = state is not None and (
                    (update.order_id is not None and update.order_id == state.active_order_id)
                    or (update.client_order_id is not None
                        and update.client_order_id == state.active_client_order_id)
                )
                if (state is not None and state.active_order_id is not None
                        and update.order_id is not None and update.order_id != state.active_order_id):
                    matches = False
                if not matches:
                    return WorkflowChange(raw)
            elif event.payload.approved_signal is not None:
                if event.payload.approved_signal.symbol.upper() != symbol:
                    raise ValueError("Risk decision and signal symbols differ")

            repo = PlannedPositionRepository(state)
            buffer = BufferedPublisher()
            manager = PositionManager(repo, PositionKafkaEventBus(buffer, self.settings))
            processor = PositionEngineMessageProcessor(manager, recover_on_timeout=False)
            if event.event_type is EngineEventType.RISK_DECISION_MADE:
                processor.handle_risk_decision(event)
            else:
                processor.handle_order_update(event)

            client_id = None
            for _, output, _ in buffer.events:
                if output.event_type is EngineEventType.TRADE_ACTION_REQUESTED:
                    seed = f"{self.store.base}:{event.event_id}:{output.payload.action}"
                    client_id = "te-" + hashlib.sha256(seed.encode()).hexdigest()[:32]
            if client_id is not None and repo.state is not None:
                repo.state = replace(repo.state, active_client_order_id=client_id)

            events = []
            for topic, output, key in buffer.events:
                payload = output.payload
                if output.event_type is EngineEventType.TRADE_ACTION_REQUESTED:
                    payload = replace(payload, metadata={**payload.metadata, "newClientOrderId": client_id})
                elif output.event_type is EngineEventType.POSITION_STATE_CHANGED and client_id is not None:
                    payload = replace(payload, current=replace(payload.current, active_client_order_id=client_id))
                output = replace(output, payload=payload, correlation_id=event.correlation_id,
                                 causation_id=event.event_id)
                events.append((topic, output, key))
            return WorkflowChange(None if repo.state is None else RedisPositionRepository.encode(repo.state), events)

        self.store.transact(symbol, event.event_id, decide)
