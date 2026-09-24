from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from trading_engine.common.logger import get_logger
from trading_engine.contracts.messages import PositionSignalCommand, SignalDirection
from trading_engine.debug.dashboard import PositionDebugStore
from trading_engine.infra.bus.base import EventBus
from trading_engine.position.models import (
    OrderUpdateStatus,
    PositionDecision,
    PositionDirection,
    PositionLifecycle,
    PositionOrderEvent,
    PositionState,
    PositionStateChanged,
    TradeAction,
    TradeActionCreated,
    TradeActionFailed,
    TradeActionType,
    make_flat_position,
)
from trading_engine.position.repository import PositionRepository
from trading_engine.position.cost import apply_execution, clear_progress


LOGGER = get_logger(__name__)

class PositionManager:
    """Event-driven state machine that manages positions and emits trade actions."""

    def __init__(
        self,
        repository: PositionRepository,
        publisher: EventBus | None = None,
        state_topic: str = "position.state_changed",
        action_topic: str = "position.trade_action.created",
        failed_action_topic: str = "position.trade_action.failed",
        debug_store: PositionDebugStore | None = None,
    ) -> None:
        self._repository = repository
        self._publisher = publisher
        self._state_topic = state_topic
        self._action_topic = action_topic
        self._failed_action_topic = failed_action_topic
        self._debug_store = debug_store

    def handle_signal(self, signal: PositionSignalCommand) -> PositionDecision:
        current = self._load(signal.symbol, signal.timestamp)
        next_state = current
        trade_action: TradeAction | None = None
        reason = "signal_ignored"

        if signal.direction is SignalDirection.LONG:
            if current.lifecycle is PositionLifecycle.FLAT:
                next_state = self._transition(current, PositionLifecycle.OPEN_LONG, signal.timestamp, signal)
                trade_action = self._make_trade_action(next_state, TradeActionType.OPEN_LONG, "BUY", signal)
                reason = "signal_open_long"
            elif current.lifecycle is PositionLifecycle.SHORT:
                next_state = self._transition(current, PositionLifecycle.CLOSE_SHORT, signal.timestamp, signal)
                trade_action = self._make_trade_action(next_state, TradeActionType.CLOSE_SHORT, "BUY", signal)
                reason = "signal_close_short"
        elif signal.direction is SignalDirection.SHORT:
            if current.lifecycle is PositionLifecycle.FLAT:
                next_state = self._transition(current, PositionLifecycle.OPEN_SHORT, signal.timestamp, signal)
                trade_action = self._make_trade_action(next_state, TradeActionType.OPEN_SHORT, "SELL", signal)
                reason = "signal_open_short"
            elif current.lifecycle is PositionLifecycle.LONG:
                next_state = self._transition(current, PositionLifecycle.CLOSE_LONG, signal.timestamp, signal)
                trade_action = self._make_trade_action(next_state, TradeActionType.CLOSE_LONG, "SELL", signal)
                reason = "signal_close_long"
        else:
            if current.lifecycle is PositionLifecycle.LONG:
                next_state = self._transition(current, PositionLifecycle.CLOSE_LONG, signal.timestamp, signal)
                trade_action = self._make_trade_action(next_state, TradeActionType.CLOSE_LONG, "SELL", signal)
                reason = "signal_flat_close_long"
            elif current.lifecycle is PositionLifecycle.SHORT:
                next_state = self._transition(current, PositionLifecycle.CLOSE_SHORT, signal.timestamp, signal)
                trade_action = self._make_trade_action(next_state, TradeActionType.CLOSE_SHORT, "BUY", signal)
                reason = "signal_flat_close_short"

        return self._persist_and_publish(current, next_state, signal.timestamp, reason, trade_action)

    def handle_order_event(self, event: PositionOrderEvent) -> PositionDecision:
        current = self._load(event.symbol, event.updated_at)
        # The user stream may deliver a fill before the order-response NEW event.
        # Bind only through the client identity persisted with the trade action.
        if (event.order_id is not None
                and ((current.active_order_id is not None and event.order_id == current.active_order_id)
                     or (current.active_order_id is None and current.active_client_order_id is not None
                         and event.client_order_id == current.active_client_order_id))
                and event.status in (OrderUpdateStatus.FILLED, OrderUpdateStatus.PARTIALLY_FILLED)):
            transitions = {
                PositionLifecycle.OPEN_LONG: PositionLifecycle.OPENING_LONG,
                PositionLifecycle.OPEN_SHORT: PositionLifecycle.OPENING_SHORT,
                PositionLifecycle.CLOSE_LONG: PositionLifecycle.CLOSING_LONG,
                PositionLifecycle.CLOSE_SHORT: PositionLifecycle.CLOSING_SHORT,
            }
            current = self._update_state(
                current, lifecycle=transitions.get(current.lifecycle, current.lifecycle),
                updated_at=event.updated_at, active_order_id=event.order_id,
                active_client_order_id=current.active_client_order_id,
            )
        self._validate_order_id_before_position_change(current, event)
        order_id = event.order_id or current.active_order_id
        client_order_id = event.client_order_id or current.active_client_order_id
        next_state = current
        reason = "order_event_ignored"
        failed_action: TradeActionFailed | None = None

        if event.status is OrderUpdateStatus.NEW:
            if current.lifecycle is PositionLifecycle.OPEN_LONG:
                next_state = self._update_state(
                    current,
                    lifecycle=PositionLifecycle.OPENING_LONG,
                    updated_at=event.updated_at,
                    active_order_id=order_id,
                    active_client_order_id=client_order_id,
                )
                reason = "order_new_open_long"
            elif current.lifecycle is PositionLifecycle.OPEN_SHORT:
                next_state = self._update_state(
                    current,
                    lifecycle=PositionLifecycle.OPENING_SHORT,
                    updated_at=event.updated_at,
                    active_order_id=order_id,
                    active_client_order_id=client_order_id,
                )
                reason = "order_new_open_short"
            elif current.lifecycle is PositionLifecycle.CLOSE_LONG:
                next_state = self._update_state(
                    current,
                    lifecycle=PositionLifecycle.CLOSING_LONG,
                    updated_at=event.updated_at,
                    active_order_id=order_id,
                    active_client_order_id=client_order_id,
                )
                reason = "order_new_close_long"
            elif current.lifecycle is PositionLifecycle.CLOSE_SHORT:
                next_state = self._update_state(
                    current,
                    lifecycle=PositionLifecycle.CLOSING_SHORT,
                    updated_at=event.updated_at,
                    active_order_id=order_id,
                    active_client_order_id=client_order_id,
                )
                reason = "order_new_close_short"
        elif event.status in (OrderUpdateStatus.FILLED, OrderUpdateStatus.PARTIALLY_FILLED,
                              OrderUpdateStatus.CANCELED, OrderUpdateStatus.REJECTED):
            next_state = apply_execution(current, event)
            reason = f"order_{event.status.value}"
            if event.status in (OrderUpdateStatus.CANCELED, OrderUpdateStatus.REJECTED):
                failed_action = TradeActionFailed(
                    symbol=event.symbol, status=event.status.value, reason=reason,
                    occurred_at=event.updated_at, order_id=order_id,
                    state=next_state, metadata=dict(event.metadata),
                )

        return self._persist_and_publish(current, next_state, event.updated_at, reason, None, failed_action)

    @staticmethod
    def _validate_order_id_before_position_change(
        current: PositionState,
        event: PositionOrderEvent,
    ) -> None:
        """Reject an execution update unless it identifies the active order exactly."""
        execution_statuses = (
            OrderUpdateStatus.PARTIALLY_FILLED,
            OrderUpdateStatus.FILLED,
        )
        terminal_statuses = (
            OrderUpdateStatus.CANCELED,
            OrderUpdateStatus.REJECTED,
        )
        if event.status not in execution_statuses + terminal_statuses:
            return

        # A rejection can occur before Binance assigns an exchange order ID.
        # In that case there is no active exchange identity to validate.
        if event.status in terminal_statuses and current.active_order_id is None:
            return
        if event.order_id is None:
            raise ValueError(
                f"{event.status.value} update for {event.symbol} is missing order_id"
            )
        if current.active_order_id is None:
            raise ValueError(
                f"{event.status.value} update for {event.symbol} has no active order to match"
            )
        if event.order_id != current.active_order_id:
            raise ValueError(
                f"Order update for {event.symbol} belongs to {event.order_id}, "
                f"but active order is {current.active_order_id}"
            )

    def recover_stale_transition(
        self,
        symbol: str,
        now: datetime,
        timeout_seconds: float,
    ) -> PositionDecision | None:
        current = self._load(symbol, now)
        transitional_states = {
            PositionLifecycle.OPEN_LONG,
            PositionLifecycle.OPENING_LONG,
            PositionLifecycle.OPEN_SHORT,
            PositionLifecycle.OPENING_SHORT,
            PositionLifecycle.CLOSE_LONG,
            PositionLifecycle.CLOSING_LONG,
            PositionLifecycle.CLOSE_SHORT,
            PositionLifecycle.CLOSING_SHORT,
        }
        if current.lifecycle not in transitional_states or current.updated_at is None:
            return None

        age_seconds = (now - current.updated_at).total_seconds()
        if age_seconds < timeout_seconds:
            return None

        next_state = self._rollback(current, now)
        reason = "order_timeout"
        failure_event = TradeActionFailed(
            symbol=symbol,
            status="timed_out",
            reason=reason,
            occurred_at=now,
            order_id=current.active_order_id,
            state=current,
            metadata={"timeout_seconds": float(timeout_seconds)},
        )
        return self._persist_and_publish(current, next_state, now, reason, None, failure_event)

    def _load(self, symbol: str, now: datetime) -> PositionState:
        return self._repository.get(symbol) or make_flat_position(symbol, now)

    @staticmethod
    def _transition(
        current: PositionState,
        lifecycle: PositionLifecycle,
        now: datetime,
        signal: PositionSignalCommand,
    ) -> PositionState:
        metadata = clear_progress(current.metadata)
        return replace(
            current,
            direction=current.direction,
            lifecycle=lifecycle,
            quantity=current.quantity,
            active_order_id=current.active_order_id,
            active_client_order_id=current.active_client_order_id,
            last_order_id=current.last_order_id,
            last_client_order_id=current.last_client_order_id,
            updated_at=now,
            metadata={
                **metadata,
                "signal_direction": signal.direction.value,
                "signal_score": signal.score,
            },
        )

    @staticmethod
    def _update_state(
        current: PositionState,
        *,
        direction: PositionDirection | None = None,
        lifecycle: PositionLifecycle | None = None,
        quantity: float | None = None,
        updated_at: datetime,
        active_order_id: str | None = None,
        active_client_order_id: str | None = None,
        last_order_id: str | None = None,
        last_client_order_id: str | None = None,
        metadata: dict[str, str | float] | None = None,
    ) -> PositionState:
        return replace(
            current,
            direction=direction or current.direction,
            lifecycle=lifecycle or current.lifecycle,
            quantity=current.quantity if quantity is None else quantity,
            active_order_id=active_order_id,
            active_client_order_id=active_client_order_id,
            last_order_id=current.last_order_id if last_order_id is None else last_order_id,
            last_client_order_id=(
                current.last_client_order_id
                if last_client_order_id is None
                else last_client_order_id
            ),
            updated_at=updated_at,
            metadata=dict(current.metadata) if metadata is None else dict(metadata),
        )

    @staticmethod
    def _rollback(
        current: PositionState,
        updated_at: datetime,
        *,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> PositionState:
        return apply_execution(current, PositionOrderEvent(
            symbol=current.symbol, status=OrderUpdateStatus.CANCELED,
            updated_at=updated_at, order_id=order_id or current.active_order_id,
            client_order_id=client_order_id or current.active_client_order_id,
        ))

    @staticmethod
    def _make_trade_action(
        state: PositionState,
        action_type: TradeActionType,
        side: str,
        signal: PositionSignalCommand,
    ) -> TradeAction:
        approved_quantity = signal.metadata.get("approved_quantity")
        return TradeAction(
            symbol=state.symbol,
            action_type=action_type,
            side=side,
            created_at=signal.timestamp,
            quantity=((state.quantity if approved_quantity is None
                       else min(state.quantity, float(approved_quantity)))
                      if action_type in (TradeActionType.CLOSE_LONG, TradeActionType.CLOSE_SHORT)
                      else approved_quantity if approved_quantity is not None else state.quantity),
            signal=signal,
            metadata={
                **signal.metadata,
                #"reduceOnly": "true" if action_type in (TradeActionType.CLOSE_LONG, TradeActionType.CLOSE_SHORT) else "false",
                "lifecycle": state.lifecycle.value,
                "signal_direction": signal.direction.value,
            },
        )

    def _persist_and_publish(
        self,
        previous: PositionState,
        current: PositionState,
        occurred_at: datetime,
        reason: str,
        trade_action: TradeAction | None,
        failed_action: TradeActionFailed | None = None,
    ) -> PositionDecision:
        self._repository.save(current)

        events: list[PositionStateChanged | TradeActionCreated | TradeActionFailed] = []
        if current != previous:
            state_event = PositionStateChanged(
                previous=previous,
                current=current,
                occurred_at=occurred_at,
                reason=reason,
            )
            events.append(state_event)
            if self._debug_store is not None:
                self._debug_store.record_transition(
                    symbol=current.symbol,
                    previous_lifecycle=previous.lifecycle.value,
                    current_lifecycle=current.lifecycle.value,
                    reason=reason,
                    occurred_at=occurred_at,
                    direction=current.direction.value,
                    previous_direction=previous.direction.value,
                    quantity=current.quantity,
                    previous_quantity=previous.quantity,
                    active_order_id=current.active_order_id,
                    metadata={
                        "previous_direction": previous.direction.value,
                        "current_direction": current.direction.value,
                        "previous_quantity": previous.quantity,
                        "current_quantity": current.quantity,
                    },
                )
            if self._publisher is not None:
                self._publisher.publish(self._state_topic, state_event)

        if trade_action is not None:
            action_event = TradeActionCreated(
                action=trade_action,
                state=current,
                occurred_at=occurred_at,
            )
            events.append(action_event)
            if self._publisher is not None:
                LOGGER.info("Publishing trade action created event: %s", action_event)
                self._publisher.publish(self._action_topic, action_event)

        if failed_action is not None:
            events.append(failed_action)
            if self._publisher is not None:
                LOGGER.info("Publishing trade action failed event: %s", failed_action)
                self._publisher.publish(self._failed_action_topic, failed_action)

        return PositionDecision(state=current, trade_action=trade_action, events=tuple(events))
