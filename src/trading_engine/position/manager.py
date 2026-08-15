from __future__ import annotations

from datetime import datetime

from trading_engine.contracts.messages import PositionSignalCommand, SignalDirection
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


class PositionManager:
    """Event-driven state machine that manages positions and emits trade actions."""

    def __init__(
        self,
        repository: PositionRepository,
        publisher: EventBus | None = None,
        state_topic: str = "position.state_changed",
        action_topic: str = "position.trade_action.created",
        failed_action_topic: str = "position.trade_action.failed",
    ) -> None:
        self._repository = repository
        self._publisher = publisher
        self._state_topic = state_topic
        self._action_topic = action_topic
        self._failed_action_topic = failed_action_topic

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
        next_state = current
        reason = "order_event_ignored"
        failed_action: TradeActionFailed | None = None

        if event.status is OrderUpdateStatus.NEW:
            if current.lifecycle is PositionLifecycle.OPEN_LONG:
                next_state = self._update_state(
                    current,
                    lifecycle=PositionLifecycle.OPENING_LONG,
                    updated_at=event.updated_at,
                    active_order_id=event.order_id,
                )
                reason = "order_new_open_long"
            elif current.lifecycle is PositionLifecycle.OPEN_SHORT:
                next_state = self._update_state(
                    current,
                    lifecycle=PositionLifecycle.OPENING_SHORT,
                    updated_at=event.updated_at,
                    active_order_id=event.order_id,
                )
                reason = "order_new_open_short"
            elif current.lifecycle is PositionLifecycle.CLOSE_LONG:
                next_state = self._update_state(
                    current,
                    lifecycle=PositionLifecycle.CLOSING_LONG,
                    updated_at=event.updated_at,
                    active_order_id=event.order_id,
                )
                reason = "order_new_close_long"
            elif current.lifecycle is PositionLifecycle.CLOSE_SHORT:
                next_state = self._update_state(
                    current,
                    lifecycle=PositionLifecycle.CLOSING_SHORT,
                    updated_at=event.updated_at,
                    active_order_id=event.order_id,
                )
                reason = "order_new_close_short"
        elif event.status is OrderUpdateStatus.FILLED:
            quantity = event.filled_quantity if event.filled_quantity is not None else current.quantity
            if current.lifecycle is PositionLifecycle.OPENING_LONG:
                next_state = self._update_state(
                    current,
                    direction=PositionDirection.LONG,
                    lifecycle=PositionLifecycle.LONG,
                    quantity=quantity,
                    updated_at=event.updated_at,
                    active_order_id=event.order_id,
                )
                reason = "order_filled_open_long"
            elif current.lifecycle is PositionLifecycle.OPENING_SHORT:
                next_state = self._update_state(
                    current,
                    direction=PositionDirection.SHORT,
                    lifecycle=PositionLifecycle.SHORT,
                    quantity=quantity,
                    updated_at=event.updated_at,
                    active_order_id=event.order_id,
                )
                reason = "order_filled_open_short"
            elif current.lifecycle is PositionLifecycle.CLOSING_LONG:
                next_state = make_flat_position(current.symbol, event.updated_at)
                reason = "order_filled_close_long"
            elif current.lifecycle is PositionLifecycle.CLOSING_SHORT:
                next_state = make_flat_position(current.symbol, event.updated_at)
                reason = "order_filled_close_short"
        elif event.status is OrderUpdateStatus.PARTIALLY_FILLED:
            filled_quantity = event.filled_quantity if event.filled_quantity is not None else current.quantity
            if current.lifecycle is PositionLifecycle.OPENING_LONG:
                next_state = self._update_state(
                    current,
                    direction=PositionDirection.LONG,
                    lifecycle=PositionLifecycle.OPENING_LONG,
                    quantity=filled_quantity,
                    updated_at=event.updated_at,
                    active_order_id=event.order_id,
                )
                reason = "order_partially_filled_open_long"
            elif current.lifecycle is PositionLifecycle.OPENING_SHORT:
                next_state = self._update_state(
                    current,
                    direction=PositionDirection.SHORT,
                    lifecycle=PositionLifecycle.OPENING_SHORT,
                    quantity=filled_quantity,
                    updated_at=event.updated_at,
                    active_order_id=event.order_id,
                )
                reason = "order_partially_filled_open_short"
            elif current.lifecycle is PositionLifecycle.CLOSING_LONG:
                remaining_quantity = max(current.quantity - filled_quantity, 0.0)
                next_state = self._update_state(
                    current,
                    direction=PositionDirection.LONG,
                    lifecycle=PositionLifecycle.CLOSING_LONG,
                    quantity=remaining_quantity,
                    updated_at=event.updated_at,
                    active_order_id=event.order_id,
                )
                reason = "order_partially_filled_close_long"
            elif current.lifecycle is PositionLifecycle.CLOSING_SHORT:
                remaining_quantity = max(current.quantity - filled_quantity, 0.0)
                next_state = self._update_state(
                    current,
                    direction=PositionDirection.SHORT,
                    lifecycle=PositionLifecycle.CLOSING_SHORT,
                    quantity=remaining_quantity,
                    updated_at=event.updated_at,
                    active_order_id=event.order_id,
                )
                reason = "order_partially_filled_close_short"
        elif event.status in (OrderUpdateStatus.CANCELED, OrderUpdateStatus.REJECTED):
            next_state = self._rollback(current, event.updated_at)
            reason = f"order_{event.status.value}"
            failed_action = TradeActionFailed(
                symbol=event.symbol,
                status=event.status.value,
                reason=reason,
                occurred_at=event.updated_at,
                order_id=event.order_id,
                state=current,
                metadata=dict(event.metadata),
            )

        return self._persist_and_publish(current, next_state, event.updated_at, reason, None, failed_action)

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
        return PositionState(
            symbol=current.symbol,
            direction=current.direction,
            lifecycle=lifecycle,
            quantity=current.quantity,
            active_order_id=current.active_order_id,
            updated_at=now,
            metadata={
                **current.metadata,
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
    ) -> PositionState:
        return PositionState(
            symbol=current.symbol,
            direction=direction or current.direction,
            lifecycle=lifecycle or current.lifecycle,
            quantity=current.quantity if quantity is None else quantity,
            active_order_id=active_order_id,
            updated_at=updated_at,
            metadata=dict(current.metadata),
        )

    @staticmethod
    def _rollback(current: PositionState, updated_at: datetime) -> PositionState:
        if current.lifecycle in (PositionLifecycle.OPEN_LONG, PositionLifecycle.OPENING_LONG):
            return make_flat_position(current.symbol, updated_at)
        if current.lifecycle in (PositionLifecycle.OPEN_SHORT, PositionLifecycle.OPENING_SHORT):
            return make_flat_position(current.symbol, updated_at)
        if current.lifecycle in (PositionLifecycle.CLOSE_LONG, PositionLifecycle.CLOSING_LONG):
            return PositionState(
                symbol=current.symbol,
                direction=PositionDirection.LONG,
                lifecycle=PositionLifecycle.LONG,
                quantity=current.quantity,
                updated_at=updated_at,
                metadata=dict(current.metadata),
            )
        if current.lifecycle in (PositionLifecycle.CLOSE_SHORT, PositionLifecycle.CLOSING_SHORT):
            return PositionState(
                symbol=current.symbol,
                direction=PositionDirection.SHORT,
                lifecycle=PositionLifecycle.SHORT,
                quantity=current.quantity,
                updated_at=updated_at,
                metadata=dict(current.metadata),
            )
        return PositionState(
            symbol=current.symbol,
            direction=current.direction,
            lifecycle=current.lifecycle,
            quantity=current.quantity,
            active_order_id=None,
            updated_at=updated_at,
            metadata=dict(current.metadata),
        )

    @staticmethod
    def _make_trade_action(
        state: PositionState,
        action_type: TradeActionType,
        side: str,
        signal: PositionSignalCommand,
    ) -> TradeAction:
        return TradeAction(
            symbol=state.symbol,
            action_type=action_type,
            side=side,
            created_at=signal.timestamp,
            signal=signal,
            metadata={
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
                self._publisher.publish(self._action_topic, action_event)

        if failed_action is not None:
            events.append(failed_action)
            if self._publisher is not None:
                self._publisher.publish(self._failed_action_topic, failed_action)

        return PositionDecision(state=current, trade_action=trade_action, events=tuple(events))