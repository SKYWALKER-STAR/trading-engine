"""Apply cumulative execution facts without inventing a cost for legacy holdings."""
from dataclasses import replace
from decimal import Decimal

from trading_engine.common.execution_cost import decimal_text, decimal_value
from trading_engine.position.models import (
    OrderUpdateStatus, PositionDirection, PositionLifecycle, PositionOrderEvent, PositionState,
    make_flat_position,
)


def clear_progress(metadata: dict[str, str | float]) -> dict[str, str | float]:
    return {key: value for key, value in metadata.items() if not key.startswith("active_order_")}


def apply_execution(state: PositionState, event: PositionOrderEvent) -> PositionState:
    opening = state.lifecycle in (
        PositionLifecycle.OPEN_LONG, PositionLifecycle.OPENING_LONG,
        PositionLifecycle.OPEN_SHORT, PositionLifecycle.OPENING_SHORT,
    )
    closing = state.lifecycle in (
        PositionLifecycle.CLOSE_LONG, PositionLifecycle.CLOSING_LONG,
        PositionLifecycle.CLOSE_SHORT, PositionLifecycle.CLOSING_SHORT,
    )
    if not (opening or closing):
        return state
    long = state.lifecycle in (
        PositionLifecycle.OPEN_LONG, PositionLifecycle.OPENING_LONG,
        PositionLifecycle.CLOSE_LONG, PositionLifecycle.CLOSING_LONG,
    )
    metadata = dict(state.metadata)
    previous = decimal_value(metadata.get("active_order_cumulative_filled", "0"))
    raw = event.cumulative_filled_quantity
    if raw is None:
        raw = event.filled_quantity  # legacy alias is cumulative, not a new fill
    if raw is None:
        if event.status is OrderUpdateStatus.FILLED:
            raise ValueError("Filled order requires cumulative filled quantity")
        # Quantity-only delta events cannot safely be replayed without a ledger.
        if event.last_filled_quantity is not None:
            raise ValueError("Execution update requires cumulative filled quantity")
        cumulative = previous
    else:
        cumulative = decimal_value(raw)
    if cumulative < previous:
        return state  # do not let a stale terminal report discard newer execution
    delta = cumulative - previous
    old_quantity = decimal_value(state.quantity)
    quantity = old_quantity + delta if opening else max(old_quantity - delta, Decimal(0))
    quote = None
    if event.cumulative_filled_quote is not None:
        quote = decimal_value(event.cumulative_filled_quote)
        if cumulative > 0 and quote == 0:
            quote = None
    old_quote = metadata.get("active_order_cumulative_quote")
    if quote is None and delta == 0 and old_quote is not None:
        quote = decimal_value(old_quote)
    if (quote is None and delta > 0 and event.last_filled_price is not None
            and event.last_filled_quantity is not None
            and decimal_value(event.last_filled_quantity) == delta
            and (previous == 0 or old_quote is not None)):
        price = decimal_value(event.last_filled_price)
        if price > 0:
            quote = decimal_value(old_quote or "0") + delta * price
    if quote is not None and old_quote is not None and quote < decimal_value(old_quote):
        return state

    average, complete = state.entry_avg_price, state.cost_complete
    opened = state.opened_at
    if opening:
        # Preserve the pre-order cost so a later cumulative quote can fill a gap.
        if "active_order_base_quantity" not in metadata:
            base = max(old_quantity - previous, Decimal(0))
            metadata["active_order_base_quantity"] = str(base)
            if base == 0:
                metadata["active_order_base_cost"] = "0"
            elif previous == 0 and state.cost_complete and state.entry_avg_price is not None:
                metadata["active_order_base_cost"] = str(base * decimal_value(state.entry_avg_price))
        base_cost = metadata.get("active_order_base_cost")
        if cumulative > 0:
            if quote is not None and base_cost is not None:
                average = decimal_text((decimal_value(base_cost) + quote) / quantity)
                complete = True
            else:
                average, complete = None, False
        if old_quantity == 0 and quantity > 0:
            opened = event.updated_at
    if quantity == 0:
        average, complete, opened = None, False, None
    metadata["active_order_cumulative_filled"] = str(cumulative)
    if quote is None:
        metadata.pop("active_order_cumulative_quote", None)
    else:
        metadata["active_order_cumulative_quote"] = str(quote)
    terminal = event.status in (
        OrderUpdateStatus.FILLED, OrderUpdateStatus.CANCELED, OrderUpdateStatus.REJECTED,
    )
    order_id = event.order_id or state.active_order_id
    client_id = event.client_order_id or state.active_client_order_id
    if terminal and quantity == 0:
        return make_flat_position(state.symbol, event.updated_at,
                                  last_order_id=order_id, last_client_order_id=client_id)
    direction = PositionDirection.LONG if long else PositionDirection.SHORT
    if terminal:
        lifecycle = PositionLifecycle.LONG if long else PositionLifecycle.SHORT
    elif opening:
        lifecycle = PositionLifecycle.OPENING_LONG if long else PositionLifecycle.OPENING_SHORT
    else:
        lifecycle = PositionLifecycle.CLOSING_LONG if long else PositionLifecycle.CLOSING_SHORT
    return replace(
        state, quantity=float(quantity), entry_avg_price=average, cost_complete=complete,
        opened_at=opened, direction=direction, lifecycle=lifecycle, updated_at=event.updated_at,
        active_order_id=None if terminal else order_id,
        active_client_order_id=None if terminal else client_id,
        last_order_id=order_id if terminal else state.last_order_id,
        last_client_order_id=client_id if terminal else state.last_client_order_id,
        metadata=clear_progress(metadata) if terminal else metadata,
    )
