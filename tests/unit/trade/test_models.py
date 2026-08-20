from __future__ import annotations

from datetime import UTC, datetime

from trading_engine.trade.models import TrackedOrder, TrackedOrderStatus


def test_tracked_order_represents_pending_submit_before_exchange_id_is_known() -> None:
    now = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)

    order = TrackedOrder(
        exchange="binance",
        account_id="main",
        symbol="BTCUSDT",
        client_order_id="te-client-1",
        side="BUY",
        order_type="MARKET",
        original_quantity=0.1,
        status=TrackedOrderStatus.PENDING_SUBMIT,
        created_at=now,
        updated_at=now,
    )

    assert order.order_id is None
    assert order.cumulative_filled_quantity == 0.0
    assert not order.status.is_terminal


def test_tracked_order_terminal_statuses_are_explicit() -> None:
    assert TrackedOrderStatus.FILLED.is_terminal
    assert TrackedOrderStatus.CANCELED.is_terminal
    assert TrackedOrderStatus.REJECTED.is_terminal
    assert TrackedOrderStatus.EXPIRED.is_terminal
    assert not TrackedOrderStatus.UNKNOWN.is_terminal
    assert not TrackedOrderStatus.PARTIALLY_FILLED.is_terminal
