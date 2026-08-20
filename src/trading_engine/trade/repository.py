from __future__ import annotations

from typing import Protocol

from trading_engine.trade.models import TrackedOrder


class OrderIdentityConflictError(RuntimeError):
    """A client order ID was already bound to another exchange order ID."""


class OrderRepository(Protocol):
    """Persistent storage boundary for tracked exchange orders."""

    def get_by_order_id(
        self,
        *,
        exchange: str,
        account_id: str,
        symbol: str,
        order_id: str,
    ) -> TrackedOrder | None:
        ...

    def get_by_client_order_id(
        self,
        *,
        exchange: str,
        account_id: str,
        client_order_id: str,
    ) -> TrackedOrder | None:
        ...

    def save(self, order: TrackedOrder) -> None:
        """Upsert one order; establish a new exchange order ID through bind_order_id first."""
        ...

    def bind_order_id(
        self,
        *,
        exchange: str,
        account_id: str,
        client_order_id: str,
        symbol: str,
        order_id: str,
    ) -> TrackedOrder:
        """Atomically bind a client ID, raising on a conflicting existing binding."""
        ...

    def list_active(
        self,
        *,
        exchange: str,
        account_id: str,
        symbol: str | None = None,
    ) -> list[TrackedOrder]:
        ...
