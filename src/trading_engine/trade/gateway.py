from __future__ import annotations

from typing import Protocol

from trading_engine.trade.models import TradeExecutionResult, TradeOrderRequest


class TradeExecutionGateway(Protocol):
    """Abstraction for venue-specific trade execution gateways."""

    def submit_order(self, request: TradeOrderRequest) -> TradeExecutionResult:
        ...