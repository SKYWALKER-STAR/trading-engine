"""Opt-in execution-cost exits; percentages refer to price, not leveraged ROI."""
from dataclasses import dataclass
from decimal import Decimal

from trading_engine.common.execution_cost import decimal_value
from trading_engine.contracts.messages import SignalDirection
from trading_engine.position.models import PositionDirection, PositionLifecycle
from trading_engine.strategy.models import FactorStrategyContext, StrategyDecision, StrategySignal


@dataclass(frozen=True)
class PriceExitPolicy:
    take_profit_pct: str | None = None
    stop_loss_pct: str | None = None

    def __post_init__(self) -> None:
        for value in (self.take_profit_pct, self.stop_loss_pct):
            if value is not None and decimal_value(value) <= 0:
                raise ValueError("Price exit percentages must be finite and positive")

    @property
    def enabled(self) -> bool:
        return self.take_profit_pct is not None or self.stop_loss_pct is not None

    def evaluate(self, context: FactorStrategyContext) -> StrategyDecision | None:
        if not context.position_loaded:
            return StrategyDecision.rejected(["execution_position_unavailable"])
        state = context.position
        if state is None or state.lifecycle is PositionLifecycle.FLAT:
            return None  # original algorithm still manages entries
        if state.symbol.upper() != context.factor_snapshot.symbol.upper():
            return StrategyDecision.rejected(["execution_position_symbol_mismatch"])
        if state.lifecycle not in (PositionLifecycle.LONG, PositionLifecycle.SHORT):
            return StrategyDecision.rejected(["position_transition_in_progress"])
        if state.direction.value != state.lifecycle.value:
            return StrategyDecision.rejected(["invalid_position_direction"])
        if not state.cost_complete or state.entry_avg_price is None:
            return StrategyDecision.rejected(["position_cost_unknown"])
        try:
            entry = decimal_value(state.entry_avg_price)
            price = decimal_value(context.factor_snapshot.close)
            quantity = decimal_value(state.quantity)
        except ValueError:
            return StrategyDecision.rejected(["invalid_position_or_price"])
        if entry == 0 or price == 0 or quantity == 0:
            return StrategyDecision.rejected(["invalid_position_or_price"])
        sign = Decimal(1) if state.direction is PositionDirection.LONG else Decimal(-1)
        change = sign * (price - entry) / entry * 100
        reason = None
        if self.take_profit_pct is not None and change >= decimal_value(self.take_profit_pct):
            reason = "take_profit"
        elif self.stop_loss_pct is not None and change <= -decimal_value(self.stop_loss_pct):
            reason = "stop_loss"
        if reason is None:
            return StrategyDecision.rejected(["price_exit_not_triggered"])
        return StrategyDecision.accepted_signal(StrategySignal(
            strategy_name="execution_price_exit", symbol=state.symbol,
            direction=SignalDirection.FLAT, score=0, confidence=1, timestamp=context.now,
            metadata={"reason": reason, "price_change_pct": str(change),
                      "reference_price": str(price), "entry_avg_price": str(entry),
                      "position_opened_at": state.opened_at.isoformat() if state.opened_at else "",
                      "position_last_client_order_id": state.last_client_order_id or "",
                      "exit_policy": "position_price_pct"},
        ))
