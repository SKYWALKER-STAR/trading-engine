from __future__ import annotations

from trading_engine.strategy.interfaces import StrategyAlgorithm
from trading_engine.strategy.models import SignalDirection, StrategyContext, StrategySignal


class SimpleMomentumStrategy(StrategyAlgorithm):
    """Sample strategy used for local bootstrap and tests."""

    def __init__(self, name: str = "simple_momentum", threshold: float = 0.001) -> None:
        self._name = name
        self._threshold = threshold

    def generate(self, context: StrategyContext) -> StrategySignal | None:
        reference_price = context.features.get("reference_price")
        confidence = context.features.get("confidence", 0.0)
        if reference_price is None:
            return None

        delta = (context.market_tick.mid_price - reference_price) / reference_price
        if abs(delta) < self._threshold:
            direction = SignalDirection.FLAT
        elif delta > 0:
            direction = SignalDirection.LONG
        else:
            direction = SignalDirection.SHORT

        return StrategySignal(
            strategy_name=self._name,
            symbol=context.market_tick.symbol,
            direction=direction,
            score=delta,
            confidence=confidence,
            timestamp=context.now,
            metadata={"reference_price": reference_price},
        )
