from __future__ import annotations

from typing import Protocol

from trading_engine.strategy.models import StrategyInputContext, StrategySignal


class StrategyAlgorithm(Protocol):
    """Algorithm contract that turns context into a signal."""

    def generate(self, context: StrategyInputContext) -> StrategySignal | None:
        ...


class StrategyRule(Protocol):
    """Validation rule contract evaluated before signal emission."""

    def evaluate(self, context: StrategyInputContext) -> tuple[bool, str | None]:
        ...
