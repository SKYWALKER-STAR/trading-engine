from __future__ import annotations

from collections.abc import Sequence

from trading_engine.infra.bus.base import EventBus
from trading_engine.strategy.interfaces import StrategyAlgorithm, StrategyRule
from trading_engine.strategy.models import StrategyInputContext, FactorStrategyContext, StrategyDecision
from trading_engine.strategy.price_exit import PriceExitPolicy
from trading_engine.strategy.rules import MarketDataFreshnessRule


class StrategyEngine:
    """Coordinates pre-check rules and strategy algorithm execution."""

    def __init__(
        self,
        algorithm: StrategyAlgorithm,
        rules: Sequence[StrategyRule],
        publisher: EventBus | None = None,
        signal_topic: str = "signal.generated",
        price_exit: PriceExitPolicy | None = None,
    ) -> None:
        self._price_exit = price_exit
        self._algorithm = algorithm
        self._rules = tuple(rules)
        self._publisher = publisher
        self._signal_topic = signal_topic

    def evaluate(self, context: StrategyInputContext) -> StrategyDecision:
        if self._price_exit is not None and self._price_exit.enabled:
            if not isinstance(context, FactorStrategyContext):
                return StrategyDecision.rejected(["price_exit_requires_factor_context"])
            # Exit decisions bypass entry confidence, but never freshness checks.
            for rule in self._rules:
                if isinstance(rule, MarketDataFreshnessRule):
                    passed, reason = rule.evaluate(context)
                    if not passed:
                        return StrategyDecision.rejected([reason or "market_data_invalid"])
            decision = self._price_exit.evaluate(context)
            if decision is not None:
                if decision.signal is not None and self._publisher is not None:
                    self._publisher.publish(self._signal_topic, decision.signal)
                return decision
        reasons: list[str] = []
        for rule in self._rules:
            passed, reason = rule.evaluate(context)
            if not passed and reason is not None:
                reasons.append(reason)

        if reasons:
            return StrategyDecision.rejected(reasons)

        signal = self._algorithm.generate(context)
        if signal is None:
            return StrategyDecision.rejected(["no_signal"])

        if self._publisher is not None:
            self._publisher.publish(self._signal_topic, signal)

        return StrategyDecision.accepted_signal(signal)
