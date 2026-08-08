from __future__ import annotations

from collections.abc import Sequence

from trading_engine.infra.bus.base import EventBus
from trading_engine.strategy.interfaces import StrategyAlgorithm, StrategyRule
from trading_engine.strategy.models import StrategyContext, StrategyDecision


class StrategyEngine:
    """Coordinates pre-check rules and strategy algorithm execution."""

    def __init__(
        self,
        algorithm: StrategyAlgorithm,
        rules: Sequence[StrategyRule],
        publisher: EventBus | None = None,
        signal_topic: str = "signal.generated",
    ) -> None:
        self._algorithm = algorithm
        self._rules = tuple(rules)
        self._publisher = publisher
        self._signal_topic = signal_topic

    def evaluate(self, context: StrategyContext) -> StrategyDecision:
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
