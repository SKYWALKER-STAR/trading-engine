from __future__ import annotations

from collections.abc import Sequence

from trading_engine.common.logger import get_logger
from trading_engine.infra.bus.base import EventBus
from trading_engine.strategy.interfaces import StrategyAlgorithm, StrategyRule
from trading_engine.strategy.models import StrategyInputContext, FactorStrategyContext, StrategyDecision
from trading_engine.strategy.price_exit import PriceExitPolicy
from trading_engine.strategy.rules import MarketDataFreshnessRule


LOGGER = get_logger(__name__)


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
        symbol = (context.factor_snapshot.symbol if isinstance(context, FactorStrategyContext)
                  else context.market_tick.symbol)
        price_exit_enabled = self._price_exit is not None and self._price_exit.enabled
        LOGGER.info(
            "evaluate started: symbol=%s context=%s price_exit_enabled=%s rules=%s",
            symbol, type(context).__name__, price_exit_enabled, len(self._rules),
        )
        if self._price_exit is not None and self._price_exit.enabled:
            if not isinstance(context, FactorStrategyContext):
                LOGGER.info("evaluate rejected: symbol=%s reason=price_exit_requires_factor_context", symbol)
                return StrategyDecision.rejected(["price_exit_requires_factor_context"])
            # Exit decisions bypass entry confidence, but never freshness checks.
            for rule in self._rules:
                if isinstance(rule, MarketDataFreshnessRule):
                    LOGGER.info("evaluate price-exit freshness check started: symbol=%s rule=%s",
                                symbol, type(rule).__name__)
                    passed, reason = rule.evaluate(context)
                    LOGGER.info("evaluate price-exit freshness check finished: symbol=%s passed=%s reason=%s",
                                symbol, passed, reason)
                    if not passed:
                        LOGGER.info("evaluate rejected: symbol=%s reason=%s",
                                    symbol, reason or "market_data_invalid")
                        return StrategyDecision.rejected([reason or "market_data_invalid"])
            LOGGER.info(
                "evaluate price-exit policy started: symbol=%s position_loaded=%s lifecycle=%s "
                "entry_avg_price=%s cost_complete=%s reference_close=%s",
                symbol, context.position_loaded,
                context.position.lifecycle.value if context.position else None,
                context.position.entry_avg_price if context.position else None,
                context.position.cost_complete if context.position else None,
                context.factor_snapshot.close,
            )
            decision = self._price_exit.evaluate(context)
            if decision is not None:
                LOGGER.info("evaluate price-exit policy finished: symbol=%s direction=%s reasons=%s",
                            symbol, decision.signal.direction.value if decision.signal else None,
                            decision.signal.metadata.get("reason") if decision.signal else decision.rejected_reasons)
                if decision.signal is not None and self._publisher is not None:
                    LOGGER.info("evaluate publishing signal: symbol=%s topic=%s", symbol, self._signal_topic)
                    self._publisher.publish(self._signal_topic, decision.signal)
                    LOGGER.info("evaluate signal published: symbol=%s topic=%s", symbol, self._signal_topic)
                LOGGER.info("evaluate finished: symbol=%s path=price_exit accepted=%s publisher_configured=%s",
                            symbol, decision.signal is not None, self._publisher is not None)
                return decision
            LOGGER.info("evaluate price-exit policy deferred to entry strategy: symbol=%s", symbol)
        reasons: list[str] = []
        for rule in self._rules:
            LOGGER.info("evaluate rule started: symbol=%s rule=%s", symbol, type(rule).__name__)
            passed, reason = rule.evaluate(context)
            LOGGER.info("evaluate rule finished: symbol=%s rule=%s passed=%s reason=%s",
                        symbol, type(rule).__name__, passed, reason)
            if not passed and reason is not None:
                reasons.append(reason)

        if reasons:
            LOGGER.info("evaluate rejected: symbol=%s reasons=%s", symbol, reasons)
            return StrategyDecision.rejected(reasons)

        LOGGER.info("evaluate algorithm started: symbol=%s algorithm=%s", symbol, type(self._algorithm).__name__)
        signal = self._algorithm.generate(context)
        if signal is None:
            LOGGER.info("evaluate rejected: symbol=%s reason=no_signal", symbol)
            return StrategyDecision.rejected(["no_signal"])

        LOGGER.info("evaluate algorithm finished: symbol=%s direction=%s reason=%s",
                    symbol, signal.direction.value, signal.metadata.get("reason"))
        if self._publisher is not None:
            LOGGER.info("evaluate publishing signal: symbol=%s topic=%s", symbol, self._signal_topic)
            self._publisher.publish(self._signal_topic, signal)
            LOGGER.info("evaluate signal published: symbol=%s topic=%s", symbol, self._signal_topic)

        LOGGER.info("evaluate finished: symbol=%s path=algorithm accepted=true publisher_configured=%s",
                    symbol, self._publisher is not None)
        return StrategyDecision.accepted_signal(signal)
