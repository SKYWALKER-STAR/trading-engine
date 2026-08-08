from __future__ import annotations

from trading_engine.config.settings import StrategyEngineSettings
from trading_engine.infra.bus.base import EventBus
from trading_engine.strategy.engine import StrategyEngine
from trading_engine.strategy.interfaces import StrategyAlgorithm
from trading_engine.strategy.rules import ConfidenceFeatureRule, MarketDataFreshnessRule


def build_strategy_engine(
    algorithm: StrategyAlgorithm,
    settings: StrategyEngineSettings,
    publisher: EventBus | None = None,
) -> StrategyEngine:
    rules = (
        MarketDataFreshnessRule(max_age_seconds=settings.max_market_data_age_seconds),
        ConfidenceFeatureRule(min_confidence=settings.min_confidence),
    )
    return StrategyEngine(
        algorithm=algorithm,
        rules=rules,
        publisher=publisher,
        signal_topic=settings.signal_topic,
    )
