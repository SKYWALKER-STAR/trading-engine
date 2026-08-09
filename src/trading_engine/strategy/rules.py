from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from trading_engine.common.logger import configure_logging, get_logger

from trading_engine.strategy.interfaces import StrategyRule
from trading_engine.strategy.models import (
    FactorStrategyContext,
    StrategyContext,
    StrategyInputContext,
)


class MarketDataFreshnessRule(StrategyRule):
    def __init__(self, max_age_seconds: int) -> None:
        self.logger = get_logger(__name__)
        self._max_age = timedelta(seconds=max_age_seconds)
        self._cst = timezone(timedelta(hours=8))

    def _to_cst(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(self._cst)

    def evaluate(self, context: StrategyInputContext) -> tuple[bool, str | None]:
        if isinstance(context, FactorStrategyContext):
            data_time = context.factor_snapshot.open_time
        else:
            data_time = context.market_tick.timestamp

        self.logger.debug("Evaluating market data freshness rule: now=%s, data_time=%s", context.now, data_time)
        now_cst = self._to_cst(context.now)
        data_time_cst = self._to_cst(data_time)
        age = now_cst - data_time_cst
        if age <= self._max_age:
            return True, None
        return False, "market_data_stale"


class ConfidenceFeatureRule(StrategyRule):
    def __init__(self, min_confidence: float, feature_name: str = "confidence") -> None:
        self._min_confidence = min_confidence
        self._feature_name = feature_name

    def evaluate(self, context: StrategyInputContext) -> tuple[bool, str | None]:
        if isinstance(context, FactorStrategyContext):
            snapshot = context.factor_snapshot
            score_mean = (
                snapshot.score_ema
                + snapshot.score_dmi_adx
                + snapshot.score_rsi
                + snapshot.score_flow
                + snapshot.score_funding
            ) / 5.0
            confidence = min(abs(score_mean), 1.0)
        else:
            confidence = context.features.get(self._feature_name, 0.0)

        if confidence >= self._min_confidence:
            return True, None
        return False, "confidence_too_low"
