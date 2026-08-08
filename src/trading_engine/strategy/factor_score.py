from __future__ import annotations

from trading_engine.strategy.interfaces import StrategyAlgorithm
from trading_engine.strategy.models import (
    FactorStrategyContext,
    SignalDirection,
    StrategyContext,
    StrategyInputContext,
    StrategySignal,
)


class FactorScoreStrategy(StrategyAlgorithm):
    """Score-based strategy for factor snapshots coming from ClickHouse."""

    def __init__(self, name: str = "factor_score", long_threshold: float = 0.2) -> None:
        self._name = name
        self._long_threshold = long_threshold

    def generate(self, context: StrategyInputContext) -> StrategySignal | None:
        if isinstance(context, StrategyContext):
            return None

        if not isinstance(context, FactorStrategyContext):
            return None

        snapshot = context.factor_snapshot
        score = (
            snapshot.score_ema
            + snapshot.score_dmi_adx
            + snapshot.score_rsi
            + snapshot.score_flow
            + snapshot.score_funding
        ) / 5.0

        if score > self._long_threshold:
            direction = SignalDirection.LONG
        elif score < -self._long_threshold:
            direction = SignalDirection.SHORT
        else:
            direction = SignalDirection.FLAT

        confidence = min(abs(score), 1.0)

        return StrategySignal(
            strategy_name=self._name,
            symbol=snapshot.symbol,
            direction=direction,
            score=score,
            confidence=confidence,
            timestamp=context.now,
            metadata={
                "interval": snapshot.interval,
                "rsi_14": snapshot.rsi_14,
                "adx_14": snapshot.adx_14,
                "score_ema": snapshot.score_ema,
                "score_dmi_adx": snapshot.score_dmi_adx,
                "score_rsi": snapshot.score_rsi,
                "score_flow": snapshot.score_flow,
                "score_funding": snapshot.score_funding,
            },
        )
