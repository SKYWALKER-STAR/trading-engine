from __future__ import annotations

from collections import defaultdict, deque
'''from collections.abc import Deque'''
from typing import Deque

from trading_engine.common.logger import get_logger
from trading_engine.domain.market_data import MarketFactorSnapshot
from trading_engine.strategy.interfaces import StrategyAlgorithm
from trading_engine.strategy.models import (
    FactorStrategyContext,
    SignalDirection,
    StrategyContext,
    StrategyInputContext,
    StrategySignal,
)

LOGGER = get_logger(__name__)


class FactorScoreStrategy(StrategyAlgorithm):
    """Rule-driven strategy based on 1m trending score entry/exit table."""

    def __init__(self, name: str = "factor_score") -> None:
        self._name = name
        self._position_by_symbol: dict[str, SignalDirection] = {}
        self._trend_history: dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=3))

    def generate(self, context: StrategyInputContext) -> StrategySignal | None:
        LOGGER.info(
            "generate start: context_type=%s context_now=%s position_loaded=%s position=%s",
            type(context).__name__,
            getattr(context, "now", None),
            getattr(context, "position_loaded", None),
            getattr(getattr(context, "position", None), "direction", None),
        )

        if isinstance(context, StrategyContext):
            LOGGER.info("generate rejected: context_type=%s reason=non_factor_context", type(context).__name__)
            return None

        if not isinstance(context, FactorStrategyContext):
            LOGGER.info("generate rejected: context_type=%s reason=invalid_factor_context", type(context).__name__)
            return None

        snapshot = context.factor_snapshot
        trend = snapshot.trend_score_p
        history = self._trend_history[snapshot.symbol]
        history.append(trend)

        position = (SignalDirection.FLAT if context.position is None
                    else SignalDirection(context.position.direction.value)) if context.position_loaded else (
                        self._position_by_symbol.get(snapshot.symbol, SignalDirection.FLAT))

        LOGGER.info(
            "generate factor snapshot: symbol=%s trend_score_p=%s close=%s ema_12=%s ema_26=%s adx_14=%s rsi_14=%s "
            "score_ema=%s score_dmi_adx=%s score_rsi=%s score_flow=%s score_funding=%s position=%s current_history=%s",
            snapshot.symbol,
            snapshot.trend_score_p,
            snapshot.close,
            snapshot.ema_12,
            snapshot.ema_26,
            snapshot.adx_14,
            snapshot.rsi_14,
            snapshot.score_ema,
            snapshot.score_dmi_adx,
            snapshot.score_rsi,
            snapshot.score_flow,
            snapshot.score_funding,
            position,
            list(history),
        )

        if position == SignalDirection.LONG:
            if self._should_exit_long(snapshot, history):
                direction = SignalDirection.FLAT
                reason = "exit_long"
            else:
                direction = SignalDirection.LONG
                reason = "hold_long"
        elif position == SignalDirection.SHORT:
            if self._should_exit_short(snapshot, history):
                direction = SignalDirection.FLAT
                reason = "exit_short"
            else:
                direction = SignalDirection.SHORT
                reason = "hold_short"
        elif self._is_filtered(snapshot):
            direction = SignalDirection.FLAT
            reason = "filtered"
        elif self._should_enter_long(snapshot, history):
            direction = SignalDirection.LONG
            reason = "entry_long"
        elif self._should_enter_short(snapshot, history):
            direction = SignalDirection.SHORT
            reason = "entry_short"
        else:
            direction = SignalDirection.FLAT
            reason = "no_signal"

        self._position_by_symbol[snapshot.symbol] = direction

        score = trend / 100.0
        confidence = min(abs(score), 1.0)
        return StrategySignal(
            strategy_name=self._name,
            symbol=snapshot.symbol,
            direction=direction,
            score=score,
            confidence=confidence,
            timestamp=context.now,
            metadata={
                "reason": reason,
                "interval": snapshot.interval,
                "close": snapshot.close,
                "trend_score_p": snapshot.trend_score_p,
                "rsi_14": snapshot.rsi_14,
                "adx_14": snapshot.adx_14,
                "score_ema": snapshot.score_ema,
                "score_dmi_adx": snapshot.score_dmi_adx,
                "score_rsi": snapshot.score_rsi,
                "score_flow": snapshot.score_flow,
                "score_funding": snapshot.score_funding,
            },
        )

    @staticmethod
    def _is_filtered(snapshot: MarketFactorSnapshot) -> bool:
        if abs(snapshot.trend_score_p) < 20:
            return True
        if snapshot.adx_14 < 15:
            return True
        if snapshot.score_ema * snapshot.score_dmi_adx < 0:
            return True
        return False

    @staticmethod
    def _has_two_bar_persistence(history: Deque[float], threshold: float, is_long: bool) -> bool:
        if len(history) < 2:
            return False
        current = history[-1]
        previous = history[-2]
        if is_long:
            return current >= threshold and previous >= threshold
        return current <= threshold and previous <= threshold

    def _should_enter_long(self, snapshot: MarketFactorSnapshot, history: Deque[float]) -> bool:
        if not self._has_two_bar_persistence(history, threshold=50, is_long=True):
            return False
        if snapshot.score_ema <= 0 or snapshot.score_dmi_adx <= 0:
            return False
        if not (snapshot.close > snapshot.ema_12 > snapshot.ema_26):
            return False
        if snapshot.adx_14 < 20:
            return False
        if snapshot.score_rsi < 0:
            return False
        if snapshot.score_flow < -5:
            return False
        if snapshot.score_funding < 0:
            return False
        return True

    def _should_enter_short(self, snapshot: MarketFactorSnapshot, history: Deque[float]) -> bool:
        if not self._has_two_bar_persistence(history, threshold=-50, is_long=False):
            return False
        if snapshot.score_ema >= 0 or snapshot.score_dmi_adx >= 0:
            return False
        if not (snapshot.close < snapshot.ema_12 < snapshot.ema_26):
            return False
        if snapshot.adx_14 < 20:
            return False
        if snapshot.score_rsi > 0:
            return False
        if snapshot.score_flow > 5:
            return False
        if snapshot.score_funding > 0:
            return False
        return True

    @staticmethod
    def _should_exit_long(snapshot: MarketFactorSnapshot, history: Deque[float]) -> bool:
        if snapshot.trend_score_p < 20:
            return True
        if snapshot.score_ema <= 0:
            return True
        if snapshot.close < snapshot.ema_12:
            return True
        if len(history) >= 3 and history[-1] < history[-2] < history[-3]:
            return True
        return False

    @staticmethod
    def _should_exit_short(snapshot: MarketFactorSnapshot, history: Deque[float]) -> bool:
        if snapshot.trend_score_p > -20:
            return True
        if snapshot.score_ema >= 0:
            return True
        if snapshot.close > snapshot.ema_12:
            return True
        if len(history) >= 3 and history[-1] > history[-2] > history[-3]:
            return True
        return False
