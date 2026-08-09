from __future__ import annotations

from datetime import UTC, datetime, timedelta

from trading_engine.domain.market_data import MarketFactorSnapshot
from trading_engine.strategy.factor_score import FactorScoreStrategy
from trading_engine.strategy.models import FactorStrategyContext, SignalDirection


def build_context(trend_score_p: float, now: datetime, close: float = 110.0) -> FactorStrategyContext:
    snapshot = MarketFactorSnapshot(
        symbol="BTCUSDT",
        interval="1m",
        open_time=now - timedelta(seconds=30),
        close=close,
        ema_12=105.0,
        ema_26=100.0,
        rsi_14=55.0,
        adx_14=30.0,
        plus_di=25.0,
        minus_di=15.0,
        taker_buy_ratio=0.6,
        funding_rate=0.001,
        score_ema=1.0,
        score_dmi_adx=1.0,
        score_rsi=0.5,
        score_flow=0.2,
        score_funding=0.1,
        trend_score_p=trend_score_p,
    )
    return FactorStrategyContext(factor_snapshot=snapshot, now=now)


def test_long_entry_requires_two_consecutive_strong_bars() -> None:
    strategy = FactorScoreStrategy()
    now = datetime.now(UTC)

    first = strategy.generate(build_context(trend_score_p=55.0, now=now))
    second = strategy.generate(build_context(trend_score_p=60.0, now=now + timedelta(minutes=1)))

    assert first is not None
    assert first.direction == SignalDirection.FLAT
    assert second is not None
    assert second.direction == SignalDirection.LONG
    assert second.metadata["reason"] == "entry_long"


def test_long_position_exits_when_trend_fades() -> None:
    strategy = FactorScoreStrategy()
    now = datetime.now(UTC)

    strategy.generate(build_context(trend_score_p=55.0, now=now))
    strategy.generate(build_context(trend_score_p=52.0, now=now + timedelta(minutes=1)))
    exit_signal = strategy.generate(build_context(trend_score_p=15.0, now=now + timedelta(minutes=2)))

    assert exit_signal is not None
    assert exit_signal.direction == SignalDirection.FLAT
    assert exit_signal.metadata["reason"] == "exit_long"


def test_sideways_filter_blocks_new_entries() -> None:
    strategy = FactorScoreStrategy()
    now = datetime.now(UTC)

    signal = strategy.generate(build_context(trend_score_p=10.0, now=now))

    assert signal is not None
    assert signal.direction == SignalDirection.FLAT
    assert signal.metadata["reason"] == "filtered"
