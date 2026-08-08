from __future__ import annotations

from datetime import UTC, datetime, timedelta

from trading_engine.config.settings import StrategyEngineSettings
from trading_engine.domain.market_data import MarketTick
from trading_engine.infra.bus.in_memory import InMemoryEventBus
from trading_engine.strategy.models import SignalDirection, StrategyContext
from trading_engine.strategy.simple_momentum import SimpleMomentumStrategy
from trading_engine.app.bootstrap import build_strategy_engine


def build_context(now: datetime, tick_time: datetime, confidence: float = 0.8) -> StrategyContext:
    tick = MarketTick(
        symbol="BTCUSDT",
        bid=100.0,
        ask=101.0,
        last=100.5,
        timestamp=tick_time,
    )
    return StrategyContext(
        market_tick=tick,
        features={"reference_price": 99.0, "confidence": confidence},
        now=now,
    )


def test_strategy_engine_publishes_signal_when_rules_pass() -> None:
    now = datetime.now(UTC)
    context = build_context(now=now, tick_time=now - timedelta(seconds=1))
    settings = StrategyEngineSettings(min_confidence=0.6, max_market_data_age_seconds=2)
    bus = InMemoryEventBus()
    engine = build_strategy_engine(SimpleMomentumStrategy(), settings=settings, publisher=bus)

    decision = engine.evaluate(context)

    assert decision.accepted
    assert decision.signal is not None
    assert decision.signal.direction in (SignalDirection.LONG, SignalDirection.FLAT)
    assert len(bus.history) == 1


def test_strategy_engine_rejects_stale_market_data() -> None:
    now = datetime.now(UTC)
    context = build_context(now=now, tick_time=now - timedelta(seconds=10))
    settings = StrategyEngineSettings(min_confidence=0.6, max_market_data_age_seconds=2)
    engine = build_strategy_engine(SimpleMomentumStrategy(), settings=settings)

    decision = engine.evaluate(context)

    assert not decision.accepted
    assert "market_data_stale" in decision.rejected_reasons


def test_strategy_engine_rejects_low_confidence() -> None:
    now = datetime.now(UTC)
    context = build_context(now=now, tick_time=now - timedelta(milliseconds=200), confidence=0.1)
    settings = StrategyEngineSettings(min_confidence=0.6, max_market_data_age_seconds=2)
    engine = build_strategy_engine(SimpleMomentumStrategy(), settings=settings)

    decision = engine.evaluate(context)

    assert not decision.accepted
    assert "confidence_too_low" in decision.rejected_reasons
