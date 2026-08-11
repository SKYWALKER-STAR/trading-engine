from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from trading_engine.contracts.messages import SignalDirection
from trading_engine.domain.market_data import MarketFactorSnapshot, MarketTick


@dataclass(frozen=True, slots=True)
class StrategySignal:
    strategy_name: str
    symbol: str
    direction: SignalDirection
    score: float
    confidence: float
    timestamp: datetime
    metadata: dict[str, float | str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StrategyContext:
    market_tick: MarketTick
    features: dict[str, float]
    now: datetime


@dataclass(frozen=True, slots=True)
class FactorStrategyContext:
    factor_snapshot: MarketFactorSnapshot
    now: datetime


StrategyInputContext = StrategyContext | FactorStrategyContext


@dataclass(frozen=True, slots=True)
class StrategyDecision:
    signal: StrategySignal | None
    rejected_reasons: tuple[str, ...] = ()

    @property
    def accepted(self) -> bool:
        return self.signal is not None and not self.rejected_reasons

    @classmethod
    def accepted_signal(cls, signal: StrategySignal) -> "StrategyDecision":
        return cls(signal=signal)

    @classmethod
    def rejected(cls, reasons: list[str]) -> "StrategyDecision":
        return cls(signal=None, rejected_reasons=tuple(reasons))
