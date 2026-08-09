from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class MarketTick:
    """Normalized market snapshot used by decision engines."""

    symbol: str
    bid: float
    ask: float
    last: float
    timestamp: datetime

    @property
    def mid_price(self) -> float:
        return (self.bid + self.ask) / 2.0


@dataclass(frozen=True, slots=True)
class MarketFactorSnapshot:
    """Feature-rich market snapshot loaded from factor tables."""

    symbol: str
    interval: str
    open_time: datetime
    close: float
    ema_12: float
    ema_26: float
    rsi_14: float
    adx_14: float
    plus_di: float
    minus_di: float
    taker_buy_ratio: float
    funding_rate: float
    score_ema: float
    score_dmi_adx: float
    score_rsi: float
    score_flow: float
    score_funding: float
    trend_score_p: float
