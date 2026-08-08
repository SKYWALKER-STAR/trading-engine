from __future__ import annotations

from dataclasses import dataclass
from os import getenv


@dataclass(frozen=True, slots=True)
class StrategyEngineSettings:
    min_confidence: float = 0.55
    max_market_data_age_seconds: int = 2
    signal_topic: str = "signal.generated"

    @classmethod
    def from_env(cls) -> "StrategyEngineSettings":
        return cls(
            min_confidence=float(getenv("STRATEGY_MIN_CONFIDENCE", "0.55")),
            max_market_data_age_seconds=int(getenv("STRATEGY_MAX_DATA_AGE_SECONDS", "2")),
            signal_topic=getenv("STRATEGY_SIGNAL_TOPIC", "signal.generated"),
        )
