from __future__ import annotations

from dataclasses import dataclass
from os import getenv

from trading_engine.contracts.messages import TopicNames


@dataclass(frozen=True, slots=True)
class StrategyEngineSettings:
    min_confidence: float = 0.55
    max_market_data_age_seconds: int = 2
    signal_topic: str = TopicNames.STRATEGY_SIGNAL_GENERATED

    @classmethod
    def from_env(cls) -> "StrategyEngineSettings":
        return cls(
            min_confidence=float(getenv("STRATEGY_MIN_CONFIDENCE", "0.55")),
            max_market_data_age_seconds=int(getenv("STRATEGY_MAX_DATA_AGE_SECONDS", "2")),
            signal_topic=getenv("STRATEGY_SIGNAL_TOPIC", TopicNames.STRATEGY_SIGNAL_GENERATED),
        )


@dataclass(frozen=True, slots=True)
class PositionEngineSettings:
    consumer_group: str = "position-engine"
    signal_topic: str = TopicNames.STRATEGY_SIGNAL_GENERATED
    order_update_topic: str = TopicNames.ORDER_UPDATE_RECEIVED
    position_state_topic: str = TopicNames.POSITION_STATE_CHANGED
    trade_action_topic: str = TopicNames.TRADE_ACTION_REQUESTED

    @classmethod
    def from_env(cls) -> "PositionEngineSettings":
        return cls(
            consumer_group=getenv("POSITION_ENGINE_CONSUMER_GROUP", "position-engine"),
            signal_topic=getenv("POSITION_SIGNAL_TOPIC", TopicNames.STRATEGY_SIGNAL_GENERATED),
            order_update_topic=getenv("POSITION_ORDER_UPDATE_TOPIC", TopicNames.ORDER_UPDATE_RECEIVED),
            position_state_topic=getenv("POSITION_STATE_TOPIC", TopicNames.POSITION_STATE_CHANGED),
            trade_action_topic=getenv("POSITION_TRADE_ACTION_TOPIC", TopicNames.TRADE_ACTION_REQUESTED),
        )
