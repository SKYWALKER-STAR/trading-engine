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


@dataclass(frozen=True, slots=True)
class PositionViewProjectorSettings:
    redis_url: str = "redis://127.0.0.1:6379/0"
    key_prefix: str = "binance:position:usdt_futures"
    poll_interval_seconds: float = 2.0
    enable_detail_keys: bool = True

    @classmethod
    def from_env(cls) -> "PositionViewProjectorSettings":
        detail_raw = getenv("POSITION_VIEW_ENABLE_DETAIL_KEYS", "true").strip().lower()
        return cls(
            redis_url=getenv("POSITION_VIEW_REDIS_URL", getenv("POSITION_REDIS_URL", "redis://127.0.0.1:6379/0")),
            key_prefix=getenv(
                "POSITION_VIEW_KEY_PREFIX",
                getenv("REDIS_POSITION_KEY_PREFIX", "binance:position:usdt_futures"),
            ),
            poll_interval_seconds=float(getenv("POSITION_VIEW_POLL_INTERVAL_SECONDS", "2.0")),
            enable_detail_keys=detail_raw in ("1", "true", "yes", "on"),
        )
