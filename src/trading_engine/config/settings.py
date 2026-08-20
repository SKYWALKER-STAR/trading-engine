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
    risk_decision_topic: str = TopicNames.RISK_DECISION_MADE
    order_update_topic: str = TopicNames.ORDER_UPDATE_RECEIVED
    position_state_topic: str = TopicNames.POSITION_STATE_CHANGED
    trade_action_topic: str = TopicNames.TRADE_ACTION_REQUESTED
    trade_action_failed_topic: str = TopicNames.TRADE_ACTION_FAILED
    order_update_timeout_seconds: float = 30.0

    @classmethod
    def from_env(cls) -> "PositionEngineSettings":
        return cls(
            consumer_group=getenv("POSITION_ENGINE_CONSUMER_GROUP", "position-engine"),
            risk_decision_topic=getenv(
                "POSITION_RISK_DECISION_TOPIC",
                getenv("POSITION_SIGNAL_TOPIC", TopicNames.RISK_DECISION_MADE),
            ),
            order_update_topic=getenv("POSITION_ORDER_UPDATE_TOPIC", TopicNames.ORDER_UPDATE_RECEIVED),
            position_state_topic=getenv("POSITION_STATE_TOPIC", TopicNames.POSITION_STATE_CHANGED),
            trade_action_topic=getenv("POSITION_TRADE_ACTION_TOPIC", TopicNames.TRADE_ACTION_REQUESTED),
            trade_action_failed_topic=getenv("POSITION_TRADE_ACTION_FAILED_TOPIC", TopicNames.TRADE_ACTION_FAILED),
            order_update_timeout_seconds=float(getenv("POSITION_ORDER_UPDATE_TIMEOUT_SECONDS", "30.0")),
        )


@dataclass(frozen=True, slots=True)
class RiskEngineSettings:
    consumer_group: str = "risk-engine"
    signal_topic: str = TopicNames.STRATEGY_SIGNAL_GENERATED
    position_state_topic: str = TopicNames.POSITION_STATE_CHANGED
    risk_decision_topic: str = TopicNames.RISK_DECISION_MADE
    require_position_snapshot: bool = False
    default_open_quantity: float = 1.0

    @classmethod
    def from_env(cls) -> "RiskEngineSettings":
        require_snapshot_raw = getenv("RISK_REQUIRE_POSITION_SNAPSHOT", "false").strip().lower()
        return cls(
            consumer_group=getenv("RISK_ENGINE_CONSUMER_GROUP", "risk-engine"),
            signal_topic=getenv("RISK_SIGNAL_TOPIC", TopicNames.STRATEGY_SIGNAL_GENERATED),
            position_state_topic=getenv("RISK_POSITION_STATE_TOPIC", TopicNames.POSITION_STATE_CHANGED),
            risk_decision_topic=getenv("RISK_DECISION_TOPIC", TopicNames.RISK_DECISION_MADE),
            require_position_snapshot=require_snapshot_raw in ("1", "true", "yes", "on"),
            default_open_quantity=float(getenv("RISK_DEFAULT_OPEN_QUANTITY", "1.0")),
        )


@dataclass(frozen=True, slots=True)
class TradeEngineSettings:
    consumer_group: str = "trade-engine"
    trade_action_topic: str = TopicNames.TRADE_ACTION_REQUESTED
    order_update_topic: str = TopicNames.ORDER_UPDATE_RECEIVED
    risk_decision_topic: str = TopicNames.RISK_DECISION_MADE
    exchange: str = "binance"
    request_timeout_seconds: float = 10.0
    binance_ws_api_url: str = "wss://ws-fapi.binance.com/ws-fapi/v1"
    binance_api_key: str = ""
    binance_api_secret: str = ""
    binance_order_type: str = "MARKET"
    binance_position_side: str = "BOTH"
    binance_new_order_resp_type: str = "ACK"
    binance_recv_window: int = 5000
    order_account_id: str = "default"

    @classmethod
    def from_env(cls) -> "TradeEngineSettings":
        return cls(
            consumer_group=getenv("TRADE_ENGINE_CONSUMER_GROUP", "trade-engine"),
            trade_action_topic=getenv("TRADE_ACTION_TOPIC", TopicNames.TRADE_ACTION_REQUESTED),
            order_update_topic=getenv("TRADE_ORDER_UPDATE_TOPIC", TopicNames.ORDER_UPDATE_RECEIVED),
            risk_decision_topic=getenv("TRADE_RISK_DECISION_TOPIC", TopicNames.RISK_DECISION_MADE),
            exchange=getenv("TRADE_EXCHANGE", "binance").strip().lower(),
            request_timeout_seconds=float(getenv("TRADE_REQUEST_TIMEOUT_SECONDS", "10.0")),
            binance_ws_api_url=getenv("BINANCE_WS_API_URL", "wss://ws-fapi.binance.com/ws-fapi/v1"),
            binance_api_key=getenv("BINANCE_API_KEY", ""),
            binance_api_secret=getenv("BINANCE_API_SECRET", ""),
            binance_order_type=getenv("BINANCE_ORDER_TYPE", "MARKET").strip().upper(),
            binance_position_side=getenv("BINANCE_POSITION_SIDE", "BOTH").strip().upper(),
            binance_new_order_resp_type=getenv("BINANCE_NEW_ORDER_RESP_TYPE", "ACK").strip().upper(),
            binance_recv_window=int(getenv("BINANCE_RECV_WINDOW", "5000")),
            order_account_id=getenv("ORDER_ACCOUNT_ID", "default").strip() or "default",
        )


@dataclass(frozen=True, slots=True)
class BinanceUserDataStreamSettings:
    order_update_topic: str = TopicNames.ORDER_UPDATE_RECEIVED
    rest_api_url: str = "https://fapi.binance.com"
    websocket_stream_url: str = "wss://fstream.binance.com/ws"
    api_key: str = ""
    listen_key_keepalive_seconds: float = 1800.0
    reconnect_initial_seconds: float = 1.0
    reconnect_max_seconds: float = 30.0
    request_timeout_seconds: float = 10.0

    @classmethod
    def from_env(cls) -> "BinanceUserDataStreamSettings":
        return cls(
            order_update_topic=getenv("TRADE_ORDER_UPDATE_TOPIC", TopicNames.ORDER_UPDATE_RECEIVED),
            rest_api_url=getenv(
                "BINANCE_FUTURES_REST_API_URL", "https://fapi.binance.com"
            ).rstrip("/"),
            websocket_stream_url=getenv(
                "BINANCE_FUTURES_USER_STREAM_URL", "wss://fstream.binance.com/ws"
            ).rstrip("/"),
            api_key=getenv("BINANCE_API_KEY", ""),
            listen_key_keepalive_seconds=float(
                getenv("BINANCE_LISTEN_KEY_KEEPALIVE_SECONDS", "1800.0")
            ),
            reconnect_initial_seconds=float(
                getenv("BINANCE_USER_STREAM_RECONNECT_INITIAL_SECONDS", "1.0")
            ),
            reconnect_max_seconds=float(
                getenv("BINANCE_USER_STREAM_RECONNECT_MAX_SECONDS", "30.0")
            ),
            request_timeout_seconds=float(
                getenv("BINANCE_USER_STREAM_REQUEST_TIMEOUT_SECONDS", "10.0")
            ),
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
