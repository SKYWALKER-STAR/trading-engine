from __future__ import annotations

import pytest

from trading_engine.app.run_trade_engine import _validate_settings
from trading_engine.config.settings import TradeEngineSettings


def test_validate_settings_accepts_distinct_topics() -> None:
    settings = TradeEngineSettings(
        trade_action_topic="trade.action.requested.v1",
        order_update_topic="trade.order.update.received.v1",
    )

    _validate_settings(settings)


def test_validate_settings_rejects_same_trade_and_order_update_topic() -> None:
    settings = TradeEngineSettings(
        trade_action_topic="trade.action.requested.v1",
        order_update_topic="trade.action.requested.v1",
    )

    with pytest.raises(ValueError, match="TRADE_ACTION_TOPIC and TRADE_ORDER_UPDATE_TOPIC"):
        _validate_settings(settings)
