from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from io import BytesIO
import json

import pytest

from trading_engine.infra.binance_futures_ws_gateway import BinanceFuturesWsGateway
from trading_engine.trade.models import TradeOrderRequest


@pytest.fixture
def exchange_info(monkeypatch):
    payload = {
        "symbols": [
            {"symbol": "OTHERUSDT", "filters": [
                {"filterType": "LOT_SIZE", "stepSize": "1"},
                {"filterType": "PRICE_FILTER", "tickSize": "10"},
            ]},
            {"symbol": "BTCUSDT", "filters": [
                {"filterType": "LOT_SIZE", "stepSize": "0.00100000"},
                {"filterType": "PRICE_FILTER", "tickSize": "0.10000000"},
            ]},
        ]
    }

    def urlopen(request, timeout):
        assert request.full_url == "https://example.test/fapi/v1/exchangeInfo"
        return BytesIO(json.dumps(payload).encode())

    monkeypatch.setattr("trading_engine.infra.binance_futures_ws_gateway.urlopen", urlopen)
    BinanceFuturesWsGateway._symbol_step_size_cached.cache_clear()
    BinanceFuturesWsGateway._symbol_tick_size_cached.cache_clear()
    yield payload
    BinanceFuturesWsGateway._symbol_step_size_cached.cache_clear()
    BinanceFuturesWsGateway._symbol_tick_size_cached.cache_clear()


def test_rules_match_requested_symbol_even_when_not_first(exchange_info):
    assert BinanceFuturesWsGateway._symbol_step_size_cached(
        "https://example.test", "btcusdt"
    ) == Decimal("0.001")
    assert BinanceFuturesWsGateway._symbol_tick_size_cached(
        "https://example.test", "btcusdt"
    ) == Decimal("0.1")


def test_missing_symbol_does_not_use_another_symbols_rules(exchange_info):
    assert BinanceFuturesWsGateway._symbol_step_size_cached(
        "https://example.test", "MISSINGUSDT"
    ) is None
    assert BinanceFuturesWsGateway._symbol_tick_size_cached(
        "https://example.test", "MISSINGUSDT"
    ) is None


@pytest.mark.parametrize("value,expected", [
    ("65000.10000000", "65000.1"),
    ("100", "100"),
    ("0.0000000000001", "0.0000000000001"),
    ("0.000", "0"),
])
def test_decimal_serialization_preserves_exact_value(value, expected):
    assert BinanceFuturesWsGateway._format_decimal(Decimal(value)) == expected


def test_order_uses_exact_decimal_strings_and_signs_them(exchange_info):
    class Transport:
        def request(self, endpoint, message, timeout_seconds):
            self.params = message["params"]
            return {"result": {"status": "NEW"}}

    transport = Transport()
    gateway = BinanceFuturesWsGateway(
        endpoint="wss://example.test", rest_api_url="https://example.test",
        api_key="test", api_secret="test", transport=transport,
    )
    request = TradeOrderRequest(
        symbol="BTCUSDT", side="BUY", quantity=0.123456,
        order_type="LIMIT", requested_at=datetime.now(UTC),
        correlation_id="test", causation_id="test",
        metadata={"price": "65000.199999999999999", "timeInForce": "GTC"},
    )
    gateway.submit_order(request)
    assert transport.params["quantity"] == "0.123"
    assert transport.params["price"] == "65000.1"
    unsigned = {k: v for k, v in transport.params.items() if k != "signature"}
    assert transport.params["signature"] == gateway._sign(unsigned)

    gateway.submit_order(replace(request, order_type="MARKET", quantity=65000.1019))
    assert transport.params["quantity"] == "65000.101"
    assert "price" not in transport.params
