from __future__ import annotations

from datetime import UTC, datetime
from os import getenv
from typing import Any
from uuid import uuid4

import pytest

from trading_engine.infra.binance_futures_ws_gateway import BinanceFuturesWsGateway
from trading_engine.trade.models import TradeExecutionStatus, TradeOrderRequest


class _FakeTransport:
    def __init__(self, response: dict[str, Any]) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, Any], float]] = []

    def request(self, endpoint: str, message: dict[str, Any], timeout_seconds: float) -> dict[str, Any]:
        self.calls.append((endpoint, message, timeout_seconds))
        return self.response


def _build_request() -> TradeOrderRequest:
    return TradeOrderRequest(
        symbol="BTCUSDT",
        side="BUY",
        quantity=0.1,
        order_type="market",
        requested_at=datetime(2026, 8, 16, 10, 0, tzinfo=UTC),
        correlation_id="corr-1",
        causation_id="cause-1",
        client_order_id="te-client-1",
    )


def test_binance_gateway_maps_successful_fill_response() -> None:
    transport = _FakeTransport(
        {
            "id": "1",
            "status": 200,
            "result": {
                "orderId": 12345,
                "status": "FILLED",
                "executedQty": "0.1",
            },
        }
    )
    gateway = BinanceFuturesWsGateway(
        endpoint="wss://example.test/ws",
        api_key="key",
        api_secret="secret",
        transport=transport,
    )

    result = gateway.submit_order(_build_request())

    assert result.status is TradeExecutionStatus.FILLED
    assert result.order_id == "12345"
    assert result.client_order_id == "te-client-1"
    assert result.filled_quantity == 0.1
    assert result.metadata["exchange"] == "binance"
    assert len(transport.calls) == 1
    _, sent_message, _ = transport.calls[0]
    assert sent_message["method"] == "order.place"
    assert sent_message["params"]["newClientOrderId"] == "te-client-1"
    assert "signature" in sent_message["params"]


def test_binance_gateway_maps_error_response_to_rejected() -> None:
    transport = _FakeTransport(
        {
            "id": "1",
            "error": {
                "code": -2019,
                "msg": "Margin is insufficient.",
            },
        }
    )
    gateway = BinanceFuturesWsGateway(
        endpoint="wss://example.test/ws",
        api_key="key",
        api_secret="secret",
        transport=transport,
    )

    result = gateway.submit_order(_build_request())

    assert result.status is TradeExecutionStatus.REJECTED
    assert result.metadata["error_code"] == "-2019"


def test_binance_gateway_uses_send_time_timestamp_not_request_time() -> None:
    transport = _FakeTransport(
        {
            "id": "1",
            "status": 200,
            "result": {
                "orderId": 456,
                "status": "NEW",
                "executedQty": "0",
            },
        }
    )
    gateway = BinanceFuturesWsGateway(
        endpoint="wss://example.test/ws",
        api_key="key",
        api_secret="secret",
        transport=transport,
    )
    old_request = TradeOrderRequest(
        symbol="BTCUSDT",
        side="BUY",
        quantity=0.1,
        order_type="market",
        requested_at=datetime(2000, 1, 1, tzinfo=UTC),
        correlation_id="corr-old",
        causation_id="cause-old",
    )

    gateway.submit_order(old_request)

    _, sent_message, _ = transport.calls[0]
    assert sent_message["params"]["timestamp"] != int(old_request.requested_at.timestamp() * 1000)


def test_binance_gateway_includes_optional_parameters_for_limit_orders() -> None:
    transport = _FakeTransport(
        {
            "id": "1",
            "status": 200,
            "result": {
                "orderId": 789,
                "status": "NEW",
                "executedQty": "0",
            },
        }
    )
    gateway = BinanceFuturesWsGateway(
        endpoint="wss://example.test/ws",
        api_key="key",
        api_secret="secret",
        transport=transport,
    )
    request = TradeOrderRequest(
        symbol="BTCUSDT",
        side="SELL",
        quantity=0.2,
        order_type="limit",
        requested_at=datetime(2026, 8, 16, 10, 0, tzinfo=UTC),
        correlation_id="corr-limit",
        causation_id="cause-limit",
        metadata={
            "price": "43000",
            "timeInForce": "GTC",
            "positionSide": "SHORT",
            "reduceOnly": "true",
            "newOrderRespType": "RESULT",
        },
    )

    gateway.submit_order(request)

    _, sent_message, _ = transport.calls[0]
    params = sent_message["params"]
    assert params["type"] == "LIMIT"
    assert params["timeInForce"] == "GTC"
    assert params["price"] == "43000"
    assert params["positionSide"] == "SHORT"
    assert params["reduceOnly"] == "true"
    assert params["newOrderRespType"] == "RESULT"


def test_binance_gateway_places_real_market_order() -> None:
    """Place one real order only when the destructive integration test is explicitly enabled."""
    if getenv("RUN_BINANCE_LIVE_ORDER_TEST") != "I_UNDERSTAND_THIS_PLACES_A_REAL_ORDER":
        pytest.skip(
            "set RUN_BINANCE_LIVE_ORDER_TEST=I_UNDERSTAND_THIS_PLACES_A_REAL_ORDER "
            "to enable the real-order test"
        )

    api_key = getenv("BINANCE_API_KEY", "")
    api_secret = getenv("BINANCE_API_SECRET", "")
    symbol = getenv("BINANCE_LIVE_TEST_SYMBOL", "")
    quantity_raw = getenv("BINANCE_LIVE_TEST_QUANTITY", "")
    side = getenv("BINANCE_LIVE_TEST_SIDE", "BUY").strip().upper()

    assert api_key, "BINANCE_API_KEY is required"
    assert api_secret, "BINANCE_API_SECRET is required"
    assert symbol, "BINANCE_LIVE_TEST_SYMBOL is required"
    assert quantity_raw, "BINANCE_LIVE_TEST_QUANTITY is required"
    assert side in {"BUY", "SELL"}, "BINANCE_LIVE_TEST_SIDE must be BUY or SELL"

    quantity = float(quantity_raw)
    assert quantity > 0, "BINANCE_LIVE_TEST_QUANTITY must be greater than zero"

    gateway = BinanceFuturesWsGateway(
        endpoint=getenv("BINANCE_WS_API_URL", "wss://ws-fapi.binance.com/ws-fapi/v1"),
        api_key=api_key,
        api_secret=api_secret,
    )
    request_id = str(uuid4())

    print(f"Placing real order on Binance: symbol={symbol}, side={side}, quantity={quantity}, request_id={request_id}")

    result = gateway.submit_order(
        TradeOrderRequest(
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type="MARKET",
            requested_at=datetime.now(UTC),
            correlation_id=request_id,
            causation_id=request_id,
            metadata={"newOrderRespType": "RESULT","positionSide": "LONG" if side == "BUY" else "SHORT"},
        )
    )
    print(f"status: {result.status}")
    print(f"order_id: {result.order_id}")
    print(f"filled_quantity: {result.filled_quantity}")
    print(f"metadata: {result.metadata}")   
    assert result.status in {
        TradeExecutionStatus.NEW,
        TradeExecutionStatus.PARTIALLY_FILLED,
        TradeExecutionStatus.FILLED,
    }, f"order was rejected: {result.metadata}"
    assert result.order_id is not None
