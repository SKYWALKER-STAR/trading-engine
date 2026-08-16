from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

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
    assert result.filled_quantity == 0.1
    assert result.metadata["exchange"] == "binance"
    assert len(transport.calls) == 1
    _, sent_message, _ = transport.calls[0]
    assert sent_message["method"] == "order.place"
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