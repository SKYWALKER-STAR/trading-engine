from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import uuid4

from trading_engine.trade.models import TradeExecutionResult, TradeExecutionStatus, TradeOrderRequest


class BinanceWsTransport(Protocol):
    """Transport abstraction to allow testing without real websocket connections."""

    def request(self, endpoint: str, message: dict[str, Any], timeout_seconds: float) -> dict[str, Any]:
        ...


class BinanceWsApiTransport:
    """One-shot websocket transport for Binance WS API RPC calls."""

    def request(self, endpoint: str, message: dict[str, Any], timeout_seconds: float) -> dict[str, Any]:
        return asyncio.run(self._request_async(endpoint, message, timeout_seconds))

    async def _request_async(self, endpoint: str, message: dict[str, Any], timeout_seconds: float) -> dict[str, Any]:
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("websockets is not installed. Install with: pip install websockets") from exc

        async with websockets.connect(endpoint, open_timeout=timeout_seconds, close_timeout=timeout_seconds) as ws:
            await ws.send(json.dumps(message, ensure_ascii=True))
            raw_response = await asyncio.wait_for(ws.recv(), timeout=timeout_seconds)
            if isinstance(raw_response, bytes):
                raw_response = raw_response.decode("utf-8")
            return json.loads(raw_response)


@dataclass(frozen=True, slots=True)
class BinanceFuturesWsGateway:
    """Submit Binance USD-M futures orders via WS API and normalize response for the trade engine."""

    endpoint: str
    api_key: str
    api_secret: str
    order_type: str = "MARKET"
    recv_window: int = 5000
    timeout_seconds: float = 10.0
    transport: BinanceWsTransport | None = None

    def submit_order(self, request: TradeOrderRequest) -> TradeExecutionResult:
        transport = self.transport or BinanceWsApiTransport()
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        order_type = request.order_type.strip().upper() if request.order_type else self.order_type
        params: dict[str, Any] = {
            "apiKey": self.api_key,
            "symbol": request.symbol.upper(),
            "side": request.side.upper(),
            "type": order_type,
            "quantity": self._format_quantity(request.quantity),
            "recvWindow": self.recv_window,
            "timestamp": now_ms,
        }

        position_side = request.metadata.get("positionSide", request.metadata.get("position_side"))
        if position_side is not None:
            params["positionSide"] = str(position_side).upper()

        new_order_resp_type = request.metadata.get("newOrderRespType")
        if new_order_resp_type is not None:
            params["newOrderRespType"] = str(new_order_resp_type).upper()

        new_client_order_id = request.metadata.get("newClientOrderId")
        if new_client_order_id is not None:
            params["newClientOrderId"] = str(new_client_order_id)

        reduce_only = request.metadata.get("reduceOnly")
        if reduce_only is not None:
            params["reduceOnly"] = str(reduce_only).lower()

        if order_type == "LIMIT":
            price = request.metadata.get("price")
            time_in_force = request.metadata.get("timeInForce", request.metadata.get("time_in_force"))
            if price is None or time_in_force is None:
                raise ValueError("LIMIT order requires metadata.price and metadata.timeInForce")
            params["price"] = self._format_decimal(float(price))
            params["timeInForce"] = str(time_in_force).upper()

        params["signature"] = self._sign(params)

        message = {
            "id": str(uuid4()),
            "method": "order.place",
            "params": params,
        }

        response = transport.request(self.endpoint, message, self.timeout_seconds)
        error = response.get("error")
        if isinstance(error, dict):
            error_code = str(error.get("code", "unknown"))
            error_msg = str(error.get("msg", "unknown_error"))
            return TradeExecutionResult(
                symbol=request.symbol,
                status=TradeExecutionStatus.REJECTED,
                updated_at=datetime.now(UTC),
                order_id=None,
                filled_quantity=None,
                metadata={
                    "exchange": "binance",
                    "error_code": error_code,
                    "error_message": error_msg,
                },
            )

        result = response.get("result", {})
        exchange_status = str(result.get("status", "NEW"))
        status = _map_status(exchange_status)
        order_id_raw = result.get("orderId")
        order_id = None if order_id_raw is None else str(order_id_raw)
        filled_raw = result.get("executedQty", result.get("cumQty"))
        filled_quantity: float | None = None
        if filled_raw is not None:
            try:
                filled_quantity = float(filled_raw)
            except (TypeError, ValueError):
                filled_quantity = None

        return TradeExecutionResult(
            symbol=request.symbol,
            status=status,
            updated_at=datetime.now(UTC),
            order_id=order_id,
            filled_quantity=filled_quantity,
            metadata={
                "exchange": "binance",
                "exchange_status": exchange_status,
                "exchange_response": result,
            },
        )

    def _sign(self, params: dict[str, Any]) -> str:
        serialized = "&".join(f"{key}={value}" for key, value in sorted(params.items()))
        signature = hmac.new(self.api_secret.encode("utf-8"), serialized.encode("utf-8"), hashlib.sha256)
        return signature.hexdigest()

    @staticmethod
    def _format_quantity(quantity: float) -> str:
        return BinanceFuturesWsGateway._format_decimal(quantity)

    @staticmethod
    def _format_decimal(value: float) -> str:
        return (f"{value:.12f}").rstrip("0").rstrip(".")


def _map_status(exchange_status: str) -> TradeExecutionStatus:
    normalized = exchange_status.strip().upper()
    if normalized == "PARTIALLY_FILLED":
        return TradeExecutionStatus.PARTIALLY_FILLED
    if normalized == "FILLED":
        return TradeExecutionStatus.FILLED
    if normalized in ("CANCELED", "EXPIRED"):
        return TradeExecutionStatus.CANCELED
    if normalized in ("REJECTED", "EXPIRED_IN_MATCH"):
        return TradeExecutionStatus.REJECTED
    return TradeExecutionStatus.NEW
