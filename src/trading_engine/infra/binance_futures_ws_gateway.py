from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any, Protocol
from urllib.error import URLError
from urllib.request import Request, urlopen
from uuid import uuid4
from trading_engine.common.logger import get_logger
from trading_engine.trade.models import TradeExecutionResult, TradeExecutionStatus, TradeOrderRequest

LOGGER = get_logger(__name__)

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

        last_exc: Exception = RuntimeError("unreachable")
        for attempt in range(3):
            try:
                async with websockets.connect(endpoint, open_timeout=timeout_seconds, close_timeout=timeout_seconds) as ws:
                    await ws.send(json.dumps(message, ensure_ascii=True))
                    raw_response = await asyncio.wait_for(ws.recv(), timeout=timeout_seconds)
                    if isinstance(raw_response, bytes):
                        raw_response = raw_response.decode("utf-8")
                    return json.loads(raw_response)
            except (OSError, asyncio.TimeoutError) as exc:
                last_exc = exc
                if attempt < 2:
                    LOGGER.warning("WS request failed (attempt %d/3): %s", attempt + 1, type(exc).__name__)
                    await asyncio.sleep(1.0)
        raise last_exc


@dataclass(frozen=True, slots=True)
class BinanceFuturesWsGateway:
    """Submit Binance USD-M futures orders via WS API and normalize response for the trade engine."""

    endpoint: str
    api_key: str
    api_secret: str
    rest_api_url: str = "https://fapi.binance.com"
    order_type: str = "MARKET"
    recv_window: int = 5000
    timeout_seconds: float = 10.0
    transport: BinanceWsTransport | None = None

    def submit_order(self, request: TradeOrderRequest) -> TradeExecutionResult:
        transport = self.transport or BinanceWsApiTransport()
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        order_type = request.order_type.strip().upper() if request.order_type else self.order_type
        normalized_quantity = self._normalize_quantity(request.symbol, request.quantity)
        params: dict[str, Any] = {
            "apiKey": self.api_key,
            "symbol": request.symbol.upper(),
            "side": request.side.upper(),
            "type": order_type,
            "quantity": self._format_quantity(normalized_quantity),
            "recvWindow": self.recv_window,
            "timestamp": now_ms,
        }

        position_side = request.metadata.get("positionSide", request.metadata.get("position_side"))
        if position_side is not None:
            #params["positionSide"] = str(position_side).upper()
            params["positionSide"] = "BOTH"

        new_order_resp_type = request.metadata.get("newOrderRespType")
        if new_order_resp_type is not None:
            params["newOrderRespType"] = str(new_order_resp_type).upper()

        new_client_order_id = request.client_order_id or request.metadata.get("newClientOrderId")
        if new_client_order_id is not None:
            params["newClientOrderId"] = str(new_client_order_id)

        #reduce_only = request.metadata.get("reduceOnly")
        #if reduce_only is not None:
        #    params["reduceOnly"] = str(reduce_only).lower()

        if order_type == "LIMIT":
            price = request.metadata.get("price")
            time_in_force = request.metadata.get("timeInForce", request.metadata.get("time_in_force"))
            if price is None or time_in_force is None:
                raise ValueError("LIMIT order requires metadata.price and metadata.timeInForce")
            normalized_price = self._normalize_price(request.symbol, Decimal(str(price)))
            params["price"] = self._format_decimal(normalized_price)
            params["timeInForce"] = str(time_in_force).upper()

        params["signature"] = self._sign(params)

        message = {
            "id": str(uuid4()),
            "method": "order.place",
            "params": params,
        }

        LOGGER.info("sending order request: %s", message)
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
                client_order_id=None if new_client_order_id is None else str(new_client_order_id),
                filled_quantity=None,
                metadata={
                    "exchange": "binance",
                    "error_code": error_code,
                    "error_message": error_msg,
                },
            )
        LOGGER.info("order response received: %s", response)

        result = response.get("result", {})
        exchange_status = str(result.get("status", "NEW"))
        status = _map_status(exchange_status)
        order_id_raw = result.get("orderId")
        order_id = None if order_id_raw is None else str(order_id_raw)
        client_order_id_raw = result.get("clientOrderId", new_client_order_id)
        client_order_id = None if client_order_id_raw is None else str(client_order_id_raw)
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
            client_order_id=client_order_id,
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
    def _format_quantity(quantity: Decimal) -> str:
        return BinanceFuturesWsGateway._format_decimal(quantity)

    def _normalize_quantity(self, symbol: str, quantity: float) -> Decimal:
        quantity_decimal = Decimal(str(quantity))
        step_size = self._symbol_step_size(symbol.upper())
        if step_size is None or step_size <= 0:
            return quantity_decimal
        normalized = self._floor_to_step(quantity_decimal, step_size)
        if normalized <= 0:
            return quantity_decimal
        return normalized

    def _normalize_price(self, symbol: str, price: Decimal) -> Decimal:
        tick_size = self._symbol_tick_size(symbol.upper())
        if tick_size is None or tick_size <= 0:
            return price
        normalized = self._floor_to_step(price, tick_size)
        if normalized <= 0:
            return price
        return normalized

    @staticmethod
    @lru_cache(maxsize=256)
    def _symbol_step_size_cached(rest_api_url: str, symbol: str) -> Decimal | None:
        endpoint = f"{rest_api_url.rstrip('/')}/fapi/v1/exchangeInfo"
        request = Request(endpoint, method="GET")
        try:
            with urlopen(request, timeout=10) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (URLError, TimeoutError, ValueError):
            return None

        symbols = payload.get("symbols") if isinstance(payload, dict) else None
        if not isinstance(symbols, list) or not symbols:
            return None
        symbol_info = next(
            (item for item in symbols
             if isinstance(item, dict) and item.get("symbol") == symbol.upper()),
            None,
        )
        if symbol_info is None:
            return None
        filters = symbol_info.get("filters")
        if not isinstance(filters, list):
            return None

        for item in filters:
            if not isinstance(item, dict):
                continue
            if item.get("filterType") != "LOT_SIZE":
                continue
            raw_step = item.get("stepSize")
            try:
                step_size = Decimal(str(raw_step))
            except (TypeError, ValueError, InvalidOperation):
                return None
            return step_size if step_size.is_finite() and step_size > 0 else None
        return None

    def _symbol_step_size(self, symbol: str) -> Decimal | None:
        return self._symbol_step_size_cached(self.rest_api_url, symbol)

    @staticmethod
    @lru_cache(maxsize=256)
    def _symbol_tick_size_cached(rest_api_url: str, symbol: str) -> Decimal | None:
        endpoint = f"{rest_api_url.rstrip('/')}/fapi/v1/exchangeInfo"
        request = Request(endpoint, method="GET")
        try:
            with urlopen(request, timeout=10) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (URLError, TimeoutError, ValueError):
            return None

        symbols = payload.get("symbols") if isinstance(payload, dict) else None
        if not isinstance(symbols, list) or not symbols:
            return None
        symbol_info = next(
            (item for item in symbols
             if isinstance(item, dict) and item.get("symbol") == symbol.upper()),
            None,
        )
        if symbol_info is None:
            return None
        filters = symbol_info.get("filters")
        if not isinstance(filters, list):
            return None

        for item in filters:
            if not isinstance(item, dict):
                continue
            if item.get("filterType") != "PRICE_FILTER":
                continue
            raw_tick = item.get("tickSize")
            try:
                tick_size = Decimal(str(raw_tick))
            except (TypeError, ValueError, InvalidOperation):
                return None
            return tick_size if tick_size.is_finite() and tick_size > 0 else None
        return None

    def _symbol_tick_size(self, symbol: str) -> Decimal | None:
        return self._symbol_tick_size_cached(self.rest_api_url, symbol)

    @staticmethod
    def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
        value_decimal = Decimal(str(value))
        step_decimal = Decimal(str(step))
        if step_decimal <= 0:
            return value
        steps = (value_decimal / step_decimal).to_integral_value(rounding=ROUND_DOWN)
        return steps * step_decimal

    @staticmethod
    def _format_decimal(value: Decimal) -> str:
        text = format(value, "f")
        return text.rstrip("0").rstrip(".") if "." in text else text


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
