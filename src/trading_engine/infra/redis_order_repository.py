from __future__ import annotations

import json
from dataclasses import asdict, replace
from datetime import datetime
from os import getenv
from typing import Any

from trading_engine.trade.models import TrackedOrder, TrackedOrderStatus
from trading_engine.trade.repository import OrderIdentityConflictError, OrderRepository


class RedisOrderRepository(OrderRepository):
    """Redis implementation of the storage-agnostic order repository contract."""

    def __init__(self, redis_url: str, key_prefix: str = "order") -> None:
        self._redis_url = redis_url
        self._key_prefix = key_prefix.rstrip(":")
        self._client: Any | None = None

    @classmethod
    def from_env(cls) -> "RedisOrderRepository":
        return cls(
            redis_url=getenv("ORDER_REDIS_URL", "redis://127.0.0.1:6379/0"),
            key_prefix=getenv("ORDER_REDIS_KEY_PREFIX", "order"),
        )

    def get_by_order_id(
        self,
        *,
        exchange: str,
        account_id: str,
        symbol: str,
        order_id: str,
    ) -> TrackedOrder | None:
        client_order_id = self._get_client().get(
            self._order_index_key(exchange, account_id, symbol, order_id)
        )
        if client_order_id is None:
            return None
        return self.get_by_client_order_id(
            exchange=exchange,
            account_id=account_id,
            client_order_id=str(client_order_id),
        )

    def get_by_client_order_id(
        self,
        *,
        exchange: str,
        account_id: str,
        client_order_id: str,
    ) -> TrackedOrder | None:
        raw = self._get_client().get(
            self._client_key(exchange, account_id, client_order_id)
        )
        return None if raw is None else self._decode(str(raw))

    def save(self, order: TrackedOrder) -> None:
        client = self._get_client()
        client_key = self._client_key(
            order.exchange,
            order.account_id,
            order.client_order_id,
        )
        active_key = self._active_key(order.exchange, order.account_id)
        with client.pipeline(transaction=True) as pipe:
            pipe.set(client_key, self._encode(order))
            if order.order_id is not None:
                pipe.set(
                    self._order_index_key(
                        order.exchange,
                        order.account_id,
                        order.symbol,
                        order.order_id,
                    ),
                    order.client_order_id,
                )
            if order.status.is_terminal:
                pipe.srem(active_key, order.client_order_id)
            else:
                pipe.sadd(active_key, order.client_order_id)
            pipe.execute()

    def bind_order_id(
        self,
        *,
        exchange: str,
        account_id: str,
        client_order_id: str,
        symbol: str,
        order_id: str,
    ) -> TrackedOrder:
        client = self._get_client()
        client_key = self._client_key(exchange, account_id, client_order_id)
        index_key = self._order_index_key(exchange, account_id, symbol, order_id)

        try:
            from redis.exceptions import WatchError
        except ImportError:
            class WatchError(Exception):
                """Fallback used by injected test clients when redis is not installed."""

        while True:
            with client.pipeline() as pipe:
                try:
                    pipe.watch(client_key, index_key)
                    raw = pipe.get(client_key)
                    if raw is None:
                        raise KeyError(f"Unknown client_order_id: {client_order_id}")

                    order = self._decode(str(raw))
                    if order.symbol != symbol:
                        raise OrderIdentityConflictError(
                            f"Client order {client_order_id} belongs to {order.symbol}, not {symbol}"
                        )
                    if order.order_id not in (None, order_id):
                        raise OrderIdentityConflictError(
                            f"Client order {client_order_id} is already bound to {order.order_id}"
                        )

                    indexed_client_id = pipe.get(index_key)
                    if indexed_client_id not in (None, client_order_id):
                        raise OrderIdentityConflictError(
                            f"Exchange order {order_id} is already bound to {indexed_client_id}"
                        )

                    bound = order if order.order_id == order_id else replace(order, order_id=order_id)
                    pipe.multi()
                    pipe.set(client_key, self._encode(bound))
                    pipe.set(index_key, client_order_id)
                    pipe.execute()
                    return bound
                except WatchError:
                    continue

    def list_active(
        self,
        *,
        exchange: str,
        account_id: str,
        symbol: str | None = None,
    ) -> list[TrackedOrder]:
        client = self._get_client()
        client_order_ids = client.smembers(self._active_key(exchange, account_id))
        orders: list[TrackedOrder] = []
        for client_order_id in client_order_ids:
            order = self.get_by_client_order_id(
                exchange=exchange,
                account_id=account_id,
                client_order_id=str(client_order_id),
            )
            if order is None or order.status.is_terminal:
                continue
            if symbol is None or order.symbol == symbol:
                orders.append(order)
        return orders

    @staticmethod
    def _encode(order: TrackedOrder) -> str:
        payload = asdict(order)
        payload["status"] = order.status.value
        payload["created_at"] = order.created_at.isoformat()
        payload["updated_at"] = order.updated_at.isoformat()
        return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))

    @staticmethod
    def _decode(raw: str) -> TrackedOrder:
        payload = json.loads(raw)
        return TrackedOrder(
            exchange=str(payload["exchange"]),
            account_id=str(payload["account_id"]),
            symbol=str(payload["symbol"]),
            client_order_id=str(payload["client_order_id"]),
            side=str(payload["side"]),
            order_type=str(payload["order_type"]),
            original_quantity=float(payload["original_quantity"]),
            status=TrackedOrderStatus(payload["status"]),
            created_at=datetime.fromisoformat(payload["created_at"]),
            updated_at=datetime.fromisoformat(payload["updated_at"]),
            order_id=None if payload.get("order_id") is None else str(payload["order_id"]),
            position_side=(
                None if payload.get("position_side") is None else str(payload["position_side"])
            ),
            reduce_only=bool(payload.get("reduce_only", False)),
            cumulative_filled_quantity=float(
                payload.get("cumulative_filled_quantity", 0.0)
            ),
            last_trade_id=(
                None if payload.get("last_trade_id") is None else str(payload["last_trade_id"])
            ),
            metadata={key: value for key, value in payload.get("metadata", {}).items()},
        )

    def _client_key(self, exchange: str, account_id: str, client_order_id: str) -> str:
        return f"{self._key_prefix}:{exchange}:{account_id}:client:{client_order_id}"

    def _order_index_key(
        self,
        exchange: str,
        account_id: str,
        symbol: str,
        order_id: str,
    ) -> str:
        return f"{self._key_prefix}:{exchange}:{account_id}:exchange:{symbol}:{order_id}"

    def _active_key(self, exchange: str, account_id: str) -> str:
        return f"{self._key_prefix}:{exchange}:{account_id}:active"

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            import redis
        except ImportError as exc:
            raise RuntimeError("redis is not installed. Install the runtime extras") from exc
        self._client = redis.Redis.from_url(self._redis_url, decode_responses=True)
        return self._client
