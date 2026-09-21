from __future__ import annotations

import json
from datetime import UTC, datetime
from os import getenv
from typing import Any

from trading_engine.position.models import PositionDirection, PositionLifecycle, PositionState
from trading_engine.position.repository import PositionRepository


class RedisPositionRepository(PositionRepository):
    """Redis-backed position repository.

    Stores one JSON document per symbol under a configurable key prefix.
    """

    def __init__(self, redis_url: str, key_prefix: str = "position:execution", account_id: str = "default") -> None:
        self._redis_url = redis_url
        self._key_prefix = key_prefix
        self._account_id = account_id
        self._client: Any | None = None

    @classmethod
    def from_env(cls) -> "RedisPositionRepository":
        redis_url = getenv("POSITION_REDIS_URL", "redis://127.0.0.1:6379/0")
        key_prefix = getenv("POSITION_EXECUTION_KEY_PREFIX", "position:execution")
        return cls(redis_url=redis_url, key_prefix=key_prefix,
                   account_id=getenv("ORDER_ACCOUNT_ID", "default").strip() or "default")

    def get(self, symbol: str) -> PositionState | None:
        client = self._get_client()
        raw = client.get(self._view_state_key(symbol))
        if raw is None:
            return None
        return self.decode(json.loads(raw))

    @staticmethod
    def decode(payload: dict[str, Any]) -> PositionState:
        updated_at_raw = payload.get("updated_at")
        updated_at = None
        if updated_at_raw is not None:
            updated_at = datetime.fromisoformat(updated_at_raw)
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=UTC)
        return PositionState(
            symbol=payload["symbol"],
            direction=PositionDirection(payload["direction"]),
            lifecycle=PositionLifecycle(payload["lifecycle"]),
            quantity=float(payload.get("quantity", 0.0)),
            active_order_id=payload.get("active_order_id"),
            active_client_order_id=payload.get("active_client_order_id"),
            last_order_id=payload.get("last_order_id"),
            last_client_order_id=payload.get("last_client_order_id"),
            updated_at=updated_at,
            metadata={key: value for key, value in payload.get("metadata", {}).items()},
        )

    def save(self, state: PositionState) -> None:
        self._get_client().set(self._view_state_key(state.symbol), json.dumps(self.encode(state)))

    @staticmethod
    def encode(state: PositionState) -> dict[str, Any]:
        return {
            "symbol": state.symbol,
            "direction": state.direction.value,
            "lifecycle": state.lifecycle.value,
            "quantity": state.quantity,
            "active_order_id": state.active_order_id,
            "active_client_order_id": state.active_client_order_id,
            "last_order_id": state.last_order_id,
            "last_client_order_id": state.last_client_order_id,
            "updated_at": state.updated_at.isoformat() if state.updated_at is not None else None,
            "metadata": state.metadata,
        }

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return symbol.strip().upper()

    def _view_state_key(self, symbol: str) -> str:
        normalized = self._normalize_symbol(symbol)
        return f"{self._key_prefix}:{{{self._account_id}}}:state:{normalized}"

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client

        try:
            import redis
        except ImportError as exc:
            raise RuntimeError("redis is not installed. Install with: pip install redis") from exc

        self._client = redis.Redis.from_url(self._redis_url, decode_responses=True,
                                           socket_timeout=10, socket_connect_timeout=10)
        return self._client
