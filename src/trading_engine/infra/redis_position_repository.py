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

    def __init__(self, redis_url: str, key_prefix: str = "binance:position:usdt_futures") -> None:
        self._redis_url = redis_url
        self._key_prefix = key_prefix
        self._client: Any | None = None

    @classmethod
    def from_env(cls) -> "RedisPositionRepository":
        redis_url = getenv("POSITION_REDIS_URL", "redis://127.0.0.1:6379/0")
        key_prefix = getenv(
            "POSITION_VIEW_KEY_PREFIX",
            getenv("POSITION_REDIS_KEY_PREFIX", "binance:position:usdt_futures"),
        )
        return cls(redis_url=redis_url, key_prefix=key_prefix)

    def get(self, symbol: str) -> PositionState | None:
        client = self._get_client()
        raw = client.get(self._view_state_key(symbol))
        if raw is None:
            raw = client.get(self._legacy_key(symbol))
        if raw is None:
            return None
        payload = json.loads(raw)
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
        payload = {
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
        canonical = json.dumps(payload, ensure_ascii=True)
        client = self._get_client()
        client.set(self._view_state_key(state.symbol), canonical)
        # Keep writing the legacy key during migration.
        client.set(self._legacy_key(state.symbol), canonical)

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return symbol.strip().upper()

    def _view_state_key(self, symbol: str) -> str:
        normalized = self._normalize_symbol(symbol)
        return f"{self._key_prefix}:view:state:{normalized}:v1"

    def _legacy_key(self, symbol: str) -> str:
        normalized = self._normalize_symbol(symbol)
        return f"{self._key_prefix}:{normalized}"

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client

        try:
            import redis
        except ImportError as exc:
            raise RuntimeError("redis is not installed. Install with: pip install redis") from exc

        self._client = redis.Redis.from_url(self._redis_url, decode_responses=True)
        return self._client
