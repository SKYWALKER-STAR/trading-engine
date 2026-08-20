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

    def __init__(self, redis_url: str, key_prefix: str = "position") -> None:
        self._redis_url = redis_url
        self._key_prefix = key_prefix
        self._client: Any | None = None

    @classmethod
    def from_env(cls) -> "RedisPositionRepository":
        redis_url = getenv("POSITION_REDIS_URL", "redis://127.0.0.1:6379/0")
        key_prefix = getenv("POSITION_REDIS_KEY_PREFIX", "position")
        return cls(redis_url=redis_url, key_prefix=key_prefix)

    def get(self, symbol: str) -> PositionState | None:
        raw = self._get_client().get(self._key(symbol))
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
        self._get_client().set(self._key(state.symbol), json.dumps(payload, ensure_ascii=True))

    def _key(self, symbol: str) -> str:
        return f"{self._key_prefix}:{symbol}"

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client

        try:
            import redis
        except ImportError as exc:
            raise RuntimeError("redis is not installed. Install with: pip install redis") from exc

        self._client = redis.Redis.from_url(self._redis_url, decode_responses=True)
        return self._client
