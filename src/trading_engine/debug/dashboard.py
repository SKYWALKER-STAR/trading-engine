from __future__ import annotations

import json
from datetime import datetime
from os import getenv
from typing import Any


class PositionDebugStore:
    """Minimal Redis-backed debug store for tracking position state transitions."""

    def __init__(
        self,
        redis: Any | None = None,
        redis_url: str = "redis://127.0.0.1:6379/0",
        key_prefix: str = "binance:position:usdt_futures",
    ) -> None:
        self._redis = redis
        self._redis_url = redis_url
        self._key_prefix = key_prefix

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return symbol.strip().upper()

    def _get_client(self) -> Any:
        if self._redis is not None:
            return self._redis

        try:
            import redis
        except ImportError as exc:  # pragma: no cover - runtime dependency check
            raise RuntimeError("redis is not installed. Install with: pip install redis") from exc

        self._redis = redis.Redis.from_url(self._redis_url, decode_responses=True)
        return self._redis

    def _state_key(self, symbol: str) -> str:
        normalized = self._normalize_symbol(symbol)
        return f"{self._key_prefix}:debug:state:{normalized}"

    def _history_key(self, symbol: str) -> str:
        normalized = self._normalize_symbol(symbol)
        return f"{self._key_prefix}:debug:history:{normalized}"

    def _view_state_key(self, symbol: str) -> str:
        normalized = self._normalize_symbol(symbol)
        return f"{self._key_prefix}:view:state:{normalized}:v1"

    def _execution_state_key(self, symbol: str) -> str:
        prefix = getenv("POSITION_EXECUTION_KEY_PREFIX", "position:execution")
        account = getenv("ORDER_ACCOUNT_ID", "default").strip() or "default"
        return f"{prefix}:{{{account}}}:state:{self._normalize_symbol(symbol)}"

    def record_transition(
        self,
        symbol: str,
        previous_lifecycle: str,
        current_lifecycle: str,
        reason: str,
        occurred_at: datetime,
        *,
        direction: str | None = None,
        previous_direction: str | None = None,
        quantity: float | None = None,
        previous_quantity: float | None = None,
        active_order_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_symbol = self._normalize_symbol(symbol)
        payload = {
            "symbol": normalized_symbol,
            "direction": direction,
            "previous_direction": previous_direction,
            "lifecycle": current_lifecycle,
            "previous_lifecycle": previous_lifecycle,
            "current_lifecycle": current_lifecycle,
            "quantity": quantity,
            "previous_quantity": previous_quantity,
            "active_order_id": active_order_id,
            "reason": reason,
            "occurred_at": occurred_at.isoformat(),
            "metadata": metadata or {},
        }
        client = self._get_client()
        client.set(self._state_key(normalized_symbol), json.dumps(payload, ensure_ascii=True, sort_keys=True))
        client.lpush(self._history_key(normalized_symbol), json.dumps(payload, ensure_ascii=True, sort_keys=True))
        client.ltrim(self._history_key(normalized_symbol), 0, 19)
        return payload

    def get_state(self, symbol: str) -> dict[str, Any] | None:
        state, _ = self.get_state_with_source(symbol)
        return state

    def get_state_with_source(self, symbol: str) -> tuple[dict[str, Any] | None, str | None]:
        client = self._get_client()
        view_key = self._view_state_key(symbol)
        debug_key = self._state_key(symbol)

        execution_key = self._execution_state_key(symbol)
        raw = client.get(execution_key)
        source_key = execution_key if raw is not None else None
        if raw is None:
            raw = client.get(view_key)
            source_key = view_key if raw is not None else None
        if raw is None:
            raw = client.get(debug_key)
            if raw is not None:
                source_key = debug_key
        if raw is None:
            return None, None
        return json.loads(raw), source_key

    def get_key_info(self, symbol: str) -> dict[str, Any]:
        normalized = self._normalize_symbol(symbol)
        client = self._get_client()
        keys = {
            "execution_state_key": self._execution_state_key(normalized),
            "view_state_key": self._view_state_key(normalized),
            "debug_state_key": self._state_key(normalized),
            "debug_history_key": self._history_key(normalized),
        }

        return {
            "symbol": normalized,
            "keys": keys,
            "exists": {
                name: bool(client.exists(key))
                for name, key in keys.items()
            },
        }

    def get_history(self, symbol: str) -> list[dict[str, Any]]:
        values = self._get_client().lrange(self._history_key(symbol), 0, 19)
        history: list[dict[str, Any]] = []
        for raw in values:
            if not raw:
                continue
            history.append(json.loads(raw))
        return history
