from __future__ import annotations

import json
from datetime import datetime
from typing import Any


class PositionDebugStore:
    """Minimal Redis-backed debug store for tracking position state transitions."""

    def __init__(
        self,
        redis: Any | None = None,
        redis_url: str = "redis://127.0.0.1:6379/0",
        key_prefix: str = "position",
    ) -> None:
        self._redis = redis
        self._redis_url = redis_url
        self._key_prefix = key_prefix

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
        return f"{self._key_prefix}:debug:state:{symbol}"

    def _history_key(self, symbol: str) -> str:
        return f"{self._key_prefix}:debug:history:{symbol}"

    def record_transition(
        self,
        symbol: str,
        previous_lifecycle: str,
        current_lifecycle: str,
        reason: str,
        occurred_at: datetime,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "symbol": symbol,
            "previous_lifecycle": previous_lifecycle,
            "current_lifecycle": current_lifecycle,
            "reason": reason,
            "occurred_at": occurred_at.isoformat(),
            "metadata": metadata or {},
        }
        client = self._get_client()
        client.set(self._state_key(symbol), json.dumps(payload, ensure_ascii=True, sort_keys=True))
        client.lpush(self._history_key(symbol), json.dumps(payload, ensure_ascii=True, sort_keys=True))
        client.ltrim(self._history_key(symbol), 0, 19)
        return payload

    def get_state(self, symbol: str) -> dict[str, Any] | None:
        raw = self._get_client().get(self._state_key(symbol))
        if raw is None:
            return None
        return json.loads(raw)

    def get_history(self, symbol: str) -> list[dict[str, Any]]:
        values = self._get_client().lrange(self._history_key(symbol), 0, 19)
        history: list[dict[str, Any]] = []
        for raw in values:
            if not raw:
                continue
            history.append(json.loads(raw))
        return history
