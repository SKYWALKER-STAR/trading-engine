from __future__ import annotations

from datetime import UTC, datetime

from trading_engine.debug.dashboard import PositionDebugStore


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    def set(self, key: str, value: str) -> None:
        self.store[key] = value

    def get(self, key: str) -> str | None:
        return self.store.get(key)

    def lpush(self, key: str, value: str) -> int:
        items = self.store.get(key, "")
        if items == "":
            self.store[key] = value
        else:
            self.store[key] = f"{value}\n{items}"
        return 1

    def lrange(self, key: str, start: int, end: int) -> list[str]:
        items = self.store.get(key, "")
        if not items:
            return []
        values = items.split("\n")
        if end < 0:
            end = len(values) - 1
        return values[start : end + 1]


def test_debug_store_records_transition_history() -> None:
    redis = _FakeRedis()
    store = PositionDebugStore(redis=redis, key_prefix="position")
    occurred_at = datetime(2024, 1, 1, tzinfo=UTC)

    store.record_transition(
        symbol="BTCUSDT",
        previous_lifecycle="flat",
        current_lifecycle="open_long",
        reason="signal_open_long",
        occurred_at=occurred_at,
    )

    history = store.get_history("BTCUSDT")

    assert len(history) == 1
    assert history[0]["reason"] == "signal_open_long"
    assert history[0]["previous_lifecycle"] == "flat"
    assert history[0]["current_lifecycle"] == "open_long"
