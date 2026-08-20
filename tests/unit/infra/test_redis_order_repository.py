from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest

from trading_engine.infra.redis_order_repository import RedisOrderRepository
from trading_engine.trade.models import TrackedOrder, TrackedOrderStatus
from trading_engine.trade.repository import OrderIdentityConflictError


class _FakePipeline:
    def __init__(self, client: "_FakeRedis") -> None:
        self._client = client
        self._commands: list[tuple[str, tuple[Any, ...]]] = []

    def __enter__(self) -> "_FakePipeline":
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    def watch(self, *_: str) -> None:
        return None

    def multi(self) -> None:
        return None

    def get(self, key: str) -> str | None:
        return self._client.get(key)

    def set(self, key: str, value: str) -> "_FakePipeline":
        self._commands.append(("set", (key, value)))
        return self

    def sadd(self, key: str, value: str) -> "_FakePipeline":
        self._commands.append(("sadd", (key, value)))
        return self

    def srem(self, key: str, value: str) -> "_FakePipeline":
        self._commands.append(("srem", (key, value)))
        return self

    def execute(self) -> None:
        for name, args in self._commands:
            getattr(self._client, name)(*args)
        self._commands.clear()


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.sets: dict[str, set[str]] = {}

    def pipeline(self, transaction: bool = True) -> _FakePipeline:
        return _FakePipeline(self)

    def get(self, key: str) -> str | None:
        return self.values.get(key)

    def set(self, key: str, value: str) -> None:
        self.values[key] = value

    def sadd(self, key: str, value: str) -> None:
        self.sets.setdefault(key, set()).add(value)

    def srem(self, key: str, value: str) -> None:
        self.sets.setdefault(key, set()).discard(value)

    def smembers(self, key: str) -> set[str]:
        return set(self.sets.get(key, set()))


def test_redis_order_repository_saves_binds_and_completes_order() -> None:
    now = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
    redis = _FakeRedis()
    repository = RedisOrderRepository("redis://unused", key_prefix="test-order")
    repository._client = redis
    pending = TrackedOrder(
        exchange="binance",
        account_id="main",
        symbol="BTCUSDT",
        client_order_id="te-client-1",
        side="BUY",
        order_type="MARKET",
        original_quantity=0.1,
        status=TrackedOrderStatus.PENDING_SUBMIT,
        created_at=now,
        updated_at=now,
    )

    repository.save(pending)
    bound = repository.bind_order_id(
        exchange="binance",
        account_id="main",
        client_order_id="te-client-1",
        symbol="BTCUSDT",
        order_id="12345",
    )
    repository.save(
        replace(
            bound,
            status=TrackedOrderStatus.FILLED,
            cumulative_filled_quantity=0.1,
        )
    )

    loaded = repository.get_by_order_id(
        exchange="binance",
        account_id="main",
        symbol="BTCUSDT",
        order_id="12345",
    )
    assert loaded is not None
    assert loaded.client_order_id == "te-client-1"
    assert loaded.status is TrackedOrderStatus.FILLED
    assert repository.list_active(exchange="binance", account_id="main") == []


def test_redis_order_repository_rejects_conflicting_order_binding() -> None:
    now = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
    repository = RedisOrderRepository("redis://unused", key_prefix="test-order")
    repository._client = _FakeRedis()
    repository.save(
        TrackedOrder(
            exchange="binance",
            account_id="main",
            symbol="BTCUSDT",
            client_order_id="te-client-1",
            side="BUY",
            order_type="MARKET",
            original_quantity=0.1,
            status=TrackedOrderStatus.PENDING_SUBMIT,
            created_at=now,
            updated_at=now,
        )
    )
    repository.bind_order_id(
        exchange="binance",
        account_id="main",
        client_order_id="te-client-1",
        symbol="BTCUSDT",
        order_id="12345",
    )

    with pytest.raises(OrderIdentityConflictError):
        repository.bind_order_id(
            exchange="binance",
            account_id="main",
            client_order_id="te-client-1",
            symbol="BTCUSDT",
            order_id="99999",
        )
