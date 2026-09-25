from __future__ import annotations

from datetime import UTC, datetime

from trading_engine.infra.binance_position_reconciler import BinancePositionReconciler
from trading_engine.position.models import PositionDirection, PositionLifecycle, PositionState


class FakeRepository:
    def __init__(self) -> None:
        self.states: dict[str, PositionState] = {}

    def get(self, symbol: str) -> PositionState | None:
        return self.states.get(symbol)

    def save(self, state: PositionState) -> None:
        self.states[state.symbol] = state


class FakeAccountTransport:
    def fetch_account_snapshot(self, rest_api_url: str, api_key: str, timeout_seconds: float) -> list[dict[str, object]]:
        return [
            {"symbol": "BTCUSDT", "positionAmt": "0"},
            {"symbol": "ETHUSDT", "positionAmt": "2.5"},
            {"symbol": "SOLUSDT", "positionAmt": "-1.25"},
        ]


def test_reconcile_once_updates_states_from_account_snapshot() -> None:
    repository = FakeRepository()
    repository.save(
        PositionState(
            symbol="BTCUSDT",
            direction=PositionDirection.LONG,
            lifecycle=PositionLifecycle.LONG,
            quantity=1.0,
            updated_at=datetime.now(UTC),
        )
    )

    reconciler = BinancePositionReconciler(
        rest_api_url="https://fapi.binance.com",
        api_key="test-key",
        repository=repository,
        transport=FakeAccountTransport(),
    )

    reconciler.reconcile_once(source="startup")

    btc = repository.states["BTCUSDT"]
    eth = repository.states["ETHUSDT"]
    sol = repository.states["SOLUSDT"]

    assert btc.direction is PositionDirection.FLAT
    assert btc.lifecycle is PositionLifecycle.FLAT
    assert btc.quantity == 0.0

    assert eth.direction is PositionDirection.LONG
    assert eth.lifecycle is PositionLifecycle.LONG
    assert eth.quantity == 2.5

    assert sol.direction is PositionDirection.SHORT
    assert sol.lifecycle is PositionLifecycle.SHORT
    assert sol.quantity == 1.25
