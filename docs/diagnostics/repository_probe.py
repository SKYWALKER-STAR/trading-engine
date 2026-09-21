"""Offline audit probes; uses only the standard library and never submits orders.

Run from any directory: python docs/diagnostics/repository_probe.py
These probes describe observed behavior, not a replacement for the pytest suite.
"""
from __future__ import annotations

import ast
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import hashlib
from io import BytesIO
import json
import logging
from pathlib import Path
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "src"))
logging.disable(logging.CRITICAL)

from trading_engine.app.bootstrap import build_strategy_engine
from trading_engine.common.logger import _FORMAT_OVERRIDES
from trading_engine.config.settings import StrategyEngineSettings
from trading_engine.contracts.messages import PositionSignalCommand, SignalDirection
from trading_engine.domain.market_data import MarketFactorSnapshot
from trading_engine.infra.binance_futures_ws_gateway import BinanceFuturesWsGateway
from trading_engine.infra.redis_position_repository import RedisPositionRepository
from trading_engine.infra.redis_position_view_projector import RedisPositionViewProjector
from trading_engine.position.manager import PositionManager
from trading_engine.position.models import (
    OrderUpdateStatus, PositionDirection, PositionLifecycle, PositionOrderEvent, PositionState,
)
from trading_engine.strategy.factor_score import FactorScoreStrategy
from trading_engine.strategy.models import FactorStrategyContext
from trading_engine.trade.models import TradeOrderRequest


class MemoryRepository:
    def __init__(self, state):
        self.state = state

    def get(self, symbol):
        return self.state

    def save(self, state):
        self.state = state


def main():
    results = {}
    source_files = sorted((ROOT / "src").rglob("*.py"))
    test_files = sorted((ROOT / "tests").rglob("*.py"))
    trees = {p: ast.parse(p.read_text(encoding="utf-8-sig")) for p in source_files + test_files}
    results["inventory"] = {
        "source_files": len(source_files),
        "source_lines": sum(len(p.read_text(encoding="utf-8-sig").splitlines()) for p in source_files),
        "test_files": len(test_files),
        "test_functions": sum(
            isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name.startswith("test_")
            for p in test_files for n in ast.walk(trees[p])
        ),
        "syntax_errors": 0,
    }
    ch = ROOT / "src/trading_engine/infra/clickhouse_source.py"
    results["clickhouse_password_default"] = [
        {"line": n.lineno, "nonempty_literal_default": bool(n.args[1].value)}
        for n in ast.walk(trees[ch])
        if isinstance(n, ast.Call) and len(n.args) >= 2
        and isinstance(n.args[0], ast.Constant) and n.args[0].value == "CLICKHOUSE_PASSWORD"
        and isinstance(n.args[1], ast.Constant)
    ]
    now = datetime(2026, 9, 21, tzinfo=UTC)
    repo = MemoryRepository(PositionState(
        "BTCUSDT", PositionDirection.FLAT, PositionLifecycle.OPENING_LONG,
        active_order_id="order-1", updated_at=now,
    ))
    manager = PositionManager(repo)
    manager.handle_order_event(PositionOrderEvent(
        "BTCUSDT", OrderUpdateStatus.PARTIALLY_FILLED, now,
        order_id="order-1", cumulative_filled_quantity=0.4, filled_quantity=0.4,
    ))
    before = repo.state.quantity
    manager.handle_order_event(PositionOrderEvent(
        "BTCUSDT", OrderUpdateStatus.CANCELED, now,
        order_id="order-1", cumulative_filled_quantity=0.4, filled_quantity=0.4,
    ))
    results["partial_then_cancel"] = {
        "quantity_before_cancel": before, "quantity_after_cancel": repo.state.quantity,
        "lifecycle_after_cancel": repo.state.lifecycle.value,
    }
    for name, lifecycle, active in [
        ("fill_before_new", PositionLifecycle.OPEN_LONG, None),
        ("duplicate_terminal_fill", PositionLifecycle.LONG, None),
    ]:
        repo = MemoryRepository(PositionState(
            "BTCUSDT", PositionDirection.LONG, lifecycle, quantity=1,
            active_order_id=active, last_order_id="order-1", updated_at=now,
        ))
        try:
            PositionManager(repo).handle_order_event(PositionOrderEvent(
                "BTCUSDT", OrderUpdateStatus.FILLED, now,
                order_id="order-1", filled_quantity=1,
            ))
        except ValueError as exc:
            results[name] = {"exception": type(exc).__name__, "message": str(exc)}

    projector = RedisPositionViewProjector("redis://unused")
    payload = {"symbol": "BTCUSDT", "positionSide": "BOTH", "positionAmt": "-2",
               "synced_at": now.isoformat()}
    entries = [("BTCUSDT:BOTH", payload)]
    projected = projector._build_symbol_state("BTCUSDT", entries, payload, now.isoformat())
    later = projector._build_symbol_state("BTCUSDT", entries, payload, (now + timedelta(seconds=1)).isoformat())
    digest = lambda obj: hashlib.sha1(json.dumps(obj, sort_keys=True).encode()).hexdigest()
    results["projector"] = {
        "direction": projected["direction"], "quantity": projected["quantity"],
        "same_key_as_position_repository": projector._view_state_key("BTCUSDT")
            == RedisPositionRepository("redis://unused")._view_state_key("BTCUSDT"),
        "same_snapshot_hash_changes_with_poll_time": digest(projected) != digest(later),
        "active_order_id": projected["active_order_id"],
    }

    snapshot = MarketFactorSnapshot(
        symbol="BTCUSDT", interval="1m", open_time=now, close=110,
        ema_12=105, ema_26=100, rsi_14=60, adx_14=25,
        plus_di=30, minus_di=10, taker_buy_ratio=0.6, funding_rate=0,
        score_ema=1, score_dmi_adx=1, score_rsi=1, score_flow=1, score_funding=1,
        trend_score_p=70,
    )
    strategy = FactorScoreStrategy()
    context = FactorStrategyContext(snapshot, now)
    first = strategy.generate(context)
    second = strategy.generate(context)
    results["same_bar_twice"] = {
        "first_direction": first.direction.value,
        "second_direction": second.direction.value, "open_time_unchanged": True,
    }
    fading = replace(snapshot, trend_score_p=0, score_ema=0, score_dmi_adx=0,
                     score_rsi=0, score_flow=0, score_funding=0)
    engine = build_strategy_engine(strategy, StrategyEngineSettings())
    decision = engine.evaluate(FactorStrategyContext(fading, now))
    results["exit_blocked_by_entry_confidence_rule"] = {
        "should_exit_long": strategy._should_exit_long(fading, strategy._trend_history["BTCUSDT"]),
        "signal_emitted": decision.signal is not None,
        "rejected_reasons": decision.rejected_reasons,
    }

    record = logging.LogRecord("trading_engine.app.run_strategy_engine_factor", logging.INFO,
                               __file__, 1, "Strategy engine runner started", (), None)
    record.symbol = "BTCUSDT"
    try:
        logging.Formatter(_FORMAT_OVERRIDES[record.name]).format(record)
    except ValueError as exc:
        results["logging_missing_extra"] = {"exception": str(exc)}

    class FailingPublisher:
        def publish(self, topic, event):
            raise RuntimeError("simulated publish failure")

    repo = MemoryRepository(None)
    manager = PositionManager(repo, publisher=FailingPublisher())
    signal = PositionSignalCommand("probe", "BTCUSDT", SignalDirection.LONG, 1, 1, now,
                                   {"approved_quantity": 1})
    try:
        manager.handle_signal(signal)
    except RuntimeError:
        pass
    replay = manager.handle_signal(signal)
    results["save_then_publish_failure"] = {
        "persisted_lifecycle": repo.state.lifecycle.value,
        "replayed_signal_creates_action": replay.trade_action is not None,
    }

    class CaptureTransport:
        def request(self, endpoint, message, timeout_seconds):
            self.params = message["params"]
            return {"result": {"status": "NEW"}}

    rules = {"symbols": [
        {"symbol": "OTHERUSDT", "filters": [
            {"filterType": "LOT_SIZE", "stepSize": "1"},
            {"filterType": "PRICE_FILTER", "tickSize": "10"}]},
        {"symbol": "BTCUSDT", "filters": [
            {"filterType": "LOT_SIZE", "stepSize": "0.00100000"},
            {"filterType": "PRICE_FILTER", "tickSize": "0.10000000"}]},
    ]}
    transport = CaptureTransport()
    gateway = BinanceFuturesWsGateway("wss://unused", "dummy", "dummy", transport=transport)
    request = TradeOrderRequest(
        "BTCUSDT", "SELL", 0.123456, "LIMIT", now, "probe", "probe",
        metadata={"price": "65000.199999999999999", "timeInForce": "GTC",
                  "positionSide": "SHORT", "reduceOnly": "true"},
    )
    with patch("trading_engine.infra.binance_futures_ws_gateway.urlopen",
               side_effect=lambda *a, **kw: BytesIO(json.dumps(rules).encode())):
        gateway._symbol_step_size_cached.cache_clear()
        gateway._symbol_tick_size_cached.cache_clear()
        gateway.submit_order(request)
        results["gateway_offline_request"] = {
            "quantity": transport.params["quantity"], "price": transport.params["price"],
            "positionSide": transport.params["positionSide"],
            "reduceOnly_present": "reduceOnly" in transport.params,
            "tiny_quantity_fallback": str(gateway._normalize_quantity("BTCUSDT", 0.0004)),
        }
        assert transport.params["quantity"] == "0.123"
        assert transport.params["price"] == "65000.1"
        assert gateway._format_decimal(Decimal("100")) == "100"
        assert gateway._format_decimal(Decimal("0.0000000000001")) == "0.0000000000001"
    print(json.dumps(results, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
