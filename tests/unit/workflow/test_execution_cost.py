"""Offline cost accounting and percentage-exit regression tests."""
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import ModuleType, SimpleNamespace
import sys
import unittest
from unittest.mock import patch

from test_durable_workflow import FakeRedis, WatchConflict, risk_event, action_event
from trading_engine.app.binance_user_data_stream import parse_order_trade_update
from trading_engine.app.bootstrap import build_strategy_engine
from trading_engine.app.position_workflow import PositionWorkflow
from trading_engine.app.run_strategy_engine_factor import evaluate_once
from trading_engine.app.trade_workflow import TradeWorkflow, TradeExecutionWorker
from trading_engine.common.execution_cost import cumulative_quote
from trading_engine.config.settings import PositionEngineSettings, StrategyEngineSettings, TradeEngineSettings
from trading_engine.contracts.messages import (
    EngineEventType, OrderUpdatePayload, PositionSignalCommand, SignalDirection, build_event,
)
from trading_engine.contracts.serde import encode_event, decode_event
from trading_engine.infra.redis_position_repository import RedisPositionRepository
from trading_engine.infra.redis_workflow import RedisWorkflowStore
from trading_engine.infra.binance_futures_ws_gateway import BinanceFuturesWsGateway
from trading_engine.position.cost import apply_execution
from trading_engine.position.manager import PositionManager
from trading_engine.position.models import (
    PositionState, PositionDirection as Direction, PositionLifecycle as Lifecycle,
    PositionOrderEvent, OrderUpdateStatus as Status,
)
from trading_engine.strategy.models import FactorStrategyContext
from trading_engine.strategy.price_exit import PriceExitPolicy
from trading_engine.trade.models import TradeExecutionResult, TradeExecutionStatus, TradeOrderRequest


NOW = datetime(2026, 9, 23, tzinfo=UTC)


def opening(short=False):
    return PositionState("BTCUSDT", Direction.FLAT,
                         Lifecycle.OPENING_SHORT if short else Lifecycle.OPENING_LONG,
                         active_order_id="order-1", active_client_order_id="te-1")


def fill(quantity, quote=None, status=Status.PARTIALLY_FILLED, **kwargs):
    return PositionOrderEvent("BTCUSDT", status, NOW, order_id="order-1",
                              client_order_id="te-1", cumulative_filled_quantity=quantity,
                              cumulative_filled_quote=quote, **kwargs)


def holding(short=False):
    return PositionState("BTCUSDT", Direction.SHORT if short else Direction.LONG,
                         Lifecycle.SHORT if short else Lifecycle.LONG, quantity=2,
                         entry_avg_price="100", cost_complete=True, opened_at=NOW)


def context(state, price=100, now=NOW):
    snapshot = SimpleNamespace(symbol="BTCUSDT", close=price, open_time=NOW,
                               score_ema=0, score_dmi_adx=0, score_rsi=0,
                               score_flow=0, score_funding=0)
    return FactorStrategyContext(snapshot, now, state, True)


class CostTests(unittest.TestCase):
    def test_weighted_cost_replay_and_terminal_cumulative(self):
        state = apply_execution(opening(), fill(1, "100"))
        self.assertEqual(state.entry_avg_price, "100")
        state = RedisPositionRepository.decode(RedisPositionRepository.encode(state))
        replay = apply_execution(state, fill(1, "100"))
        self.assertEqual(replay, state)
        state = apply_execution(replay, fill(2, "210", Status.FILLED))
        self.assertEqual(state.quantity, 2)
        self.assertEqual(state.entry_avg_price, "105")
        self.assertTrue(state.cost_complete)
        self.assertEqual(state.opened_at, NOW)
        self.assertIsNone(state.active_order_id)

    def test_cancel_keeps_fills_and_applies_final_increment(self):
        for short in (False, True):
            state = apply_execution(opening(short), fill(.4, "40"))
            state = apply_execution(state, fill(.6, "62", Status.CANCELED))
            self.assertEqual(state.quantity, .6)
            self.assertEqual(Decimal(state.entry_avg_price), Decimal(62) / Decimal(".6"))
            self.assertEqual(state.lifecycle, Lifecycle.SHORT if short else Lifecycle.LONG)

    def test_partial_close_keeps_cost_full_close_clears(self):
        for short in (False, True):
            state = replace(holding(short), lifecycle=Lifecycle.CLOSING_SHORT if short else Lifecycle.CLOSING_LONG)
            state = apply_execution(state, fill(.5, "55", Status.FILLED))
            self.assertEqual(state.quantity, 1.5)
            self.assertEqual(state.entry_avg_price, "100")
            self.assertEqual(state.opened_at, NOW)
            state = replace(state, lifecycle=Lifecycle.CLOSING_SHORT if short else Lifecycle.CLOSING_LONG)
            state = apply_execution(state, fill(1.5, "165", Status.FILLED))
            self.assertEqual(state.lifecycle, Lifecycle.FLAT)
            self.assertIsNone(state.entry_avg_price)
            self.assertIsNone(state.opened_at)

    def test_missing_cost_can_be_completed_by_later_cumulative_quote(self):
        state = apply_execution(opening(), fill(1))
        self.assertFalse(state.cost_complete)
        state = apply_execution(state, fill(1, "100"))
        self.assertEqual(state.quantity, 1)
        self.assertEqual(state.entry_avg_price, "100")
        state = apply_execution(state, fill(2))
        self.assertFalse(state.cost_complete)
        self.assertIsNone(state.entry_avg_price)
        state = apply_execution(state, fill(2, "210", Status.FILLED))
        self.assertEqual(state.entry_avg_price, "105")

    def test_old_unknown_holding_is_not_repaired_by_new_order(self):
        legacy = RedisPositionRepository.decode({
            "symbol": "BTCUSDT", "direction": "long", "lifecycle": "long", "quantity": 1,
        })
        state = apply_execution(replace(legacy, lifecycle=Lifecycle.OPENING_LONG), fill(1, "110", Status.FILLED))
        self.assertEqual(state.quantity, 2)
        self.assertFalse(state.cost_complete)
        self.assertIsNone(state.entry_avg_price)

    def test_known_base_weighting_and_exact_money(self):
        state = replace(holding(), lifecycle=Lifecycle.OPENING_LONG)
        state = apply_execution(state, fill(1, "110", Status.FILLED))
        self.assertEqual(Decimal(state.entry_avg_price), Decimal(310) / Decimal(3))
        self.assertEqual(cumulative_quote("0.12345678", "65000.123456789"),
                         str(Decimal("0.12345678") * Decimal("65000.123456789")))

    def test_stale_terminal_does_not_drop_newer_fills(self):
        state = apply_execution(opening(), fill(2, "210"))
        self.assertEqual(apply_execution(state, fill(1, "100", Status.CANCELED)), state)

    def test_invalid_values_fail_before_persistence(self):
        for value in ("NaN", "Infinity", "-1"):
            with self.assertRaises(ValueError):
                apply_execution(opening(), fill(1, value))
            with self.assertRaises(ValueError):
                cumulative_quote("1", value)
        with self.assertRaises(ValueError):
            apply_execution(opening(), fill(None, None, Status.FILLED))

    def test_last_price_only_covers_exact_fill_increment(self):
        state = apply_execution(opening(), fill(1, last_filled_quantity=1, last_filled_price="100"))
        self.assertEqual(state.entry_avg_price, "100")
        state = apply_execution(state, fill(3, last_filled_quantity=1, last_filled_price="110"))
        self.assertIsNone(state.entry_avg_price)  # a missing fill cannot use last price

    def test_manager_preserves_cost_on_close_and_uses_execution_quantity(self):
        repo = SimpleNamespace(state=holding())
        repo.get = lambda symbol: repo.state
        repo.save = lambda state: setattr(repo, "state", state)
        manager = PositionManager(repo)
        command = PositionSignalCommand("test", "BTCUSDT", SignalDirection.FLAT, 1, 1, NOW,
                                        {"approved_quantity": 99})
        decision = manager.handle_signal(command)
        self.assertEqual(decision.trade_action.quantity, 2)
        self.assertEqual(decision.trade_action.metadata["reduceOnly"], "true")
        self.assertEqual(decision.state.entry_avg_price, "100")
        manager.handle_order_event(PositionOrderEvent("BTCUSDT", Status.NEW, NOW, order_id="order-1"))
        decision = manager.handle_order_event(fill(.5, "55", Status.CANCELED))
        self.assertEqual(decision.state.quantity, 1.5)
        self.assertEqual(decision.state.entry_avg_price, "100")

    def test_user_stream_and_serde_keep_decimal_cost(self):
        payload = parse_order_trade_update({"e": "ORDER_TRADE_UPDATE", "o": {
            "s": "BTCUSDT", "i": 1, "X": "PARTIALLY_FILLED", "z": "0.3",
            "l": "0.1", "ap": "100.123456789", "L": "101.12", "c": "te-1",
        }})
        self.assertEqual(payload.cumulative_filled_quote, "30.0370370367")
        event = build_event(EngineEventType.ORDER_UPDATE_RECEIVED, payload, producer="test", occurred_at=NOW)
        self.assertEqual(decode_event(encode_event(event)).payload, payload)

    def test_gateway_query_preserves_cumulative_quote(self):
        transport = SimpleNamespace(request=lambda *args: {"result": {
            "orderId": 1, "clientOrderId": "te-1", "symbol": "BTCUSDT", "status": "FILLED",
            "executedQty": "2", "avgPrice": "105", "cumQuote": "210.0000000001",
        }})
        gateway = BinanceFuturesWsGateway("unused", "dummy", "dummy", transport=transport)
        result = gateway.query_order("BTCUSDT", "te-1")
        self.assertEqual(result.cumulative_filled_quote, "210.0000000001")

    def test_gateway_final_close_parameters(self):
        for side in ("BOTH", "SHORT", "LONG"):
            sent = []
            transport = SimpleNamespace(request=lambda endpoint, message, timeout: (
                sent.append(message) or {"result": {"status": "FILLED", "executedQty": "1",
                                                    "avgPrice": "100", "orderId": 1}}))
            gateway = BinanceFuturesWsGateway("unused", "dummy", "dummy", transport=transport)
            with patch.object(BinanceFuturesWsGateway, "_normalize_quantity", return_value=Decimal(1)):
                result = gateway.submit_order(TradeOrderRequest(
                    "BTCUSDT", "SELL", 1, "MARKET", NOW, "c", "e",
                    metadata={"positionSide": side, "reduceOnly": "true"}))
            self.assertEqual(sent[0]["params"]["positionSide"], side)
            self.assertEqual(sent[0]["params"].get("reduceOnly"), "true" if side == "BOTH" else None)
            self.assertEqual(result.cumulative_filled_quote, "100")


class ExitTests(unittest.TestCase):
    def test_runner_reads_execution_position_and_publishes_flat(self):
        settings = StrategyEngineSettings(take_profit_pct="5")
        engine = build_strategy_engine(SimpleNamespace(generate=lambda ctx: None), settings)
        observed = []
        repository = SimpleNamespace(get=lambda symbol: (observed.append(symbol) or holding()))
        published = []
        with patch("trading_engine.app.run_strategy_engine_factor.row_to_context",
                   return_value=replace(context(None, 105), position_loaded=False)):
            evaluate_once(engine, SimpleNamespace(fetch_latest=lambda **kwargs: {}),
                          SimpleNamespace(publish=published.append), "BTCUSDT", repository)
        self.assertEqual(observed, ["BTCUSDT"])
        self.assertEqual(published[0].direction, SignalDirection.FLAT)

    def test_flat_position_delegates_to_original_entry_algorithm(self):
        seen = []
        engine = build_strategy_engine(SimpleNamespace(generate=lambda ctx: seen.append(ctx)),
                                       StrategyEngineSettings(take_profit_pct="5", min_confidence=0))
        engine.evaluate(context(None))
        self.assertEqual(len(seen), 1)

    def test_long_short_profit_loss_and_inclusive_thresholds(self):
        policy = PriceExitPolicy("5", "2")
        for short, price, reason in ((False, 105, "take_profit"), (False, 98, "stop_loss"),
                                     (True, 95, "take_profit"), (True, 102, "stop_loss")):
            decision = policy.evaluate(context(holding(short), price))
            self.assertEqual(decision.signal.direction, SignalDirection.FLAT)
            self.assertEqual(decision.signal.metadata["reason"], reason)
        self.assertIsNone(policy.evaluate(context(holding(), 104.99)).signal)

    def test_unknown_cost_transition_and_invalid_price_do_not_exit(self):
        policy = PriceExitPolicy("5", "2")
        for state, price in ((replace(holding(), cost_complete=False), 110),
                             (replace(holding(), lifecycle=Lifecycle.CLOSING_LONG), 110),
                             (holding(), float("nan")), (holding(), 0)):
            self.assertIsNone(policy.evaluate(context(state, price)).signal)
        self.assertIsNone(policy.evaluate(replace(context(holding(), 110), position_loaded=False)).signal)

    def test_enabled_exits_bypass_confidence_but_not_freshness(self):
        algorithm = SimpleNamespace(generate=lambda context: self.fail("entry algorithm must not manage exits"))
        engine = build_strategy_engine(algorithm, StrategyEngineSettings(take_profit_pct="5", stop_loss_pct="2"))
        self.assertIsNotNone(engine.evaluate(context(holding(), 105)).signal)
        self.assertEqual(engine.evaluate(context(holding(), 101)).rejected_reasons, ("price_exit_not_triggered",))
        for time in (NOW + timedelta(seconds=3), NOW - timedelta(seconds=1)):
            self.assertIsNone(engine.evaluate(context(holding(), 105, time)).signal)

    def test_disabled_policy_preserves_existing_entry_rules(self):
        algorithm = SimpleNamespace(generate=lambda context: self.fail("low confidence must be rejected"))
        engine = build_strategy_engine(algorithm, StrategyEngineSettings())
        self.assertEqual(engine.evaluate(context(holding(), 105)).rejected_reasons, ("confidence_too_low",))

    def test_invalid_configuration_rejected(self):
        for value in ("0", "-1", "nan", "inf", "hello"):
            with self.assertRaises(ValueError):
                PriceExitPolicy(value)


class DurableCostTests(unittest.TestCase):
    def setUp(self):
        redis_module, exceptions = ModuleType("redis"), ModuleType("redis.exceptions")
        exceptions.WatchError = WatchConflict
        redis_module.exceptions = exceptions
        modules = patch.dict(sys.modules, {"redis": redis_module, "redis.exceptions": exceptions})
        modules.start()
        self.addCleanup(modules.stop)
        self.redis = FakeRedis()
        self.store = RedisWorkflowStore(self.redis, "position:execution", "test")
        self.position = PositionWorkflow(self.store, PositionEngineSettings())

    def update(self, qty, quote, status="partially_filled"):
        state = self.store.get("BTCUSDT")
        payload = OrderUpdatePayload("BTCUSDT", "order-1", status, NOW,
                                     cumulative_filled_quantity=qty, cumulative_filled_quote=quote,
                                     client_order_id=state["active_client_order_id"])
        return build_event(EngineEventType.ORDER_UPDATE_RECEIVED, payload, producer="test", occurred_at=NOW)

    def test_transaction_restart_replay_cancel_and_snapshot(self):
        self.position.handle(risk_event())
        event = self.update(.4, "40")
        self.position.handle(event)
        self.position = PositionWorkflow(self.store, PositionEngineSettings())
        self.position.handle(event)
        self.assertEqual(self.store.get("BTCUSDT")["quantity"], .4)
        event = self.update(.6, "62", "canceled")
        self.redis.fail_execute = True
        with self.assertRaises(ConnectionError):
            self.position.handle(event)
        self.assertEqual(self.store.get("BTCUSDT")["quantity"], .4)
        self.position.handle(event)
        state = self.store.get("BTCUSDT")
        self.assertEqual(state["quantity"], .6)
        self.assertTrue(state["cost_complete"])
        events = [decode_event(fields["body"]) for _, fields in self.redis.xrange(self.store.outbox, count=30)]
        snapshots = [e.payload.current for e in events if e.event_type is EngineEventType.POSITION_STATE_CHANGED]
        self.assertEqual(snapshots[-1].entry_avg_price, state["entry_avg_price"])

    def test_trade_query_result_cost_is_persisted_and_published(self):
        store = RedisWorkflowStore(self.redis, "trade:execution", "test")
        workflow = TradeWorkflow(store, TradeEngineSettings())
        workflow.handle_action(action_event())
        result = TradeExecutionResult("BTCUSDT", TradeExecutionStatus.FILLED, NOW,
                                      order_id="order-1", client_order_id="te-1", filled_quantity=1,
                                      cumulative_filled_quote="100.123456789")
        gateway = SimpleNamespace(submit_order=lambda req: result)
        TradeExecutionWorker(workflow, gateway).run_once()
        self.assertEqual(store.get("te-1")["cumulative_filled_quote"], "100.123456789")
        events = [decode_event(fields["body"]) for _, fields in self.redis.xrange(store.outbox, count=10)]
        self.assertEqual(events[-1].payload.cumulative_filled_quote, "100.123456789")

    def test_unknown_recovery_query_propagates_cost_to_position(self):
        self.position.handle(risk_event())
        client_id = self.store.get("BTCUSDT")["active_client_order_id"]
        store = RedisWorkflowStore(self.redis, "trade:execution", "test")
        workflow = TradeWorkflow(store, TradeEngineSettings())
        action = action_event()
        action = replace(action, payload=replace(action.payload, metadata={"newClientOrderId": client_id}))
        workflow.handle_action(action)
        gateway = SimpleNamespace(submit_order=lambda req: (_ for _ in ()).throw(TimeoutError()),
            query_order=lambda *args: TradeExecutionResult(
                "BTCUSDT", TradeExecutionStatus.FILLED, NOW, order_id="order-1",
                client_order_id=client_id, filled_quantity=1, cumulative_filled_quote="105"))
        TradeExecutionWorker(workflow, gateway).run_once()
        self.assertEqual(store.get(client_id)["phase"], "unknown")
        import time
        with patch("trading_engine.app.trade_workflow.time.time", return_value=time.time() + 40):
            TradeExecutionWorker(workflow, gateway).run_once()
        for _, fields in self.redis.xrange(store.outbox, count=10):
            self.position.handle(decode_event(fields["body"]))
        self.assertEqual(self.store.get("BTCUSDT")["entry_avg_price"], "105")

    def test_trade_amount_tracks_quantity_and_rejects_regression(self):
        state = {"status": "new", "cumulative_filled_quantity": 0, "cumulative_filled_quote": "0"}
        TradeWorkflow.apply_result(state, "partially_filled", "order-1", 1, "100")
        TradeWorkflow.apply_result(state, "partially_filled", "order-1", 2)
        self.assertIsNone(state["cumulative_filled_quote"])
        TradeWorkflow.apply_result(state, "partially_filled", "order-1", 2, "210")
        self.assertEqual(state["cumulative_filled_quote"], "210")
        self.assertFalse(TradeWorkflow.apply_result(state, "filled", "order-1", 1, "100"))
        self.assertEqual(state["status"], "partially_filled")

    def test_percentage_exit_is_bound_to_current_holding(self):
        self.position.handle(risk_event())
        self.position.handle(self.update(1, "100", "filled"))
        raw = self.store.get("BTCUSDT")
        state = RedisPositionRepository.decode(raw)
        signal = PriceExitPolicy("5").evaluate(context(state, 105)).signal
        risk = risk_event()
        approved = replace(risk.payload.approved_signal, direction="flat", metadata=signal.metadata)
        event = replace(risk, event_id="exit-1", payload=replace(risk.payload, approved_signal=approved))
        stale = replace(event, event_id="stale", payload=replace(event.payload, approved_signal=replace(
            approved, metadata={**approved.metadata, "position_opened_at": "old"})))
        self.position.handle(stale)
        self.assertEqual(self.store.get("BTCUSDT")["lifecycle"], "long")
        self.position.handle(event)
        self.assertEqual(self.store.get("BTCUSDT")["lifecycle"], "close_long")
        events = [decode_event(fields["body"]) for _, fields in self.redis.xrange(self.store.outbox, count=30)]
        action = [e.payload for e in events if e.event_type is EngineEventType.TRADE_ACTION_REQUESTED][-1]
        self.assertEqual(action.quantity, 1)
        self.assertEqual(action.metadata["reduceOnly"], "true")


if __name__ == "__main__":
    unittest.main()
