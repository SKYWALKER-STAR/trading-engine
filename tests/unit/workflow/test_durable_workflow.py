"""Offline transaction-protocol tests. Real Redis/Kafka tests are separate."""
from __future__ import annotations

from collections import namedtuple
from copy import deepcopy
from datetime import UTC, datetime
import json
import fnmatch
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from trading_engine.app.position_workflow import PositionWorkflow
from trading_engine.app.trade_workflow import TradeExecutionWorker, TradeWorkflow
from trading_engine.config.settings import PositionEngineSettings, TradeEngineSettings
from trading_engine.contracts.messages import (
    EngineEventType, OrderUpdatePayload, RiskAction, RiskDecisionPayload,
    StrategySignalPayload, TradeActionPayload, build_event,
)
from trading_engine.contracts.serde import decode_event, encode_event
from trading_engine.infra.kafka_event_bus import KafkaEventConsumer
from trading_engine.infra.redis_workflow import OutboxRelay, RedisWorkflowStore
from trading_engine.infra.binance_futures_ws_gateway import BinanceFuturesWsGateway, BinanceWsApiTransport
from trading_engine.app.run_position_state_migrate import migrate_states
from trading_engine.trade.models import TradeExecutionResult, TradeExecutionStatus


class WatchConflict(Exception):
    pass


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.versions = {}
        self.counter = 0
        self.before_execute = None
        self.fail_execute = False
        self.fail_delete = False

    def type(self, key):
        value = self.values.get(key)
        return "none" if value is None else "stream" if isinstance(value, list) else "zset" if isinstance(value, dict) else "string"

    def get(self, key):
        return self.values.get(key)

    def exists(self, key):
        return key in self.values

    def set(self, key, value, nx=False):
        if nx and key in self.values:
            return False
        self.values[key] = value
        self.versions[key] = self.versions.get(key, 0) + 1
        return True

    def xadd(self, key, fields):
        self.counter += 1
        entry_id = f"{self.counter}-0"
        self.values.setdefault(key, []).append((entry_id, deepcopy(fields)))
        return entry_id

    def xrange(self, key, count=1):
        return deepcopy(self.values.get(key, [])[:count])

    def xdel(self, key, identity):
        if self.fail_delete:
            self.fail_delete = False
            raise ConnectionError("crash after broker ack")
        self.values[key] = [item for item in self.values.get(key, []) if item[0] != identity]

    def zadd(self, key, mapping):
        self.values.setdefault(key, {}).update(mapping)

    def zrem(self, key, identity):
        self.values.get(key, {}).pop(identity, None)

    def zrangebyscore(self, key, minimum, maximum, start=0, num=100):
        entries = sorted(self.values.get(key, {}).items(), key=lambda item: item[1])
        return [identity for identity, due in entries if due <= maximum][start:start + num]

    def pipeline(self):
        return Pipeline(self)

    def scan_iter(self, match):
        return iter([key for key in self.values if fnmatch.fnmatchcase(key, match)])

    def lock(self, key, timeout):
        return Lock()


class Lock:
    def __init__(self):
        self.active = False

    def acquire(self, blocking):
        self.active = True
        return True

    def owned(self):
        return self.active

    def extend(self, *args, **kwargs):
        pass

    def release(self):
        self.active = False


class Pipeline:
    def __init__(self, redis):
        self.redis = redis
        self.commands = []
        self.watched = {}
        self.queued = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def watch(self, *keys):
        self.watched = {key: self.redis.versions.get(key, 0) for key in keys}

    def multi(self):
        self.queued = True

    def __getattr__(self, name):
        def call(*args, **kwargs):
            if self.queued:
                self.commands.append((name, args, kwargs))
            else:
                return getattr(self.redis, name)(*args, **kwargs)
        return call

    def execute(self):
        if self.redis.before_execute:
            callback, self.redis.before_execute = self.redis.before_execute, None
            callback()
        if any(self.redis.versions.get(k, 0) != v for k, v in self.watched.items()):
            raise WatchConflict()
        if self.redis.fail_execute:
            self.redis.fail_execute = False
            raise ConnectionError("crash before commit")
        return [getattr(self.redis, name)(*args, **kwargs) for name, args, kwargs in self.commands]


class Publisher:
    def __init__(self):
        self.events = []
        self.fail = False

    def publish(self, topic, event, key):
        if self.fail:
            raise ConnectionError("Kafka unavailable")
        self.events.append((topic, event, key))


def risk_event():
    now = datetime.now(UTC)
    return build_event(EngineEventType.RISK_DECISION_MADE, RiskDecisionPayload(
        symbol="BTCUSDT", action=RiskAction.APPROVE,
        approved_signal=StrategySignalPayload("test", "BTCUSDT", "long", 1, 1, now,
                                              {"approved_quantity": 1.0}),
        reason="open", decided_at=now,
    ), producer="test", occurred_at=now, correlation_id="chain-1")


def action_event():
    now = datetime.now(UTC)
    return build_event(EngineEventType.TRADE_ACTION_REQUESTED, TradeActionPayload(
        symbol="BTCUSDT", action="open_long", side="BUY", requested_at=now, quantity=1,
        metadata={"newClientOrderId": "te-1"},
    ), producer="test", occurred_at=now, correlation_id="chain-1")


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        redis_module, exceptions = ModuleType("redis"), ModuleType("redis.exceptions")
        exceptions.WatchError = WatchConflict
        redis_module.exceptions = exceptions
        self.modules = patch.dict(sys.modules, {"redis": redis_module, "redis.exceptions": exceptions})
        self.modules.start()
        self.addCleanup(self.modules.stop)
        self.redis = FakeRedis()
        self.store = RedisWorkflowStore(self.redis, "position:execution", "account-1")
        self.position = PositionWorkflow(self.store, PositionEngineSettings())

    def test_position_commits_state_receipt_and_complete_outbox(self):
        event = risk_event()
        self.position.handle(event)
        state = self.store.get("BTCUSDT")
        self.assertEqual(state["lifecycle"], "open_long")
        rows = self.redis.xrange(self.store.outbox, count=10)
        self.assertEqual(len(rows), 2)
        action = decode_event(rows[1][1]["body"])
        self.assertEqual(action.correlation_id, "chain-1")
        self.assertEqual(action.causation_id, event.event_id)
        self.assertEqual(action.payload.metadata["newClientOrderId"], state["active_client_order_id"])
        # Simulated process restart and redelivery: no additional outgoing event.
        PositionWorkflow(self.store, PositionEngineSettings()).handle(event)
        self.assertEqual(len(self.redis.xrange(self.store.outbox, count=10)), 2)

    def test_crash_before_commit_leaves_no_receipt_or_state(self):
        event = risk_event()
        self.redis.fail_execute = True
        with self.assertRaises(ConnectionError):
            self.position.handle(event)
        self.assertEqual(self.redis.values, {})
        self.position.handle(event)
        self.assertIsNotNone(self.store.get("BTCUSDT"))

    def test_watch_conflict_recomputes_from_latest_state(self):
        event = risk_event()
        self.redis.before_execute = lambda: self.redis.set(self.store.state_key("BTCUSDT"), json.dumps({
            "symbol": "BTCUSDT", "direction": "long", "lifecycle": "long", "quantity": 2,
        }))
        self.position.handle(event)
        self.assertEqual(self.store.get("BTCUSDT")["quantity"], 2)
        self.assertEqual(self.redis.xrange(self.store.outbox), [])

    def test_relay_replays_identical_event_after_ack_crash(self):
        self.position.handle(risk_event())
        publisher = Publisher()
        relay = OutboxRelay(self.store, publisher)
        self.redis.fail_delete = True
        with self.assertRaises(ConnectionError):
            relay.run_once()
        first_id = publisher.events[0][1].event_id
        self.assertEqual(relay.run_once(), 2)
        self.assertEqual(publisher.events[1][1].event_id, first_id)
        self.assertEqual(self.redis.xrange(self.store.outbox), [])

    def test_kafka_failure_keeps_oldest_event_and_order(self):
        self.position.handle(risk_event())
        publisher = Publisher()
        publisher.fail = True
        with self.assertRaises(ConnectionError):
            OutboxRelay(self.store, publisher).run_once()
        self.assertEqual(len(self.redis.xrange(self.store.outbox, count=10)), 2)

    def test_matching_fill_before_new_and_repeated_terminal_are_safe(self):
        self.position.handle(risk_event())
        client_id = self.store.get("BTCUSDT")["active_client_order_id"]
        now = datetime.now(UTC)
        def fill():
            return build_event(EngineEventType.ORDER_UPDATE_RECEIVED, OrderUpdatePayload(
                symbol="BTCUSDT", status="filled", order_id="1", client_order_id=client_id,
                updated_at=now, filled_quantity=1, cumulative_filled_quantity=1,
            ), producer="stream", occurred_at=now)
        self.position.handle(fill())
        self.position.handle(fill())
        self.assertEqual(self.store.get("BTCUSDT")["quantity"], 1)
        self.assertEqual(self.store.get("BTCUSDT")["lifecycle"], "long")

    def test_projection_namespace_is_distinct(self):
        self.position.handle(risk_event())
        self.redis.set("binance:position:usdt_futures:view:state:BTCUSDT:v1", "flat")
        self.assertEqual(self.store.get("BTCUSDT")["lifecycle"], "open_long")

    def trade_setup(self):
        self.store = RedisWorkflowStore(self.redis, "trade:execution")
        flow = TradeWorkflow(self.store, TradeEngineSettings())
        flow.handle_action(action_event())
        return flow

    def test_trade_intake_persists_task_and_deduplicates_business_identity(self):
        flow = self.trade_setup()
        flow.handle_action(action_event())
        self.assertEqual(self.store.get("te-1")["phase"], "pending_submit")
        self.assertEqual(len(self.redis.values[self.store.tasks]), 1)
        self.assertEqual(self.redis.xrange(self.store.outbox), [])

    def test_unknown_is_queried_and_not_resubmitted(self):
        flow = self.trade_setup()
        gateway = SimpleNamespace(submits=0, queries=0)
        def submit(request):
            gateway.submits += 1
            raise TimeoutError()
        def query(symbol, client_id):
            gateway.queries += 1
            return None
        gateway.submit_order, gateway.query_order = submit, query
        worker = TradeExecutionWorker(flow, gateway)
        worker.run_once()
        self.assertEqual(self.store.get("te-1")["phase"], "unknown")
        with patch("time.time", return_value=10**12):
            worker.run_once()
        self.assertEqual((gateway.submits, gateway.queries), (1, 1))
        self.assertEqual(self.store.get("te-1")["last_error"], "order_not_found")

    def test_crash_after_send_recovers_by_query_and_publishes_result(self):
        flow = self.trade_setup()
        calls = []
        def submit(request):
            calls.append("submit")
            raise SystemExit("process died after exchange accepted")
        def query(symbol, client_id):
            calls.append("query")
            return TradeExecutionResult(symbol, TradeExecutionStatus.FILLED, datetime.now(UTC),
                                        order_id="1", filled_quantity=1)
        worker = TradeExecutionWorker(flow, SimpleNamespace(submit_order=submit, query_order=query))
        with self.assertRaises(SystemExit):
            worker.run_once()
        with patch("time.time", return_value=10**12):
            TradeExecutionWorker(flow, worker.gateway).run_once()
        self.assertEqual(calls, ["submit", "query"])
        self.assertEqual(self.store.get("te-1")["phase"], "done")
        self.assertEqual(len(self.redis.xrange(self.store.outbox, count=10)), 2)
        self.assertEqual(self.store.due(10**12), [])

    def test_terminal_user_update_does_not_echo_or_regress(self):
        flow = self.trade_setup()
        now = datetime.now(UTC)
        def update(status, cumulative):
            return build_event(EngineEventType.ORDER_UPDATE_RECEIVED, OrderUpdatePayload(
                symbol="BTCUSDT", order_id="1", client_order_id="te-1", status=status,
                updated_at=now, cumulative_filled_quantity=cumulative,
            ), producer="stream", occurred_at=now)
        flow.handle_update(update("filled", 1))
        flow.handle_update(update("new", 0))
        self.assertEqual(self.store.get("te-1")["status"], "filled")
        self.assertEqual(self.redis.xrange(self.store.outbox), [])

    def test_manual_commit_only_after_handler_success(self):
        kafka = ModuleType("kafka")
        structs = ModuleType("kafka.structs")
        kafka.TopicPartition = namedtuple("TopicPartition", "topic partition")
        structs.OffsetAndMetadata = namedtuple("OffsetAndMetadata", "offset metadata")
        consumer = KafkaEventConsumer("unused", "test", manual_commit=True)
        event = risk_event()
        message = SimpleNamespace(topic="risk", partition=2, offset=7, value=encode_event(event))
        class FakeConsumer:
            commits = []
            def __iter__(self):
                return iter([message])
            def commit(self, offsets):
                self.commits.append(offsets)
        transport = FakeConsumer()
        consumer._consumer = transport
        consumer.subscribe("risk", lambda event: self.position.handle(event))
        with patch.dict(sys.modules, {"kafka": kafka, "kafka.structs": structs}):
            consumer.consume_forever(["risk"])
            self.assertEqual(list(transport.commits[0].values())[0].offset, 8)
            self.redis.fail_execute = True
            message.value = encode_event(risk_event())
            with self.assertRaises(ConnectionError):
                consumer.consume_forever(["risk"])
            self.assertEqual(len(transport.commits), 1)

    def test_invalid_duplicate_does_not_cancel_existing_execution_task(self):
        flow = self.trade_setup()
        event = action_event()
        from dataclasses import replace
        event = replace(event, payload=replace(event.payload, quantity=0))
        flow.handle_action(event)
        self.assertEqual(self.store.get("te-1")["phase"], "pending_submit")
        self.assertEqual(len(self.redis.values[self.store.tasks]), 1)
        self.assertEqual(self.redis.xrange(self.store.outbox), [])

    def test_commit_failure_after_exchange_response_does_not_repeat_submit(self):
        flow = self.trade_setup()
        calls = []
        result = TradeExecutionResult("BTCUSDT", TradeExecutionStatus.FILLED, datetime.now(UTC),
                                      order_id="1", filled_quantity=1)
        def submit(request):
            calls.append("submit")
            self.redis.fail_execute = True
            return result
        def query(symbol, identity):
            calls.append("query")
            return result
        worker = TradeExecutionWorker(flow, SimpleNamespace(submit_order=submit, query_order=query))
        with self.assertRaises(ConnectionError):
            worker.run_once()
        with patch("time.time", return_value=10**12):
            worker.run_once()
        self.assertEqual(calls, ["submit", "query"])
        self.assertEqual(self.store.get("te-1")["status"], "filled")

    def test_user_fill_during_submit_wins_over_late_new_response(self):
        flow = self.trade_setup()
        now = datetime.now(UTC)
        def submit(request):
            flow.handle_update(build_event(EngineEventType.ORDER_UPDATE_RECEIVED, OrderUpdatePayload(
                symbol="BTCUSDT", order_id="1", client_order_id="te-1", status="filled",
                updated_at=now, cumulative_filled_quantity=1,
            ), producer="stream", occurred_at=now))
            return TradeExecutionResult("BTCUSDT", TradeExecutionStatus.NEW, now, order_id="1", filled_quantity=0)
        TradeExecutionWorker(flow, SimpleNamespace(submit_order=submit)).run_once()
        self.assertEqual(self.store.get("te-1")["status"], "filled")
        self.assertEqual(self.store.due(10**12), [])

    def test_exchange_uncertain_errors_keep_task_for_query(self):
        from decimal import Decimal
        for code, status in ((-1000, 400), (-1006, 400), (-1007, 400), (-1, 503)):
            with self.subTest(code=code):
                self.redis = FakeRedis()
                flow = self.trade_setup()
                calls = []
                def respond(endpoint, message, timeout):
                    calls.append(message["method"])
                    if message["method"] == "order.status":
                        return {"error": {"code": -2013}}
                    return {"status": status, "error": {"code": code, "msg": "uncertain"}}
                gateway = BinanceFuturesWsGateway("unused", "key", "secret",
                                                 transport=SimpleNamespace(request=respond))
                worker = TradeExecutionWorker(flow, gateway)
                with patch.object(BinanceFuturesWsGateway, "_normalize_quantity", return_value=Decimal("1")):
                    worker.run_once()
                state = self.store.get("te-1")
                self.assertEqual(state["phase"], "unknown")
                self.assertIn(str(code), state["last_error"])
                self.assertEqual(self.redis.xrange(self.store.outbox), [])
                with patch("time.time", return_value=10**12):
                    worker.run_once()
                self.assertEqual(calls, ["order.place", "order.status"])

    def test_explicit_precision_rejection_is_terminal(self):
        from decimal import Decimal
        flow = self.trade_setup()
        transport = SimpleNamespace(request=lambda *args: {
            "status": 400, "error": {"code": -1111, "msg": "Bad precision"},
        })
        gateway = BinanceFuturesWsGateway("unused", "key", "secret", transport=transport)
        with patch.object(BinanceFuturesWsGateway, "_normalize_quantity", return_value=Decimal("1")):
            TradeExecutionWorker(flow, gateway).run_once()
        self.assertEqual(self.store.get("te-1")["phase"], "done")
        self.assertEqual(self.store.get("te-1")["status"], "rejected")
        self.assertEqual(len(self.redis.xrange(self.store.outbox)), 1)

    def test_legacy_pending_order_is_only_reconciled(self):
        order = SimpleNamespace(symbol="BTCUSDT", client_order_id="old-1", side="BUY",
                                original_quantity=1, order_type="MARKET", created_at=datetime.now(UTC),
                                metadata={}, order_id=None)
        flow = TradeWorkflow(self.store, TradeEngineSettings(), SimpleNamespace(list_active=lambda **kw: [order]))
        flow.recover_legacy()
        self.assertEqual(self.store.get("old-1")["phase"], "unknown")
        queries = []
        gateway = SimpleNamespace(query_order=lambda symbol, identity: queries.append(identity))
        TradeExecutionWorker(flow, gateway).run_once()
        self.assertEqual(queries, ["old-1"])

    def test_migration_is_preview_first_and_does_not_overwrite(self):
        raw = json.dumps({"symbol": "BTCUSDT", "direction": "long", "lifecycle": "long", "quantity": 1})
        self.redis.set("old:BTCUSDT", raw)
        self.redis.set("old:view:state:BTCUSDT:v1", "projected snapshot")
        preview = migrate_states(self.redis, self.store, "old")
        self.assertEqual(len(preview), 1)
        self.assertIsNone(self.store.get("BTCUSDT"))
        migrate_states(self.redis, self.store, "old", apply=True)
        self.redis.set("old:BTCUSDT", raw.replace('"quantity": 1', '"quantity": 2'))
        migrate_states(self.redis, self.store, "old", apply=True)
        self.assertEqual(self.store.get("BTCUSDT")["quantity"], 1)
        self.assertTrue(self.redis.exists(f"{self.store.base}:initialized"))

    def test_empty_migration_requires_explicit_initialization(self):
        with self.assertRaises(ValueError):
            migrate_states(self.redis, self.store, "old", apply=True)
        self.assertFalse(self.redis.exists(f"{self.store.base}:initialized"))
        migrate_states(self.redis, self.store, "old", apply=True, initialize_empty=True)
        self.assertTrue(self.redis.exists(f"{self.store.base}:initialized"))

    def test_query_order_uses_stable_identity_and_maps_not_found(self):
        messages = []
        def request(endpoint, message, timeout):
            messages.append(message)
            return {"error": {"code": -2013}}
        gateway = BinanceFuturesWsGateway("wss://unused", "dummy", "dummy",
                                         transport=SimpleNamespace(request=request))
        self.assertIsNone(gateway.query_order("BTCUSDT", "te-1"))
        self.assertEqual(messages[0]["method"], "order.status")
        self.assertEqual(messages[0]["params"]["origClientOrderId"], "te-1")

    def test_order_place_transport_does_not_retry_after_send_timeout(self):
        calls = []
        class Connection:
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                pass
            async def send(self, body):
                calls.append(body)
            async def recv(self):
                raise TimeoutError()
        module = ModuleType("websockets")
        module.connect = lambda *args, **kwargs: Connection()
        with patch.dict(sys.modules, {"websockets": module}):
            with self.assertRaises(TimeoutError):
                BinanceWsApiTransport().request("wss://unused", {"method": "order.place"}, 1)
        self.assertEqual(len(calls), 1)

    def test_end_to_end_durable_open_position(self):
        trade_store = RedisWorkflowStore(self.redis, "trade:execution", "account-1")
        trade = TradeWorkflow(trade_store, TradeEngineSettings())
        def route(topic, event, key):
            if event.event_type is EngineEventType.TRADE_ACTION_REQUESTED:
                trade.handle_action(event)
            elif event.event_type is EngineEventType.ORDER_UPDATE_RECEIVED:
                self.position.handle(event)
                trade.handle_update(event)
        publisher = SimpleNamespace(publish=route)
        self.position.handle(risk_event())
        OutboxRelay(self.store, publisher).run_once()
        gateway = SimpleNamespace(submit_order=lambda request: TradeExecutionResult(
            request.symbol, TradeExecutionStatus.FILLED, datetime.now(UTC),
            order_id="1", client_order_id=request.client_order_id, filled_quantity=1,
        ))
        TradeExecutionWorker(trade, gateway).run_once()
        OutboxRelay(trade_store, publisher).run_once()
        OutboxRelay(self.store, publisher).run_once()
        state = self.store.get("BTCUSDT")
        self.assertEqual((state["lifecycle"], state["quantity"]), ("long", 1))
        self.assertEqual(trade_store.get(state["last_client_order_id"])["phase"], "done")
        self.assertEqual(self.redis.xrange(trade_store.outbox), [])

    def test_position_runtime_requires_migration_marker(self):
        from trading_engine.infra.redis_position_repository import RedisPositionRepository
        from trading_engine.app.position_engine_kafka import build_position_engine_consumer
        repo = RedisPositionRepository("unused", account_id="account-1")
        repo._client = self.redis
        with self.assertRaisesRegex(RuntimeError, "not initialized"):
            build_position_engine_consumer(repo, PositionEngineSettings())
        self.redis.set(f"{self.store.base}:initialized", "1")
        consumer = build_position_engine_consumer(repo, PositionEngineSettings())
        self.assertTrue(consumer._manual_commit)


if __name__ == "__main__":
    unittest.main()
