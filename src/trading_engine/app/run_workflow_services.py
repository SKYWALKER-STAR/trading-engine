from __future__ import annotations

import argparse
import time
from os import getenv
from typing import Any

from trading_engine.app.run_trade_engine import _build_gateway, _validate_settings
from trading_engine.app.trade_workflow import TradeExecutionWorker, TradeWorkflow
from trading_engine.common.logger import configure_logging, get_logger
from trading_engine.config.settings import TradeEngineSettings
from trading_engine.contracts.messages import EngineEvent, EngineEventType
from trading_engine.debug.dashboard import PositionDebugStore
from trading_engine.infra.kafka_event_bus import KafkaEventPublisher
from trading_engine.infra.redis_order_repository import RedisOrderRepository
from trading_engine.infra.redis_workflow import OutboxRelay, RedisWorkflowStore, create_workflow_store


LOGGER = get_logger(__name__)


def workflow_store(kind: str) -> RedisWorkflowStore:
    account = getenv("ORDER_ACCOUNT_ID", "default").strip() or "default"
    if kind == "position":
        return create_workflow_store(
            getenv("POSITION_EXECUTION_KEY_PREFIX", "position:execution"),
            getenv("POSITION_REDIS_URL", "redis://127.0.0.1:6379/0"), account,
        )
    return create_workflow_store(
        getenv("TRADE_WORKFLOW_KEY_PREFIX", "trade:execution"),
        getenv("ORDER_REDIS_URL", "redis://127.0.0.1:6379/0"), account,
    )


def run_outbox(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Publish durable position and trade outboxes")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    configure_logging()
    publisher = KafkaEventPublisher.from_env()
    if publisher._acks not in (-1, "all"):
        raise ValueError("Durable outbox requires KAFKA_ACKS=all")
    debug = PositionDebugStore(
        redis_url=getenv("POSITION_REDIS_URL", "redis://127.0.0.1:6379/0"),
        key_prefix=getenv("POSITION_VIEW_KEY_PREFIX", getenv("POSITION_REDIS_KEY_PREFIX", "binance:position:usdt_futures")),
    )

    def observe(event: EngineEvent[Any]) -> None:
        if event.event_type is EngineEventType.POSITION_STATE_CHANGED:
            payload = event.payload
            debug.record_transition(
                payload.current.symbol, payload.previous.lifecycle, payload.current.lifecycle,
                payload.reason, payload.occurred_at, direction=payload.current.direction,
                quantity=payload.current.quantity, active_order_id=payload.current.active_order_id,
            )

    relays = [OutboxRelay(workflow_store(kind), publisher, observe if kind == "position" else None)
              for kind in ("position", "trade")]
    while True:
        for relay in relays:
            try:
                relay.run_once()
            except Exception:
                if args.once:
                    raise
                LOGGER.exception("Outbox publish failed; retaining pending events")
        if args.once:
            return
        time.sleep(1)


def run_trade_worker(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Execute durable orders and reconcile uncertain results")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    configure_logging()
    settings = TradeEngineSettings.from_env()
    _validate_settings(settings)
    workflow = TradeWorkflow(workflow_store("trade"), settings, RedisOrderRepository.from_env())
    gateway = _build_gateway(settings)
    workflow.recover_legacy()
    worker = TradeExecutionWorker(workflow, gateway, lease_seconds=max(60, settings.request_timeout_seconds * 3))
    while True:
        try:
            worker.run_once()
        except Exception:
            if args.once:
                raise
            LOGGER.exception("Trade worker failed; persisted tasks remain recoverable")
        if args.once:
            return
        time.sleep(1)
