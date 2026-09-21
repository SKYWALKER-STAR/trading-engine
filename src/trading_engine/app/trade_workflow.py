from __future__ import annotations

import hashlib
import math
import time
from dataclasses import asdict, replace
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from trading_engine.app.trade_engine_kafka import _to_trade_order_request
from trading_engine.config.settings import TradeEngineSettings
from trading_engine.contracts.messages import EngineEvent, EngineEventType, OrderUpdatePayload, build_event
from trading_engine.infra.redis_workflow import RedisWorkflowStore, WorkflowChange
from trading_engine.trade.models import TradeExecutionResult, TradeExecutionStatus, TradeOrderRequest


TERMINAL = {"filled", "canceled", "rejected", "expired"}


def request_document(request: TradeOrderRequest) -> dict[str, Any]:
    document = asdict(request)
    document["requested_at"] = request.requested_at.isoformat()
    return document


def request_from_document(document: dict[str, Any]) -> TradeOrderRequest:
    return TradeOrderRequest(**{**document, "requested_at": datetime.fromisoformat(document["requested_at"])})


class TradeWorkflow:
    """Kafka intake only persists work. Workers own exchange I/O and reconciliation."""

    def __init__(self, store: RedisWorkflowStore, settings: TradeEngineSettings,
                 legacy_repository: Any = None) -> None:
        self.store = store
        self.settings = settings
        self.legacy_repository = legacy_repository

    def _new_record(self, request: TradeOrderRequest) -> dict[str, Any]:
        return {
            "request": request_document(request), "symbol": request.symbol.upper(),
            "client_order_id": request.client_order_id, "order_id": None,
            "status": "pending_submit", "phase": "pending_submit",
            "cumulative_filled_quantity": 0.0, "next_due": time.time(),
            "lease_token": None, "lease_until": 0.0,
            "created_at": request.requested_at.isoformat(), "updated_at": datetime.now(UTC).isoformat(),
        }

    def handle_action(self, event: EngineEvent[Any]) -> None:
        if event.event_type is not EngineEventType.TRADE_ACTION_REQUESTED:
            raise ValueError("Unexpected trade input")
        try:
            request = _to_trade_order_request(event.payload, event, self.settings)
            if request is None or not math.isfinite(request.quantity) or request.quantity <= 0:
                raise ValueError("Invalid order quantity or parameters")
            if request.side.upper() not in ("BUY", "SELL"):
                raise ValueError("Invalid order side")
        except (ValueError, TypeError) as exc:
            client_id = str(event.payload.metadata.get("newClientOrderId") or
                            "te-" + hashlib.sha256(event.event_id.encode()).hexdigest()[:32])
            result = TradeExecutionResult(
                symbol=event.payload.symbol, status=TradeExecutionStatus.REJECTED,
                updated_at=datetime.now(UTC), client_order_id=client_id,
                metadata={"error_message": str(exc), "execution_source": "local_validation"},
            )
            self.store.transact(client_id, event.event_id, lambda state: WorkflowChange(
                state, [] if state is not None else self.updates(result, event.correlation_id, event.event_id),
                due_at=(state or {}).get("next_due")))
            return

        identity = str(request.client_order_id)
        legacy = None
        if self.legacy_repository is not None:
            legacy = self.legacy_repository.get_by_client_order_id(
                exchange=self.settings.exchange, account_id=self.settings.order_account_id,
                client_order_id=identity,
            )

        def decide(state: dict[str, Any] | None) -> WorkflowChange:
            if state is not None:
                if state["symbol"] != request.symbol.upper():
                    raise ValueError("Client order ID already belongs to another symbol")
                return WorkflowChange(state, due_at=state.get("next_due"))
            state = self._new_record(request)
            if legacy is not None:
                # A pre-upgrade record may already have reached the exchange, even
                # if it says PENDING_SUBMIT. Reconcile it; never blindly resubmit.
                state.update(phase="unknown", status="unknown", order_id=legacy.order_id)
            return WorkflowChange(state, due_at=state["next_due"])

        self.store.transact(identity, event.event_id, decide)

    def recover_legacy(self) -> None:
        if self.legacy_repository is None:
            return
        for order in self.legacy_repository.list_active(
            exchange=self.settings.exchange, account_id=self.settings.order_account_id,
        ):
            request = TradeOrderRequest(
                symbol=order.symbol, side=order.side, quantity=order.original_quantity,
                order_type=order.order_type, requested_at=order.created_at,
                correlation_id=str(order.metadata.get("correlation_id", order.client_order_id)),
                causation_id=str(order.metadata.get("causation_id", order.client_order_id)),
                client_order_id=order.client_order_id,
            )

            def decide(state: dict[str, Any] | None) -> WorkflowChange:
                if state is None:
                    state = self._new_record(request)
                    state.update(phase="unknown", status="unknown", order_id=order.order_id)
                return WorkflowChange(state, due_at=state.get("next_due"))

            self.store.transact(order.client_order_id, None, decide)

    def updates(self, result: TradeExecutionResult, correlation_id: str,
                causation_id: str) -> list[tuple[str, EngineEvent[Any], str]]:
        statuses = [result.status]
        if result.status not in (TradeExecutionStatus.NEW, TradeExecutionStatus.REJECTED):
            statuses.insert(0, TradeExecutionStatus.NEW)
        return [(
            self.settings.order_update_topic,
            build_event(
                EngineEventType.ORDER_UPDATE_RECEIVED,
                OrderUpdatePayload(
                    symbol=result.symbol, order_id=result.order_id, client_order_id=result.client_order_id,
                    status=status.value, updated_at=result.updated_at,
                    filled_quantity=result.filled_quantity,
                    cumulative_filled_quantity=result.filled_quantity, metadata=dict(result.metadata),
                ),
                producer="trade-engine", occurred_at=result.updated_at,
                correlation_id=correlation_id, causation_id=causation_id,
            ), result.symbol,
        ) for status in statuses]

    def handle_update(self, event: EngineEvent[Any]) -> None:
        if event.event_type is not EngineEventType.ORDER_UPDATE_RECEIVED:
            raise ValueError("Unexpected order update")
        payload = event.payload
        if payload.client_order_id is None:
            return

        def decide(state: dict[str, Any] | None) -> WorkflowChange:
            if state is None:
                return WorkflowChange(None)
            if payload.symbol.upper() != state["symbol"]:
                raise ValueError("Order update symbol mismatch")
            if state.get("order_id") not in (None, payload.order_id) and payload.order_id is not None:
                raise ValueError("Order update exchange identity mismatch")
            quantity = payload.cumulative_filled_quantity
            if quantity is None:
                quantity = payload.filled_quantity
            if self.apply_result(state, payload.status, payload.order_id, quantity):
                state["updated_at"] = payload.updated_at.isoformat()
            return WorkflowChange(state, due_at=state.get("next_due"))

        # User-stream updates are already on Kafka. Do not echo them into outbox.
        self.store.transact(payload.client_order_id, event.event_id, decide)

    @staticmethod
    def apply_result(state: dict[str, Any], status: str, order_id: str | None,
                     quantity: float | None) -> bool:
        if status not in TERMINAL | {"new", "partially_filled"}:
            raise ValueError(f"Unsupported order status: {status}")
        if state["status"] in TERMINAL:
            return False
        previous_quantity = state["cumulative_filled_quantity"]
        if quantity is not None and (not math.isfinite(quantity) or quantity < previous_quantity):
            return False
        if state["status"] == "partially_filled" and status == "new":
            return False
        changed = state["status"] != status or (quantity is not None and quantity != previous_quantity)
        state["status"] = status
        state["order_id"] = order_id or state.get("order_id")
        if quantity is not None:
            state["cumulative_filled_quantity"] = quantity
        state["phase"] = "done" if status in TERMINAL else "active"
        state["next_due"] = None if status in TERMINAL else time.time() + 30
        return changed


class TradeExecutionWorker:
    def __init__(self, workflow: TradeWorkflow, gateway: Any, lease_seconds: float = 60) -> None:
        self.workflow = workflow
        self.store = workflow.store
        self.gateway = gateway
        self.lease_seconds = lease_seconds

    def run_once(self) -> int:
        processed = 0
        for identity in self.store.due(time.time()):
            token = str(uuid4())
            claimed: list[tuple[dict[str, Any], bool]] = []

            def claim(state: dict[str, Any] | None) -> WorkflowChange:
                claimed.clear()
                now = time.time()
                if state is None or state["phase"] == "done":
                    return WorkflowChange(state)
                if state.get("lease_until", 0) > now or state.get("next_due", now) > now:
                    return WorkflowChange(state, due_at=state.get("next_due"))
                submit = state["phase"] == "pending_submit"
                state.update(lease_token=token, lease_until=now + self.lease_seconds,
                             next_due=now + self.lease_seconds)
                if submit:
                    state["phase"] = "submitting"
                claimed.append((dict(state), submit))
                return WorkflowChange(state, due_at=state["next_due"])

            self.store.transact(identity, None, claim)
            if not claimed:
                continue
            state, submit = claimed[0]
            result = None
            error = "order_not_found"
            try:
                if submit:
                    result = self.gateway.submit_order(request_from_document(state["request"]))
                else:
                    result = self.gateway.query_order(state["symbol"], identity)
                if result is not None:
                    if result.symbol.upper() != state["symbol"]:
                        raise ValueError("Exchange response symbol mismatch")
                    if result.client_order_id not in (None, identity):
                        raise ValueError("Exchange response client identity mismatch")
                    if result.status is not TradeExecutionStatus.REJECTED and result.order_id is None:
                        raise ValueError("Exchange response missing order identity")
            except Exception as exc:
                result = None
                error = type(exc).__name__
                if getattr(exc, "error_code", None) is not None:
                    error += f":{exc.error_code}"

            def finish(current: dict[str, Any] | None) -> WorkflowChange:
                if current is None or current.get("lease_token") != token:
                    return WorkflowChange(current, due_at=(current or {}).get("next_due"))
                current.update(lease_until=0, lease_token=None)
                if current["phase"] == "done":
                    return WorkflowChange(current)
                events = []
                identity_conflict = (result is not None and result.order_id is not None
                                     and current.get("order_id") not in (None, result.order_id))
                if result is None or identity_conflict:
                    current["phase"] = "unknown"
                    if current["status"] in ("pending_submit", "unknown"):
                        current["status"] = "unknown"
                    current["last_error"] = "order_identity_conflict" if identity_conflict else error
                    current["updated_at"] = datetime.now(UTC).isoformat()
                    current["reconciliation_attempts"] = current.get("reconciliation_attempts", 0) + 1
                    current["next_due"] = time.time() + 30
                else:
                    resolved = replace(result, client_order_id=identity)
                    changed = self.workflow.apply_result(
                        current, resolved.status.value, resolved.order_id, resolved.filled_quantity,
                    )
                    if changed:
                        current["last_error"] = None
                        current["updated_at"] = resolved.updated_at.isoformat()
                        events = self.workflow.updates(
                            resolved, current["request"]["correlation_id"],
                            current["request"]["causation_id"],
                        )
                return WorkflowChange(current, events, current.get("next_due"))

            self.store.transact(identity, None, finish)
            processed += 1
        return processed
