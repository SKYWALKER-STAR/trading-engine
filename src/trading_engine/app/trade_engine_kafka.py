from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast

from trading_engine.common.logger import get_logger
from trading_engine.config.settings import TradeEngineSettings
from trading_engine.contracts.messages import (
    EngineEvent,
    EngineEventType,
    OrderUpdatePayload,
    TradeActionPayload,
    build_event,
)
from trading_engine.infra.kafka_event_bus import KafkaEventConsumer, KafkaEventPublisher
from trading_engine.infra.redis_order_repository import RedisOrderRepository
from trading_engine.trade.gateway import TradeExecutionGateway
from trading_engine.trade.models import (
    TrackedOrder,
    TrackedOrderStatus,
    TradeExecutionResult,
    TradeExecutionStatus,
    TradeOrderRequest,
)
from trading_engine.trade.repository import OrderRepository


LOGGER = get_logger(__name__)


class TradeEngineMessageProcessor:
    """Consumes trade action events and submits orders to the configured exchange gateway."""

    def __init__(
        self,
        publisher: KafkaEventPublisher,
        settings: TradeEngineSettings,
        gateway: TradeExecutionGateway,
        *,
        order_repository: OrderRepository | None = None,
        producer_name: str = "trade-engine",
    ) -> None:
        self._publisher = publisher
        self._settings = settings
        self._gateway = gateway
        self._order_repository = order_repository
        self._producer_name = producer_name

    def handle_trade_action(self, event: EngineEvent[Any]) -> None:
        if event.event_type is not EngineEventType.TRADE_ACTION_REQUESTED:
            raise ValueError(f"Unexpected event type: {event.event_type.value}")

        payload = cast(TradeActionPayload, event.payload)
        request = _to_trade_order_request(payload, event, self._settings)
        if request == None:
            self._publish_single_update(
                symbol = payload.symbol,
                status = TradeExecutionStatus.REJECTED,
                source_event = event, 
                result = TradeExecutionResult(
                    symbol=payload.symbol,
                    status=TradeExecutionStatus.REJECTED,
                    order_id=None,
                    filled_quantity=event.payload.quantity if isinstance(event.payload.quantity, float) else 0.0,
                    updated_at=datetime.now(UTC),
                    metadata={"reason": "Invalid Trade Event to Order Request Conversion"},
                ),
            )
            return
        tracked_order = self._make_pending_order(request)
        if self._order_repository is not None:
            self._order_repository.save(tracked_order)
        try:
            result = self._gateway.submit_order(request)
        except Exception as exc:
            if self._order_repository is not None:
                self._order_repository.save(
                    replace(
                        tracked_order,
                        status=TrackedOrderStatus.UNKNOWN,
                        updated_at=datetime.now(UTC),
                        metadata={
                            **tracked_order.metadata,
                            "submission_error": type(exc).__name__,
                        },
                    )
                )
            raise
        if result.client_order_id is None:
            result = replace(result, client_order_id=request.client_order_id)
        if self._order_repository is not None:
            self._save_execution_result(tracked_order, result)
        self._publish_order_updates(payload.symbol, event, result)

    def _make_pending_order(self, request: TradeOrderRequest) -> TrackedOrder:
        if request.client_order_id is None:
            raise ValueError("TradeOrderRequest.client_order_id is required")
        position_side = request.metadata.get(
            "positionSide",
            request.metadata.get("position_side"),
        )
        reduce_only_raw = request.metadata.get("reduceOnly", False)
        reduce_only = str(reduce_only_raw).strip().lower() in {"1", "true", "yes", "on"}
        return TrackedOrder(
            exchange=self._settings.exchange,
            account_id=self._settings.order_account_id,
            symbol=request.symbol.upper(),
            client_order_id=request.client_order_id,
            side=request.side.upper(),
            order_type=request.order_type.upper(),
            original_quantity=request.quantity,
            status=TrackedOrderStatus.PENDING_SUBMIT,
            created_at=request.requested_at,
            updated_at=request.requested_at,
            position_side=None if position_side is None else str(position_side).upper(),
            reduce_only=reduce_only,
            metadata={
                "correlation_id": request.correlation_id,
                "causation_id": request.causation_id,
            },
        )

    def _save_execution_result(
        self,
        pending_order: TrackedOrder,
        result: TradeExecutionResult,
    ) -> None:
        if self._order_repository is None:
            raise RuntimeError("Order repository is required to save execution results")
        tracked_order = pending_order
        if result.order_id is not None:
            tracked_order = self._order_repository.bind_order_id(
                exchange=pending_order.exchange,
                account_id=pending_order.account_id,
                client_order_id=pending_order.client_order_id,
                symbol=pending_order.symbol,
                order_id=result.order_id,
            )
        self._order_repository.save(
            replace(
                tracked_order,
                status=TrackedOrderStatus(result.status.value),
                updated_at=result.updated_at,
                cumulative_filled_quantity=(
                    tracked_order.cumulative_filled_quantity
                    if result.filled_quantity is None
                    else result.filled_quantity
                ),
                metadata={
                    **tracked_order.metadata,
                    "execution_source": "order_response",
                },
            )
        )

    def _publish_order_updates(
        self,
        symbol: str,
        source_event: EngineEvent[Any],
        result: TradeExecutionResult,
    ) -> None:
        if result.status is TradeExecutionStatus.REJECTED:
            self._publish_single_update(
                symbol=symbol,
                status=TradeExecutionStatus.REJECTED,
                source_event=source_event,
                result=result,
            )
            return

        self._publish_single_update(
            symbol=symbol,
            status=TradeExecutionStatus.NEW,
            source_event=source_event,
            result=result,
        )

        if result.status is not TradeExecutionStatus.NEW:
            self._publish_single_update(
                symbol=symbol,
                status=result.status,
                source_event=source_event,
                result=result,
            )

    def _publish_single_update(
        self,
        *,
        symbol: str,
        status: TradeExecutionStatus,
        source_event: EngineEvent[Any],
        result: TradeExecutionResult,
    ) -> None:
        updated_at = result.updated_at if result.updated_at.tzinfo is not None else datetime.now(UTC)
        payload = OrderUpdatePayload(
            symbol=symbol,
            order_id=result.order_id,
            client_order_id=result.client_order_id,
            status=status.value,
            updated_at=updated_at,
            filled_quantity=result.filled_quantity,
            cumulative_filled_quantity=result.filled_quantity,
            metadata=dict(result.metadata),
        )
        order_update_event = build_event(
            EngineEventType.ORDER_UPDATE_RECEIVED,
            payload,
            producer=self._producer_name,
            occurred_at=updated_at,
            correlation_id=source_event.correlation_id,
            causation_id=source_event.event_id,
        )
        self._publisher.publish(self._settings.order_update_topic, order_update_event, key=symbol)


def build_trade_engine_consumer(
    settings: TradeEngineSettings,
    gateway: TradeExecutionGateway,
    *,
    order_repository: OrderRepository | None = None,
    producer_name: str = "trade-engine",
) -> KafkaEventConsumer:
    publisher = KafkaEventPublisher.from_env()
    resolved_order_repository = (
        order_repository if order_repository is not None else RedisOrderRepository.from_env()
    )
    processor = TradeEngineMessageProcessor(
        publisher=publisher,
        settings=settings,
        gateway=gateway,
        order_repository=resolved_order_repository,
        producer_name=producer_name,
    )

    consumer = KafkaEventConsumer.from_env(group_id=settings.consumer_group)
    consumer.subscribe(settings.trade_action_topic, processor.handle_trade_action)
    return consumer


def _to_trade_order_request(
    payload: TradeActionPayload,
    event: EngineEvent[Any],
    settings: TradeEngineSettings,
) -> TradeOrderRequest:
    raw_quantity: float | int | str | None = payload.quantity
    if raw_quantity is None:
        raw_metadata_quantity = payload.metadata.get("approved_quantity")
        if isinstance(raw_metadata_quantity, (int, float, str)):
            raw_quantity = raw_metadata_quantity

    if raw_quantity is None:
        LOGGER.error(f"Missing quantity for trade action: {payload.action}")
        return None
        # raise ValueError(f"Missing quantity for trade action: {payload.action}")'''

    quantity = float(raw_quantity)
    if quantity <= 0:
        LOGGER.error(f"Invalid quantity for trade action: {quantity}")
        return None
        # raise ValueError(f"Invalid quantity for trade action: {quantity}")

    order_type = settings.binance_order_type.strip().lower()
    if order_type not in ("market", "limit"):
        LOGGER.error(f"Unsupported order type for Binance WS trade engine: {settings.binance_order_type}")
        return None
        # raise ValueError(f"Unsupported order type for Binance WS trade engine: {settings.binance_order_type}")

    metadata = dict(payload.metadata)
    metadata["trade_action"] = payload.action

    client_order_id_raw = metadata.get("newClientOrderId")
    client_order_id = (
        str(client_order_id_raw)
        if client_order_id_raw is not None
        else _client_order_id_for_event(event.event_id)
    )
    metadata["newClientOrderId"] = client_order_id

    if "position_side" not in metadata and "positionSide" not in metadata:
        metadata["positionSide"] = settings.binance_position_side

    if "newOrderRespType" not in metadata:
        metadata["newOrderRespType"] = settings.binance_new_order_resp_type

    if "reduceOnly" not in metadata and "risk_action" in metadata:
        metadata["reduceOnly"] = "true" if str(metadata["risk_action"]).lower() == "reduce_only" else "false"

    if order_type == "limit":
        has_price = any(key in metadata for key in ("price",))
        has_tif = any(key in metadata for key in ("timeInForce", "time_in_force"))
        if not has_price or not has_tif:
            LOGGER.error("LIMIT order requires metadata.price and metadata.timeInForce")
            return None
            # raise ValueError("LIMIT order requires metadata.price and metadata.timeInForce")

    return TradeOrderRequest(
        symbol=payload.symbol,
        side=payload.side,
        quantity=quantity,
        order_type=order_type,
        requested_at=payload.requested_at,
        correlation_id=event.correlation_id,
        causation_id=event.event_id,
        client_order_id=client_order_id,
        metadata=metadata,
    )


def _client_order_id_for_event(event_id: str) -> str:
    """Return a Binance-safe, deterministic ID for one logical order request."""
    digest = hashlib.sha256(event_id.encode("utf-8")).hexdigest()[:32]
    return f"te-{digest}"
