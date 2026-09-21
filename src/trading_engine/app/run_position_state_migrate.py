from __future__ import annotations

import argparse
import json
import math
from os import getenv
from typing import Any

from trading_engine.app.run_workflow_services import workflow_store
from trading_engine.infra.redis_position_repository import RedisPositionRepository
from trading_engine.infra.redis_order_repository import RedisOrderRepository
from trading_engine.infra.redis_workflow import RedisWorkflowStore


def migrate_states(client: Any, target: RedisWorkflowStore, legacy_prefix: str, *,
                   apply: bool = False, initialize_empty: bool = False,
                   active_orders: Any = (), normalize_signed_shorts: tuple[str, ...] = ()) -> list[dict[str, Any]]:
    """Copy old execution documents only. Raw/projected snapshots are not execution state.

    Run with all old writers stopped. NX protects an already migrated/live target.
    This command does not create actions, reset Kafka offsets or delete old data.
    """
    prefix = legacy_prefix.rstrip(":") + ":"
    planned = []
    approved = {symbol.strip().upper() for symbol in normalize_signed_shorts}
    active_orders = tuple(active_orders)
    for key in client.scan_iter(match=prefix + "*"):
        suffix = key[len(prefix):]
        if not suffix or ":" in suffix or client.type(key) != "string":
            continue
        payload = json.loads(client.get(key))
        if not isinstance(payload, dict) or "lifecycle" not in payload or "source" in payload:
            continue
        state = RedisPositionRepository.decode(payload)
        encoded = RedisPositionRepository.encode(state)
        encoded["revision"] = 1
        candidates = [order for order in active_orders if order.symbol.upper() == state.symbol.upper()]
        quantity_review = None
        blockers = []
        order_details = [{"client_order_id": getattr(order, "client_order_id", None),
                          "order_id": getattr(order, "order_id", None),
                          "status": getattr(getattr(order, "status", None), "value",
                                            getattr(order, "status", None))}
                         for order in candidates]
        normalized = False
        if not math.isfinite(state.quantity):
            quantity_review = "non_finite_quantity"
        elif state.quantity < 0:
            if "projector_version" not in state.metadata:
                blockers.append("missing_projector_version")
            if state.direction.value != "short":
                blockers.append(f"direction={state.direction.value}")
            if state.lifecycle.value != "short":
                blockers.append(f"lifecycle={state.lifecycle.value}")
            if state.active_order_id or state.active_client_order_id:
                blockers.append("position_has_active_order_identity")
            if candidates:
                blockers.append("legacy_repository_has_active_orders")
            eligible = not blockers
            if eligible and state.symbol.upper() in approved:
                encoded["quantity"] = abs(state.quantity)
                encoded["metadata"] = {**state.metadata, "migration_source_key": key,
                                       "migration_original_quantity": state.quantity,
                                       "migration_normalization": "signed_short_to_magnitude"}
                normalized = True
            else:
                quantity_review = ("confirm_signed_short" if eligible else "inconsistent_signed_quantity")
        if apply and quantity_review:
            raise ValueError(f"Review quantity before migrating {state.symbol}: {quantity_review}; "
                             f"source={key}; blockers={blockers}; legacy_active_orders={order_details}; "
                             + ("verify positions/orders then use --normalize-signed-short SYMBOL"
                                if quantity_review == "confirm_signed_short" else
                                "resolve the reported conflicts before normalization; preview without --apply"))
        if (state.lifecycle.value not in ("flat", "long", "short")
                and state.active_order_id is None and state.active_client_order_id is None):
            if len(candidates) == 1:
                encoded["active_client_order_id"] = candidates[0].client_order_id
                encoded["active_order_id"] = candidates[0].order_id
            elif apply:
                raise ValueError(f"Review active order identity before migrating {state.symbol}")
        target_key = target.state_key(state.symbol.upper())
        planned.append((key, target_key, encoded, quantity_review, normalized, state.quantity,
                        blockers, order_details))
    if apply and not planned and not initialize_empty:
        raise ValueError("No legacy execution states found; review account positions before --initialize-empty")
    result = []
    for source, destination, payload, quantity_review, normalized, original_quantity, blockers, order_details in planned:
        copied = bool(client.set(destination, json.dumps(payload, allow_nan=False), nx=True)) if apply else False
        result.append({"source": source, "destination": destination, "copied": copied,
                       "lifecycle": payload["lifecycle"],
                       "original_quantity": original_quantity if math.isfinite(original_quantity) else str(original_quantity),
                       "quantity": payload["quantity"] if math.isfinite(payload["quantity"]) else str(payload["quantity"]),
                       "quantity_review": quantity_review, "quantity_normalized": normalized,
                       "normalization_blockers": blockers, "legacy_active_orders": order_details,
                       "needs_identity_review": payload["lifecycle"] not in ("flat", "long", "short")
                       and not payload.get("active_order_id") and not payload.get("active_client_order_id")})
    if apply:
        client.set(f"{target.base}:initialized", "1")
    return result


def run(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Preview/copy legacy position execution states")
    parser.add_argument("--legacy-prefix", default=getenv(
        "POSITION_VIEW_KEY_PREFIX", getenv("POSITION_REDIS_KEY_PREFIX", "binance:position:usdt_futures")))
    parser.add_argument("--apply", action="store_true", help="Copy with NX; stop old writers first")
    parser.add_argument("--initialize-empty", action="store_true", help="Initialize a verified empty/new execution store")
    parser.add_argument("--normalize-signed-short", action="append", default=[], metavar="SYMBOL",
                        help="Confirm a settled projected short after checking positions/orders; repeat per symbol")
    args = parser.parse_args(argv)
    target = workflow_store("position")
    orders = RedisOrderRepository.from_env().list_active(
        exchange=getenv("TRADE_EXCHANGE", "binance"),
        account_id=getenv("ORDER_ACCOUNT_ID", "default").strip() or "default")
    print(json.dumps(migrate_states(target.client, target, args.legacy_prefix, apply=args.apply,
                                    initialize_empty=args.initialize_empty, active_orders=orders,
                                    normalize_signed_shorts=tuple(args.normalize_signed_short)), indent=2))


if __name__ == "__main__":
    run()
