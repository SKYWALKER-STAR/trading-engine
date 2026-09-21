from __future__ import annotations

import argparse
import json
from os import getenv
from typing import Any

from trading_engine.app.run_workflow_services import workflow_store
from trading_engine.infra.redis_position_repository import RedisPositionRepository
from trading_engine.infra.redis_order_repository import RedisOrderRepository
from trading_engine.infra.redis_workflow import RedisWorkflowStore


def migrate_states(client: Any, target: RedisWorkflowStore, legacy_prefix: str, *,
                   apply: bool = False, initialize_empty: bool = False,
                   active_orders: Any = ()) -> list[dict[str, Any]]:
    """Copy old execution documents only. Raw/projected snapshots are not execution state.

    Run with all old writers stopped. NX protects an already migrated/live target.
    This command does not create actions, reset Kafka offsets or delete old data.
    """
    prefix = legacy_prefix.rstrip(":") + ":"
    planned = []
    for key in client.scan_iter(match=prefix + "*"):
        suffix = key[len(prefix):]
        if not suffix or ":" in suffix or client.type(key) != "string":
            continue
        payload = json.loads(client.get(key))
        if not isinstance(payload, dict) or "lifecycle" not in payload or "source" in payload:
            continue
        state = RedisPositionRepository.decode(payload)
        if state.quantity < 0:
            raise ValueError(f"Review signed legacy quantity before migrating {state.symbol}")
        encoded = RedisPositionRepository.encode(state)
        encoded["revision"] = 1
        if (state.lifecycle.value not in ("flat", "long", "short")
                and state.active_order_id is None and state.active_client_order_id is None):
            candidates = [order for order in active_orders if order.symbol == state.symbol]
            if len(candidates) == 1:
                encoded["active_client_order_id"] = candidates[0].client_order_id
                encoded["active_order_id"] = candidates[0].order_id
            elif apply:
                raise ValueError(f"Review active order identity before migrating {state.symbol}")
        target_key = target.state_key(state.symbol.upper())
        planned.append((key, target_key, encoded))
    if apply and not planned and not initialize_empty:
        raise ValueError("No legacy execution states found; review account positions before --initialize-empty")
    result = []
    for source, destination, payload in planned:
        copied = bool(client.set(destination, json.dumps(payload, allow_nan=False), nx=True)) if apply else False
        result.append({"source": source, "destination": destination, "copied": copied,
                       "lifecycle": payload["lifecycle"],
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
    args = parser.parse_args(argv)
    target = workflow_store("position")
    orders = RedisOrderRepository.from_env().list_active(
        exchange=getenv("TRADE_EXCHANGE", "binance"),
        account_id=getenv("ORDER_ACCOUNT_ID", "default").strip() or "default")
    print(json.dumps(migrate_states(target.client, target, args.legacy_prefix, apply=args.apply,
                                    initialize_empty=args.initialize_empty, active_orders=orders), indent=2))


if __name__ == "__main__":
    run()
