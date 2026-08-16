from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from trading_engine.common.logger import get_logger


LOGGER = get_logger(__name__)


class RedisPositionViewProjector:
    """Projects Binance raw position snapshots into trading-engine view keys."""

    def __init__(
        self,
        redis_url: str,
        key_prefix: str = "binance:position:usdt_futures",
        enable_detail_keys: bool = True,
    ) -> None:
        self._redis_url = redis_url
        self._key_prefix = key_prefix
        self._enable_detail_keys = enable_detail_keys
        self._client: Any | None = None

        self._raw_snapshot_key = f"{self._key_prefix}:raw:snapshot:all:v1"
        self._raw_symbols_key = f"{self._key_prefix}:raw:index:positions:v1"
        self._raw_meta_key = f"{self._key_prefix}:raw:meta:v1"

        self._view_symbols_key = f"{self._key_prefix}:view:index:symbols:v1"
        self._view_meta_key = f"{self._key_prefix}:view:meta:v1"
        self._view_hashes_key = f"{self._key_prefix}:view:hash:state:v1"

    def project_once(self) -> dict[str, int]:
        """Project raw hash snapshots to view state keys once."""
        raw_positions = self._get_client().hgetall(self._raw_snapshot_key)
        if not raw_positions:
            LOGGER.info("No raw positions found to project", extra={"raw_snapshot_key": self._raw_snapshot_key})
            return {"symbols": 0, "detail_keys": 0, "updated": 0, "unchanged": 0}

        by_symbol: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        for field_key, payload_raw in raw_positions.items():
            try:
                payload = json.loads(payload_raw)
            except json.JSONDecodeError:
                LOGGER.warning("Skip invalid raw payload", extra={"field_key": field_key})
                continue

            symbol = str(payload.get("symbol") or "")
            if not symbol:
                symbol = str(field_key).split(":", 1)[0]
            symbol = symbol.upper()
            if not symbol:
                continue

            by_symbol.setdefault(symbol, []).append((field_key, payload))

        now_iso = datetime.now(UTC).isoformat()
        pipe = self._get_client().pipeline(transaction=True)

        updated = 0
        unchanged = 0
        detail_keys = 0

        for symbol, entries in by_symbol.items():
            primary = self._select_primary_position(entries)
            state_payload = self._build_symbol_state(symbol, entries, primary, now_iso)
            canonical = json.dumps(state_payload, ensure_ascii=True, sort_keys=True)
            state_hash = hashlib.sha1(canonical.encode("utf-8")).hexdigest()

            previous_hash = self._get_client().hget(self._view_hashes_key, symbol)
            if previous_hash == state_hash:
                unchanged += 1
            else:
                view_state_key = self._view_state_key(symbol)
                pipe.set(view_state_key, canonical)
                pipe.hset(self._view_hashes_key, symbol, state_hash)
                updated += 1

            pipe.sadd(self._view_symbols_key, symbol)

            if self._enable_detail_keys:
                for _, payload in entries:
                    side = str(payload.get("positionSide", "BOTH")).upper()
                    detail = {
                        "schema_version": "v1",
                        "symbol": symbol,
                        "positionSide": side,
                        "synced_at": str(payload.get("synced_at", now_iso)),
                        "binance_position": payload,
                    }
                    pipe.set(
                        self._view_detail_key(symbol, side),
                        json.dumps(detail, ensure_ascii=True, sort_keys=True),
                    )
                    detail_keys += 1

        pipe.hset(
            self._view_meta_key,
            mapping={
                "last_projected_ts": now_iso,
                "state_count": str(len(by_symbol)),
                "projector_version": "1.0.0",
            },
        )
        pipe.execute()

        result = {
            "symbols": len(by_symbol),
            "detail_keys": detail_keys,
            "updated": updated,
            "unchanged": unchanged,
        }
        LOGGER.debug("Position view projection finished")
        return result

    @staticmethod
    def _position_amount(payload: dict[str, Any]) -> float:
        raw = payload.get("positionAmt", 0)
        try:
            return float(raw)
        except (TypeError, ValueError):
            return 0.0

    def _select_primary_position(self, entries: list[tuple[str, dict[str, Any]]]) -> dict[str, Any]:
        # Prefer BOTH in one-way mode, otherwise use the leg with largest absolute size.
        for _, payload in entries:
            if str(payload.get("positionSide", "")).upper() == "BOTH":
                return payload

        return max(entries, key=lambda item: abs(self._position_amount(item[1])))[1]

    def _build_symbol_state(
        self,
        symbol: str,
        entries: list[tuple[str, dict[str, Any]]],
        primary: dict[str, Any],
        projected_at: str,
    ) -> dict[str, Any]:
        quantity = self._position_amount(primary)
        if quantity > 0:
            direction = "long"
            lifecycle = "long"
        elif quantity < 0:
            direction = "short"
            lifecycle = "short"
        else:
            direction = "flat"
            lifecycle = "flat"

        raw_fields = [field_key for field_key, _ in entries]
        source_synced_at = str(primary.get("synced_at", projected_at))
        side_values = {str(payload.get("positionSide", "BOTH")).upper() for _, payload in entries}
        position_mode = "hedge" if ("LONG" in side_values or "SHORT" in side_values) and "BOTH" not in side_values else "oneway"

        return {
            "schema_version": "v1",
            "symbol": symbol,
            "direction": direction,
            "lifecycle": lifecycle,
            "quantity": str(primary.get("positionAmt", "0")),
            "active_order_id": None,
            "updated_at": projected_at,
            "source": {
                "name": "binance_raw_projector",
                "raw_key": self._raw_snapshot_key,
                "raw_fields": raw_fields,
                "source_synced_at": source_synced_at,
                "position_mode": position_mode,
            },
            "binance_position": primary,
            "metadata": {
                "projector_version": "1.0.0",
            },
        }

    def _view_state_key(self, symbol: str) -> str:
        return f"{self._key_prefix}:view:state:{symbol}:v1"

    def _view_detail_key(self, symbol: str, side: str) -> str:
        return f"{self._key_prefix}:view:state:{symbol}:{side}:v1"

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client

        try:
            import redis
        except ImportError as exc:
            raise RuntimeError("redis is not installed. Install with: pip install redis") from exc

        self._client = redis.Redis.from_url(self._redis_url, decode_responses=True)
        return self._client
