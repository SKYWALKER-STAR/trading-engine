from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from trading_engine.common.logger import get_logger
from trading_engine.position.manager import PositionManager
from trading_engine.position.models import PositionDirection, PositionLifecycle, PositionState
from trading_engine.position.repository import PositionRepository

LOGGER = get_logger(__name__)


class BinanceAccountSnapshotTransport(Protocol):
    def fetch_account_snapshot(
        self,
        rest_api_url: str,
        api_key: str,
        timeout_seconds: float,
        *,
        api_secret: str | None = None,
        recv_window: int = 5000,
    ) -> list[dict[str, Any]]:
        ...


class BinanceHttpAccountSnapshotTransport:
    """Fetch USD-M Futures account positions from Binance REST."""

    def fetch_account_snapshot(
        self,
        rest_api_url: str,
        api_key: str,
        timeout_seconds: float,
        *,
        api_secret: str | None = None,
        recv_window: int = 5000,
    ) -> list[dict[str, Any]]:
        if not api_key:
            raise RuntimeError("BINANCE_API_KEY is required for account reconciliation")

        params = {
            "timestamp": int(datetime.now(UTC).timestamp() * 1000),
            "recvWindow": recv_window,
        }
        if api_secret:
            query = urlencode(params, doseq=True)
            signature = hmac.new(
                api_secret.encode("utf-8"),
                query.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            params["signature"] = signature

        endpoint = f"{rest_api_url.rstrip('/')}/fapi/v2/account?{urlencode(params, doseq=True)}"
        LOGGER.debug(
            "Requesting Binance futures account snapshot",
            extra={
                "api_key_present": bool(api_key),
                "api_secret_present": bool(api_secret),
                "endpoint": endpoint,
                "recv_window": recv_window,
            },
        )
        request = Request(
            endpoint,
            method="GET",
            headers={"X-MBX-APIKEY": api_key},
        )
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception:
            LOGGER.exception(
                "Binance futures account request failed",
                extra={
                    "endpoint": endpoint,
                    "api_key_present": bool(api_key),
                    "api_secret_present": bool(api_secret),
                    "recv_window": recv_window,
                },
            )
            raise

        LOGGER.debug(
            "Binance futures account response received",
            extra={
                "response_type": type(payload).__name__,
                "response_keys": list(payload.keys()) if isinstance(payload, dict) else None,
            },
        )

        if not isinstance(payload, dict):
            raise RuntimeError("Unexpected Binance account response")

        positions = payload.get("positions")
        if not isinstance(positions, list):
            return []
        return [item for item in positions if isinstance(item, dict)]


class BinancePositionReconciler:
    """Synchronize account positions from Binance into the local Redis position state."""

    def __init__(
        self,
        rest_api_url: str,
        api_key: str,
        repository: PositionRepository,
        *,
        timeout_seconds: float = 10.0,
        transport: BinanceAccountSnapshotTransport | None = None,
        api_secret: str | None = None,
        recv_window: int = 5000,
    ) -> None:
        self._rest_api_url = rest_api_url
        self._api_key = api_key
        self._repository = repository
        self._timeout_seconds = timeout_seconds
        self._transport = transport or BinanceHttpAccountSnapshotTransport()
        self._api_secret = api_secret
        self._recv_window = recv_window

    def reconcile_once(self, *, source: str = "manual") -> int:
        LOGGER.info(
            "Starting Binance position reconciliation",
            extra={
                "source": source,
                "rest_api_url": self._rest_api_url,
                "api_key_present": bool(self._api_key),
                "api_secret_present": bool(self._api_secret),
                "recv_window": self._recv_window,
            },
        )
        try:
            positions = self._transport.fetch_account_snapshot(
                self._rest_api_url,
                self._api_key,
                self._timeout_seconds,
                api_secret=self._api_secret,
                recv_window=self._recv_window,
            )
        except Exception:
            LOGGER.exception(
                "Binance account snapshot request failed during reconciliation",
                extra={
                    "source": source,
                    "rest_api_url": self._rest_api_url,
                    "recv_window": self._recv_window,
                },
            )
            raise

        LOGGER.info(
            "Binance account snapshot loaded",
            extra={
                "source": source,
                "positions_count": len(positions),
                "positions": positions,
            },
        )

        aggregated: dict[str, float] = {}
        for position in positions:
            symbol = str(position.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            quantity_raw = position.get("positionAmt")
            try:
                quantity = float(quantity_raw or 0)
            except (TypeError, ValueError):
                quantity = 0.0
            aggregated[symbol] = aggregated.get(symbol, 0.0) + quantity

        LOGGER.info(
            "Aggregated Binance account snapshot by symbol",
            extra={
                "source": source,
                "aggregated_positions": {
                    symbol: quantity for symbol, quantity in sorted(aggregated.items())
                },
            },
        )

        updated = 0
        now = datetime.now(UTC)

        for symbol, quantity in sorted(aggregated.items()):
            if quantity > 0:
                state = PositionState(
                    symbol=symbol,
                    direction=PositionDirection.LONG,
                    lifecycle=PositionLifecycle.LONG,
                    quantity=abs(quantity),
                    updated_at=now,
                )
            elif quantity < 0:
                state = PositionState(
                    symbol=symbol,
                    direction=PositionDirection.SHORT,
                    lifecycle=PositionLifecycle.SHORT,
                    quantity=abs(quantity),
                    updated_at=now,
                )
            else:
                state = PositionState(
                    symbol=symbol,
                    direction=PositionDirection.FLAT,
                    lifecycle=PositionLifecycle.FLAT,
                    quantity=0.0,
                    updated_at=now,
                )

            previous = self._repository.get(symbol)
            if previous != state:
                self._repository.save(state)
                updated += 1
                LOGGER.info(
                    "Reconciled Binance position from account snapshot: "
                    "symbol=%s source=%s direction=%s lifecycle=%s quantity=%s "
                    "previous_direction=%s previous_lifecycle=%s previous_quantity=%s",
                    symbol,
                    source,
                    state.direction.value,
                    state.lifecycle.value,
                    state.quantity,
                    previous.direction.value if previous is not None else None,
                    previous.lifecycle.value if previous is not None else None,
                    previous.quantity if previous is not None else None,
                )
            else:
                LOGGER.info(
                    "Binance position unchanged after reconciliation: "
                    "symbol=%s source=%s direction=%s lifecycle=%s quantity=%s",
                    symbol,
                    source,
                    state.direction.value,
                    state.lifecycle.value,
                    state.quantity,
                )

        LOGGER.info(
            "Position reconciliation complete",
            extra={
                "source": source,
                "updated": updated,
                "positions_seen": len(positions),
                "position_symbols": [str(position.get("symbol") or "").strip().upper() for position in positions if str(position.get("symbol") or "").strip()],
            },
        )
        return updated

    def run_periodic(self, interval_seconds: float, *, stop_event: Any | None = None) -> None:
        import time

        while True:
            if stop_event is not None and stop_event.is_set():
                return
            try:
                self.reconcile_once(source="periodic")
            except Exception:
                LOGGER.exception("Periodic Binance position reconciliation failed")
            if stop_event is not None:
                stop_event.wait(interval_seconds)
            else:
                time.sleep(interval_seconds)


def _position_manager_for_repository(repository: PositionRepository) -> PositionManager:
    return PositionManager(repository=repository)
