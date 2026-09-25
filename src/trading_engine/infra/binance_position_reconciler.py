from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Protocol
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
    ) -> list[dict[str, Any]]:
        ...


class BinanceHttpAccountSnapshotTransport:
    """Fetch USD-M Futures account positions from Binance REST."""

    def fetch_account_snapshot(
        self,
        rest_api_url: str,
        api_key: str,
        timeout_seconds: float,
    ) -> list[dict[str, Any]]:
        endpoint = f"{rest_api_url.rstrip('/')}/fapi/v2/account"
        request = Request(
            endpoint,
            method="GET",
            headers={"X-MBX-APIKEY": api_key},
        )
        with urlopen(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))

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
    ) -> None:
        self._rest_api_url = rest_api_url
        self._api_key = api_key
        self._repository = repository
        self._timeout_seconds = timeout_seconds
        self._transport = transport or BinanceHttpAccountSnapshotTransport()

    def reconcile_once(self, *, source: str = "manual") -> int:
        positions = self._transport.fetch_account_snapshot(
            self._rest_api_url,
            self._api_key,
            self._timeout_seconds,
        )
        updated = 0
        now = datetime.now(UTC)

        for position in positions:
            symbol = str(position.get("symbol") or "").strip().upper()
            if not symbol:
                continue

            quantity_raw = position.get("positionAmt")
            try:
                quantity = float(quantity_raw or 0)
            except (TypeError, ValueError):
                quantity = 0.0

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
                    "Reconciled Binance position from account snapshot",
                    extra={
                        "symbol": symbol,
                        "source": source,
                        "direction": state.direction.value,
                        "lifecycle": state.lifecycle.value,
                        "quantity": state.quantity,
                    },
                )

        LOGGER.info(
            "Position reconciliation complete",
            extra={
                "source": source,
                "updated": updated,
                "positions_seen": len(positions),
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
