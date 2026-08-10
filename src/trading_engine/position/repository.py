from __future__ import annotations

from typing import Protocol

from trading_engine.position.models import PositionState


class PositionRepository(Protocol):
    def get(self, symbol: str) -> PositionState | None:
        ...

    def save(self, state: PositionState) -> None:
        ...