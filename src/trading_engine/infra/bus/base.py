from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol


class EventBus(Protocol):
    def publish(self, topic: str, event: Any) -> None:
        ...

    def subscribe(self, topic: str, handler: Callable[[Any], None]) -> None:
        ...
