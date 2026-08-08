from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from typing import Any


class InMemoryEventBus:
    """Simple synchronous bus for local development and tests."""

    def __init__(self) -> None:
        self._handlers: dict[str, list[Callable[[Any], None]]] = defaultdict(list)
        self._history: list[tuple[str, Any]] = []

    def publish(self, topic: str, event: Any) -> None:
        self._history.append((topic, event))
        for handler in self._handlers[topic]:
            handler(event)

    def subscribe(self, topic: str, handler: Callable[[Any], None]) -> None:
        self._handlers[topic].append(handler)

    @property
    def history(self) -> list[tuple[str, Any]]:
        return list(self._history)
