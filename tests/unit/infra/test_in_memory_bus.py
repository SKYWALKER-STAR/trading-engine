from __future__ import annotations

from trading_engine.infra.bus.in_memory import InMemoryEventBus


def test_in_memory_bus_subscribe_and_publish() -> None:
    bus = InMemoryEventBus()
    captured: list[int] = []

    def handler(value: int) -> None:
        captured.append(value)

    bus.subscribe("numbers", handler)
    bus.publish("numbers", 42)

    assert captured == [42]
    assert bus.history == [("numbers", 42)]
