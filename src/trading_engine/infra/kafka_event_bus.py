from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable
from os import getenv
from typing import Any

from trading_engine.common.logger import get_logger
from trading_engine.contracts.messages import EngineEvent
from trading_engine.contracts.serde import decode_event, encode_event


LOGGER = get_logger(__name__)


class KafkaEventPublisher:
    """Publishes versioned engine events to Kafka topics."""

    def __init__(self, bootstrap_servers: str, acks: int | str = 1) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._acks = acks
        self._producer: Any | None = None

    @classmethod
    def from_env(cls) -> "KafkaEventPublisher":
        raw_acks = getenv("KAFKA_ACKS", "1").strip().lower()
        acks: int | str
        if raw_acks == "all":
            acks = "all"
        else:
            try:
                acks = int(raw_acks)
            except ValueError:
                LOGGER.warning("Invalid KAFKA_ACKS value, fallback to 1", extra={"raw_acks": raw_acks})
                acks = 1

        return cls(
            bootstrap_servers=getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            acks=acks,
        )

    def publish(self, topic: str, event: EngineEvent[Any], key: str) -> None:
        future = self._get_producer().send(topic=topic, key=key.encode("utf-8"), value=encode_event(event))
        future.get(timeout=10)
        LOGGER.info(
            "Published engine event to Kafka",
            extra={"topic": topic, "key": key, "event_type": event.event_type.value},
        )

    def _get_producer(self) -> Any:
        if self._producer is not None:
            return self._producer

        try:
            from kafka import KafkaProducer
        except ImportError as exc:
            raise RuntimeError(
                "kafka-python is not installed. Install with: pip install kafka-python"
            ) from exc

        self._producer = KafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=lambda value: value,
            acks=self._acks,
        )
        return self._producer


class KafkaEventConsumer:
    """Consumes versioned engine events from Kafka and dispatches to local handlers."""

    def __init__(self, bootstrap_servers: str, group_id: str) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._consumer: Any | None = None
        self._handlers: dict[str, list[Callable[[EngineEvent[Any]], None]]] = defaultdict(list)

    @classmethod
    def from_env(cls, group_id: str) -> "KafkaEventConsumer":
        return cls(
            bootstrap_servers=getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            group_id=group_id,
        )

    def subscribe(self, topic: str, handler: Callable[[EngineEvent[Any]], None]) -> None:
        self._handlers[topic].append(handler)

    def consume_forever(self, topics: Iterable[str]) -> None:
        consumer = self._get_consumer(topics)
        LOGGER.info("Kafka event consumer started", extra={"topics": list(topics), "group_id": self._group_id})
        for message in consumer:
            event = decode_event(message.value)
            LOGGER.debug(
                "Dispatching Kafka event",
                extra={"topic": message.topic, "offset": message.offset, "event_type": event.event_type.value},
            )
            for handler in self._handlers[message.topic]:
                handler(event)

    def _get_consumer(self, topics: Iterable[str]) -> Any:
        if self._consumer is not None:
            return self._consumer

        try:
            from kafka import KafkaConsumer
        except ImportError as exc:
            raise RuntimeError(
                "kafka-python is not installed. Install with: pip install kafka-python"
            ) from exc

        self._consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            enable_auto_commit=True,
            value_deserializer=lambda value: value,
            key_deserializer=lambda value: None if value is None else value.decode("utf-8"),
            auto_offset_reset="earliest",
        )
        return self._consumer