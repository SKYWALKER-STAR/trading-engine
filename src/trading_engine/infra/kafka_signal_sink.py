from __future__ import annotations

import json
from os import getenv
from typing import Any

from trading_engine.common.logger import get_logger
from trading_engine.strategy.models import StrategySignal


LOGGER = get_logger(__name__)


class KafkaSignalSink:
    """Publishes accepted strategy signals to Kafka."""

    def __init__(self, bootstrap_servers: str, topic: str, acks: int | str = 1) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._acks = acks
        self._producer: Any | None = None

    @classmethod
    def from_env(cls, topic: str) -> "KafkaSignalSink":
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
            topic=topic,
            acks=acks,
        )

    def publish(self, signal: StrategySignal) -> None:
        producer = self._get_producer()
        payload = {
            "strategy_name": signal.strategy_name,
            "symbol": signal.symbol,
            "direction": signal.direction.value,
            "score": signal.score,
            "confidence": signal.confidence,
            "timestamp": signal.timestamp.isoformat(),
            "metadata": signal.metadata,
        }
        future = producer.send(
            topic=self._topic,
            key=signal.symbol.encode("utf-8"),
            value=payload,
        )
        future.get(timeout=10)
        LOGGER.info(
            "Published strategy signal to Kafka",
            extra={"topic": self._topic, "symbol": signal.symbol, "direction": signal.direction.value},
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
            value_serializer=lambda value: json.dumps(value, ensure_ascii=True).encode("utf-8"),
            acks=self._acks,
        )
        LOGGER.info(
            "Initialized Kafka producer",
            extra={"bootstrap_servers": self._bootstrap_servers, "topic": self._topic, "acks": self._acks},
        )
        return self._producer
