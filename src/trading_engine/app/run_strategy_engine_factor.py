from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from typing import Any

from trading_engine.app.bootstrap import build_strategy_engine
from trading_engine.common.logger import configure_logging, get_logger
from trading_engine.config.settings import StrategyEngineSettings
from trading_engine.domain.market_data import MarketFactorSnapshot
from trading_engine.infra.clickhouse_source import ClickHouseMarketDataSource
from trading_engine.infra.kafka_signal_sink import KafkaSignalSink
from trading_engine.strategy.engine import StrategyEngine
from trading_engine.strategy.factor_score import FactorScoreStrategy
from trading_engine.strategy.models import FactorStrategyContext


LOGGER = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the strategy engine")
    parser.add_argument("--symbol", default=None, help="Optional symbol filter for ClickHouse query")
    parser.add_argument("--once", action="store_true", help="Run a single cycle and exit")
    parser.add_argument(
        "--stream",
        action="store_true",
        help="Run continuously and evaluate every interval",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=1.0,
        help="Evaluation interval in stream mode",
    )
    return parser


def parse_timestamp(raw_timestamp: Any) -> datetime:
    if isinstance(raw_timestamp, datetime):
        if raw_timestamp.tzinfo is None:
            return raw_timestamp.replace(tzinfo=UTC)
        return raw_timestamp

    if isinstance(raw_timestamp, str):
        timestamp = datetime.fromisoformat(raw_timestamp)
        if timestamp.tzinfo is None:
            return timestamp.replace(tzinfo=UTC)
        return timestamp

    raise ValueError("Unsupported timestamp value from ClickHouse row")


def row_to_context(row: dict[str, Any]) -> FactorStrategyContext:
    open_time = parse_timestamp(row["open_time"])
    trend_score_p = float(
        row.get(
            "trend_score_p",
            (
                float(row["score_ema"])
                + float(row["score_dmi_adx"])
                + float(row["score_rsi"])
                + float(row["score_flow"])
                + float(row["score_funding"])
            )
            / 5.0
            * 100.0,
        )
    )

    snapshot = MarketFactorSnapshot(
        symbol=str(row["symbol"]),
        interval=str(row["interval"]),
        open_time=open_time,
        close=float(row["close"]),
        ema_12=float(row["ema_12"]),
        ema_26=float(row["ema_26"]),
        rsi_14=float(row["rsi_14"]),
        adx_14=float(row["adx_14"]),
        plus_di=float(row["plus_di"]),
        minus_di=float(row["minus_di"]),
        taker_buy_ratio=float(row["taker_buy_ratio"]),
        funding_rate=float(row["funding_rate"]),
        score_ema=float(row["score_ema"]),
        score_dmi_adx=float(row["score_dmi_adx"]),
        score_rsi=float(row["score_rsi"]),
        score_flow=float(row["score_flow"]),
        score_funding=float(row["score_funding"]),
        trend_score_p=trend_score_p,
    )

    now = datetime.now(UTC)
    return FactorStrategyContext(factor_snapshot=snapshot, now=now)


def evaluate_once(
    engine: StrategyEngine,
    source: ClickHouseMarketDataSource,
    sink: KafkaSignalSink,
    symbol: str | None,
) -> None:
    row = source.fetch_latest(symbol=symbol)
    if row is None:
        LOGGER.warning("No market data available for strategy evaluation", extra={"symbol": symbol})
        print(json.dumps({"accepted": False, "rejected_reasons": ["no_market_data"]}, ensure_ascii=True))
        return

    context = row_to_context(row)

    decision = engine.evaluate(context)

    if decision.signal is None:
        
        LOGGER.info(
            "Strategy decision rejected",
            extra={"reasons": list(decision.rejected_reasons), "symbol": context.factor_snapshot.symbol},
        )
        payload = {
            "accepted": False,
            "rejected_reasons": list(decision.rejected_reasons),
        }
    else:
        sink.publish(decision.signal)
        LOGGER.info(
            "Strategy decision accepted",
            extra={"symbol": decision.signal.symbol, "direction": decision.signal.direction.value},
        )
        payload = {
            "accepted": True,
            "signal": {
                "strategy_name": decision.signal.strategy_name,
                "symbol": decision.signal.symbol,
                "direction": decision.signal.direction.value,
                "score": decision.signal.score,
                "confidence": decision.signal.confidence,
                "timestamp": decision.signal.timestamp.isoformat(),
                "metadata": decision.signal.metadata,
            },
        }
    LOGGER.debug("Strategy evaluation result: %s", payload)
    # print(json.dumps(payload, ensure_ascii=True))


def run() -> None:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()

    settings = StrategyEngineSettings.from_env()
    engine = build_strategy_engine(
        algorithm=FactorScoreStrategy(),
        settings=settings,
        publisher=None,
    )
    source = ClickHouseMarketDataSource.from_env()
    sink = KafkaSignalSink.from_env(topic=settings.signal_topic)

    LOGGER.info(
        "Strategy engine runner started",
        extra={
            "stream": args.stream,
            "once": args.once,
            "interval_seconds": args.interval_seconds,
            "symbol": args.symbol,
            "signal_topic": settings.signal_topic,
        },
    )

    if args.once or not args.stream:
        evaluate_once(engine=engine, source=source, sink=sink, symbol=args.symbol)
        return

    while True:
        evaluate_once(engine=engine, source=source, sink=sink, symbol=args.symbol)
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    run()
