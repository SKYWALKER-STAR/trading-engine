from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from trading_engine.app.run_position_engine import run as run_position_engine
from trading_engine.app.run_risk_engine import run as run_risk_engine
from trading_engine.app.run_position_view_projector import run as run_position_view_projector
from trading_engine.app.run_strategy_engine_factor import run as run_strategy_engine_factor


EngineRunner = Callable[[list[str] | None], None]


@dataclass(frozen=True, slots=True)
class EngineSpec:
    name: str
    description: str
    runner: EngineRunner


def get_engine_specs() -> dict[str, EngineSpec]:
    """Returns the centralized engine registry used by the CLI router."""

    specs = (
        EngineSpec(
            name="strategy",
            description="Run strategy engine factor pipeline",
            runner=run_strategy_engine_factor,
        ),
        EngineSpec(
            name="position",
            description="Run Kafka-backed position engine",
            runner=run_position_engine,
        ),
        EngineSpec(
            name="risk",
            description="Run Kafka-backed risk engine",
            runner=run_risk_engine,
        ),
        EngineSpec(
            name="position-projector",
            description="Project Binance Redis raw snapshots into trading-engine view keys",
            runner=run_position_view_projector,
        ),
    )
    return {spec.name: spec for spec in specs}