# Trading Engine Platform

This repository is the foundation for a modular trading system. It currently includes the **Strategy Engine** only, with interfaces and composition designed for future expansion into:

- Risk Engine
- Position Engine
- Execution Engine

## Design Goals

- Keep each engine bounded and independently testable.
- Use explicit domain models and protocols for clear contracts.
- Start with in-process execution, while keeping interfaces ready for distributed runtime later.
- Favor deterministic behavior and replay-friendly flow.

## Current Scope

Implemented in this repository:

- Strategy domain models (market tick, strategy signal, strategy context)
- Strategy rules pipeline
- Strategy algorithm protocol and a sample momentum strategy
- Position manager state machine and repository abstraction
- In-memory event bus abstraction
- Bootstrap wiring for the strategy engine
- Unit tests for core behavior

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
pytest
```

## Start The Strategy Engine

Install runtime dependencies first:

```bash
pip install -e .[runtime]
```

Single run pipeline (ClickHouse -> strategy rules -> Kafka):

```bash
strategy-engine-factor --once --symbol BTCUSDT
```

Or run through Python module directly:

```bash
python -m trading_engine --once --symbol BTCUSDT
```

Stream mode (continuous evaluations):

```bash
strategy-engine-factor --stream --interval-seconds 1
```

Environment settings from `.env` or shell variables:

- `STRATEGY_MIN_CONFIDENCE`
- `STRATEGY_MAX_DATA_AGE_SECONDS`
- `STRATEGY_SIGNAL_TOPIC`
- `LOG_LEVEL` (`DEBUG` | `INFO` | `WARNING` | `ERROR`)
- `CLICKHOUSE_HOST`
- `CLICKHOUSE_PORT`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE`
- `CLICKHOUSE_TABLE`
- `CLICKHOUSE_QUERY` (optional override)
- `KAFKA_BOOTSTRAP_SERVERS`

## Repository Layout

```text
src/trading_engine/
  app/          # wiring and bootstrap
  config/       # runtime settings
  domain/       # core market and event models
  infra/        # infrastructure adapters
  position/     # event-driven position state machine
  strategy/     # strategy engine and strategy logic
tests/unit/     # focused unit tests
docs/           # architecture notes
```

## Position Manager

The position manager is designed as an event-driven state machine:

1. `StrategySignal` enters `PositionManager`
2. `PositionManager` updates position lifecycle state
3. `PositionManager` emits `TradeAction`
4. Risk / Order engines consume `TradeAction`
5. Binance order updates are fed back as `PositionOrderEvent`
6. `PositionManager` transitions from request state to opening/closing and finally to stable long/short/flat state

State layers:

- Direction: `flat`, `long`, `short`
- Lifecycle: `flat`, `open_long`, `opening_long`, `long`, `close_long`, `closing_long`, `open_short`, `opening_short`, `short`, `close_short`, `closing_short`

Repository abstraction:

- `PositionManager`
- `PositionRepository`
- `RedisPositionRepository`

Redis configuration:

- `POSITION_REDIS_URL`
- `POSITION_REDIS_KEY_PREFIX`

## Next Steps

1. Add an application runner that consumes market feed and calls the strategy engine.
2. Introduce a command/event contract that Risk, Position, and Execution engines can consume later.
3. Add integration tests for end-to-end replay.
