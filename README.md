# Trading Engine Platform

This repository is the foundation for a modular trading system. It currently includes the **Strategy Engine** and a Kafka-oriented **Position Engine** skeleton, with interfaces and composition designed for future expansion into:

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
- Shared Kafka message contracts with versioned envelopes
- Position manager state machine and repository abstraction
- Kafka publisher / consumer skeleton for independent engine processes
- Redis raw->view projector skeleton for position state interoperability
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

Or use the unified engine router:

```bash
python -m trading_engine strategy -- --once --symbol BTCUSDT
```

Installed script form:

```bash
trading-engine strategy -- --once --symbol BTCUSDT
```

Stream mode (continuous evaluations):

```bash
strategy-engine-factor --stream --interval-seconds 1
```

Strategy engine command-line arguments:

- `--symbol <SYMBOL>`: optional symbol filter for ClickHouse query, default `None`
- `--once`: run a single evaluation cycle and exit
- `--stream`: run continuously
- `--interval-seconds <FLOAT>`: evaluation interval used in stream mode, default `1.0`

Behavior notes:

- If `--stream` is not provided, the runner executes once and exits.
- `--once` is optional and also forces single-run behavior.

Strategy engine startup examples:

```bash
# Single run for all symbols
python -m trading_engine strategy

# Single run for one symbol
python -m trading_engine strategy -- --once --symbol BTCUSDT

# Continuous mode every 2 seconds
python -m trading_engine strategy -- --stream --interval-seconds 2
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

## Start The Position Engine

The position engine is designed to run as its own process and consume Kafka contracts:

```bash
position-engine
```

Or through the unified router:

```bash
python -m trading_engine position
```

Position engine command-line arguments:

- None currently. The runner only accepts default help parsing and starts directly.

Position engine startup examples:

```bash
# Unified router
python -m trading_engine position

# Direct script
position-engine
```

Position engine input topics:

- `strategy.signal.generated.v1`
- `trade.order.update.received.v1`

Position engine output topics:

- `position.state.changed.v1`
- `trade.action.requested.v1`

Position engine settings:

- `POSITION_ENGINE_CONSUMER_GROUP`
- `POSITION_SIGNAL_TOPIC`
- `POSITION_ORDER_UPDATE_TOPIC`
- `POSITION_STATE_TOPIC`
- `POSITION_TRADE_ACTION_TOPIC`
- `POSITION_REDIS_URL`
- `POSITION_REDIS_KEY_PREFIX`

Position engine required runtime dependencies:

- Kafka cluster reachable through `KAFKA_BOOTSTRAP_SERVERS`
- Redis reachable through `POSITION_REDIS_URL`

## Start The Position View Projector

The position view projector reads Binance raw Redis snapshots and projects them into
trading-engine view keys so the position domain can consume normalized state.

Direct script:

```bash
position-view-projector --once
```

Unified router:

```bash
python -m trading_engine position-projector -- --once
```

Stream mode:

```bash
python -m trading_engine position-projector -- --stream --interval-seconds 2
```

Projector settings:

- `POSITION_VIEW_REDIS_URL` (fallback to `POSITION_REDIS_URL`)
- `POSITION_VIEW_KEY_PREFIX` (default `binance:position:usdt_futures`)
- `POSITION_VIEW_POLL_INTERVAL_SECONDS` (default `2.0`)
- `POSITION_VIEW_ENABLE_DETAIL_KEYS` (`true` / `false`, default `true`)

Redis key model reference:

- [docs/position-redis-view-spec-v1.md](docs/position-redis-view-spec-v1.md)

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

The position manager is an event-driven state machine behind the Kafka-facing position engine process:

1. Strategy engine publishes `strategy.signal.generated.v1`
2. Position engine maps strategy signals into `PositionSignalCommand` and updates `PositionManager`
3. Position engine publishes `trade.action.requested.v1`
4. Trade engine publishes `trade.order.update.received.v1`
5. Position engine feeds order updates back into `PositionManager`
6. Position engine publishes `position.state.changed.v1`
7. Risk engine consumes strategy signals plus position messages and produces downstream risk actions in its own process

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

## Kafka Contracts

All cross-engine messages should use the shared contracts package under `src/trading_engine/contracts/`.

Envelope fields:

- `event_id`
- `event_type`
- `schema_version`
- `occurred_at`
- `producer`
- `correlation_id`
- `causation_id`
- `payload`

Current topic map:

- `strategy.signal.generated.v1`
- `trade.action.requested.v1`
- `trade.order.update.received.v1`
- `position.state.changed.v1`

Shared command model for position domain:

- `PositionSignalCommand`: the only signal-shaped input that `PositionManager` accepts
- `PositionOrderEvent`: the order/execution feedback input that advances lifecycle state

See `src/trading_engine/contracts/messages.py` for payload schemas and `src/trading_engine/contracts/serde.py` for wire encoding.

## Unified Engine CLI

List available engines:

```bash
python -m trading_engine --list-engines
```

Pattern:

```bash
python -m trading_engine <engine-name> -- <engine-specific-args>
```

CLI arguments:

- `<engine-name>`: one of the names reported by `--list-engines` (currently `strategy`, `position`, `position-projector`)
- `--list-engines`: print available engines and exit
- `<engine-specific-args>`: forwarded to the selected engine runner after `--`

Examples:

- `python -m trading_engine strategy -- --stream --interval-seconds 1`
- `python -m trading_engine position`

How to add a new engine while preserving this workflow:

1. Add a runner function `run(argv: list[str] | None = None)` in `src/trading_engine/app/`.
2. Register it once in `src/trading_engine/app/engine_registry.py` via a new `EngineSpec`.
3. Optionally expose a dedicated script in `pyproject.toml` under `[project.scripts]`.

## Next Steps

1. Add an application runner that consumes market feed and calls the strategy engine.
2. Implement a dedicated risk engine process that consumes `strategy.signal.generated.v1` and `position.state.changed.v1` and emits risk actions.
3. Implement a dedicated trade engine process that consumes `trade.action.requested.v1` and emits `trade.order.update.received.v1`.
4. Add integration tests for end-to-end replay across Kafka topics.
