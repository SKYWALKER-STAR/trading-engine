# Trading Engine Platform

[English](#trading-engine-platform) | [中文](#交易引擎平台中文版)

A modular, event-driven trading platform written in Python 3.11. It currently implements
strategy evaluation, risk decisions, position lifecycle management, Binance Futures order
submission, continuous Binance User Data Stream ingestion, and Redis position-view projection as
independently runnable processes.

## Architecture

```text
ClickHouse factors
      |
      v
Strategy Engine -- strategy.signal.generated.v1 --> Risk Engine
                                                        |
                                             risk.decision.made.v1
                                                        v
                                                 Position Engine
                                                        |
                                           trade.action.requested.v1
                                                        v
                                                  Trade Engine
                                                        |
                                             Binance Futures WS API
                                                        |
                                             Binance order matching
                                                        |
                                      Binance User Data Stream Adapter
                                                        |
                                      trade.order.update.received.v1
                                                        v
                                                 Position Engine
                                                        |
                                        position.state.changed.v1
                                                        +----> Risk Engine
```

Kafka is the integration boundary between engines. Redis stores position state, ClickHouse is the
strategy data source, and the Binance Futures WebSocket API is the supported execution venue. A
separate, long-running User Data Stream adapter publishes subsequent order and fill changes to
Kafka.

Implemented components:

- Factor-score strategy backed by ClickHouse market-factor snapshots
- Strategy rules for market-data freshness and signal confidence
- Risk decisions using strategy signals and local position snapshots
- Position state machine covering open, close, partial fill, rejection, cancellation, and timeout
- Redis-backed position repository
- Binance Futures WebSocket execution gateway
- Binance Futures User Data Stream adapter with listen-key renewal and reconnect handling
- Versioned Kafka event contracts and serializers
- Redis raw-to-view position projector
- Unified CLI, dedicated console scripts, and unit tests

## Repository Layout

```text
src/trading_engine/
  app/          # runners, Kafka processors, and composition roots
  common/       # logging utilities
  config/       # environment-backed settings
  contracts/    # cross-engine event contracts and serialization
  domain/       # shared domain models
  infra/        # Kafka, Redis, ClickHouse, and Binance adapters
  position/     # position state machine and repository protocol
  strategy/     # strategy rules, algorithms, and engine
  trade/        # execution models and gateway protocol
tests/unit/     # unit tests grouped by component
docs/           # architecture and Redis view specifications
```

## Installation

Requirements:

- Python 3.11+
- Kafka
- Redis
- ClickHouse for the strategy runner
- Binance Futures API credentials for the trade engine

Linux/macOS:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev,runtime]'
```

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev,runtime]"
```

## Running Engines

List the registered engines:

```bash
python -m trading_engine --list-engines
```

Unified command format:

```bash
python -m trading_engine <engine-name> -- <engine-specific-arguments>
```

| Engine | Purpose |
| --- | --- |
| `strategy` | Read ClickHouse factors and publish strategy signals |
| `risk` | Evaluate signals against position snapshots |
| `position` | Manage the position lifecycle and create trade actions |
| `trade` | Submit trade actions to Binance Futures |
| `binance-user-stream` | Continuously publish Binance order and fill updates to Kafka |
| `position-projector` | Project Binance raw Redis snapshots into normalized views |

### Strategy Engine

```bash
# One evaluation; --symbol is optional
python -m trading_engine strategy -- --once --symbol BTCUSDT

# Continuous evaluation
python -m trading_engine strategy -- --stream --interval-seconds 1

# Dedicated script
strategy-engine-factor --once --symbol BTCUSDT
```

Without `--stream`, the runner evaluates once and exits.

### Risk Engine

```bash
python -m trading_engine risk
# or: risk-engine
```

It rejects signals during position transitions and repeated same-direction entries. An opposite
direction signal becomes `reduce_only`, closing the current position before a reversal.

### Position Engine

```bash
python -m trading_engine position
# or: position-engine
```

It consumes risk decisions and order updates. The lifecycle is:

```text
flat
open_long -> opening_long -> long -> close_long -> closing_long -> flat
open_short -> opening_short -> short -> close_short -> closing_short -> flat
```

Canceled and rejected orders roll back to the previous stable state. A stale transition is
recovered when a later risk decision arrives after `POSITION_ORDER_UPDATE_TIMEOUT_SECONDS`.

### Trade Engine

```bash
python -m trading_engine trade
# or: trade-engine
```

The current implementation supports `TRADE_EXCHANGE=binance` and requires `BINANCE_API_KEY` and
`BINANCE_API_SECRET`. Market orders are the default. Limit orders require `price` and
`timeInForce` in trade-action metadata.

### Binance User Data Stream

Run this process alongside the trade and position engines:

```bash
python -m trading_engine binance-user-stream
# or: binance-user-data-stream
```

The adapter creates and renews a USD-M Futures listen key, consumes `ORDER_TRADE_UPDATE`, and
publishes each order update to `trade.order.update.received.v1`. It reconnects with exponential
backoff when the WebSocket disconnects or the listen key expires.

Order updates preserve both Binance fill semantics:

- `last_filled_quantity`: quantity executed by the latest trade (`o.l`)
- `cumulative_filled_quantity`: total quantity executed for the order (`o.z`)
- `filled_quantity`: backward-compatible alias for the cumulative quantity during phase one

The current PositionManager still consumes `filled_quantity`; cumulative-fill accounting and
order-event idempotency remain follow-up work.

### Position View Projector

```bash
# One projection
python -m trading_engine position-projector -- --once

# Continuous projection
python -m trading_engine position-projector -- --stream --interval-seconds 2
```

See [the Redis position view specification](docs/position-redis-view-spec-v1.md) for its keys and
schema.

### Shell Launcher

On Unix-like systems, `start.sh` loads `.env` when present and starts engines in the background:

```bash
./start.sh strategy --once --symbol BTCUSDT
./start.sh position
./start.sh all
```

It uses `.trading/bin/python` by default; override this with `ENGINE_PYTHON`. Output is written to
`nohup-<engine>.log` files.

## Event Contracts

Every cross-engine message uses an envelope from `trading_engine.contracts` containing:

- `event_id`, `event_type`, and `schema_version`
- `occurred_at` and `producer`
- `correlation_id` and `causation_id`
- `payload`

| Topic | Producer | Consumer |
| --- | --- | --- |
| `strategy.signal.generated.v1` | Strategy | Risk |
| `risk.decision.made.v1` | Risk | Position |
| `trade.action.requested.v1` | Position | Trade |
| `trade.action.failed.v1` | Position | External consumers |
| `trade.order.update.received.v1` | Trade and Binance User Data Stream | Position |
| `position.state.changed.v1` | Position | Risk and external consumers |

Compatible payload additions retain the topic version and increment `schema_version`. Breaking
changes require a new topic suffix such as `.v2`.

## Configuration

Configuration comes from environment variables. `start.sh` also sources a repository `.env` file.

### Shared

| Variable | Default |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `KAFKA_ACKS` | `1` (`all` is accepted) |
| `LOG_LEVEL` | `INFO` |
| `LOG_FILE` | `./trading_engine.log` |

### Strategy and ClickHouse

| Variable | Default |
| --- | --- |
| `STRATEGY_MIN_CONFIDENCE` | `0.55` |
| `STRATEGY_MAX_DATA_AGE_SECONDS` | `2` |
| `STRATEGY_SIGNAL_TOPIC` | `strategy.signal.generated.v1` |
| `CLICKHOUSE_HOST` | `127.0.0.1` |
| `CLICKHOUSE_PORT` | `8123` |
| `CLICKHOUSE_USER` | adapter default |
| `CLICKHOUSE_PASSWORD` | adapter default |
| `CLICKHOUSE_DATABASE` | `binance` |
| `CLICKHOUSE_TABLE` | `v_usdt_futures_trend_score_calc` |
| `CLICKHOUSE_QUERY` | unset optional override |

### Risk Engine

| Variable | Default |
| --- | --- |
| `RISK_ENGINE_CONSUMER_GROUP` | `risk-engine` |
| `RISK_SIGNAL_TOPIC` | `strategy.signal.generated.v1` |
| `RISK_POSITION_STATE_TOPIC` | `position.state.changed.v1` |
| `RISK_DECISION_TOPIC` | `risk.decision.made.v1` |
| `RISK_REQUIRE_POSITION_SNAPSHOT` | `false` |
| `RISK_DEFAULT_OPEN_QUANTITY` | `1.0` |

When snapshots are optional, a missing snapshot is treated as flat. Set
`RISK_REQUIRE_POSITION_SNAPSHOT=true` to reject signals until a snapshot is received.

### Position Engine

| Variable | Default |
| --- | --- |
| `POSITION_ENGINE_CONSUMER_GROUP` | `position-engine` |
| `POSITION_RISK_DECISION_TOPIC` | `risk.decision.made.v1` |
| `POSITION_SIGNAL_TOPIC` | legacy fallback for the risk topic |
| `POSITION_ORDER_UPDATE_TOPIC` | `trade.order.update.received.v1` |
| `POSITION_STATE_TOPIC` | `position.state.changed.v1` |
| `POSITION_TRADE_ACTION_TOPIC` | `trade.action.requested.v1` |
| `POSITION_TRADE_ACTION_FAILED_TOPIC` | `trade.action.failed.v1` |
| `POSITION_ORDER_UPDATE_TIMEOUT_SECONDS` | `30.0` |
| `POSITION_REDIS_URL` | `redis://127.0.0.1:6379/0` |
| `POSITION_REDIS_KEY_PREFIX` | `position` |

### Trade Engine and Binance

| Variable | Default |
| --- | --- |
| `TRADE_ENGINE_CONSUMER_GROUP` | `trade-engine` |
| `TRADE_ACTION_TOPIC` | `trade.action.requested.v1` |
| `TRADE_ORDER_UPDATE_TOPIC` | `trade.order.update.received.v1` |
| `TRADE_EXCHANGE` | `binance` |
| `TRADE_REQUEST_TIMEOUT_SECONDS` | `10.0` |
| `BINANCE_WS_API_URL` | `wss://ws-fapi.binance.com/ws-fapi/v1` |
| `BINANCE_API_KEY` | required |
| `BINANCE_API_SECRET` | required |
| `BINANCE_ORDER_TYPE` | `MARKET` |
| `BINANCE_POSITION_SIDE` | `BOTH` |
| `BINANCE_NEW_ORDER_RESP_TYPE` | `ACK` |
| `BINANCE_RECV_WINDOW` | `5000` |

### Binance User Data Stream

| Variable | Default |
| --- | --- |
| `BINANCE_API_KEY` | required |
| `TRADE_ORDER_UPDATE_TOPIC` | `trade.order.update.received.v1` |
| `BINANCE_FUTURES_REST_API_URL` | `https://fapi.binance.com` |
| `BINANCE_FUTURES_USER_STREAM_URL` | `wss://fstream.binance.com/ws` |
| `BINANCE_LISTEN_KEY_KEEPALIVE_SECONDS` | `1800.0` |
| `BINANCE_USER_STREAM_RECONNECT_INITIAL_SECONDS` | `1.0` |
| `BINANCE_USER_STREAM_RECONNECT_MAX_SECONDS` | `30.0` |
| `BINANCE_USER_STREAM_REQUEST_TIMEOUT_SECONDS` | `10.0` |

### Position View Projector

| Variable | Default |
| --- | --- |
| `POSITION_VIEW_REDIS_URL` | falls back to `POSITION_REDIS_URL` |
| `POSITION_VIEW_KEY_PREFIX` | `binance:position:usdt_futures` |
| `POSITION_VIEW_POLL_INTERVAL_SECONDS` | `2.0` |
| `POSITION_VIEW_ENABLE_DETAIL_KEYS` | `true` |

`REDIS_POSITION_KEY_PREFIX` remains a legacy fallback for the projector key prefix.

## Development

```bash
make test       # pytest
make lint       # ruff check src tests
make typecheck  # mypy src
```

Unit tests use in-memory repositories, publishers, and fake gateways; they do not require live
Kafka, Redis, ClickHouse, or Binance services.

## Risk Register and Technical Debt

The items below are known limitations of the current implementation. They should be resolved
before the platform is used for unattended production trading with material funds.

### Critical

- [ ] **Remove the ClickHouse password default from source and rotate the credential.**
  `ClickHouseMarketDataSource.from_env()` currently contains a password-like default value. Runtime
  credentials must come from environment variables or a secret manager. If the current value has
  ever been valid, rotate it and review Git history for exposure.

- [ ] **Finish cumulative partial-fill accounting in PositionManager.** User Data Stream contracts
  now distinguish `last_filled_quantity` from `cumulative_filled_quantity`, while the legacy
  `filled_quantity` field remains a cumulative alias. PositionManager still subtracts that cumulative
  value from a changing position quantity, so repeated close updates can produce an incorrect
  remainder. Track the order's initial position quantity and last applied cumulative fill before
  enabling unattended production trading.

- [ ] **Replace Kafka automatic offset commits with reliable processing.** Consumers currently use
  automatic commits, so an offset can be committed before Redis persistence and downstream Kafka
  publication complete. A crash can permanently lose a risk decision or order update. Use manual
  commits after successful processing and introduce an inbox/outbox or equivalent consistency
  mechanism for state changes and emitted events.

### High

- [ ] **Add idempotent event processing.** Kafka delivery may be duplicated, but the position and
  trade paths do not persist processed `event_id` values or enforce an equivalent idempotency key.
  Duplicate risk decisions must never create duplicate orders, and duplicate order updates must not
  apply fills twice. This is already observable because Trade Engine may publish an initial `NEW` or
  `FILLED` update from the order response and User Data Stream can publish the same state again.
  Idempotency records must survive process restarts and trade fills should be deduplicated with an
  exchange-scoped key such as `(symbol, order_id, trade_id)`.

- [ ] **Validate order identity and monotonic order progression in PositionManager.** The manager
  stores `active_order_id`, but `handle_order_event()` does not reject an update whose `order_id`
  belongs to an older or unrelated order. Delayed and out-of-order events can therefore mutate the
  current transition. Match every update to the active order, persist the last applied cumulative
  fill/status, and define legal monotonic transitions, including partial-fill-then-cancel behavior.

- [ ] **Reconcile gaps after User Data Stream disconnects and ambiguous order submissions.** The
  adapter renews the listen key and reconnects with exponential backoff, but events emitted between
  disconnect and reconnection are not replayed. Likewise, a timed-out order request can have an
  unknown execution outcome. After reconnect or timeout, query open orders, order status, trades,
  and authoritative positions before allowing additional exposure.

- [ ] **Connect authoritative real-position synchronization to Risk Engine startup and updates.** A
  separate module synchronizes actual exchange positions, but the current Risk Engine starts with an
  empty in-memory position map and only learns from `position.state.changed.v1`. The synchronization
  feature resolves this risk only when Risk Engine loads a complete authoritative snapshot before
  approving entries and receives subsequent actual-position updates. Until initialization succeeds,
  missing or stale position data should fail closed: closing may be allowed, but new exposure should
  be rejected. Production configuration should set `RISK_REQUIRE_POSITION_SNAPSHOT=true`.

- [ ] **Separate expected position state from actual exchange position state.** PositionManager
  produces an internally inferred state, while the synchronization module provides exchange truth.
  Publishing both as the same conceptual state can cause last-write-wins races. Prefer distinct
  contracts such as `position.expected.state.changed.v1` and
  `position.actual.snapshot.updated.v1`; risk decisions should prefer fresh actual state and alert on
  divergence between actual and expected state.

- [ ] **Validate position snapshot completeness and freshness.** A symbol missing from a partial
  synchronization result must not automatically mean flat. Actual snapshots should carry source,
  synchronization time, generation/version, and full-snapshot status. Risk Engine should reject new
  exposure when the snapshot is expired, incomplete, or older than the signal.

### Medium

- [ ] **Preserve correlation and causation through Position Engine.** Strategy and Risk Engine keep
  the incoming trace identifiers, but Position Engine currently creates fresh envelopes without the
  source event context. Propagate `correlation_id` and set `causation_id` on position-state, trade-
  action, and failure events so one signal can be traced through the complete order lifecycle.

- [ ] **Run order-transition timeout recovery proactively.** Stale transitions are currently checked
  only when another risk decision arrives for the symbol. Without a later signal, a lost order update
  can leave the position in an opening or closing state indefinitely. Add a periodic persistent scan,
  delayed job, or scheduler-based timeout mechanism.

- [ ] **Externalize or deterministically rebuild strategy state.** `FactorScoreStrategy` keeps trend
  history and its believed direction in process memory. Restarts lose persistence history, multiple
  instances can disagree, and inferred direction can diverge from actual fills. Rebuild indicator
  history from the market source and obtain position context from the authoritative position path.

- [ ] **Align type contracts with runtime behavior.** `_to_trade_order_request()` is annotated as
  returning `TradeOrderRequest` but returns `None` for invalid requests. Change the return type to
  `TradeOrderRequest | None` and keep strict type checking green.

- [ ] **Validate Binance account mode against order parameters at startup.**
  `BINANCE_POSITION_SIDE` only controls the per-order `positionSide`; the gateway does not query or
  change the account's one-way/hedge mode. A mismatch (`BOTH` versus `LONG`/`SHORT`) causes live
  orders to be rejected. Query and fail fast on mode mismatch, or manage the mode explicitly with a
  guarded operational command.

- [ ] **Expand risk policy coverage.** Current rules mainly protect position direction and lifecycle.
  Add account equity, available margin, leverage, per-symbol and portfolio exposure, order-size,
  drawdown, concentration, liquidity, price-deviation, and market-data freshness limits.

### Testing and Operations

- [ ] Add end-to-end integration and deterministic replay tests across Kafka, Redis, ClickHouse, and
  the exchange gateway.
- [ ] Add failure tests for duplicate, delayed, missing, and out-of-order messages; crashes between
  persistence and publication; Redis/Kafka outages; reconnects; and process restarts.
- [ ] Promote live Binance probes out of the unit-test tree and add a controlled scenario that
  produces and verifies `NEW`, `PARTIALLY_FILLED`, `FILLED`, and canceled-after-partial-fill events.
  The current opt-in User Data Stream probe validates listen-key creation and the WebSocket handshake
  but does not require a business event when the account is idle.
- [ ] Add retry policies, dead-letter handling, reconciliation jobs, metrics, alerts, and structured
  audit logs.
- [ ] Add execution venues beyond Binance Futures when required.

---

# 交易引擎平台（中文版）

[English](#trading-engine-platform) | [中文](#交易引擎平台中文版)

这是一个使用 Python 3.11 开发的模块化、事件驱动交易平台。目前已经将策略计算、
风险决策、仓位生命周期管理、Binance Futures 下单、Binance User Data Stream 持续接收
以及 Redis 仓位视图投影实现为可以独立运行的进程。

## 系统架构

```text
ClickHouse 行情因子
       |
       v
策略引擎 -- strategy.signal.generated.v1 --> 风控引擎
                                                   |
                                        risk.decision.made.v1
                                                   v
                                                仓位引擎
                                                   |
                                      trade.action.requested.v1
                                                   v
                                                交易引擎
                                                   |
                                      Binance Futures WS API
                                                   |
                                           Binance 订单撮合
                                                   |
                                     User Data Stream 适配器
                                                   |
                                 trade.order.update.received.v1
                                                   v
                                                仓位引擎
                                                   |
                                   position.state.changed.v1
                                                   +----> 风控引擎
```

Kafka 是引擎之间的集成边界。Redis 用于保存仓位状态，ClickHouse 是策略的数据源，
Binance Futures WebSocket API 是当前支持的交易执行通道。独立的 User Data Stream 适配器
负责持续接收后续订单和成交变化并发布到 Kafka。

当前已实现：

- 基于 ClickHouse 行情因子快照的因子评分策略
- 行情新鲜度与信号置信度检查
- 结合策略信号和仓位快照的风险决策
- 覆盖开仓、平仓、部分成交、拒单、撤单和超时的仓位状态机
- Redis 仓位仓储
- Binance Futures WebSocket 交易网关
- 支持 listenKey 续期和断线重连的 Binance Futures User Data Stream 适配器
- 版本化 Kafka 事件契约与序列化
- Redis 原始仓位到领域视图的投影器
- 统一 CLI、独立启动命令和单元测试

## 仓库结构

```text
src/trading_engine/
  app/          # 运行入口、Kafka 处理器和组件装配
  common/       # 日志工具
  config/       # 环境变量配置
  contracts/    # 跨引擎事件契约与序列化
  domain/       # 公共领域模型
  infra/        # Kafka、Redis、ClickHouse 和 Binance 适配器
  position/     # 仓位状态机与仓储协议
  strategy/     # 策略规则、算法和策略引擎
  trade/        # 交易执行模型与网关协议
tests/unit/     # 按组件组织的单元测试
docs/           # 架构与 Redis 仓位视图规范
```

## 安装

运行要求：

- Python 3.11 或更高版本
- Kafka
- Redis
- 策略引擎需要 ClickHouse
- 交易引擎需要 Binance Futures API 凭据

Linux/macOS：

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev,runtime]'
```

Windows PowerShell：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev,runtime]"
```

## 启动引擎

查看已经注册的引擎：

```bash
python -m trading_engine --list-engines
```

统一命令格式：

```bash
python -m trading_engine <engine-name> -- <engine-specific-arguments>
```

| 引擎 | 用途 |
| --- | --- |
| `strategy` | 读取 ClickHouse 因子并发布策略信号 |
| `risk` | 根据仓位快照评估策略信号 |
| `position` | 管理仓位生命周期并生成交易动作 |
| `trade` | 向 Binance Futures 提交交易动作 |
| `binance-user-stream` | 持续将 Binance 订单和成交更新发布到 Kafka |
| `position-projector` | 将 Binance 原始 Redis 快照转换成标准视图 |

### 策略引擎

```bash
# 执行一次，--symbol 可选
python -m trading_engine strategy -- --once --symbol BTCUSDT

# 持续执行
python -m trading_engine strategy -- --stream --interval-seconds 1

# 独立命令
strategy-engine-factor --once --symbol BTCUSDT
```

未指定 `--stream` 时，策略引擎执行一次后退出。

### 风控引擎

```bash
python -m trading_engine risk
# 或：risk-engine
```

风控引擎会拒绝仓位转换期间的信号和重复的同方向开仓信号。已有反方向仓位时，信号会被
标记为 `reduce_only`，先平掉当前仓位，而不是直接反向开仓。

### 仓位引擎

```bash
python -m trading_engine position
# 或：position-engine
```

仓位引擎消费风险决策和订单更新。仓位生命周期为：

```text
flat
open_long -> opening_long -> long -> close_long -> closing_long -> flat
open_short -> opening_short -> short -> close_short -> closing_short -> flat
```

撤单或拒单会回滚到之前的稳定状态。收到后续风险决策时，如果某个转换状态已经超过
`POSITION_ORDER_UPDATE_TIMEOUT_SECONDS`，系统会尝试恢复该状态。

### 交易引擎

```bash
python -m trading_engine trade
# 或：trade-engine
```

当前支持 `TRADE_EXCHANGE=binance`，并且要求配置 `BINANCE_API_KEY` 和
`BINANCE_API_SECRET`。默认提交市价单；限价单必须在交易动作元数据中提供 `price` 和
`timeInForce`。

### Binance User Data Stream

该进程需要与交易引擎、仓位引擎同时运行：

```bash
python -m trading_engine binance-user-stream
# 或：binance-user-data-stream
```

适配器会创建并续期 USD-M Futures listenKey，持续消费 `ORDER_TRADE_UPDATE`，并将订单
变化发布到 `trade.order.update.received.v1`。WebSocket 断线或 listenKey 失效后，会使用
指数退避策略自动重连。

订单更新同时保留 Binance 的两种成交数量语义：

- `last_filled_quantity`：最近一笔成交的执行数量（`o.l`）
- `cumulative_filled_quantity`：该订单的累计成交数量（`o.z`）
- `filled_quantity`：第一阶段为兼容旧代码而保留，等于累计成交数量

当前 PositionManager 仍然消费 `filled_quantity`；累计成交量计算和订单事件幂等将在后续
阶段完成。

### 仓位视图投影器

```bash
# 执行一次
python -m trading_engine position-projector -- --once

# 持续执行
python -m trading_engine position-projector -- --stream --interval-seconds 2
```

Redis 键名和数据结构参见
[Redis 仓位视图规范](docs/position-redis-view-spec-v1.md)。

### Shell 启动脚本

在 Unix 类系统上，`start.sh` 会在 `.env` 存在时加载它，并在后台启动引擎：

```bash
./start.sh strategy --once --symbol BTCUSDT
./start.sh position
./start.sh all
```

脚本默认使用 `.trading/bin/python`，可以通过 `ENGINE_PYTHON` 覆盖。输出写入
`nohup-<engine>.log` 文件。

## 事件契约

所有跨引擎消息都使用 `trading_engine.contracts` 中定义的统一信封，包含：

- `event_id`、`event_type` 和 `schema_version`
- `occurred_at` 和 `producer`
- `correlation_id` 和 `causation_id`
- `payload`

| Topic | 生产者 | 消费者 |
| --- | --- | --- |
| `strategy.signal.generated.v1` | 策略引擎 | 风控引擎 |
| `risk.decision.made.v1` | 风控引擎 | 仓位引擎 |
| `trade.action.requested.v1` | 仓位引擎 | 交易引擎 |
| `trade.action.failed.v1` | 仓位引擎 | 外部消费者 |
| `trade.order.update.received.v1` | 交易引擎和 Binance User Data Stream | 仓位引擎 |
| `position.state.changed.v1` | 仓位引擎 | 风控引擎和外部消费者 |

向后兼容的字段增加可以保留 Topic 版本并递增 `schema_version`。破坏兼容性的修改需要使用
新的 Topic 后缀，例如 `.v2`。

## 配置

配置来自环境变量。使用 `start.sh` 时，脚本还会加载仓库根目录下的 `.env` 文件。

### 公共配置

| 环境变量 | 默认值 |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `KAFKA_ACKS` | `1`，也接受 `all` |
| `LOG_LEVEL` | `INFO` |
| `LOG_FILE` | `./trading_engine.log` |

### 策略与 ClickHouse

| 环境变量 | 默认值 |
| --- | --- |
| `STRATEGY_MIN_CONFIDENCE` | `0.55` |
| `STRATEGY_MAX_DATA_AGE_SECONDS` | `2` |
| `STRATEGY_SIGNAL_TOPIC` | `strategy.signal.generated.v1` |
| `CLICKHOUSE_HOST` | `127.0.0.1` |
| `CLICKHOUSE_PORT` | `8123` |
| `CLICKHOUSE_USER` | 适配器默认值 |
| `CLICKHOUSE_PASSWORD` | 适配器默认值 |
| `CLICKHOUSE_DATABASE` | `binance` |
| `CLICKHOUSE_TABLE` | `v_usdt_futures_trend_score_calc` |
| `CLICKHOUSE_QUERY` | 默认不设置，可覆盖查询语句 |

### 风控引擎配置

| 环境变量 | 默认值 |
| --- | --- |
| `RISK_ENGINE_CONSUMER_GROUP` | `risk-engine` |
| `RISK_SIGNAL_TOPIC` | `strategy.signal.generated.v1` |
| `RISK_POSITION_STATE_TOPIC` | `position.state.changed.v1` |
| `RISK_DECISION_TOPIC` | `risk.decision.made.v1` |
| `RISK_REQUIRE_POSITION_SNAPSHOT` | `false` |
| `RISK_DEFAULT_OPEN_QUANTITY` | `1.0` |

仓位快照不是必需项时，缺失的快照会被视为空仓。生产环境建议设置
`RISK_REQUIRE_POSITION_SNAPSHOT=true`，在收到有效仓位快照前拒绝开仓信号。

### 仓位引擎配置

| 环境变量 | 默认值 |
| --- | --- |
| `POSITION_ENGINE_CONSUMER_GROUP` | `position-engine` |
| `POSITION_RISK_DECISION_TOPIC` | `risk.decision.made.v1` |
| `POSITION_SIGNAL_TOPIC` | 风险 Topic 的旧版回退配置 |
| `POSITION_ORDER_UPDATE_TOPIC` | `trade.order.update.received.v1` |
| `POSITION_STATE_TOPIC` | `position.state.changed.v1` |
| `POSITION_TRADE_ACTION_TOPIC` | `trade.action.requested.v1` |
| `POSITION_TRADE_ACTION_FAILED_TOPIC` | `trade.action.failed.v1` |
| `POSITION_ORDER_UPDATE_TIMEOUT_SECONDS` | `30.0` |
| `POSITION_REDIS_URL` | `redis://127.0.0.1:6379/0` |
| `POSITION_REDIS_KEY_PREFIX` | `position` |

### 交易引擎与 Binance 配置

| 环境变量 | 默认值 |
| --- | --- |
| `TRADE_ENGINE_CONSUMER_GROUP` | `trade-engine` |
| `TRADE_ACTION_TOPIC` | `trade.action.requested.v1` |
| `TRADE_ORDER_UPDATE_TOPIC` | `trade.order.update.received.v1` |
| `TRADE_EXCHANGE` | `binance` |
| `TRADE_REQUEST_TIMEOUT_SECONDS` | `10.0` |
| `BINANCE_WS_API_URL` | `wss://ws-fapi.binance.com/ws-fapi/v1` |
| `BINANCE_API_KEY` | 必填 |
| `BINANCE_API_SECRET` | 必填 |
| `BINANCE_ORDER_TYPE` | `MARKET` |
| `BINANCE_POSITION_SIDE` | `BOTH` |
| `BINANCE_NEW_ORDER_RESP_TYPE` | `ACK` |
| `BINANCE_RECV_WINDOW` | `5000` |

### Binance User Data Stream 配置

| 环境变量 | 默认值 |
| --- | --- |
| `BINANCE_API_KEY` | 必填 |
| `TRADE_ORDER_UPDATE_TOPIC` | `trade.order.update.received.v1` |
| `BINANCE_FUTURES_REST_API_URL` | `https://fapi.binance.com` |
| `BINANCE_FUTURES_USER_STREAM_URL` | `wss://fstream.binance.com/ws` |
| `BINANCE_LISTEN_KEY_KEEPALIVE_SECONDS` | `1800.0` |
| `BINANCE_USER_STREAM_RECONNECT_INITIAL_SECONDS` | `1.0` |
| `BINANCE_USER_STREAM_RECONNECT_MAX_SECONDS` | `30.0` |
| `BINANCE_USER_STREAM_REQUEST_TIMEOUT_SECONDS` | `10.0` |

### 仓位视图投影器配置

| 环境变量 | 默认值 |
| --- | --- |
| `POSITION_VIEW_REDIS_URL` | 回退到 `POSITION_REDIS_URL` |
| `POSITION_VIEW_KEY_PREFIX` | `binance:position:usdt_futures` |
| `POSITION_VIEW_POLL_INTERVAL_SECONDS` | `2.0` |
| `POSITION_VIEW_ENABLE_DETAIL_KEYS` | `true` |

`REDIS_POSITION_KEY_PREFIX` 仍作为投影器键名前缀的旧版回退配置。

## 开发命令

```bash
make test       # pytest
make lint       # ruff check src tests
make typecheck  # mypy src
```

单元测试使用内存仓储、模拟发布器和模拟交易网关，不需要连接实际的 Kafka、Redis、
ClickHouse 或 Binance 服务。

## 风险与技术债清单

以下是当前实现中已知的限制。在使用真实资金进行无人值守的生产交易之前，应完成这些事项。

### 严重风险

- [ ] **删除源码中的 ClickHouse 默认密码并轮换凭据。**
  `ClickHouseMarketDataSource.from_env()` 当前包含类似真实密码的默认值。运行时凭据应来自
  环境变量或密钥管理服务。如果该值曾经有效，需要立即轮换并检查 Git 历史记录。

- [ ] **完成 PositionManager 的累计部分成交计算。** User Data Stream 契约现在已经区分
  `last_filled_quantity` 和 `cumulative_filled_quantity`，旧的 `filled_quantity` 暂时作为累计值
  别名保留。PositionManager 仍会从不断变化的仓位数量中减去累计成交量，多次平仓更新仍可能
  得到错误的剩余仓位。无人值守实盘前，需要记录订单开始时的持仓数量和上次已应用的累计成交量。

- [ ] **使用可靠处理机制替代 Kafka 自动提交 offset。** 当前消费者启用了自动提交，offset
  可能在 Redis 持久化和下游 Kafka 发布完成前被提交。进程崩溃可能造成风险决策或订单更新
  永久丢失。应在处理成功后手动提交，并引入 inbox/outbox 或同等的一致性机制。

### 高风险

- [ ] **增加事件幂等处理。** Kafka 可能重复投递，但仓位和交易链路目前没有持久化已经处理的
  `event_id` 或等价幂等键。重复风险决策不能产生重复订单，重复订单更新也不能重复计算成交量。
  当前 Trade Engine 可能根据下单响应发布初始 `NEW` 或 `FILLED`，User Data Stream 随后又发布
  同一状态，因此重复更新已经可能实际发生。幂等记录必须能够跨进程重启保存，成交事件应使用
  `(symbol, order_id, trade_id)` 等包含交易所范围的键去重。

- [ ] **在 PositionManager 中校验订单身份和状态单调性。** Manager 虽然保存了
  `active_order_id`，但 `handle_order_event()` 并未拒绝来自旧订单或无关订单的 `order_id`。
  延迟或乱序事件可能错误修改当前转换。每条更新都应与活动订单匹配，并持久化上次已应用的累计
  成交量和状态，同时明确合法的单调状态转换，包括“部分成交后撤单”。

- [ ] **补偿 User Data Stream 断线和下单结果不确定造成的数据缺口。** 当前适配器能够续期
  listenKey，并使用指数退避重连，但断线到重连期间的事件不会回放；下单请求超时也可能处于
  “交易所已受理、客户端未知”的状态。重连或超时后应查询未完成订单、订单状态、成交记录和
  权威持仓，并在完成对账前禁止增加敞口。

- [ ] **将真实持仓同步接入风控引擎的启动和增量更新路径。** 已有独立模块同步交易所真实持仓，
  但当前风控引擎启动时的内存仓位为空，并且只消费 `position.state.changed.v1`。只有当风控引擎
  在批准开仓前加载完整的权威快照，并持续接收真实仓位更新时，该同步功能才能消除风险。
  初始化完成前应采用失败关闭策略：可以允许降低风险的平仓，但拒绝增加敞口。生产环境应设置
  `RISK_REQUIRE_POSITION_SNAPSHOT=true`。

- [ ] **区分预期仓位状态与交易所真实仓位。** PositionManager 产生的是系统推导状态，持仓同步
  模块提供的是交易所事实。如果两者被视为同一种状态，可能出现最后写入覆盖的竞态。建议使用
  `position.expected.state.changed.v1` 和 `position.actual.snapshot.updated.v1` 等独立契约。
  风险决策应优先使用新鲜的真实状态，并在预期状态和真实状态不一致时报警。

- [ ] **校验仓位快照的完整性与新鲜度。** 某个 symbol 没有出现在一次局部同步结果中，并不一定
  表示空仓。真实快照应携带数据源、同步时间、版本以及是否为完整快照等信息。快照过期、不完整
  或早于策略信号时，风控引擎应拒绝增加敞口。

### 中等风险

- [ ] **在仓位引擎中延续关联和因果信息。** 策略与风控引擎保留了输入事件的追踪标识，但仓位
  引擎发布事件时重新创建了信封。仓位状态、交易动作和失败事件应继承 `correlation_id`，并设置
  `causation_id`，以便追踪一次信号的完整订单生命周期。

- [ ] **主动执行订单转换超时恢复。** 当前只在同一 symbol 收到下一条风险决策时检查超时。如果
  后续没有信号，丢失的订单更新可能让仓位永久停留在开仓或平仓过程中。应增加持久化的定时扫描、
  延迟任务或调度机制。

- [ ] **外部化或确定性重建策略状态。** `FactorScoreStrategy` 在进程内存中保存趋势历史和它认为的
  仓位方向。重启会丢失历史，多实例可能产生分歧，推导方向也可能与真实成交不一致。指标历史应
  从行情源重建，仓位上下文应来自权威持仓链路。

- [ ] **让类型契约与运行行为保持一致。** `_to_trade_order_request()` 标注返回
  `TradeOrderRequest`，但无效请求会返回 `None`。返回类型应改为
  `TradeOrderRequest | None`，并确保严格类型检查通过。

- [ ] **启动时校验 Binance 账户持仓模式与订单参数。** `BINANCE_POSITION_SIDE` 只设置每笔
  订单的 `positionSide`，当前网关不会查询或切换账户的单向/双向持仓模式。账户模式与 `BOTH`
  或 `LONG`/`SHORT` 不匹配会导致实盘拒单。应在启动时查询并快速失败，或通过受保护的运维命令
  显式管理持仓模式。

- [ ] **扩展风险规则。** 当前规则主要保护仓位方向和生命周期。后续应增加账户权益、可用保证金、
  杠杆、单 symbol 和组合敞口、订单大小、回撤、集中度、流动性、价格偏差及行情新鲜度限制。

### 测试与运维

- [ ] 增加跨 Kafka、Redis、ClickHouse 和交易网关的端到端集成测试及确定性回放测试。
- [ ] 增加重复、延迟、丢失和乱序消息测试，以及持久化与发布之间崩溃、Redis/Kafka 故障、
  断线重连和进程重启测试。
- [ ] 将 Binance 实盘探针移出单元测试目录，并增加可控场景，验证 `NEW`、`PARTIALLY_FILLED`、
  `FILLED` 和部分成交后撤单事件。当前显式启用的 User Data Stream 实盘探针只验证 listenKey
  创建和 WebSocket 握手，账户空闲时不强制要求收到业务事件。
- [ ] 增加重试策略、死信处理、仓位对账任务、指标、报警和结构化审计日志。
- [ ] 在需要时增加 Binance Futures 以外的交易执行通道。

### 改造计划

- [ ] 扩展 PositionOrderEvent，不再丢失成交字段。
- [ ] 新增 TrackedOrder 和持久化 repository。
- [ ] 下单时生成并传递 newClientOrderId。
- [ ] 绑定 client_order_id → order_id。
- [ ] 校验 active_order_id。
- [ ] 使用累计成交差值更新仓位。
- [ ] 使用 trade_id 实现持久化幂等。
- [ ] 修正部分成交后撤单。
- [ ] 实现订单状态单调性。
- [ ] 增加启动、重连和超时后的 Binance 对账。
