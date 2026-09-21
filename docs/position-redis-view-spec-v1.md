# Redis Position View Spec v1

## 1. 目标

本规范定义 Binance U 本位仓位在 Redis 中的分层键模型与领域视图 Schema，解决以下问题：

- 键名不一致（按 symbol 分键 vs 总快照键）
- Redis 类型不一致（hash vs string）
- 领域层缺失 Binance 原始仓位字段

本规范要求：

- 保留原始层（raw）完整数据
- 提供实际仓位视图（view），与订单驱动的执行仓位状态分开存储
- 领域层显式携带 Binance 仓位字段（完整保留）
- 明确当前已实现的去重机制及尚未实现的乱序保护

本文按当前代码描述。生产 PositionWorkflow / RedisPositionRepository 读取独立 execution 键，**不再读取或回退到 view 键**。实际仓位投影也没有直接接入风控启动加载。

## 2. 命名规则

- 视图前缀默认：`binance:position:usdt_futures`
- 版本：raw/view 键带 `v1`；execution 和 debug 键不带该后缀
- symbol：统一大写，如 `SNDKUSDT`
- side：`BOTH` / `LONG` / `SHORT`

格式：

`{prefix}:{layer}:{entity}:{scope}:{version}`

上述格式只描述 raw/view 的主要键形态，不适用于全部键。例如 `view:meta:v1` 没有 scope，执行状态使用 `{execution_prefix}:{account}:state:{symbol}`，其中 account 外的一对大括号是 Redis 键的实际字符。

### 2.1 配置与数据源

| 用途 | Redis 地址 | 键前缀来源（从左到右回退） |
| --- | --- | --- |
| 原始快照读取、实际视图投影 | `POSITION_VIEW_REDIS_URL` → `POSITION_REDIS_URL` → `redis://127.0.0.1:6379/0` | `POSITION_VIEW_KEY_PREFIX` → `POSITION_REDIS_KEY_PREFIX` → `REDIS_POSITION_KEY_PREFIX` → `binance:position:usdt_futures` |
| 执行仓位及仓位 inbox/outbox | `POSITION_REDIS_URL` → 本机 DB 0 | `POSITION_EXECUTION_KEY_PREFIX` → `position:execution` |
| 交易任务及交易 inbox/outbox | `ORDER_REDIS_URL` → 本机 DB 0 | `TRADE_WORKFLOW_KEY_PREFIX` → `trade:execution` |
| 调试看板及 outbox 调试 observer | `POSITION_REDIS_URL` → 本机 DB 0 | `POSITION_VIEW_KEY_PREFIX` → `POSITION_REDIS_KEY_PREFIX` → `binance:position:usdt_futures` |

工作流账户取 `ORDER_ACCOUNT_ID`，去除首尾空白后为空则使用 `default`。raw/view/debug 键没有独立账户字段，多账户需通过 Redis 地址或前缀隔离。
若投影使用独立的 `POSITION_VIEW_REDIS_URL`，看板不会自动切换到该数据库读取 view 回退数据。

## 3. 键空间

### 3.1 Raw 层（事实源）

1. `binance:position:usdt_futures:raw:snapshot:all:v1`
- 类型：`HASH`
- field：`{symbol}:{side}`，例如 `SNDKUSDT:LONG`
- value：Binance 原始仓位 JSON（附加 `synced_at`）

2. `binance:position:usdt_futures:raw:index:positions:v1`
- 类型：`SET`
- member：`{symbol}:{side}`

3. `binance:position:usdt_futures:raw:meta:v1`
- 类型：`HASH`
- fields：`last_sync_ts`, `position_count`, `source`

当前 projector 仅通过 `HGETALL raw:snapshot:all:v1` 读取数据；raw 索引和元信息是外部同步方的约定，projector 虽声明了它们的键名，但不读取、不写入。以上 raw 数据由外部同步模块维护，本仓库投影器不直接向 Binance 拉取持仓。

### 3.2 View 层（Trading Engine View）

投影器维护 symbol 主视图和 symbol + side 明细视图。它们用于表达实际仓位，不能作为执行状态机的存储键。

1. 主视图键

`binance:position:usdt_futures:view:state:{symbol}:v1`

- 类型：`STRING`（JSON）
- 用途：实际仓位展示、排障；调试看板在执行状态缺失时可回退读取
- 粒度：symbol
- 内容：所选主仓位的稳定态、带符号数量、完整原始字段及来源信息；不是 LONG/SHORT 两侧的净额聚合

2. 明细状态键（保留 side 全字段）

`binance:position:usdt_futures:view:state:{symbol}:{side}:v1`

- 类型：`STRING`（JSON）
- 用途：完整保留 Binance side 级仓位字段
- 粒度：symbol + side
- `POSITION_VIEW_ENABLE_DETAIL_KEYS=false` 时不再写明细，但不会清理已有明细键

3. 视图索引

`binance:position:usdt_futures:view:index:symbols:v1`

- 类型：`SET`
- member：`{symbol}`

4. 视图元信息

`binance:position:usdt_futures:view:meta:v1`

- 类型：`HASH`
- fields：`last_projected_ts`, `state_count`, `projector_version`

`state_count` 是本轮有效 symbol 数量，不一定等于索引成员数。投影器不删除消失的 symbol/side；raw 为空时直接返回，也不清理旧 view 或更新元信息。

5. 主视图内容摘要

`binance:position:usdt_futures:view:hash:state:v1`

- 类型：`HASH`
- field：大写 symbol
- value：主视图 JSON 按键排序序列化后的 SHA-1
- 用途：摘要一致时跳过主视图 SET；明细键和视图元信息仍会写入

### 3.3 执行仓位工作流（独立于 view）

以下示例使用默认账户，`{default}` 中的大括号必须保留：

| 键 | 类型 | 写入方及用途 |
| --- | --- | --- |
| `position:execution:{default}:initialized` | STRING | 迁移/初始化命令写入 `1`，仓位进程启动检查 |
| `position:execution:{default}:state:SNDKUSDT` | STRING / JSON | PositionWorkflow 保存执行状态，RedisPositionRepository 读取 |
| `position:execution:{default}:inbox:<event_id>` | STRING | 已处理输入回执，值为聚合标识（仓位通常为 symbol） |
| `position:execution:{default}:outbox` | STREAM | 待发布事件，由工作流追加、relay 发布后删除 |
| `position:execution:{default}:relay-lock` | STRING | relay 的 Redis 锁，60 秒租约，每条消息前续期 |
| `position:execution:{default}:tasks` | ZSET（预留） | 通用存储支持的任务索引；当前仓位流程不调度任务，通常不存在 |

执行 JSON 包含 `symbol`、`direction`、`lifecycle`、非负数值 `quantity`、`active_order_id`、`active_client_order_id`、`last_order_id`、`last_client_order_id`、`updated_at`、`metadata`。通过工作流事务保存时还包含递增 `revision`。

例如空头持仓量为 0.03 时，执行状态是 `direction=short, quantity=0.03`；view 可能是字符串 `quantity="-0.03"`。不要将 view JSON 直接复制到执行键。

### 3.4 交易工作流关联键

| 键 | 类型 | 用途 |
| --- | --- | --- |
| `trade:execution:{default}:state:<client_order_id>` | STRING / JSON | 请求、已知订单状态、执行阶段、累计成交、租约和对账信息 |
| `trade:execution:{default}:inbox:<event_id>` | STRING | 交易输入去重回执 |
| `trade:execution:{default}:tasks` | ZSET | member 为 client_order_id，score 为到期 Unix 时间（秒） |
| `trade:execution:{default}:outbox` | STREAM | 待发布订单更新 |
| `trade:execution:{default}:relay-lock` | STRING | 交易事件 relay 锁 |

两个 outbox 的每条记录包含 `topic`、`key`、完整序列化事件 `body`、`aggregate` 和 `revision`。发布收到 Kafka ACK 后 XDEL；失败保留记录，重放使用同一事件 ID。
inbox、状态、outbox 和 tasks 不自动设置 TTL；relay-lock 有租约。worker 的租约保存在订单文档中，不是独立的锁键。

### 3.5 Debug 键

| 默认键 | 类型 | 用途 |
| --- | --- | --- |
| `binance:position:usdt_futures:debug:state:SNDKUSDT` | STRING / JSON | 最近一次调试转换快照 |
| `binance:position:usdt_futures:debug:history:SNDKUSDT` | LIST | 最近 20 条转换记录，最新在前 |

生产链路由仓位 outbox 的 observer 尽力记录调试历史；记录可能遗漏或重复，不是业务状态的权威来源。看板读取优先级为 **execution → view → debug**，该回退只属于看板，仓位引擎没有此回退。

### 3.6 旧规划但尚未实现的键

`{prefix}:ops:idempotency:{fingerprint}:v1` 和 `{prefix}:ops:lock:projector:v1` 是旧设计中的规划，当前 projector 不创建、不使用它们。不要将其与已实现的工作流 inbox 和 relay-lock 混淆。

## 4. View Schema

### 4.1 主状态（symbol 级）JSON Schema（逻辑定义）

```json
{
  "schema_version": "v1",
  "symbol": "SNDKUSDT",
  "direction": "long",
  "lifecycle": "long",
  "quantity": "0.03",
  "active_order_id": null,
  "updated_at": "2026-08-12T09:00:00Z",
  "source": {
    "name": "binance_raw_projector",
    "raw_key": "binance:position:usdt_futures:raw:snapshot:all:v1",
    "raw_fields": ["SNDKUSDT:LONG"],
    "source_synced_at": "2026-08-12T09:00:00Z",
    "position_mode": "hedge"
  },
  "binance_position": {
    "symbol": "SNDKUSDT",
    "positionSide": "LONG",
    "positionAmt": "0.03",
    "entryPrice": "1314.94",
    "breakEvenPrice": "1315.465976",
    "markPrice": "1304.88780710",
    "unRealizedProfit": "-0.30156578",
    "liquidationPrice": "986.21717061",
    "isolatedMargin": "0",
    "notional": "39.14663421",
    "marginAsset": "USDT",
    "isolatedWallet": "0",
    "initialMargin": "7.82932685",
    "maintMargin": "0.25445312",
    "positionInitialMargin": "7.82932685",
    "openOrderInitialMargin": "0",
    "adl": 1,
    "bidNotional": "0",
    "askNotional": "0"
  },
  "metadata": {
    "projector_version": "1.0.0"
  }
}
```

说明：

- 为避免精度损失，Binance 的数值字符串字段在 view 中保持字符串。
- `direction`/`lifecycle` 在 view 中仅表示所选主仓位的稳定状态，不驱动执行状态机。
- `binance_position` 保存原始语义字段，满足排障和风控透传需求。

### 4.2 明细状态（symbol+side）JSON Schema

```json
{
  "schema_version": "v1",
  "symbol": "SNDKUSDT",
  "positionSide": "LONG",
  "synced_at": "2026-08-12T09:00:00Z",
  "binance_position": {
    "symbol": "SNDKUSDT",
    "positionSide": "LONG",
    "positionAmt": "0.03",
    "entryPrice": "1314.94",
    "breakEvenPrice": "1315.465976",
    "markPrice": "1304.88780710",
    "unRealizedProfit": "-0.30156578",
    "liquidationPrice": "986.21717061",
    "isolatedMargin": "0",
    "notional": "39.14663421",
    "marginAsset": "USDT",
    "isolatedWallet": "0",
    "initialMargin": "7.82932685",
    "maintMargin": "0.25445312",
    "positionInitialMargin": "7.82932685",
    "openOrderInitialMargin": "0",
    "adl": 1,
    "bidNotional": "0",
    "askNotional": "0"
  }
}
```

## 5. 字段映射规则

Raw -> View 映射（核心）：

1. `symbol` <- `symbol`
2. `quantity` <- `positionAmt`
3. `direction`：
- `positionAmt > 0` => `long`
- `positionAmt < 0` => `short`
- `positionAmt == 0` => `flat`
4. `lifecycle`：
- 固定映射为稳定态：`long` / `short` / `flat`
- 订单驱动中间态由独立 execution 状态保存
5. `updated_at`：本轮投影时间；`source.source_synced_at` 取主仓位 `synced_at`，缺失时回退为投影时间

同一 symbol 有多条记录时，优先选 `positionSide=BOTH`；否则选择 `abs(positionAmt)` 最大的一侧作为主视图。LONG/SHORT 的所有侧数据由明细键保留，主视图并不相加或净额化。`position_mode` 根据侧字段推断，不是调用交易所账户配置接口确认的结果。

## 6. 幂等与乱序保护

当前 projector 实现了 `view:hash:state:v1` 内容摘要比较，并将本轮写操作放入事务 pipeline。摘要读取在事务之外，没有 WATCH 或 Lua 时间戳 CAS，也没有 last_applied_ts 或分布式投影锁。

主视图包含每轮更新的 `updated_at`，因此即使原始持仓不变，摘要通常也会改变。当前机制不能作为严格的消息幂等或乱序保护；projector 本身不广播 Kafka 事件。

执行工作流的 inbox 去重、WATCH 冲突重算和 outbox 重放属于另一条链路，不能视为 raw/view 已获得同样的保护。

## 7. 兼容策略

1. 外部同步继续维护 raw，projector 只写 view、view 索引/摘要和元信息。
2. PositionWorkflow / RedisPositionRepository 只使用独立 execution 状态，不从实际视图自动导入仓位。
3. 旧 `{legacy_prefix}:{symbol}` 是迁移扫描的兼容副本，不是新运行时键。迁移跳过含顶层 source 的记录，并检查数量及活动订单身份。
4. 迁移使用 SET NX，不覆盖现有执行状态，不删除旧键；完成后写入 initialized。空库初始化及带符号空仓转换见 [操作说明](inbox-outbox-operations.md)。
5. 配置过 fresh1 等自定义前缀时，本文所有默认示例都应替换为实际前缀，不能因查不到默认键就判断无仓位。

## 8. 非目标

- 本规范不定义 Kafka 事件结构
- 本规范不替代 PositionManager 的状态机生命周期逻辑
- 本规范不改变 Binance 原始字段命名
