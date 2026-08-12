# Redis Position View Spec v1

## 1. 目标

本规范定义 Binance U 本位仓位在 Redis 中的分层键模型与领域视图 Schema，解决以下问题：

- 键名不一致（按 symbol 分键 vs 总快照键）
- Redis 类型不一致（hash vs string）
- 领域层缺失 Binance 原始仓位字段

本规范要求：

- 保留原始层（raw）完整数据
- 提供领域层（view）可直接被 PositionManager 消费
- 领域层显式携带 Binance 仓位字段（完整保留）
- 支持幂等与乱序保护

## 2. 命名规则

- 前缀：`binance:position:usdt_futures`
- 版本：所有键必须显式带 `v1`
- symbol：统一大写，如 `SNDKUSDT`
- side：`BOTH` / `LONG` / `SHORT`

格式：

`{prefix}:{layer}:{entity}:{scope}:{version}`

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

### 3.2 View 层（Trading Engine View）

为了兼容现有 PositionManager（按 symbol 单状态读取）并完整表达 Binance 字段，定义两类视图键。

1. 主状态键（兼容 PositionManager）

`binance:position:usdt_futures:view:state:{symbol}:v1`

- 类型：`STRING`（JSON）
- 用途：PositionManager 主读取键
- 粒度：symbol
- 内容：领域主状态 + 聚合后的仓位字段 + 原始字段快照摘要

2. 明细状态键（保留 side 全字段）

`binance:position:usdt_futures:view:state:{symbol}:{side}:v1`

- 类型：`STRING`（JSON）
- 用途：完整保留 Binance side 级仓位字段
- 粒度：symbol + side

3. 视图索引

`binance:position:usdt_futures:view:index:symbols:v1`

- 类型：`SET`
- member：`{symbol}`

4. 视图元信息

`binance:position:usdt_futures:view:meta:v1`

- 类型：`HASH`
- fields：`last_projected_ts`, `state_count`, `projector_version`

### 3.3 Ops 层（幂等与控制）

1. 幂等键

`binance:position:usdt_futures:ops:idempotency:{fingerprint}:v1`

- 类型：`STRING`
- 写入规则：`SET key 1 NX EX 86400`

2. 投影锁

`binance:position:usdt_futures:ops:lock:projector:v1`

- 类型：`STRING`
- 写入规则：`SET key owner NX EX 10`

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
- `direction`/`lifecycle` 是领域字段，供 PositionManager 消费。
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
- 初始建议直接映射为稳定态：`long` / `short` / `flat`
- 订单驱动中间态仍由 PositionManager 事件推进
5. `updated_at`：优先使用 Binance 事件时间；无事件时间时用 `synced_at`

## 6. 幂等与乱序保护

### 6.1 消息级幂等

- 指纹建议：

`sha1(symbol|positionSide|positionAmt|entryPrice|markPrice|unRealizedProfit|synced_at_bucket)`

- 写入：`SET ops:idempotency:{fingerprint}:v1 1 NX EX 86400`
- 失败则判定重复，直接跳过

### 6.2 时序保护

- 为每个 `{symbol}` 维护 `last_applied_ts`
- 若新记录时间 `<= last_applied_ts`，判定 stale 丢弃
- 通过 Lua 原子执行 compare-and-set

### 6.3 状态短路

- 对将写入 view 的 JSON 做 canonical hash
- 若与当前 `state_hash` 相同，跳过写入与事件广播

## 7. 兼容策略

1. 保留现有 raw hash 键不变
2. 新增 projector 写入 view string 键
3. PositionManager 先读取 `view:state:{symbol}:v1`
4. 兜底读取旧键仅作为迁移期方案，稳定后下线

## 8. 非目标

- 本规范不定义 Kafka 事件结构
- 本规范不替代 PositionManager 的状态机生命周期逻辑
- 本规范不改变 Binance 原始字段命名
