# 当前开仓/平仓交易策略总结（2026-09-30）

本文基于当前代码实现整理，覆盖 strategy、risk、position 三段核心逻辑，用于说明“现在系统实际上如何开仓、平仓与反手”。

## 1. 总体链路（当前生效）

1. `strategy` 从 ClickHouse 读取 1m 因子，生成方向信号（`long` / `short` / `flat`）。
2. `risk` 结合本地仓位快照审批信号，输出 `approve` / `reject` / `reduce_only`。
3. `position` 将审批后的信号推进到仓位状态机，生成交易动作（开多、平多、开空、平空）。
4. `trade` 与 `trade-worker` 执行下单与查单；订单回报（NEW/部分成交/全成/撤单）再回流到 `position` 更新仓位。

## 2. 策略层（何时给开仓/平仓信号）

策略实现：`factor_score`（1 分钟趋势评分策略）。

### 2.1 前置拦截规则

在策略算法之前，先执行两条规则：

1. 新鲜度规则：行情时间与当前时间差必须 `<= STRATEGY_MAX_DATA_AGE_SECONDS`（默认 2 秒）。
2. 置信度规则：
   - 因子上下文下，置信度取 `(score_ema + score_dmi_adx + score_rsi + score_flow + score_funding) / 5` 的绝对值，再截断到 `[0,1]`。
   - 必须 `>= STRATEGY_MIN_CONFIDENCE`（默认 0.55）。

任一规则不通过，策略直接拒绝，不产生信号。

### 2.2 过滤条件（先过滤再考虑入场）

命中任意条件即 `filtered`，方向输出 `flat`：

1. `abs(trend_score_p) < 20`
2. `adx_14 < 15`
3. `score_ema` 与 `score_dmi_adx` 方向冲突（乘积 < 0）

### 2.3 开多条件（entry_long）

需同时满足：

1. `trend_score_p` 连续 2 根 K 线 `>= 50`
2. `score_ema > 0`
3. `score_dmi_adx > 0`
4. `close > ema_12 > ema_26`
5. `adx_14 >= 20`
6. `score_rsi >= 0`
7. `score_flow >= -5`
8. `score_funding >= 0`

### 2.4 开空条件（entry_short）

需同时满足：

1. `trend_score_p` 连续 2 根 K 线 `<= -50`
2. `score_ema < 0`
3. `score_dmi_adx < 0`
4. `close < ema_12 < ema_26`
5. `adx_14 >= 20`
6. `score_rsi <= 0`
7. `score_flow <= 5`
8. `score_funding <= 0`

### 2.5 平多条件（exit_long）

持有多头时，命中任意一条即输出 `flat`（平仓意图）：

1. `trend_score_p < 20`
2. `score_ema <= 0`
3. `close < ema_12`
4. 趋势分连续 3 根递减（`h[-1] < h[-2] < h[-3]`）

### 2.6 平空条件（exit_short）

持有空头时，命中任意一条即输出 `flat`：

1. `trend_score_p > -20`
2. `score_ema >= 0`
3. `close > ema_12`
4. 趋势分连续 3 根递增（`h[-1] > h[-2] > h[-3]`）

## 3. 风控层（信号如何被批准）

风控按“当前仓位方向 + 新信号方向”做状态化审批。

### 3.1 审批动作

1. `approve`：允许直接执行（开仓或平仓）。
2. `reduce_only`：只允许先平掉反向仓（例如有空仓却收到 `long`）。
3. `reject`：拒绝执行。

### 3.2 数量决定

1. 默认开仓数量：`RISK_DEFAULT_OPEN_QUANTITY`（默认 1.0）。
2. 若设置了 `RISK_DEFAULT_OPEN_NOTIONAL > 0`，优先按金额换算：`quantity = notional / price`。
3. `price` 从信号 metadata 里按顺序读取：`price` -> `close` -> `mark_price` -> `reference_price`。
4. 若价格缺失或非法，回退默认数量。

### 3.3 方向审批语义（核心）

1. `flat -> long/short`：通常 `approve`。
2. `long -> long` 或 `short -> short`：`reject`（避免重复开同向）。
3. `short -> long`：`reduce_only`（先平空，不直接反手开多）。
4. `long -> short`：`reduce_only`（先平多，不直接反手开空）。
5. 收到 `flat` 信号且当前有仓：`approve` 平仓；若本来就空仓则 `reject`。

## 4. 仓位状态机（真正的开平仓动作）

### 4.1 信号到动作

在仓位稳定态下的动作如下：

1. `flat + long` -> `open_long`，发 `BUY`（开多）
2. `flat + short` -> `open_short`，发 `SELL`（开空）
3. `long + flat/short` -> `close_long`，发 `SELL`（平多）
4. `short + flat/long` -> `close_short`，发 `BUY`（平空）

说明：`short` 信号在 `long` 状态下并不直接“平多+开空”一次完成，而是先走平多阶段；后续是否开空取决于下一轮信号与审批。

### 4.2 订单回报推进

1. `NEW`：从意图态进入进行态（如 `open_long -> opening_long`）。
2. `PARTIALLY_FILLED`：
   - 开仓进行态：仓位数量递增。
   - 平仓进行态：仓位数量递减。
   - 使用累计成交量做增量，避免重复累计。
3. `FILLED`：
   - 开仓全成：进入 `long/short` 稳定持仓态。
   - 平仓全成：进入 `flat`。
4. `CANCELED/REJECTED`：触发回滚。

### 4.3 回滚语义

1. 开仓链路（`open_* / opening_*`）失败：回到 `flat`。
2. 平仓链路（`close_* / closing_*`）失败：回到原持仓（`long` 或 `short`），保留剩余数量。

## 5. 当前“反手”行为结论

当前实现不是“一笔反手”（不是同一时刻直接从 long 到 short 或从 short 到 long），而是“两步反手”模型：

1. 第一步：风控输出 `reduce_only`，仓位先平掉反向仓。
2. 第二步：后续新的方向信号再次触发，才可能进入新方向开仓。

这意味着策略信号节奏、风控快照更新以及订单回报时序，会影响“反手速度”。

## 6. 与你关注的“开仓/平仓”直接相关的配置项

1. `STRATEGY_MIN_CONFIDENCE`（默认 0.55）：越高越保守。
2. `STRATEGY_MAX_DATA_AGE_SECONDS`（默认 2）：越小越严格，可能减少信号。
3. `RISK_DEFAULT_OPEN_QUANTITY`（默认 1.0）：默认下单数量。
4. `RISK_DEFAULT_OPEN_NOTIONAL`（默认 0）：大于 0 时按金额定仓。
5. `RISK_REQUIRE_POSITION_SNAPSHOT`（默认 false）：true 时缺少仓位快照会直接拒绝信号。

## 7. 结论（当前策略画像）

1. 入场偏趋势确认，要求双 K 线持续与多因子同向共振。
2. 出场偏保护性，任一关键弱化信号触发即倾向先平仓。
3. 风控把反手拆成“先平后开”，降低直接反向开错仓风险。
4. 仓位状态机是事件驱动，依赖订单回报推进，支持部分成交与失败回滚。

如果你希望，我可以再补一版“参数调优建议”（激进/均衡/保守三档），直接映射到以上配置项。