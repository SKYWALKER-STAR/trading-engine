# 执行仓位成本与百分比平仓（第一版）

交易链路根据实际成交回报维护持仓均价。启用百分比规则后，已有仓位由该规则管理退出，原因子策略只负责空仓入场。默认关闭，不自动改变部署配置。本版不增加加仓动作。

## 启用方式

在 **strategy 进程**的环境中配置，例如：

```bash
export STRATEGY_TAKE_PROFIT_PCT=5
export STRATEGY_STOP_LOSS_PCT=2
```

以上数字只演示配置：`5` 表示价格收益率达到 **+5%**，`2` 表示达到 **-2%**。阈值取正数，支持小数；0、负数、NaN、Infinity 或无法解析的值启动时报错。可以仅配置一个阈值；两个变量都不设置/为空时禁用。

PowerShell 对应写法：

```powershell
$env:STRATEGY_TAKE_PROFIT_PCT = '5'
$env:STRATEGY_STOP_LOSS_PCT = '2'
```

启动仍使用现有 `strategy` 命令及轮询参数。启用时 strategy 必须连接 **执行仓位 Redis**，与 position 使用相同的 `POSITION_REDIS_URL`、`POSITION_EXECUTION_KEY_PREFIX` 和 `ORDER_ACCOUNT_ID`。启动检查 initialized 标志；读取失败不当作空仓继续交易。没有某币种记录时，在已初始化库内按无仓位处理。该标志不代替交易所持仓核对。

## 触发口径

参考价使用当前因子快照的 `close`，不是标记价格，也不是独立实时 tick。执行频率、覆盖币种、行情时间及延迟沿用现有 strategy 轮询链路；它不是挂在交易所的止损单，程序停止或没有新鲜行情时不会触发。

```text
多仓价格收益率 = (close - entry_avg_price) / entry_avg_price × 100
空仓价格收益率 = (entry_avg_price - close) / entry_avg_price × 100
```

达到阈值（含相等）发送 FLAT 信号，经现有 risk → position → trade → worker 链路平仓。不计算杠杆 ROI、手续费、资金费或已实现盈亏。

- 持仓期间不执行原因子策略的反向/平仓信号；未达阈值保持持仓。
- 百分比退出绕过入场置信度过滤，仍检查行情新鲜度并拒绝未来时间。
- 过渡状态不再创建退出动作；成本未知、非法价格/数量时拒绝价格规则并记录拒绝原因。
- 信号携带仓位开仓时间、均价及最近订单身份；position 再次比对，避免排队的旧持仓退出信号作用于新持仓。
- 仓位引擎以当前执行仓位数量为平仓上限，并保留风控批准的更小数量，表达 reduceOnly。网关在 BOTH 下发送该参数；LONG/SHORT 持仓腿保留 positionSide，不发送不适用于 Hedge Mode 的 reduceOnly。本系统完整双向仓位建模仍不在本版范围内。

行情新鲜度仍沿用因子 `open_time` 与 `STRATEGY_MAX_DATA_AGE_SECONDS`。应先确认数据源时间契约；一分钟 K 线的开盘时间不能直接等同于实时行情更新时间。本版未重构行情源或多币种调度。

## 数据流与存储

```text
用户流成交 / 下单响应 / order.status 查询
    → OrderUpdatePayload（累计数量、累计成交金额）
    → TradeWorkflow：保存订单累计事实
    → PositionWorkflow：PositionManager 应用成交并事务保存仓位、inbox、outbox
    → strategy 读取执行仓位，计算百分比退出
```

位置和交易消费者都消费订单事件；不是先写完订单库再同步写仓位库。`redis_workflow.py` 保持通用事务职责，不包含价格计算。

| 位置 | 新字段 | 含义 |
|---|---|---|
| OrderUpdatePayload / PositionOrderEvent / TradeExecutionResult | `cumulative_filled_quote` | 订单累计成交金额，十进制字符串或 null |
| 同上 | `last_filled_price` | 最近一笔成交价，十进制字符串或 null |
| `trade:execution:{account}:state:{client_id}` | `cumulative_filled_quote` | 与该记录累计成交数量对应的金额；数量增加但金额缺失时置 null |
| `position:execution:{account}:state:{symbol}` | `entry_avg_price` | 当前持仓加权开仓均价，十进制字符串或 null |
| 同上 | `cost_complete` | 是否具备完整持仓成本；默认 false |
| 同上 | `opened_at` | 本轮持仓从零变为非零时，首次处理成交事件的时间；查询恢复时不是精确历史首笔成交时间 |

仓位状态事件也携带以上三个仓位字段。执行快照示例：

```json
{
  "symbol": "BTCUSDT",
  "direction": "long",
  "lifecycle": "long",
  "quantity": 2.0,
  "entry_avg_price": "105",
  "cost_complete": true,
  "opened_at": "2026-09-23T00:00:00+00:00"
}
```

价格、金额和均价运算使用 Decimal，持久化为字符串；既有数量契约仍为 float，本版没有将全仓库数量类型迁移到 Decimal。不会从委托限价、信号价格或当前市价推导开仓成本，也不读取 actual/view 同步链路补造成本。

用户流以累计数量 `z` 与订单均价 `ap` 计算累计金额，保留 `L` 为最近成交价；查询/下单回复优先取 `cumQuote`，缺失时用 `executedQty × avgPrice`。缺失或为零的均价不能用于已成交仓位。字段与请求参数参照 [Binance 用户流文档](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/user-data-streams) 和 [WebSocket 交易 API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-api/trade)。累计均价可能已由交易所舍入，Decimal 不会恢复源数据未提供的精度。

## 成交、撤单与恢复

同一订单累计成交数量从 1 增至 2、金额从 100 增至 210，新增成交是 1、金额 110，持仓均价变为 105。每个活动订单记录已应用数量、金额和订单开始前的持仓成本；累计数量不变不重复增加仓位，累计数量回退的回报被忽略。

开仓部分成交后撤单/拒绝，先应用新增累计成交，再保留已成交仓位。部分减仓保持剩余持仓均价；减仓订单 FILLED 不再自动将全部仓位置零。全部平仓才清空均价、成本标志和 opened_at。进程重启和事务重试使用持久化的成交进度，状态和输出一并提交。

尚在活动状态的订单，先收到缺价格成交、后收到完整累计金额，可以补齐该订单成本。最近一笔成交价只在其数量恰好覆盖新增成交且之前累计成本完整时用于补齐，不能代表跳过多笔成交的累计均价。只有数量增量、没有累计数量的事件不进入自动记账。

## 旧数据与上线边界

新字段均有缺省值，旧事件和旧仓位仍可读取；旧仓位默认 `entry_avg_price=null`、`cost_complete=false`，不会自动回填。旧未知持仓后来新增成交，也不能凭新成交恢复原仓位成本。

本版不提供历史成本回填工具或终态订单的自动成本修补。终态后重复回报继续按原工作流忽略；若终态成交缺少价格，仓位保持成本未知，需要独立核对。启用后，这类仓位不会由价格规则自动退出，也不会退回原因子退出逻辑。

部署时先停止相关生产者/消费者并完成数据核对，再统一升级 user-stream、position、trade、worker、outbox、risk、strategy，避免旧版消费者读写新事件时丢失成本字段。不要把有成本的执行状态交给旧版本继续改写。价格规则保持关闭，确认新成交已产生完整成本后再配置阈值。

本版仍依赖实际执行数量与交易所一致；外部手动交易、账户全面对账和独立成交 ID 账本属于后续工作。没有进行真实下单或线上迁移。

## 离线验证

新增测试位于 `tests/unit/workflow/test_execution_cost.py`，覆盖部分成交、撤单补量、部分平仓、成本缺失与补齐、重复/乱序、事务失败与重启、查询成本、序列化、最终平仓参数、多空阈值、置信度与新鲜度、旧持仓信号隔离。

```powershell
$env:PYTHONPATH = 'src'
.\.venv\Scripts\python.exe -B -m unittest discover -s tests/unit/workflow -v
```

这些用例不连接外部服务；真实 Kafka/Redis 故障切换和 Binance 联调仍需单独完成。
