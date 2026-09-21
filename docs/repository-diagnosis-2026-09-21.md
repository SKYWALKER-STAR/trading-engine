**Trading Engine 全仓库代码诊断报告**

诊断日期：2026-09-21。基线：`f015241`，诊断开始时工作区干净。范围为 `trading-engine` 仓库，不包括相邻的 `emp` 仓库或仓库外的持仓采集服务。

**总体判断**

项目已有清晰的事件驱动架构和可替换的基础设施接口，具备继续迭代的良好基础。当前主要短板是交易执行、订单事件、仓位状态和真实持仓之间的一致性，以及异常后的恢复闭环。建议优先修复可能导致错仓、重复执行、丢失交易动作的问题，再优化吞吐和部署方式。

本报告不评估策略收益率，也不将代码层面的策略规则等同于经过回测验证的交易优势。对生产可靠性的判断来自下面列出的实际代码路径和离线复现结果。

**扫描范围与验证方式**

| 项目 | 结果 |
|---|---|
| Python 源文件 | 54 个，5,949 行，包含包初始化文件与内嵌网页 |
| Python 测试文件 | 17 个 |
| 测试函数定义 | 65 个；不等于参数化展开后的用例数，也不代表已通过数 |
| 语法检查 | 对全部 71 个源代码及测试文件进行 AST 解析，无语法错误 |
| 人工代码审阅 | 核心交易链路、策略、风控、存储、事件契约、调试界面、启动脚本、依赖与现有测试断言 |
| 运行验证 | 使用本机 Python 3.13.14 执行标准库离线复现脚本；外部接口均未连接 |
| 未执行 | 完整 pytest、ruff、mypy、覆盖率、压力测试及 Kafka/Redis/ClickHouse/Binance 集成测试 |

Python 位于 `D:/Data/software/Python3.13.14/python.exe`，未加入 PATH；该解释器未安装 pytest、ruff、mypy 等开发依赖。未为了诊断修改全局 Python 环境。语法检查和离线验证不能替代完整测试，也未验证最低声明版本 Python 3.11 的兼容性。

可复查的产物：[离线复现脚本](diagnostics/repository_probe.py)、[本次运行结果](diagnostics/probe-results.json)。脚本使用模拟存储与模拟下单传输，不读取真实交易凭据，不发送订单。

**值得保留的设计**

| 优点 | 代码证据 | 实际价值 |
|---|---|---|
| 业务模块边界较明确 | `strategy/`、`position/`、`trade/`、`contracts/`、`infra/`、`app/` | 适合逐步修复，无需推倒重写 |
| 网关、仓库与事件总线使用 Protocol | [trade/repository.py](../src/trading_engine/trade/repository.py)、[trade/gateway.py](../src/trading_engine/trade/gateway.py)、[bus/base.py](../src/trading_engine/infra/bus/base.py) | 可注入假实现，替换存储或交易渠道时影响范围可控 |
| 大部分业务模型使用 frozen dataclass 与枚举 | [position/models.py](../src/trading_engine/position/models.py)、[contracts/messages.py](../src/trading_engine/contracts/messages.py) | 状态转换显式，减少原地修改；但内部 metadata 字典仍可变 |
| 事件有版本、关联及因果标识 | [contracts/messages.py](../src/trading_engine/contracts/messages.py#L166) | 为追踪、重放和版本演进提供基础 |
| 下单前记录 PENDING_SUBMIT，使用确定性客户端订单 ID | [trade_engine_kafka.py](../src/trading_engine/app/trade_engine_kafka.py#L101) | 已具备跨进程恢复和防止重复提交所需的重要基础 |
| Redis 订单 ID 绑定使用 WATCH/MULTI 检查冲突 | [redis_order_repository.py](../src/trading_engine/infra/redis_order_repository.py#L85) | 比简单覆盖索引可靠，说明已关注订单身份一致性 |
| 当前仓位实现已引入累计成交差值和活动订单校验 | [manager.py](../src/trading_engine/position/manager.py#L255)、[manager.py](../src/trading_engine/position/manager.py#L413) | 普通部分成交更新及累计值重放已有保护，不应再认定为完全未实现 |
| Binance 用户流有续期、重连、任务取消与清理 | [binance_futures_user_data_stream.py](../src/trading_engine/infra/binance_futures_user_data_stream.py#L119) | 连接生命周期管理有基础；下一步应补断线后的业务补偿 |
| 网关已修复交易对匹配与 Decimal 序列化 | [binance_futures_ws_gateway.py](../src/trading_engine/infra/binance_futures_ws_gateway.py#L177) | 离线验证得到数量 `0.123`、价格 `65000.1`，不再使用首个交易对规则或暴露浮点尾差 |
| 有单元测试、CLI、文档和本地调试视图 | `tests/unit/`、[engine_registry.py](../src/trading_engine/app/engine_registry.py)、`docs/` | 维护和排障入口齐全，适合建立持续验证机制 |

**问题分级**

P0 表示可能造成错误持仓或交易动作丢失，应先修复；P1 表示高影响的业务正确性、恢复或凭据问题；P2 表示工程可靠性、可维护性或性能问题。共整理 22 组问题：P0 四组、P1 八组、P2 十组。分级表达修复顺序，不代表这些故障已经在你的实盘账户发生。

**P0：优先修复的四组问题**

**F01．仓位状态机与投影器写入同一个状态键。已离线确认。**

证据：[RedisPositionRepository.save](../src/trading_engine/infra/redis_position_repository.py#L59) 与 [RedisPositionViewProjector](../src/trading_engine/infra/redis_position_view_projector.py#L135)。默认配置下，两者都写 `binance:position:usdt_futures:view:state:{symbol}:v1`。仓位引擎保存 `opening_long`、活动订单和已应用成交量；投影器把这些内容替换为 `long/short/flat`，同时将 `active_order_id` 置空。

触发方式：订单尚未完成时执行一次投影，随后成交回报找不到活动订单，或下一次信号把真实存在的执行过程视为稳定仓位。`start.sh all` 默认同时启动这两个写入者，因此这不是只有特殊部署才会出现的设计冲突。

建议：拆分真实持仓快照、订单执行状态、策略期望状态。投影器只更新 `actual` 快照，PositionManager 独占执行状态；由显式对账流程比较并协调两者。为业务状态增加版本号和条件写入，避免多实例读改写覆盖。

验收：`OPENING_LONG` 状态下更新真实持仓快照，活动订单 ID、已处理成交量和执行阶段均保留。

**F02．终态处理不保留部分成交的经济事实。已离线复现。**

证据：[manager.py 的撤单分支](../src/trading_engine/position/manager.py#L231) 与 [_rollback](../src/trading_engine/position/manager.py#L436)。开仓部分成交 `0.4` 后撤单，状态从 `opening_long / 0.4` 变为 `flat / 0.0`，但撤单并不会撤销已经发生的成交。

此外，撤单事件里的最新累计成交量未应用；如果最后一笔成交仅体现在撤单回报中，会遗漏它。平仓 `FILLED` 分支直接清空整个仓位，也没有判断本次订单量是否等于实际持仓，部分减仓或数量截断后的残余可能被清零。

建议：所有携带累计成交量的事件先统一计算并应用增量，再处理订单终态。开仓撤单后若持仓仍大于零，应回到稳定的 long/short；平仓按实际减仓量计算剩余持仓。将订单终态与仓位是否为空分开建模。

验收：开仓部分成交后撤单、撤单回报补充成交、部分减仓订单全成、平仓后残余量均有明确预期。

**F03．最终下单参数丢失平仓保护，且持仓方向被强制覆盖。已离线复现。**

证据：[binance_futures_ws_gateway.py](../src/trading_engine/infra/binance_futures_ws_gateway.py#L84)。传入 `positionSide=SHORT` 后实际发送 `BOTH`；`reduceOnly` 发送代码被注释，即使上游传了 true，最终请求也没有这个字段。

影响：双向持仓账户可能拒单；单向模式下，如果实际仓位与本地数量不一致，原本用于平仓的订单可能建立反向仓位。另一个上游缺口是风控的 FLAT 平仓分支返回 APPROVE，后续会导出 `reduceOnly=false`，所以仅取消网关注释仍不完整。

建议：按账户模式显式构建订单，所有 close 动作都表达减仓意图。单向模式使用适用的 reduceOnly；双向模式使用正确的持仓腿和减仓数量，不能机械地发送该字段。Binance 明确区分 BOTH 与 LONG/SHORT，并规定 Hedge Mode 不能发送 reduceOnly。[官方订单参数](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-api/trade#new-order-trade)

验收：比较最终网络请求，而不只检查上游 metadata；覆盖单向平多/平空、双向各持仓腿及数量变化。

**F04．状态持久化、消息发布和消费确认没有形成可恢复事务。已复现其中的丢动作路径。**

证据：[manager.py](../src/trading_engine/position/manager.py#L521)、[kafka_event_bus.py](../src/trading_engine/infra/kafka_event_bus.py#L88)、[trade_engine_kafka.py](../src/trading_engine/app/trade_engine_kafka.py#L101)。仓位先保存，再记录调试历史、发布状态、发布交易动作。离线模拟发布失败后，仓位已是 `open_long`；重放同一信号不会重新生成交易动作。

交易引擎也先保存订单结果再发事件，之后相同 client ID 会被直接跳过。如果首次发布失败，重放不负责补发。Kafka 开启自动提交，业务代码没有显式把 offset 确认与这些副作用关联；切成手动提交仍不能单独解决上述跨存储发布缺口。Kafka 官方将自动提交定义为定期提交 offset 的机制，而不是应用状态与消息发布的事务。[消费者配置](https://kafka.apache.org/41/configuration/consumer-configs/#enable.auto.commit)

建议：引入持久化 inbox/outbox。把状态变化与待发事件一起原子保存，独立发布器可靠重试；消费成功的定义应是副作用已持久化且可恢复。调试记录不能成为交易动作发布的必经成功条件。

验收：在每个“保存/发布/确认”边界模拟崩溃，重启后交易动作不丢失，重复投递不重复执行。

**P1：业务正确性与恢复闭环**

**F05．订单身份校验有进步，但重复和乱序回报仍会使消费者退出。已离线复现。**

证据：[manager.py](../src/trading_engine/position/manager.py#L85)、[_validate_order_id_before_position_change](../src/trading_engine/position/manager.py#L255)、[Kafka 分发循环](../src/trading_engine/infra/kafka_event_bus.py#L88)。FILLED 先于 NEW 到达时没有活动订单可匹配，抛 ValueError；已经处理完的同一 FILLED 再次到达也抛 ValueError。分发循环没有逐条异常隔离，因此异常会退出消费循环。

系统有交易响应和用户流两个回报生产者，不能依赖二者的全局到达顺序。NEW 没有身份校验；无活动 order ID 时撤单/拒单直接放行，也不能证明该事件属于当前待提交动作。外部手工订单回报同样可能进入这条消费链路。

建议：动作创建时就保存稳定的客户端订单身份；根据 client ID 绑定初次交易所 ID。维护每笔订单终态和累计成交量，重复终态无副作用，旧订单/外部订单分类处理，不用抛异常代替去重。区分可重试错误、无效事件及应报警的身份冲突。

**F06．UNKNOWN、断线与超时缺少查询补偿；本地超时回滚不能代表交易所失败。静态确认。**

证据：[交易提交异常分支](../src/trading_engine/app/trade_engine_kafka.py#L125)、[recover_stale_transition](../src/trading_engine/position/manager.py#L289)、[WS 重试](../src/trading_engine/infra/binance_futures_ws_gateway.py#L33)、[用户流重连](../src/trading_engine/infra/binance_futures_user_data_stream.py#L119)。异常后保存 UNKNOWN 并返回，没有运行中的对账任务消费它；仓位等待超时后直接回滚，旧订单可能仍然活动或已经成交。超时检查还依赖下一条风控事件，没有新事件就不触发。

WS 传输对某些发送后超时会重新发送原下单请求。确定性 client ID 有帮助，但不能当作永久幂等承诺：官方要求的是未完成订单之间唯一。不能据此证明在首次订单已终结、请求仍有效等条件下重复执行绝无可能。[官方客户端订单 ID 约束](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-api/trade#new-order-trade)

建议：增加定时订单对账，覆盖启动、UNKNOWN、重连及执行超时。先按客户端订单 ID 查询订单和成交，再决定重试、继续等待或恢复状态；将“结果未知”与“已明确失败”区分。用户流重连补齐缺口，而不只是重新建立连接。

**F07．风控启动时没有权威仓位，快照也不检查新鲜度。静态确认。**

证据：[risk_engine_kafka.py](../src/trading_engine/app/risk_engine_kafka.py#L39)、[settings.py](../src/trading_engine/config/settings.py#L56)。风控只在内存保存事件快照，默认快照缺失视为空仓，重启不会从 Redis 或交易所加载初始状态，也不比较版本和时间。仅开启 require_position_snapshot 可能让已有持仓长期等待新事件，不能替代初始化流程。

开仓数量默认 1；配置按金额定仓但价格缺失时，也回退到默认数量。针对不同价格的币种，这个回退可能与预期金额相差很大。当前风控主要保护方向和生命周期，尚未纳入账户可用保证金、整体敞口、订单金额上限等输入。

建议：启动加载完整、带时间和来源的实际持仓；完成初始化再增加敞口。价格无效时拒绝按金额开仓，不回退到固定币数。先补单笔金额、单币种/账户敞口、数据过期约束，再逐步扩展风控。

**F08．投影器的数量与新鲜度语义不符合业务仓位模型。已部分离线确认。**

证据：[redis_position_view_projector.py](../src/trading_engine/infra/redis_position_view_projector.py#L36)、[_build_symbol_state](../src/trading_engine/infra/redis_position_view_projector.py#L135)。空头 `positionAmt=-2` 被直接写为 quantity=-2，而风控和 PositionManager 多处将 quantity 当作非负持仓规模。这会影响平仓定量和减仓计算。双向模式只选择绝对值较大的一条腿，另一条腿没有进入汇总业务状态。

相同原始快照的 hash 因 `updated_at=projected_at` 每次改变，导致“未变化跳过写入”失效；旧原始数据也被赋予新的业务更新时间。完整快照删除某个币种或返回空集合时，没有清理旧视图的明确流程。

建议：明确单向数量为绝对值、方向独立表达；双向以 account/symbol/positionSide 为键。分离 source_updated_at 与 projected_at，内容 hash 不包含轮询时间。引入快照完整性/批次标识，只有确认完整快照后才处理缺失币种。

**F09．连续 K 线判断实际按轮询次数累计，多币种与周期选择不明确。已复现重复 K 线入场。**

证据：[factor_score.py](../src/trading_engine/strategy/factor_score.py#L26)、[策略轮询](../src/trading_engine/app/run_strategy_engine_factor.py#L178)、[ClickHouse 查询](../src/trading_engine/infra/clickhouse_source.py#L48)。每次 generate 都 append 趋势分数，未检查 open_time；同一快照调用两次，结果从 FLAT 变 LONG，所谓“两根强势 K 线”可在两次轮询后满足。

未指定 symbol 时查询全表最新一行，不是遍历所有币种；查询没有 interval 条件，如果数据源包含多周期，会把不同周期混在同一 symbol 历史中。最新行也未明确要求已收盘。

建议：按 `(symbol, interval, open_time)` 去重，只让新的有效 K 线推进窗口；为周期和收盘状态建立契约。显式配置交易币种集合，按币种查询/批量读取最近 N 根，以支持冷启动恢复和公平调度。

**F10．入场过滤器会阻断退出信号，策略内部持仓与成交脱节。已离线复现前者。**

证据：[StrategyEngine.evaluate](../src/trading_engine/strategy/engine.py#L25)、[ConfidenceFeatureRule](../src/trading_engine/strategy/rules.py#L40)、[factor_score.py](../src/trading_engine/strategy/factor_score.py#L67)。规则先于算法执行；趋势衰退到应该退出时，置信度也降低，导致 confidence_too_low 直接拒绝，本应生成的平仓信号未产生。

策略在生成信号时就更新 `_position_by_symbol`，没有等待风险审批或成交反馈。拒单、发布失败、平仓失败及进程重启都可能让这个内存方向与实际持仓不一致。

建议：将入场条件与持仓退出条件分开；退出不能机械受入场置信度限制。策略方向若表示“目标仓位”应明确命名和语义；真实持仓上下文从执行反馈/权威快照获得，历史通过最近有效 K 线重建。

**F11．源码存在非空默认密码，订单 INFO 日志包含认证材料。静态确认。**

证据：[clickhouse_source.py:42](../src/trading_engine/infra/clickhouse_source.py#L42)、[网关日志](../src/trading_engine/infra/binance_futures_ws_gateway.py#L118)。AST 检查确认 CLICKHOUSE_PASSWORD 使用了非空字符串默认值；报告不复制该值，也未验证其是否有效。下单日志输出整个 message，包括 apiKey 和 signature；这不等于输出了 api_secret，但仍是不必要的认证材料暴露。

建议：删除源码密码默认值，改为缺失配置时报错；若该密码曾被使用，应轮换并按实际暴露范围处理历史记录。订单日志采用字段白名单，只记录订单 ID、币种、方向、数量、价格、错误码和必要关联信息。

**F12．start.sh all 没有启动 Binance 用户数据流。静态确认。**

证据：[start.sh](../start.sh) 与 [engine_registry.py](../src/trading_engine/app/engine_registry.py)。CLI 注册了 binance-user-stream，但 start/stop 脚本的分支及 all 清单没有它。按 README 使用 all 启动时，默认 ACK 下单响应之后的成交更新可能没有采集进程负责发布，进而导致仓位卡在过渡状态或触发错误超时恢复。

建议：统一进程清单；完整运行模式应包含用户流并检查 readiness。文档明确仅下单响应不能替代持续成交回报。部署中有仓库外 supervisor 单独启动用户流时需另行核对，不能仅凭仓库推断线上一定缺失。

**P2：工程可靠性、可维护性与性能**

| ID | 问题与证据 | 优化意见 |
|---|---|---|
| F13 | 精度修复后的边界仍不完整：[网关归一化](../src/trading_engine/infra/binance_futures_ws_gateway.py#L177) 在规则缺失或取整为零时返回原值；失败的 None 也被 lru_cache 保存。没有 MARKET_LOT_SIZE、数量范围和名义金额的完整校验。离线确认 `0.0004` 在步长 `0.001` 时仍被返回。 | 规则失败明确拒绝或暂停提交，成功缓存增加有效期；本地检验适用过滤器。保留此次完成的 Decimal 与 symbol 匹配修复。 |
| F14 | [网关 timestamp](../src/trading_engine/infra/binance_futures_ws_gateway.py#L69) 在同步读取规则之前生成，规则读取最多等待十秒而默认 recvWindow 五秒；网络重试复用原时间戳。[_build_gateway](../src/trading_engine/app/run_trade_engine.py#L52) 也未传递 REST URL，切换 WS 环境不一定切换规则来源。 | 在规则获取后、发送前签名；重试与不确定结果处理配合，避免盲目重发。配置统一的交易环境及 REST/WS URL，并校验组合。 |
| F15 | [用户流处理器](../src/trading_engine/app/binance_user_data_stream.py#L31) 只发布 Kafka，不更新 TrackedOrder；当前订单库主要记录下单响应，ACK 后可能长期停留 NEW。get-then-save 去重不是原子认领，save 也没有状态版本保护。 | 建立统一订单事件归并器，负责身份、累计成交、状态单调性与 active 索引；用原子 create-if-absent/唯一约束认领动作，明确 UNKNOWN 的恢复职责。 |
| F16 | [日志配置](../src/trading_engine/common/logger.py#L9) 要求每条记录有 direction/reasons 或 action 等 extra，但启动和超时日志没有提供。离线 Formatter 调用已复现缺字段异常；常规 logging handler 通常丢失该条日志并报告 logging error，不应说成必然使业务崩溃。多个 FileHandler/多个进程写同一文件且没有轮转；默认格式也丢弃大量 extra。 | 统一结构化日志或设置缺省字段；增加关联 ID、消费分区/offset、对账状态和脱敏。采用集中收集或明确轮转策略，并监控 UNKNOWN 数量、消费延迟和状态差异。 |
| F17 | [serde.py](../src/trading_engine/contracts/serde.py#L25) 读取 schema_version 但不检查支持范围，数值转换不检查有限值、正值或领域一致性；`float('nan')` 可绕过 `quantity <= 0`。[_to_trade_order_request](../src/trading_engine/app/trade_engine_kafka.py#L329) 标注非空但返回 None；StrategyEngine 标注只接受 StrategyContext，却实际接收 FactorStrategyContext；metadata 声明标量但保存嵌套响应。 | 对输入使用明确 schema/校验器，统一 UTC、有穷 Decimal 和枚举；检查 signal.symbol、risk.symbol、side/action 的一致性。修正类型契约后开启类型门禁，不靠 Any 或注释吞掉不一致。 |
| F18 | 测试门禁与当前行为不一致：[网关测试](../tests/unit/infra/test_binance_futures_ws_gateway.py#L174) 期望 SHORT/reduceOnly，但实现覆盖/忽略；[提交异常测试](../tests/unit/app/test_trade_engine_kafka.py#L269) 期望抛 TimeoutError，而实现保存 UNKNOWN 后返回；[LIMIT 测试](../tests/unit/app/test_trade_engine_kafka.py#L358) 期望 ValueError，而实现发布拒绝事件。以上是静态断言冲突，并非本次 pytest 实测失败计数。部分“单元”网关测试只替换 WS，会真实请求 exchangeInfo。 | 先确定预期业务行为再统一代码和测试。单元测试统一禁止外部网络，实盘测试移到独立目录/标记。加入 CI，先运行 pytest、ruff、mypy，再加集成与故障注入测试。 |
| F19 | 性能路径有重复工作：[网关](../src/trading_engine/infra/binance_futures_ws_gateway.py#L33) 每单新建连接；同币种数量/价格规则分别读取全市场 exchangeInfo；[用户流 dispatch](../src/trading_engine/infra/binance_futures_user_data_stream.py#L147) 在事件循环同步执行等待 Kafka future 的 handler；投影逐币种 hget，订单 list_active 逐单 get。 | 正确性修复后测量 P95/P99。考虑持久 WS 与请求 ID 分发、整份规则快照缓存、批量 Redis 读取；用户流采用有界队列和有序发布任务，配合持久缓冲/对账，避免阻塞和无界积压。 |
| F20 | 关联追踪与层次隔离不完整：[PositionKafkaEventBus](../src/trading_engine/app/position_engine_kafka.py#L50) 重建事件时不传入原 correlation/causation；PositionManager 直接依赖具体 DebugStore，而且记录调试历史失败会阻断后续业务发布。 | 将输入关联上下文传入领域输出/发布器；调试使用可选观察者或独立订阅者。核心业务仍保持 Protocol，但不为抽象而增加层数。 |
| F21 | 交付可重复性不足：requirements 与 pyproject 的 websockets 下限不同，未发现依赖锁文件或 CI 工作流。start.sh 依赖固定代理脚本，没有重复启动防护；stop.sh 五秒后强杀。轮询间隔和超时配置缺少正值校验，Kafka/Redis 连接参数缺少统一运维配置入口。 | 统一依赖来源并锁定经验证版本；用 supervisor/systemd/容器编排管理进程、健康和优雅退出；使代理可选。启动时集中校验配置。先完善现有部署方式，无需为了“微服务化”立刻引入复杂平台。 |
| F22 | [调试网页](../src/trading_engine/debug/server.py#L498) 把 symbol、reason、Redis key 等动态字符串插入 innerHTML；若上游可写入恶意文本或用户粘贴特殊输入，存在 DOM 注入面。页面缺少认证但默认仅监听 localhost，因此不应直接定性为公网暴露。一个约 700 行 Python 文件包含服务、JS 和 CSS。 | 动态文本使用 textContent/安全 DOM 构建；确需远程访问时由受控入口提供认证。分离静态资源，增加错误提示及请求超时。无需改变默认本地调试定位。 |

补充：交易相关过滤器的适用范围与参数含义以 [Binance 官方过滤器定义](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/common-definition) 为依据；上表 F13 的工程结论来自本仓库代码。

**跨模块的扩展约束**

当前 PositionState、风控缓存和事件路由主要按 symbol 标识，而订单库已经包含 exchange/account。若未来多个账户、策略或双向持仓共享相同 topic/key，仅有订单库的 account_id 不足以隔离仓位。建议先明确“单账户、单向、每币种一个持仓”的支持边界；需要扩展时统一业务主键，而不是局部追加字段。

多个消费者实例订阅不同 topic 时，同一个 symbol 的风险消息、仓位消息、订单消息不天然由同一个实例处理。未来横向扩展前，需要明确分区共置、状态恢复和独占写入策略。当前代码里的本地字典和无版本仓位写入不足以支持直接增加副本。

**分阶段优化路线**

| 阶段 | 工作内容 | 完成标准 |
|---|---|---|
| 第一阶段：纠正交易与仓位语义 | F01–F03、F05；同时解决 F11、F12 的局部配置/日志问题 | 已成交持仓不会被撤单清零；投影不覆盖订单状态；平仓保护正确；重复回报不终止消费者 |
| 第二阶段：建立可恢复执行链路 | F04、F06、F07、F15；订单身份与待发事件在持久化层落实 | 任意保存/发布边界崩溃后可恢复；UNKNOWN 可对账；初始化前不盲目增加敞口 |
| 第三阶段：纠正策略输入与状态 | F08–F10、F13、F14、F17 | 同一 K 线不重复推进；退出不受入场过滤阻断；快照、数量、时间与订单环境语义一致 |
| 第四阶段：建立交付与运行保障 | F16、F18–F22 | CI 可重复运行；外部网络测试隔离；日志完整脱敏；健康、重启、积压、对账指标可观测 |

建议把每个阶段拆成小改动并带对应的故障复现测试。P0/P1 关闭前，不应把增加策略数量、并发度或交易频率作为主要优化目标。是否更换 Redis 或引入 PostgreSQL，应围绕事务、恢复和审计需求决策；仅换数据库不会自动解决状态与发布的一致性。

**应优先补充的验证场景**

1. 开仓部分成交后撤单，保留已成交仓位；撤单事件能补足最新累计成交。
2. FILLED 先于 NEW、重复 FILLED、旧订单 NEW/REJECTED、外部手工订单更新。
3. 相同累计成交量重放、累计值回退、进程重启后重放；仅提供旧兼容字段时也明确其累计语义。
4. 单向/双向平仓的最终请求，持仓已被外部减仓时不意外反向开仓。
5. Redis 状态写入后 Kafka 发布失败；订单结果保存后发布失败；重放能补发但不重下。
6. 请求发送后断线、结果未知、用户流重连、启动时已有持仓及未完成订单。
7. 投影与仓位状态机同时工作、完整快照删除币种、空快照、旧快照、新增持仓腿。
8. 同一 K 线重复轮询、混合周期、多币种轮询、重启重建历史。
9. 置信度降低时仍可退出；交易拒绝后策略目标与实际仓位可以重新协调。
10. 规则获取失败/过期、数量小于一步、非十进制幂步长、价格较大或极小、不同交易环境。
11. NaN/Infinity、缺字段、错误 schema 版本、未来时间、延迟信号及快照过期。
12. 所有日志调用在 INFO/DEBUG 下均可格式化，认证字段不会落盘；健康检查能识别消费者已退出。

**文档与代码需要同步的地方**

README 仍写着“未按累计成交处理”“不校验活动订单”，但当前代码已部分实现这些保护；另一部分任务清单仍把 TrackedOrder、client ID 绑定列为待做。应改成“已完成基础能力，剩余终态、乱序、跨重启与补偿缺口”。`docs/architecture.md` 的 planned flow 也与当前策略→风控→仓位的实际顺序存在出入。

市场数据新鲜度默认两秒，却针对 factor.open_time 检查。若一根一分钟 K 线用开盘时间表达，则正常分钟内多数时刻或刚收盘数据都可能被判旧；需要确认 ClickHouse 视图是否只含收盘 K 线，以及 open_time 的真实定义。未来时间当前也会通过 freshness 条件。此处应先明确数据契约，再设阈值，不能仅把两秒随意调大。

**本次交付边界**

本次新增诊断报告、离线复现脚本和结果文件，未修改业务代码，未读取实盘订单、部署配置或账户状态。已复现的问题对应确定输入下的代码行为；静态发现描述可到达的代码路径；线上发生频率、外部服务配置、依赖漏洞与策略收益仍需独立验证。
