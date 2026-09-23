**Trading Engine 全仓库代码诊断报告**

初次诊断：2026-09-21，原基线 `f015241`。更新日期：2026-09-23，复核基线 `1a7b7ff`，复核开始时工作区干净。保留原文件名及 F01–F22 编号，便于追踪。范围限于 `trading-engine`，不包含相邻 `emp` 或仓库外持仓采集服务。

**总体判断**

生产仓位和交易入口已切换到 Redis 持久化工作流，具备 inbox/outbox、事务后手动确认、带租约的订单任务和 UNKNOWN 查询恢复；执行状态与实际持仓投影已分离。原报告中“没有可靠发布闭环”“UNKNOWN 无查询补偿”“all 未启动用户流”等结论不再适用于当前生产入口。

当前最高优先级仍是部分成交终态处理（F02）和平仓参数保护（F03）。风控初始化、策略输入与退出逻辑、全账户对账和真实服务故障验证仍有缺口。持久化工作流解决了部分执行可靠性问题，但不会自动纠正仓位业务语义，也不等于生产部署已经验证。

**扫描范围与验证方式**

| 项目 | 2026-09-23 复核结果 |
|---|---|
| Python 源文件 | 59 个，6,757 行，包含初始化文件与内嵌网页 |
| Python 测试文件 | 18 个，92 个测试函数定义；不等于实际展开用例数 |
| 语法检查 | 77 个源码与测试文件 AST 解析通过 |
| 工作流测试 | 27 项标准库 unittest 全部通过 |
| 离线探针 | 重跑原探针，使用模拟存储与模拟下单传输 |
| 人工复核重点 | 相对原基线的生产工厂、仓位/交易工作流、Redis 事务、网关、迁移、启动脚本及文档变更 |
| 未执行 | 完整 pytest、ruff、mypy、覆盖率、压力测试和真实 Redis/Kafka/ClickHouse/Binance 集成测试 |

本次使用 `.venv/Scripts/python.exe`（Python 3.13.14），确认该环境没有 pytest、ruff、mypy，未安装依赖或修改全局环境。未验证最低声明版本 Python 3.11 的兼容性。

复核命令（PowerShell）：

```powershell
$env:PYTHONPATH = 'src'
.\.venv\Scripts\python.exe -B -m unittest discover -s tests/unit/workflow -v
.\.venv\Scripts\python.exe -B docs/diagnostics/repository_probe.py
```

证据：[工作流测试](../tests/unit/workflow/test_durable_workflow.py)、[离线探针](diagnostics/repository_probe.py)。[probe-results.json](diagnostics/probe-results.json) 保留的是 **2026-09-21 历史结果**，本次未覆盖。27 项测试采用内存模拟 Redis 事务接口，不证明真实服务器在断电、内存耗尽或故障切换时的持久性。

**问题状态总览**

沿用原始优先级：P0 为可能造成错仓或动作丢失的问题，P1 为高影响业务、恢复或凭据问题，P2 为工程可靠性与维护问题。原 22 项中，4 项原生产路径缺陷已修复，6 项部分修复，12 项仍待处理；不应再将原始“P0 四组”理解为四组均未修复。分级不代表这些故障已经在实盘发生。

| 编号 | 原级别 | 当前状态 | 结论与代码证据 |
|---|---|---|---|
| F01 | P0 | 已修复原缺陷 | [执行仓位键](../src/trading_engine/infra/redis_position_repository.py) 与实际投影键隔离；工作流通过 WATCH 冲突重算保护写入。仍需实际持仓对账。 |
| F02 | P0 | 待修复 | [PositionManager](../src/trading_engine/position/manager.py) 撤单回滚仍会清空开仓部分成交；平仓 FILLED 仍可能清空残余仓位。 |
| F03 | P0 | 待修复 | [网关](../src/trading_engine/infra/binance_futures_ws_gateway.py) 仍强制 BOTH，未发送 reduceOnly；上游 close 意图也需统一。 |
| F04 | P0 | 已修复原生产路径 | [RedisWorkflowStore / OutboxRelay](../src/trading_engine/infra/redis_workflow.py) 提供状态、回执、输出和任务的事务提交及发布重试；真实服务验证待补。 |
| F05 | P1 | 部分修复 | [PositionWorkflow](../src/trading_engine/app/position_workflow.py) 处理活动身份匹配、成交先到、旧终态重放；通用坏消息隔离/死信仍缺失。 |
| F06 | P1 | 部分修复 | [TradeExecutionWorker](../src/trading_engine/app/trade_workflow.py) 查询已知订单并恢复 UNKNOWN，生产仓位关闭本地超时回滚；全账户补账及人工处置仍待补。 |
| F07 | P1 | 待修复 | [风控入口](../src/trading_engine/app/risk_engine_kafka.py) 仍依赖内存快照；启动权威持仓加载、过期检查及敞口约束不完整。 |
| F08 | P1 | 待修复 | [投影器](../src/trading_engine/infra/redis_position_view_projector.py) 仍保留负的空仓数量、轮询时间改变快照 hash；持仓腿和完整快照删除语义待完善。 |
| F09 | P1 | 待修复 | [因子策略](../src/trading_engine/strategy/factor_score.py) 同一 K 线可重复推进窗口；[数据源](../src/trading_engine/infra/clickhouse_source.py) 周期、收盘与多币种历史加载约束待补。 |
| F10 | P1 | 待修复 | [策略引擎](../src/trading_engine/strategy/engine.py) 入场置信度过滤仍会阻断退出；策略内存方向与实际成交未闭环。 |
| F11 | P1 | 部分修复 | 网关下单日志已改白名单；[ClickHouse 数据源](../src/trading_engine/infra/clickhouse_source.py) 仍有非空密码默认值。 |
| F12 | P1 | 已修复原缺项 | [start.sh](../start.sh)、[stop.sh](../stop.sh) 已纳入用户流、outbox、trade-worker；仍无 readiness 屏障。 |
| F13 | P2 | 待修复 | 网关在规则缺失或数量取整为零时仍可能返回原值；失败规则缓存、数量范围和名义金额校验待补。 |
| F14 | P2 | 部分修复 | 网关已在规则读取后刷新 timestamp，order.place 不自动重试；[构建入口](../src/trading_engine/app/run_trade_engine.py) 已传 REST URL。环境组合仍缺集中校验。 |
| F15 | P2 | 部分修复 | TradeWorkflow 已事务登记任务、归并用户流、保护终态及累计量；独立成交账本、终态后补充成交与全账户对账仍待补。 |
| F16 | P2 | 待修复 | [日志配置](../src/trading_engine/common/logger.py) 缺少 extra 时仍格式化失败；轮转、结构化日志及运行指标需完善。 |
| F17 | P2 | 部分修复 | 工作流已补有限正数量、BUY/SELL 与 risk/signal 币种检查；[serde](../src/trading_engine/contracts/serde.py) 版本范围、其他字段校验和类型契约仍不完整。 |
| F18 | P2 | 待完善 | 新增工作流测试有效，但完整旧套件尚未运行；部分旧断言与当前行为冲突，网络测试隔离及 CI 待补。 |
| F19 | P2 | 待优化 | 每单建立 WS、数量/价格规则分别读取、用户流同步等待 Kafka、投影逐币种读取仍在；旧订单逐单读取现在主要影响迁移恢复。 |
| F20 | P2 | 已修复原生产路径 | PositionWorkflow 传递 correlation/causation；[relay observer](../src/trading_engine/app/run_workflow_services.py) best-effort 记录调试历史，失败不再阻止业务发布。 |
| F21 | P2 | 待完善 | requirements 与 pyproject 的 websockets 下限仍不同；依赖锁定、CI、进程健康、配置集中校验和优雅退出仍需完善。 |
| F22 | P2 | 待修复 | [调试页面](../src/trading_engine/debug/server.py) 仍将动态文本插入 innerHTML；默认 localhost 不等于公网暴露，但 DOM 注入面仍在。 |

“已修复”仅指原缺陷在默认生产代码路径中已处理，不表示部署完成或全部相关风险关闭。

**P0：当前最优先的业务缺陷**

**F02．终态处理仍不保留部分成交的经济事实。**

本次离线复现：开仓部分成交 `0.4` 后撤单，数量从 `0.4` 变为 `0.0`，生命周期变为 flat。撤单并不会撤销已发生的成交；撤单回报携带的最新累计成交量也没有先统一应用。平仓 FILLED 分支仍直接清空仓位，不能准确表示部分减仓或精度截断后的残余。

生产 PositionWorkflow 仍调用同一个 PositionManager，因此 inbox/outbox 改造没有消除该缺陷。应先对所有携带累计成交量的事件应用增量，再处理订单终态；开仓撤单后保留已成交 long/short，平仓按实际减仓量保留剩余。验收覆盖部分成交撤单、撤单补充成交、部分减仓全成及残余数量。

**F03．最终请求仍缺少平仓保护并覆盖持仓方向。**

本次模拟最终请求：输入 `positionSide=SHORT`、`reduceOnly=true`，输出仍为 `positionSide=BOTH` 且没有 reduceOnly。数量和价格 Decimal 精度修复仍有效，得到 `0.123` 和 `65000.1`，但与平仓保护是独立问题。上游根据 risk_action 推导 reduceOnly，FLAT 平仓获 APPROVE 时仍可能导出 false。

应统一 close 的减仓意图，按账户持仓模式构建最终请求，并用模拟传输断言实际参数。验收覆盖平多、平空、各持仓腿、外部减仓后的数量变化；不能仅检查上游 metadata。

**持久化改造已解决的内容及边界**

F01：执行状态默认使用 `position:execution:{account}:state:{symbol}`，投影仍写 `binance:position:usdt_futures:view:state:{symbol}:v1`。探针键比较已从历史 true 变为 false。配置不得复用投影命名空间，旧、新执行写入者不能混跑。

F04：仓位、交易生产工厂已接入 PositionWorkflow / TradeWorkflow，并在事务成功后手动提交当前分区下一 offset。状态、inbox、完整输出事件及任务调度在 WATCH/MULTI/EXEC 中提交；relay 等 Kafka 确认后删记录，删除前崩溃会重发相同 event_id 和内容。运行入口要求 `KAFKA_ACKS=all` 或 `-1`。旧 PositionManager 直接保存再发布仍能复现丢动作，但生产流程先在临时仓库计算，再提交状态与 outbox，不能将旧探针结果外推到新链路。

这些机制提供至少一次投递与去重，不是 Redis/Kafka/Binance 分布式事务。Redis 持久化、淘汰策略、键类型独占和故障切换仍需验证；Redis 事务运行时命令错误不会自动回滚。策略、风控和用户流也未因此获得相同的持久化发布保证。

F05：动作创建时在仓位事务内保存稳定 client ID；成交先于 NEW 时可绑定活动订单。生产工作流忽略不匹配活动身份的旧终态与外部订单，离线测试覆盖成交先到、重复终态和回报竞争。原探针没有活动 client ID、直接调用 manager 的异常仍存在，但不代表同一生产缺陷未修复。通用解码错误、坏消息和身份冲突尚无逐条隔离/死信机制，仍可能退出消费循环；忽略外部订单也不等于实际仓位已对账。

F06：worker 先持久化 submitting 与租约 token，再调用下单；崩溃、不确定回复或保存失败后按原 client ID 查询，不盲目重发。旧 PENDING_SUBMIT 导入后也先查询。生产仓位流程使用 `recover_on_timeout=False`，不再仅因等待超时回滚。已知未终结订单定期查询；查询未找到保留 unknown。

剩余边界是“提交标志已保存、实际发送前崩溃”也会进入查询，可能需要人工核对；查询仅覆盖本系统已知订单，不覆盖所有持仓、资金、外部订单和历史成交。应补长期 UNKNOWN 告警、人工处置与全账户对账。

F15：新工作流订单文档成为权威记录，用户流推进状态与累计成交，迟到 NEW 不覆盖终态；旧 RedisOrderRepository 主要作为迁移来源，不再双写。仍读旧 order:* 键的外部工具需适配。终态保护也意味着终态后补充成交的处理规则需单独设计，不能代替成交账本。

F20：输出保留输入 correlation_id，以输入 event_id 作为 causation_id。调试历史在 relay 发布后由可选 observer 写入，失败不阻断交易事件；历史可能重复或遗漏，不能作为完整审计账本。

**仍需处理的其他问题**

- F07：风控启动不加载权威持仓，默认允许缺失快照视为空仓；打开 require_position_snapshot 也不能代替初始化。快照缺少新鲜度/版本检查，按金额定仓时缺价格回退默认数量的语义仍有风险。应补启动加载、过期拒绝及单笔/账户敞口约束。
- F08：探针仍得到空仓 quantity 为 `-2`，相同原始快照的 hash 随投影时间变化。执行键隔离已阻止直接覆盖，但实际视图的非负规模、来源时间、双向腿、删除币种和空集合语义仍需修正。
- F09–F10：同一 K 线连续评估仍由 flat 变为 long；趋势衰退满足退出条件时仍因 confidence_too_low 不产生信号。需要按 `(symbol, interval, open_time)` 去重、明确收盘契约并重建历史；退出条件与入场过滤分开，策略目标方向与实际成交反馈闭环。
- F11：网关下单 INFO 已只记录 symbol、client ID、数量和类型，不再打印含 apiKey/signature 的完整请求。AST 仍确认 CLICKHOUSE_PASSWORD 存在非空默认值；报告不复制或验证该值。应删除默认值；若曾使用，应轮换并按实际暴露范围处理历史记录。完整日志脱敏审计尚未完成。
- F13–F14：步长 `0.001` 下 `0.0004` 仍返回原值；规则获取失败缓存和适用过滤器校验需补齐。timestamp 与 REST URL 传递已修复，仍需验证 REST/WS/用户流环境组合。
- F16–F18：Formatter 缺 direction 仍可复现，常见 handler 会丢日志并报告 logging error，不应直接描述为必然业务崩溃。生产入口新增校验不等于完整事件 schema 校验。旧测试关于网关 SHORT/reduceOnly、提交异常及 LIMIT 拒绝的断言与实现仍需统一；这是静态冲突，不是本次 pytest 失败计数。部分网关测试只替换 WS，仍可能请求 exchangeInfo，完整测试前应完成网络隔离。
- F19、F21、F22：先修正确性再测吞吐与 P95/P99，考虑规则缓存、批量读取及用户流有界队列。统一依赖与进程管理，补 readiness、告警和存储容量指标；页面动态文本改用 textContent/安全 DOM，按需要分离静态资源。

**跨模块约束与更新后的路线**

仓位与交易 Redis 工作流键已有 account hash tag，事务有 revision/WATCH 并发保护，不应再说执行状态完全没有账户隔离或版本保护。但 PositionState、风控内存与消息路由仍主要按 symbol 表达，账户、策略和双向持仓腿尚未在全链路统一建模。增加多账户或副本前仍需验证 topic 隔离、分区归属、状态恢复与事件顺序。

| 顺序 | 工作内容 | 完成标准 |
|---|---|---|
| 1 | F02、F03；同时清理 F11 默认凭据 | 撤单不丢已成交持仓，部分平仓保留残余，最终请求正确表达持仓方向与减仓意图 |
| 2 | F05–F07、F15 剩余恢复和风控工作 | 坏消息可定位/隔离，UNKNOWN 可告警处置，重启加载权威持仓，外部交易可对账 |
| 3 | F08–F10、F13、F14、F17 | 快照、K 线、数量、时间及环境语义一致，退出不受入场过滤阻断 |
| 4 | F16、F18、F19、F21、F22 与真实服务验证 | 完整测试与 CI、真实故障注入、日志和健康检查、任务积压及存储增长可观测 |

27 项已通过的工作流用例应保留为回归门禁，不再列作从零实现任务。优先新增/完成：部分成交终态与最终平仓参数测试；真实 Redis/Kafka 提交、发布、确认边界的崩溃和故障切换；用户流断线与全账户持仓核账；旧/空快照、混合周期与策略重启；NaN/Infinity、未来时间、schema 不匹配及快照过期。

**文档、迁移与运行说明**

[README](../README.md) 已同步持久化链路、累计成交、活动订单、执行/投影隔离和剩余工作，原报告关于这些内容全面滞后的判断不再适用。[操作说明](inbox-outbox-operations.md) 验证段仍写 26 项测试，本次实际发现并通过 27 项，以本报告复核记录为准。

生产仓位入口要求 initialized 标志；升级需迁移执行状态或显式初始化已核对的空账户/执行库。迁移默认预览、SET NX 不覆盖，带符号空仓需按币种明确确认。标志仅代表完成初始化步骤，不证明从交易所加载并核对了全账户持仓。`start.sh all` 已补进程清单但无 readiness 屏障，应按操作说明确认恢复与消费者就绪后再启用策略。

`trade-worker --once` 会执行到期真实任务，不能作为诊断 dry-run，本次没有运行。Inbox 不自动过期、outbox 不提前裁剪、订单历史保留，需要根据重放范围制定容量与归档策略。市场数据新鲜度仍需明确 open_time/收盘时间契约，再确定阈值，不能只调大默认两秒。

**本次交付边界**

本次仅更新诊断报告，复核代码改造并运行离线验证，未修改业务代码或历史探针结果，未连接外部服务、读取实盘凭据、执行迁移或发送订单。代码状态、离线测试通过与生产部署完成是不同证据；线上配置、真实服务持久性、依赖漏洞及策略收益不在本次验证范围内。
