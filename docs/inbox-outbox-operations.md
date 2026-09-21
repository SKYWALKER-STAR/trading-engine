**Inbox/outbox 与持久化交易执行：实现及升级说明**

本次将生产入口的仓位处理和交易处理接入 Redis 事务工作流。原来的 PositionManager 和 TradeEngineMessageProcessor 仍可用于局部测试，但生产 consumer 工厂已切换到 PositionWorkflow / TradeWorkflow。

**运行链路**

```text
risk.decision -> position consumer
                    |
          Redis：执行仓位 + inbox + outbox（同一事务）
                    |
                 outbox relay -> trade.action
                                     |
                                trade consumer
                                     |
                        Redis：订单任务 + inbox（同一事务）
                                     |
                                trade-worker -> Binance
                                     |
                       Redis：订单结果 + outbox（同一事务）
                                     |
                  outbox relay -> order.update -> position / trade consumers

Binance 用户流 ------------------> order.update -> position / trade consumers
```

仓位与交易消费者在事务成功后，仅确认当前分区已处理消息的下一 offset。事务成功但确认失败时，重放由 inbox 去重；事务成功、Kafka 发布失败时，消息保留在 outbox。Relay 按最早记录顺序发布，等待 Kafka 确认后删除记录；删除前崩溃会重发相同 event_id 和内容。

状态、回执、完整输出事件以及任务调度由 WATCH/MULTI/EXEC 一起提交。冲突后重新读取并计算；业务计算在临时仓库中进行，不调用外部服务。Redis 不提供运行时命令错误的回滚，因此工作流键必须由本组件独占、类型固定，参数与 JSON 在提交前校验。不要把工作流实例当作可淘汰缓存使用；确认 AOF、备份及故障切换的数据丢失窗口与业务要求一致。该实现不宣称 Redis 与 Kafka 或 Binance 之间存在分布式事务。

**新增进程**

| 命令 | 职责 |
|---|---|
| `python -m trading_engine position` | 仓位输入去重、执行状态与 outbox 原子提交 |
| `python -m trading_engine trade` | 持久化下单任务，同时合并用户流的订单更新 |
| `python -m trading_engine trade-worker` | 执行未发送任务、查询 UNKNOWN 和未终结订单 |
| `python -m trading_engine outbox` | 发布仓位和交易 outbox |
| `python -m trading_engine binance-user-stream` | 持续采集交易所回报 |

`outbox`、`trade-worker` 支持 `-- --once`。注意：trade-worker 的 once 也会执行到期的真实下单任务，不是 dry-run。start.sh / stop.sh 已包含这些进程，all 同时包含用户流。start.sh 默认 KAFKA_ACKS=all；若环境中显式配置了 1，relay 会拒绝启动，需修改配置。

**存储配置**

| 配置 | 默认值 |
|---|---|
| POSITION_EXECUTION_KEY_PREFIX | `position:execution` |
| TRADE_WORKFLOW_KEY_PREFIX | `trade:execution` |
| ORDER_ACCOUNT_ID | `default` |
| POSITION_REDIS_URL | `redis://127.0.0.1:6379/0` |
| ORDER_REDIS_URL | `redis://127.0.0.1:6379/0` |
| KAFKA_ACKS | relay 要求 `all` 或 `-1` |
| BINANCE_FUTURES_REST_API_URL | `https://fapi.binance.com`，现同时传给下单网关用于规则读取 |

默认键举例：

```text
position:execution:{default}:initialized
position:execution:{default}:state:BTCUSDT
position:execution:{default}:inbox:<event_id>
position:execution:{default}:outbox

trade:execution:{default}:state:<client_order_id>
trade:execution:{default}:inbox:<event_id>
trade:execution:{default}:tasks
trade:execution:{default}:outbox
```

每个工作流独立命名空间，一个账户的键使用相同 hash tag。当前客户端按单 Redis endpoint 连接；尚未验证 Redis Cluster 客户端部署。Inbox 不自动过期；outbox 不提前裁剪；订单历史保留。应监控 inbox/订单增长量，再根据允许的重放范围设计归档策略，不能沿用原来 900 秒进程内去重的保留期。

投影器继续写原来的 `binance:position:usdt_futures:view:...`，不再覆盖执行仓位。调试页面优先读取新执行状态；调试历史由 relay 的可选 observer 记录。调试记录是 best-effort，可能重复或遗漏，但其失败不会阻止交易事件发布。

**旧版本升级步骤**

1. 停止旧版策略、仓位、交易、用户流和投影进程，保存现有 Kafka consumer group offset 与 Redis 备份。不要让旧、新写入者混跑；不要为了升级重置消费 offset。
2. 保持原来的 POSITION_REDIS_URL、ORDER_REDIS_URL、ORDER_ACCOUNT_ID。新执行前缀不得指向实际持仓投影键。
3. 预览执行状态迁移：

   ```bash
   python -m trading_engine position-state-migrate
   ```

   默认读取旧的 `{旧前缀}:{symbol}` 执行副本，跳过 raw/view/debug 等键，不把投影快照当作执行状态。旧前缀可用 `-- --legacy-prefix <prefix>` 指定。预览无写入，不删除旧数据。

4. 核对预览中的仓位、数量和活动订单后执行：

   ```bash
   python -m trading_engine position-state-migrate -- --apply
   ```

   复制使用 SET NX，不覆盖已经迁移的新状态。负数量要求先检查旧数据；过渡状态缺少订单身份时，仅在旧订单库有同币种唯一活动订单时自动补充身份，否则拒绝应用。迁移完成会写 initialized 标志。生产仓位入口没有这个标志时拒绝启动，避免升级后静默把缺失的执行状态当作空仓。

5. 对全新且已核对为空的账户/执行库，可显式初始化：

   ```bash
   python -m trading_engine position-state-migrate -- --apply --initialize-empty
   ```

   这不是从 Binance 自动导入实际持仓。没有旧执行数据但交易所仍有持仓时，应先完成持仓核对与初始化，不使用空库初始化跳过它。

6. 设置 KAFKA_ACKS=all，启动用户流、outbox、trade、trade-worker、position，确认恢复情况后再启用策略。worker 启动会把旧订单库中的活动订单导入新工作流，全部先进入查询恢复路径，包括旧 PENDING_SUBMIT；不会因为旧状态叫 pending 就重新发送。

旧 RedisOrderRepository 现在是兼容迁移来源；新工作流订单文档是新执行链路的权威记录，位于 `trade:execution:{account}:state:{client_id}`。新结果不双写旧订单索引，避免引入第二个非事务写入点。若有仓库外工具依赖旧 order:* 键，应同步改读新工作流记录或增加独立只读投影。新记录中 request 保存原始执行请求，status 保存已知订单状态，phase 保存执行/恢复阶段。

回滚代码前，必须处理新 outbox 和 tasks 中的工作；旧版程序不能识别这些持久任务。不能仅切回旧可执行文件后立即继续下单。

**UNKNOWN 与并发恢复策略**

- 网关将 `-1000`、`-1006`、`-1007`、缺少错误码的错误回复及服务端 `5xx` 回复保留为执行结果不确定，由 worker 持久化 UNKNOWN 并查询原客户端订单 ID；不会发布拒单事件使仓位提前回滚。其中 `-1006` / `-1007` 的执行状态不确定语义来自 [Binance USDⓈ-M 错误码文档](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/error-code)，其余上述情况采用保守查询策略。明确的参数拒绝（例如 `-1111`）仍按 REJECTED 终结任务。
- 消费者只登记任务。worker 先在事务中把 pending_submit 改为 submitting，并记录租约 token，再进行唯一一次下单调用。下单 WS 传输不再自动重发 order.place。
- 进程崩溃、回复丢失或保存结果失败后，租约到期由后续 worker 查询同一个 client_order_id。租约 token 用于阻止旧 worker 的迟到结果覆盖新持有者。
- 普通未终结订单每约 30 秒查询一次；查询失败或未找到保留 unknown 阶段并继续查询。记录 last_error、reconciliation_attempts、next_due，供排障使用。已知部分成交状态不会仅因查询失败退回 NEW。
- 查询使用 `order.status`。查询未找到不等于从未下单，旧订单可能已超出 API 可查询范围，故不会自动转成新的下单任务。[Binance Query Order 文档](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-api/trade#query-order-user-data)
- 如进程恰好在 submitting 已保存、网络发送尚未发生时崩溃，任务也会进入查询路径。这类无法自动证明“未发送”的任务可能需要人工核对；当前实现优先避免重复下单，没有提供自动重置为 pending_submit 的入口。
- 用户流更新同时推进新订单文档，已知终态不被迟到 NEW 覆盖。用户流消息已在 Kafka，不会再次回写 outbox 形成循环。
- 生产仓位流程关闭了“等三十秒就本地回滚”的逻辑，由订单结果/对账回报驱动仓位恢复。新增动作的客户端 ID 在仓位事务内保存，因此成交先于 NEW 到达仍能绑定；旧终态重放不会使生产消费者退出。

当前对账覆盖本系统已知订单的状态与累计成交量，不是全账户持仓/资金/历史成交的全面核账。诊断报告中的部分成交撤单、减仓保护、风控快照等独立业务缺陷仍需分别处理；inbox/outbox 本身不会修正它们。

**验证**

最新离线验证：24 项 unittest 测试通过，包含不确定错误回复转查询，以及明确精度拒绝正常结束任务的回归测试。

本次使用 Python 3.13 标准库 unittest 执行事务协议测试，无外部网络调用。命令：

```powershell
$env:PYTHONPATH = 'src'
.\.venv\Scripts\python.exe -B -m unittest discover -s tests/unit/workflow -v
```

覆盖：状态/回执/完整事件共同提交、提交前失败、WATCH 冲突重算、进程重启后重复输入、Kafka 不可用、发送成功后删除失败、手动 offset 边界、同客户端订单重复任务、发送后进程崩溃、交易结果持久化失败、UNKNOWN 未找到不重发、用户流终态与下单 ACK 竞争、旧 pending 迁移只查询、迁移预览/NX、查询身份及下单传输不重试。

这些测试使用内存模拟 Redis 的事务接口，不证明真实服务器在断电、内存耗尽或故障切换时的持久性。完整 pytest、ruff、mypy 和真实 Redis/Kafka/Binance 集成测试尚未运行：测试依赖下载被本次环境权限限制，安装请求未获批准。上线前应在已有依赖的开发/测试环境完成这些检查；未执行任何真实下单或线上迁移。
