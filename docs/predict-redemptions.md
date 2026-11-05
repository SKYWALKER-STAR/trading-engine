# Predict.fun 已赎回记录查询

`scripts/predict_redemptions.py` 是独立的只读 Python 3.11+ 脚本，仅用标准库，无需安装依赖、Predict API Key、JWT、私钥或钱包签名。需要支持 BSC 主网历史 `eth_getLogs`、交易回执、区块查询的 RPC URL。

## Windows PowerShell 运行

在仓库根目录执行；本仓库已有 `.venv/Scripts/python.exe`，也可以替换为自己的 `python`。

```powershell
$env:BSC_RPC_URL = 'https://你的BSC-RPC地址/你的密钥'

.\.venv\Scripts\python.exe .\scripts\predict_redemptions.py `
  --account 0xadD1Dd62b94a8C9744e6B5b1a3DAe8db0d84e108 `
  --trades-json 'C:\Users\15462\Desktop\predict-trades-all.json' `
  --output 'C:\Users\15462\Desktop\predict-redemptions.json'
```

`--trades-json` 可省略，只用于按 conditionId 补充市场 ID 和名称。文件中没有的市场仍保留，不会遗漏链上事件。不需要联网调用 Predict API。

默认从区块 0 扫描到当前高度减 64；全历史可能耗时较长，取决于节点允许的区块跨度和限速。节点应完整返回日志，不能静默截断。默认每段 50,000 区块，遇到明确跨度/结果限制自动缩小；服务商只允许较小范围时可设置 `--chunk-size 1000`。如果服务商结果上限低于 1,000，设置 `--result-cap` 为该上限或更低。

知道**账户首次使用之前的区块**时，可以加 `--from-block 区块号` 加速。不要从首次成交或最近赎回区块开始并把结果称为完整历史：账户可能通过转账等途径持有仓位。

可用 `--to-block` 固定终点。命令不会覆盖已有输出或进度文件；再次运行请换输出文件名。仅将区块范围参数留给未来增量查询，目前不实现轮询、断点续跑、前端或自动重组修复。

## 输出与口径

主 JSON 包含 `metadata` 与 `records`。只有全部区间成功扫描、事件与成功回执核对后，才写入主文件，`metadata.complete=true` 仅表示指定区块范围和配置合约扫描完成，不保证 RPC 服务商未静默漏数据或未来合约仍在本列表中。

每条记录包括：

- `id`：链 ID + 交易哈希 + 日志序号，供未来前端/数据库去重。
- `transaction_hash`、`block_number`、`block_hash`、`log_index`、`timestamp_utc`。
- `redeemer`、合约地址/名称、`condition_id`，以及可选的市场 ID/名称。
- `payout_raw`：事件记录的原始结算款，使用字符串保留 uint256 精度。
- `payout_usdt`：USDT 的 18 位小数换算结果，字符串；未知抵押资产为 null。
- `index_sets`：普通 CTF 事件中的赎回选项集合。
- `redeemed_amounts_raw` / `redeemed_amounts`：NegRisk 适配器事件的有序份额数组，保留原顺序；普通 CTF 事件没有该字段，明确输出 null，不把 payout 当成份额。
- `raw_log`：完整原始链上日志，便于复核。

金额是事件声明的 payout，不是盈亏、收益率，也不是单独核对过的账户实际净入账。不要把两种解释混在一起。保留零 payout 的成功赎回事件。

另有 `.partial.jsonl`（逐段保存已验证记录）和 `.progress.json`（最后完整扫描区块）。失败/中断时这些只供检查，不是完整结果；错误退出码非零。成功后它们仍保留。

本脚本查询的是**已经发生成功赎回事件的记录**。已结算但没有执行赎回（例如输钱后一直不操作）的仓位不在此范围内。合并、拆分、转换不是赎回，不混入结果。一次交易可能含多条赎回事件，不能仅按交易哈希去重。

## 合约和事件来源

2026-11-05 核对官方 SDK：

- [部署地址](https://github.com/PredictDotFun/sdk-python/blob/main/src/predict_sdk/constants.py)
- [ConditionalTokens ABI](https://github.com/PredictDotFun/sdk-python/blob/main/src/predict_sdk/abis/ConditionalTokens.json)
- [NegRiskAdapter ABI](https://github.com/PredictDotFun/sdk-python/blob/main/src/predict_sdk/abis/NegRiskAdapter.json)
- [SDK 赎回路由](https://github.com/PredictDotFun/sdk-python/blob/main/src/predict_sdk/order_builder.py)

扫描三个不同的 ConditionalTokens 地址和两个 NegRiskAdapter 地址，包含标准和 yield-bearing 部署。普通/NegRisk 共用的 CTF 地址只扫描一次。两种 `PayoutRedemption` 的 ABI 签名不同；均按 indexed redeemer 过滤，而不是 transaction.from（智能钱包的外层交易发送方可能是代付者）。适配器内部以适配器自身为 redeemer 的 CTF 事件不会被计入账户结果。

如官方后续更换合约，需更新 `CONTRACTS` 并从相应部署区块补扫。

## 验证

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/test_predict_redemptions.py -q
```

测试使用模拟 RPC，覆盖两种 ABI、精确金额、零金额、重复日志、范围拆分、结果上限保护、失败回执和输出完成标记。不等于真实主网查询成功；实际运行需提供自己的 RPC URL。
