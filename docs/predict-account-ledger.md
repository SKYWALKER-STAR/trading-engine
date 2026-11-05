# 仅用链上数据重建账户账本

入口：`scripts/predict_account_ledger.py`。Python 3.11+，仅标准库；不访问 Predict API、不读取成交 JSON、不需要 JWT 或私钥。不实现前端或轮询。

## 执行

沿用你已验证能返回日志的 BSC RPC。需同时支持历史 `eth_call`：用于核对起始区块的已有份额以及赎回时的兑付比例。**历史日志可用不代表历史状态查询可用**。

```powershell
$env:BSC_RPC_URL = '你的 BSC RPC URL'
python ./scripts/predict_account_ledger.py `
  --account 0xadD1Dd62b94a8C9744e6B5b1a3DAe8db0d84e108 `
  --from-block 125600000 `
  --chunk-size 100 `
  --request-delay 0.15 `
  --output predict-account-ledger.json
```

`125600000` 只是之前使用的时间范围示例。计算完整成本时，起点应早于**首次取得相关仓位**，不是首次领取。默认 `--from-block 0` 覆盖全部历史，但很慢。非零起点会查询每个 token 在起点前一块的余额：已有份额保留，但其成本标记未知。后续完全清仓再买入可以恢复已知成本。不能为了得到非空成本而把期初份额当零。

终点默认最新区块减 64，也可用 `--to-block` 指定。扫描仍为串行，每段最多发出三类日志查询；比仅查赎回需要更多 RPC 请求。HTTP 413 或明确区块跨度限制会缩小区间。旧脚本的诊断输出仍可用于检查节点日志服务。

## 输出

- `predict-account-ledger.json`：完成余额核对后生成的主报表。
- `predict-account-ledger.chain.jsonl`：相关交易的完整链上回执和区块时间，便于核查。
- `predict-account-ledger.progress.json`：扫描进度；不是断点续跑文件。

主报表包含：

1. `redemptions`：每个赎回事件中的每个 indexSet 一行。包含 token、conditionId、份额、均价、成本、兑付金额、盈亏、赎回时间、hash 和问题说明。
2. `remaining_holdings`：扫描终点剩余份额、成本，以及通过链上注册映射可识别的已结算未赎回状态和理论兑付额。
3. `transaction_audit`：每笔交易的账户净 USDT 变动、份额变动、成本归属依据。
4. `metadata`：扫描区间、完成标记、成本是否完整及计算约定。

金额和 token ID 使用字符串，避免前端 JavaScript 丢失精度。内部成本使用有理数，不提前四舍五入；输出小数最多保留 36 位。不得将 null 当作 0。

## 计算口径

- 数量依据已配置 CTF 合约的 ERC1155 `TransferSingle` / `TransferBatch`，同时查询账户转入、转出，按区块和交易顺序重放。每个交易回执中的转移只处理一次。
- 普通 CTF 赎回通过 `getCollectionId` / `getPositionId` 找到 token，数量从此前对应的销毁事件提取；NegRisk 赎回结合适配器 `ctf()` / `wcol()` 和金额数组，核对转给适配器的份额。
- `OrderFilled` 只使用账户自身作为订单 maker 的事件，避免把对手方及 taker 汇总事件重复计算。只有各 token 净份额及总 USDT 流与回执逐项相符，才接受事件的成本分配（买入手续费扣份额、卖出手续费扣现金的解释必须经过该核对）。
- 单仓位和多仓位成交均必须同时核对份额与现金流。交易包含额外充值、转账或无法归属的费用导致核对失败时，新增仓位成本保持未知，不直接把整笔现金流当作买入成本。
- 成本采用**移动加权平均**。买入累加成本，卖出或赎回按剩余持仓均价分摊成本。赎回盈亏仅针对本次赎回份额，不包含此前已卖出的利润。不计 BNB gas，也不混入单独领取的利息奖励。
- 外部转入、拆分、转换或无法唯一归属的复杂交易：仍重放数量，但新增份额成本为 null。多资产成本分配无法唯一从链上事实推出，不能擅自均分。合并和转出减少对应份额及其加权成本，不伪装成赎回。
- 赢/输/部分兑付读取赎回区块的 payout 分子和分母。`partial_payout` 不擅自命名为“平局”（也可能是其他部分兑付情形）。逐选项金额之和必须与赎回事件 payout 一致，否则该行金额/盈亏为空并附问题说明。
- 零份额赎回事件保留 `zero_quantity_event=true`，不应在前端当作一笔真实亏损持仓。成功调用赎回不一定有持仓或有付款。
- 最终所有已发现 token 的重放余额必须与指定终点链上 `balanceOf` 相同，否则失败退出，不生成主报表。无法证明节点没有遗漏买卖后归零的完整交易，因此余额一致不等于第三方数据源绝对完整。

## 无法承诺与网页完全一致的字段

- `market_name`、`outcome_name`：目前保留 null，使用 conditionId/tokenId/indexSet 标识。没有接入网页或 API 名称映射，也不把 1/2 擅自映射为 Yes/No 或球队名称。
- `market_resolved_at_utc`：保留 null。`redeemed_at_utc` 是实际领取交易时间，不替代市场裁定时间。
- 均价、成本、盈亏采用上述公开会计约定，未验证平台自身的成本算法、gas 处理和展示舍入规则；未知成本保持 null。
- `estimated_payout_collateral_units` 是未赎回份额的理论抵押资产兑付量，不是已到账 USDT。
- 未配置的后续新合约、其他钱包地址、链外记账不会被自动纳入。当前注册表来自官方 SDK，部署更新时需维护。

## 数据依据和验证

- [官方 CTFExchange ABI：OrderFilled、getConditionId、getCollateral](https://github.com/PredictDotFun/sdk-python/blob/main/src/predict_sdk/abis/CTFExchange.json)
- [官方 ConditionalTokens ABI](https://github.com/PredictDotFun/sdk-python/blob/main/src/predict_sdk/abis/ConditionalTokens.json)
- [官方 NegRiskAdapter ABI](https://github.com/PredictDotFun/sdk-python/blob/main/src/predict_sdk/abis/NegRiskAdapter.json)
- [官方部署地址](https://github.com/PredictDotFun/sdk-python/blob/main/src/predict_sdk/constants.py)

```powershell
python -m pytest tests/unit/test_predict_account_ledger.py tests/unit/test_predict_redemptions.py -q
```

测试涵盖加权成本、部分卖出、输/部分兑付、外部转入、期初未知成本、手续费、复合买入归属、ABI 批量转移、余额不足失败和零份额赎回。模拟测试不等于已对目标账户完整主网历史验算。
