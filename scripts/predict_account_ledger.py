#!/usr/bin/env python3
"""Chain-only Predict account ledger. No Predict API, private key or JWT.

See docs/predict-account-ledger.md. Amounts are exact decimal strings. Unknown
cost is never replaced by zero. Transfers are authoritative for share quantity.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from fractions import Fraction
import json
import os
from pathlib import Path
import sys

from predict_redemptions import (
    Rpc, RpcError, RangeLimit, CONTRACTS, USDT, DEFAULT_ACCOUNT,
    CTF_TOPIC, ADAPTER_TOPIC, address, decode_redemption, write_json,
)

ZERO = '0x' + '0' * 40
ZERO32 = '0x' + '0' * 64
SINGLE = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
BATCH = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb'
TRANSFER = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
FILLED = '0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6'
CTFS = [a for a, (kind, _) in CONTRACTS.items() if kind == 'ctf']
EXCHANGES = {
    '0x8bc070bedab741406f4b1eb65a72bee27894b689': CTFS[0],
    '0x365fb81bd4a24d6303cd2f19c349de6894d8d58a': CTFS[0],
    '0x6beb5a40c032afc305961162d8204cda16decfa5': CTFS[1],
    '0x8a289d458f5a134ba40015085a8f50ffb681b41d': CTFS[2],
}


def topic_address(value):
    return '0x' + value[2:].rjust(64, '0')


def word(value):
    if isinstance(value, int):
        return f'{value:064x}'
    return value.removeprefix('0x').rjust(64, '0')


def number(value, scale=10**18):
    if value is None:
        return None
    f = Fraction(value) / scale
    with localcontext() as ctx:
        ctx.prec = 90
        text = format(Decimal(f.numerator) / Decimal(f.denominator), '.36f')
    return text.rstrip('0').rstrip('.') if '.' in text else text


def words(log):
    data = bytes.fromhex(log['data'][2:])
    if len(data) % 32:
        raise ValueError('Malformed ABI event data')
    return [int.from_bytes(data[i:i+32], 'big') for i in range(0, len(data), 32)]


def array_at(values, head):
    offset = values[head]
    if offset % 32 or offset // 32 >= len(values):
        raise ValueError('Invalid ABI array offset')
    start = offset // 32
    count = values[start]
    if start + 1 + count > len(values):
        raise ValueError('Truncated ABI array')
    return values[start+1:start+1+count]


def transfer_rows(log, account):
    t = [x.lower() for x in log['topics']]
    if log['address'].lower() not in CTFS or not t or t[0] not in (SINGLE, BATCH):
        return []
    if len(t) != 4:
        raise ValueError('Invalid ERC1155 event topics')
    sender, receiver = '0x'+t[2][-40:], '0x'+t[3][-40:]
    if account not in (sender, receiver) or sender == receiver:
        return []
    v = words(log)
    if t[0] == SINGLE:
        if len(v) != 2:
            raise ValueError('Invalid TransferSingle')
        ids, quantities = [v[0]], [v[1]]
    else:
        ids, quantities = array_at(v, 0), array_at(v, 1)
        if len(ids) != len(quantities):
            raise ValueError('Invalid TransferBatch lengths')
    return [{'key': (log['address'].lower(), token), 'quantity': q,
             'incoming': receiver == account, 'from': sender, 'to': receiver,
             'log_index': int(log['logIndex'], 16)} for token, q in zip(ids, quantities)]


class Chain:
    def __init__(self, rpc, end):
        self.rpc, self.end, self.cache = rpc, end, {}

    def call(self, contract, selector, args=(), block=None):
        block = self.end if block is None else block
        data = '0x' + selector + ''.join(word(x) for x in args)
        key = (contract, data, block)
        if key not in self.cache:
            value = self.rpc.call('eth_call', [{'to': contract, 'data': data}, hex(block)])
            if not isinstance(value, str) or len(value) != 66:
                raise RpcError('eth_call returned invalid 32-byte result; archive state may be unavailable')
            self.cache[key] = value.lower()
        return self.cache[key]

    def integer(self, *args, **kwargs):
        return int(self.call(*args, **kwargs), 16)

    def balance(self, key, account, block):
        try:
            return self.integer(key[0], '00fdd58e', (account, key[1]), block)
        except RpcError:
            if self.rpc.call('eth_getCode', [key[0], hex(block)]) == '0x':
                return 0
            raise

    def position(self, ctf, collateral, parent, condition, index_set):
        collection = self.call(ctf, '856296f7', (parent, condition, index_set))
        return ctf, self.integer(ctf, '39dd7530', (collateral, collection))

    def redemption_positions(self, row):
        condition = row['condition_id']
        if CONTRACTS[row['contract']][0] == 'ctf':
            ctf, collateral = row['contract'], row['collateral_token']
            indexes = [int(x) for x in row['index_sets']]
            parent = row['parent_collection_id']
            quantities = [None] * len(indexes)
        else:
            ctf = '0x' + self.call(row['contract'], '22a9339f')[-40:]
            collateral = '0x' + self.call(row['contract'], '7e3b74c3')[-40:]
            parent = ZERO32
            quantities = [int(x) for x in row['redeemed_amounts_raw']]
            indexes = [1 << i for i in range(len(quantities))]
        if ctf not in CTFS:
            raise RpcError('Adapter uses an unconfigured CTF contract; update registry before scanning')
        result = []
        for index, quantity in zip(indexes, quantities):
            key = self.position(ctf, collateral, parent, condition, index)
            # Query historical resolution at redemption block, not a later market state.
            block = row['block_number']
            denominator = self.integer(ctf, 'dd34de67', (condition,), block)
            slots = self.integer(ctf, 'd42dc0c2', (condition,), block)
            if not 1 <= slots <= 256 or index <= 0 or index >= 1 << slots:
                raise RpcError('Invalid condition outcome/indexSet')
            numerator = sum(self.integer(ctf, '0504c814', (condition, i), block)
                            for i in range(slots) if index & (1 << i))
            result.append((key, index, quantity, numerator, denominator))
        return result

    def describe_position(self, key):
        """Resolve token identity from registered exchange mappings, never web metadata."""
        ctf, token = key
        for exchange, exchange_ctf in EXCHANGES.items():
            if exchange_ctf != ctf:
                continue
            try:
                condition = self.call(exchange, 'd7fb272f', (token,))
                if condition == ZERO32:
                    continue
                collateral = '0x' + self.call(exchange, '5c1548fb')[-40:]
            except RpcError:
                # Not all exchanges registered every token. Do not invent a mapping.
                continue
            candidates = [collateral]
            for adapter, (kind, _) in CONTRACTS.items():
                if kind == 'adapter':
                    try:
                        if '0x'+self.call(adapter, '22a9339f')[-40:] == ctf:
                            candidates.append('0x'+self.call(adapter, '7e3b74c3')[-40:])
                    except RpcError:
                        continue
            for col in set(candidates):
                for index in (1, 2):
                    if self.position(ctf, col, ZERO32, condition, index) == key:
                        denominator = self.integer(ctf, 'dd34de67', (condition,))
                        numerator = self.integer(ctf, '0504c814', (condition, index-1))
                        return {'condition_id': condition, 'index_set': str(index),
                                'payout_numerator': numerator, 'payout_denominator': denominator}
        return {'condition_id': None, 'index_set': None,
                'payout_numerator': None, 'payout_denominator': None}


class Position:
    def __init__(self, quantity=0):
        self.quantity = quantity
        self.cost = Fraction(0) if quantity == 0 else None
        self.reason = None if quantity == 0 else 'opening_balance_cost_unknown'

    def add(self, quantity, cost, reason=None):
        if quantity < 0:
            raise ValueError('Negative acquisition quantity')
        if self.cost is None or cost is None:
            self.cost = None
            self.reason = self.reason or reason or 'acquisition_cost_unknown'
        else:
            self.cost += cost
        self.quantity += quantity

    def remove(self, quantity):
        if quantity < 0 or quantity > self.quantity:
            raise RpcError('Share ledger underflow; missing history, opening balance or transfer events')
        cost = (self.cost * quantity / self.quantity
                if self.cost is not None and self.quantity else Fraction(0) if not quantity else None)
        reason = self.reason
        self.quantity -= quantity
        if self.quantity == 0:
            self.cost, self.reason = Fraction(0), None
        elif self.cost is not None:
            self.cost -= cost
        return cost, reason


def scan(rpc, account, start, end, size, cap):
    filters = [
        {'address': CTFS, 'topics': [[SINGLE, BATCH], None, topic_address(account)]},
        {'address': CTFS, 'topics': [[SINGLE, BATCH], None, None, topic_address(account)]},
        {'address': list(CONTRACTS), 'topics': [[CTF_TOPIC, ADAPTER_TOPIC], topic_address(account)]},
    ]
    cursor = start
    while cursor <= end:
        upper = min(end, cursor + size - 1)
        logs = {}
        try:
            for query in filters:
                result = rpc.call('eth_getLogs', [{**query, 'fromBlock': hex(cursor), 'toBlock': hex(upper)}])
                if not isinstance(result, list):
                    raise RpcError('Invalid eth_getLogs response')
                if len(result) >= cap:
                    raise RangeLimit('Possible log truncation')
                for log in result:
                    if log.get('removed') or not cursor <= int(log['blockNumber'], 16) <= upper:
                        raise RpcError('Removed/out-of-range log')
                    logs[(log['transactionHash'].lower(), int(log['logIndex'], 16))] = log
        except RangeLimit:
            if cursor == upper:
                raise RpcError('Cannot retrieve complete single-block logs') from None
            size = max(1, (upper-cursor+1)//2)
            print(f'Reducing block chunk to {size}', file=sys.stderr)
            continue
        yield upper, list(logs.values())
        cursor = upper + 1


class Ledger:
    def __init__(self, chain, account, start):
        self.chain, self.account, self.start = chain, account, start
        self.positions, self.rows, self.audit = {}, [], []

    def position(self, key):
        if key not in self.positions:
            opening = self.chain.balance(key, self.account, self.start-1) if self.start else 0
            self.positions[key] = Position(opening)
        return self.positions[key]

    def process(self, receipt, timestamp):
        logs = sorted(receipt['logs'], key=lambda l: int(l['logIndex'], 16))
        movements = [m for log in logs for m in transfer_rows(log, self.account)]
        for m in movements:
            self.position(m['key'])
        cash = 0
        has_trade = False
        fill_plan = defaultdict(lambda: {'shares': 0, 'cash': 0, 'buy': False, 'sell': False})
        for log in logs:
            topics = [t.lower() for t in log['topics']]
            if log['address'].lower() == USDT and topics and topics[0] == TRANSFER:
                if len(topics) != 3 or len(words(log)) != 1:
                    raise ValueError('Malformed USDT Transfer')
                value = words(log)[0]
                cash += value * (int(topics[2] == topic_address(self.account)) - int(topics[1] == topic_address(self.account)))
            if log['address'].lower() in EXCHANGES and topics and topics[0] == FILLED:
                if topic_address(self.account) in topics[2:4]:
                    has_trade = True
                # Own order fills only. Counterparty fills with account as taker
                # can describe the same match and must not be counted twice.
                if len(topics) == 4 and topics[2] == topic_address(self.account):
                    v = words(log)
                    if len(v) != 5:
                        raise ValueError('Unexpected OrderFilled ABI')
                    maker_asset, taker_asset, maker_qty, taker_qty, fee = v
                    ctf = EXCHANGES[log['address'].lower()]
                    if maker_asset == 0 and taker_asset != 0 and taker_qty >= fee:
                        plan = fill_plan[(ctf, taker_asset)]
                        plan['shares'] += taker_qty - fee
                        plan['cash'] -= maker_qty
                        plan['buy'] = True
                    elif maker_asset != 0 and taker_asset == 0 and taker_qty >= fee:
                        plan = fill_plan[(ctf, maker_asset)]
                        plan['shares'] -= maker_qty
                        plan['cash'] += taker_qty - fee
                        plan['sell'] = True
        remaining = [dict(m) for m in movements]
        redemption_count = 0
        for log in logs:
            topics = [t.lower() for t in log['topics']]
            if (log['address'].lower() not in CONTRACTS or len(topics) < 2
                    or topics[0] not in (CTF_TOPIC, ADAPTER_TOPIC)
                    or topics[1] != topic_address(self.account)):
                continue
            redemption_count += 1
            event = decode_redemption(log, self.account)
            parts = []
            issues = []
            for key, index, explicit_qty, numerator, denominator in self.chain.redemption_positions(event):
                # Only direct burns or transfers to this adapter before this event.
                eligible = [m for m in remaining if m['key'] == key and not m['incoming']
                            and m['log_index'] < event['log_index']
                            and m['to'] in (ZERO, event['contract'])]
                available = sum(m['quantity'] for m in eligible)
                qty = available if explicit_qty is None else explicit_qty
                if qty > available:
                    raise RpcError('Redemption quantities do not reconcile to account share transfers')
                needed = qty
                for m in eligible:
                    take = min(needed, m['quantity'])
                    m['quantity'] -= take
                    needed -= take
                pos = self.position(key)
                # Transactions with incoming shares before redemption need event-level cost
                # allocation. Preserve quantities, but never borrow later known costs.
                incoming = [m for m in remaining if m['key'] == key and m['incoming']
                            and m['log_index'] < event['log_index'] and m['quantity']]
                for m in incoming:
                    pos.add(m['quantity'], None, 'acquire_and_redeem_same_transaction')
                    m['quantity'] = 0
                cost, reason = pos.remove(qty)
                value = qty * numerator // denominator if denominator else None
                parts.append((key, index, qty, numerator, denominator, cost, reason, value))
            expected = sum(p[7] for p in parts if p[7] is not None)
            allocatable = all(p[4] for p in parts) and expected == int(event['payout_raw'])
            if not allocatable:
                issues.append('event_payout_does_not_match_quantity_times_resolution')
            for key, index, qty, numerator, denominator, cost, reason, value in parts:
                payout = value if allocatable and event['collateral_token'] == USDT else None
                row = {
                    'id': event['id'] + ':' + str(index),
                    'condition_id': event['condition_id'], 'ctf_contract': key[0],
                    'token_id': str(key[1]), 'index_set': str(index),
                    'market_name': None, 'outcome_name': None,
                    'result': ('unresolved' if not denominator else 'lost' if numerator == 0
                               else 'won' if numerator == denominator else 'partial_payout'),
                    'payout_ratio': number(Fraction(numerator, denominator), 1) if denominator else None,
                    'shares': number(qty), 'average_cost_price': number(cost/qty, 1) if cost is not None and qty else None,
                    'holding_cost_usdt': number(cost), 'payout_usdt': number(payout),
                    'pnl_usdt': number(Fraction(payout)-cost) if payout is not None and cost is not None else None,
                    'redeemed_at_utc': timestamp, 'market_resolved_at_utc': None,
                    'transaction_hash': event['transaction_hash'], 'block_number': event['block_number'],
                    'log_index': event['log_index'],
                    'issues': issues + ([reason] if reason else []),
                    'gas_included_in_pnl': False,
                    'zero_quantity_event': qty == 0,
                }
                self.rows.append(row)
        net = defaultdict(int)
        incoming_keys, outgoing_keys = set(), set()
        for m in remaining:
            if m['quantity']:
                net[m['key']] += m['quantity'] if m['incoming'] else -m['quantity']
                (incoming_keys if m['incoming'] else outgoing_keys).add(m['key'])
        net = {k: q for k, q in net.items() if q}
        # Accept allocations only when both token and cash legs reconcile.
        reconciled_fills = (not redemption_count and bool(fill_plan)
                            and set(fill_plan) == set(net)
                            and sum(v['cash'] for v in fill_plan.values()) == cash
                            and all(v['shares'] == net[k] and not (v['buy'] and v['sell'])
                                    for k, v in fill_plan.items()))
        for key, delta in net.items():
            pos = self.position(key)
            if delta > 0:
                assigned_cash = fill_plan[key]['cash'] if reconciled_fills else 0
                known = reconciled_fills and assigned_cash < 0
                pos.add(delta, Fraction(-assigned_cash) if known else None,
                        'complex_transaction_or_external_transfer_cost_unknown')
            else:
                pos.remove(-delta)
        # Mixed in/out transactions can hide changes in composition even with net zero.
        for key in (incoming_keys & outgoing_keys) if not reconciled_fills else ():
            pos = self.position(key)
            if pos.quantity:
                pos.cost, pos.reason = None, 'mixed_in_out_transaction_cost_unknown'
        self.audit.append({'transaction_hash': receipt['transactionHash'],
                           'block_number': int(receipt['blockNumber'], 16),
                           'net_usdt': number(cash), 'redemption_events': redemption_count,
                           'has_account_order_fill': has_trade,
                           'trade_cost_attribution': 'reconciled_order_fills' if reconciled_fills else 'not_applied',
                           'share_movements': [{**m, 'key': [m['key'][0], str(m['key'][1])],
                                                'quantity': str(m['quantity'])} for m in movements]})

    def verify(self):
        for key, pos in self.positions.items():
            actual = self.chain.balance(key, self.account, self.chain.end)
            if actual != pos.quantity:
                raise RpcError(f'Ending share balance mismatch for token {key[1]}; export not complete')


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--account', default=DEFAULT_ACCOUNT)
    p.add_argument('--rpc-env', default='BSC_RPC_URL')
    p.add_argument('--from-block', type=int, default=0)
    p.add_argument('--to-block', type=int)
    p.add_argument('--chunk-size', type=int, default=100)
    p.add_argument('--confirmations', type=int, default=64)
    p.add_argument('--result-cap', type=int, default=1000)
    p.add_argument('--request-delay', type=float, default=0.15)
    p.add_argument('--timeout', type=float, default=60)
    p.add_argument('--output', default='predict-account-ledger.json')
    args = p.parse_args(argv)
    if min(args.from_block, args.confirmations, args.request_delay) < 0 or min(args.chunk_size, args.result_cap, args.timeout) <= 0:
        p.error('Invalid numeric arguments')
    url = os.environ.get(args.rpc_env, '')
    if not url.startswith(('https://', 'http://')):
        p.error(f'Set {args.rpc_env} to a BSC RPC URL')
    output = Path(args.output).resolve()
    raw_path = output.with_suffix('.chain.jsonl')
    progress = output.with_suffix('.progress.json')
    if any(x.exists() for x in (output, raw_path, progress)):
        p.error('Output/cache exists; choose a new --output name')
    try:
        account = address(args.account)
        rpc = Rpc(url, timeout=args.timeout, delay=args.request_delay)
        if int(rpc.call('eth_chainId', []), 16) != 56:
            raise RpcError('Expected BSC mainnet chain ID 56')
        latest = int(rpc.call('eth_blockNumber', []), 16)
        end = args.to_block if args.to_block is not None else max(0, latest-args.confirmations)
        if not args.from_block <= end <= latest:
            raise ValueError('Invalid block range')
        chain = Chain(rpc, end)
        ledger = Ledger(chain, account, args.from_block)
        meta = {'account': account, 'chain_id': 56, 'from_block': args.from_block,
                'to_block': end, 'complete': False, 'last_scanned_block': args.from_block-1,
                'cost_method': 'moving_weighted_average; transfer-reconciled OrderFilled; gas excluded',
                'limitations': ['Names absent without on-chain text mapping.',
                    'External transfers, opening holdings, split/conversion and complex multi-position costs may be unknown.',
                    'Redemption rows only; unredeemed settled positions are not claimed history.',
                    'Scan completion does not mean all cost fields are known or platform accounting is identical.']}
        output.parent.mkdir(parents=True, exist_ok=True)
        seen = set()
        with raw_path.open('w', encoding='utf-8') as raw:
            for upper, discovered in scan(rpc, account, args.from_block, end, args.chunk_size, args.result_cap):
                discovered.sort(key=lambda l: (int(l['blockNumber'], 16), int(l['transactionIndex'], 16), int(l['logIndex'], 16)))
                grouped = defaultdict(list)
                for log in discovered:
                    grouped[log['transactionHash'].lower()].append(log)
                for tx, tx_logs in grouped.items():
                    if tx in seen:
                        continue
                    receipt = rpc.call('eth_getTransactionReceipt', [tx])
                    if not receipt or int(receipt['status'], 16) != 1:
                        raise RpcError('Missing/failed receipt')
                    if any(receipt['blockHash'].lower() != l['blockHash'].lower() for l in tx_logs):
                        raise RpcError('Receipt block mismatch/reorg')
                    for log in tx_logs:
                        if not any(all(other.get(k) == log.get(k) for k in ('address', 'topics', 'data', 'logIndex')) for other in receipt['logs']):
                            raise RpcError('Discovered log missing from receipt')
                    block = rpc.call('eth_getBlockByNumber', [receipt['blockNumber'], False])
                    if not block or block['hash'].lower() != receipt['blockHash'].lower():
                        raise RpcError('Block reorg detected')
                    timestamp = datetime.fromtimestamp(int(block['timestamp'], 16), timezone.utc).isoformat()
                    raw.write(json.dumps({'receipt': receipt, 'timestamp_utc': timestamp})+'\n')
                    raw.flush()
                    ledger.process(receipt, timestamp)
                    seen.add(tx)
                meta['last_scanned_block'] = upper
                meta['transactions_processed'] = len(seen)
                write_json(progress, meta)
                print(f'Block {upper}/{end}; transactions={len(seen)}; redeemed rows={len(ledger.rows)}', file=sys.stderr)
        ledger.verify()
        meta['complete'] = True
        meta['costs_complete'] = all(r['holding_cost_usdt'] is not None for r in ledger.rows)
        meta['record_count'] = len(ledger.rows)
        holdings = []
        for key, pos in ledger.positions.items():
            if not pos.quantity:
                continue
            identity = chain.describe_position(key)
            numerator, denominator = identity.pop('payout_numerator'), identity.pop('payout_denominator')
            value = pos.quantity * numerator // denominator if denominator else None
            holdings.append({'ctf_contract': key[0], 'token_id': str(key[1]), **identity,
                             'market_name': None, 'outcome_name': None,
                             'shares': number(pos.quantity), 'cost_usdt': number(pos.cost),
                             'average_cost_price': number(pos.cost/pos.quantity, 1) if pos.cost is not None else None,
                             'status': 'settled_unredeemed' if denominator else 'unresolved' if denominator == 0 else 'unknown',
                             'result': ('lost' if numerator == 0 else 'won' if numerator == denominator else 'partial_payout') if denominator else None,
                             'estimated_payout_collateral_units': number(value),
                             'cost_unknown_reason': pos.reason})
        write_json(output, {'metadata': meta, 'redemptions': ledger.rows, 'remaining_holdings': holdings,
                            'transaction_audit': ledger.audit})
        write_json(progress, meta)
        print(f'Exported {len(ledger.rows)} redemption rows to {output}')
        return 0
    except (RpcError, ValueError, KeyError, OSError) as exc:
        message = str(exc) if isinstance(exc, (RpcError, ValueError)) else type(exc).__name__
        print(f'Incomplete ledger: {message}. Raw cache is not a complete report.', file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print('Interrupted; raw cache/progress are incomplete.', file=sys.stderr)
        return 130


if __name__ == '__main__':
    sys.exit(main())
