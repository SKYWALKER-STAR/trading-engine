#!/usr/bin/env python3
"""Read-only Predict.fun BSC redemption scanner. Python 3.11+, standard library only.

ABI and addresses: https://github.com/PredictDotFun/sdk-python/tree/main/src/predict_sdk
Snapshot: 2026-11-05. See docs/predict-redemptions.md for coverage and limitations.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import sys
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

USDT = '0x55d398326f99059ff775485246999027b3197955'
DEFAULT_ACCOUNT = '0xadd1dd62b94a8c9744e6b5b1a3dae8db0d84e108'
# Ethereum Keccak-256 (NOT hashlib.sha3_256) of the official ABI signatures.
CTF_TOPIC = '0x2682012a4a4f1973119f1c9b90745d1bd91fa2bab387344f044cb3586864d18d'
ADAPTER_TOPIC = '0x9140a6a270ef945260c03894b3c6b3b2695e9d5101feef0ff24fec960cfd3224'
CONTRACTS = {
    '0x22da1810b194ca018378464a58f6ac2b10c9d244': ('ctf', 'ConditionalTokens'),
    '0x9400f8ad57e9e0f352345935d6d3175975eb1d9f': ('ctf', 'YieldBearingConditionalTokens'),
    '0xf64b0b318aaf83bd9071110af24d24445719a07f': ('ctf', 'YieldBearingNegRiskConditionalTokens'),
    '0xc3cf7c252f65e0d8d88537df96569ae94a7f1a6e': ('adapter', 'NegRiskAdapter'),
    '0x41dce1a4b8fb5e6327701750af6231b7cd0b2a40': ('adapter', 'YieldBearingNegRiskAdapter'),
}


class RpcError(RuntimeError):
    pass


class RangeLimit(RpcError):
    pass


class Rpc:
    def __init__(self, url, timeout=45, retries=4, delay=0.15):
        self.url, self.timeout, self.retries, self.delay = url, timeout, retries, delay
        self.counter = 0

    def call(self, method, params):
        self.counter += 1
        request = Request(self.url, data=json.dumps({
            'jsonrpc': '2.0', 'id': self.counter, 'method': method, 'params': params,
        }).encode(), headers={'Content-Type': 'application/json', 'User-Agent': 'PredictRedemptions/1'})
        for attempt in range(self.retries):
            time.sleep(self.delay)
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    body = json.load(response)
                if 'error' in body:
                    error = body['error']
                    message = str(error.get('message', '')).lower()
                    # Never print provider messages, which may contain credential-bearing URLs.
                    range_terms = ('block range', 'too many results', 'response size',
                                   'query returned more', 'limit the query', 'log response size',
                                   'range is too wide', 'maximum block', 'max block',
                                   'limited to a', 'query timeout')
                    if method == 'eth_getLogs' and any(t in message for t in range_terms):
                        raise RangeLimit('RPC log range/result limit')
                    if error.get('code') in (-32005, -32016) or 'rate limit' in message or 'too many requests' in message:
                        raise TimeoutError('RPC temporarily limited')
                    raise RpcError(f'{method}: JSON-RPC error code {error.get("code")}; '
                                   'check provider permissions/history/log support')
                if 'result' not in body:
                    raise RpcError(f'{method}: missing result')
                return body['result']
            except HTTPError as exc:
                if exc.code not in (408, 429, 500, 502, 503, 504):
                    raise RpcError(f'{method}: HTTP {exc.code}; check RPC credentials/plan') from None
            except (URLError, TimeoutError, OSError, ValueError):
                pass
            if attempt + 1 < self.retries:
                time.sleep(min(2 ** attempt, 15))
        raise RpcError(f'{method}: failed after {self.retries} attempts; no range was skipped')


def address(value):
    if not re.fullmatch(r'0x[0-9a-fA-F]{40}', value):
        raise ValueError('Expected a 0x-prefixed 20-byte account address')
    return value.lower()


def units(raw, decimals=18):
    """Exact decimal string, safe for uint256 and frontend JSON consumers."""
    whole, fraction = divmod(int(raw), 10 ** decimals)
    suffix = str(fraction).zfill(decimals).rstrip('0')
    return str(whole) + ('.' + suffix if suffix else '')


def uint_word(data, offset):
    if offset < 0 or offset % 32 or offset + 32 > len(data):
        raise ValueError('Invalid ABI offset/truncated event')
    return int.from_bytes(data[offset:offset + 32], 'big')


def uint_array(data, head, head_size):
    offset = uint_word(data, head)
    if offset < head_size:
        raise ValueError('ABI array overlaps event head')
    count = uint_word(data, offset)
    if offset + 32 + count * 32 != len(data):
        raise ValueError('Invalid ABI array length')
    return [uint_word(data, offset + 32 + n * 32) for n in range(count)]


def decode_redemption(log, account):
    contract = address(log['address'])
    kind, label = CONTRACTS[contract]
    topics = [t.lower() for t in log['topics']]
    expected = CTF_TOPIC if kind == 'ctf' else ADAPTER_TOPIC
    if topics[0] != expected or len(topics) != (4 if kind == 'ctf' else 3):
        raise ValueError('Unexpected redemption ABI for configured contract')
    if topics[1] != '0x' + account[2:].rjust(64, '0'):
        raise ValueError('RPC returned another redeemer')
    data = bytes.fromhex(log['data'][2:])
    if kind == 'ctf':
        condition = '0x' + data[:32].hex()
        index_sets = uint_array(data, 32, 96)
        amounts = None  # CTF event does not contain redeemed share quantities.
        payout = uint_word(data, 64)
        collateral = '0x' + topics[2][-40:]
        parent = topics[3]
    else:
        condition = topics[2]
        amounts = uint_array(data, 0, 64)
        index_sets = None  # Keep original ordered amounts, do not guess outcome labels.
        payout = uint_word(data, 32)
        collateral, parent = USDT, None
    return {
        'id': f"56:{log['transactionHash'].lower()}:{int(log['logIndex'], 16)}",
        'transaction_hash': log['transactionHash'].lower(),
        'block_number': int(log['blockNumber'], 16),
        'block_hash': log['blockHash'].lower(),
        'transaction_index': int(log['transactionIndex'], 16),
        'log_index': int(log['logIndex'], 16),
        'redeemer': account, 'contract': contract, 'contract_name': label,
        'condition_id': condition, 'parent_collection_id': parent,
        'collateral_token': collateral,
        'payout_raw': str(payout),
        'payout_usdt': units(payout) if collateral == USDT else None,
        'index_sets': [str(x) for x in index_sets] if index_sets is not None else None,
        'redeemed_amounts_raw': [str(x) for x in amounts] if amounts is not None else None,
        'redeemed_amounts': [units(x) for x in amounts] if amounts is not None else None,
        'raw_log': log,
    }


def scan_logs(rpc, account, start, end, chunk_size, result_cap=1000):
    """Yield only complete block ranges. A suspicious full page is split, not truncated."""
    cursor, size = start, chunk_size
    while cursor <= end:
        upper = min(cursor + size - 1, end)
        try:
            logs = rpc.call('eth_getLogs', [{
                'fromBlock': hex(cursor), 'toBlock': hex(upper),
                'address': list(CONTRACTS),
                'topics': [[CTF_TOPIC, ADAPTER_TOPIC], '0x' + account[2:].rjust(64, '0')],
            }])
            if not isinstance(logs, list):
                raise RpcError('eth_getLogs returned a non-list')
            if len(logs) >= result_cap:
                raise RangeLimit('Possible provider result truncation')
        except RangeLimit:
            if upper == cursor:
                raise RpcError('RPC cannot return a complete single block; use another provider') from None
            size = max(1, (upper - cursor + 1) // 2)
            print(f'Reducing query range to {size} blocks', file=sys.stderr)
            continue
        for log in logs:
            if log.get('removed'):
                raise RpcError('Removed log encountered; rerun against a stable block range')
            if not cursor <= int(log['blockNumber'], 16) <= upper:
                raise RpcError('RPC returned a log outside requested range')
        yield upper, logs
        cursor = upper + 1


def load_markets(path):
    if not path:
        return {}
    with Path(path).open(encoding='utf-8-sig') as stream:
        trades = json.load(stream)
    markets = {}
    for trade in trades:
        market = trade.get('market', {})
        if market.get('conditionId'):
            markets[market['conditionId'].lower()] = {
                'market_id': str(market['id']),
                'market_name': market.get('question') or market.get('title'),
            }
    return markets


def write_json(path, data):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    temporary.replace(path)


def collect(rpc, account, start, end, chunk_size, output, markets, result_cap=1000):
    metadata = {
        'schema_version': 1, 'chain_id': 56, 'account': account,
        'from_block': start, 'to_block': end, 'last_scanned_block': start - 1,
        'complete': False, 'contracts': CONTRACTS,
        'coverage': 'Configured contracts and inclusive block range only; redemption events, not all settled positions.',
        'amount_note': 'payout_usdt is the event payout, not profit or a separately verified wallet balance change.',
    }
    partial = output.with_suffix('.partial.jsonl')
    progress = output.with_suffix('.progress.json')
    seen, records, blocks, receipts = set(), [], {}, {}
    write_json(progress, metadata)
    with partial.open('w', encoding='utf-8') as stream:
        for upper, logs in scan_logs(rpc, account, start, end, chunk_size, result_cap):
            for log in sorted(logs, key=lambda x: (int(x['blockNumber'], 16), int(x['logIndex'], 16))):
                row = decode_redemption(log, account)
                if row['id'] in seen:
                    continue
                tx = row['transaction_hash']
                if tx not in receipts:
                    receipts[tx] = rpc.call('eth_getTransactionReceipt', [tx])
                receipt = receipts[tx]
                if not receipt or int(receipt['status'], 16) != 1 or receipt['blockHash'].lower() != row['block_hash']:
                    raise RpcError('Receipt missing/failed/reorg detected; no complete output written')
                matching = [x for x in receipt['logs'] if int(x['logIndex'], 16) == row['log_index']]
                if len(matching) != 1 or any(matching[0][k] != log[k] for k in ('address', 'topics', 'data')):
                    raise RpcError('Log does not match transaction receipt')
                block_hash = row['block_hash']
                if block_hash not in blocks:
                    blocks[block_hash] = rpc.call('eth_getBlockByNumber', [hex(row['block_number']), False])
                block = blocks[block_hash]
                if not block or block['hash'].lower() != block_hash:
                    raise RpcError('Block reorg detected; rerun scan')
                row['timestamp_utc'] = datetime.fromtimestamp(int(block['timestamp'], 16), timezone.utc).isoformat()
                row['market_id'], row['market_name'] = None, None
                row.update(markets.get(row['condition_id'], {}))
                row['transaction_status'] = 'success'
                seen.add(row['id'])
                records.append(row)
                stream.write(json.dumps(row, ensure_ascii=False) + '\n')
            stream.flush()
            metadata['last_scanned_block'] = upper
            metadata['record_count'] = len(records)
            write_json(progress, metadata)
            print(f'Scanned through {upper}/{end}; redemptions={len(records)}', file=sys.stderr)
    metadata['complete'] = True
    metadata['completed_at_utc'] = datetime.now(timezone.utc).isoformat()
    write_json(output, {'metadata': metadata, 'records': records})
    write_json(progress, metadata)
    return records


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--account', default=DEFAULT_ACCOUNT)
    parser.add_argument('--rpc-env', default='BSC_RPC_URL', help='Environment variable holding RPC URL')
    parser.add_argument('--from-block', type=int, default=0, help='Inclusive; 0 scans all chain history')
    parser.add_argument('--to-block', type=int, help='Inclusive; default latest minus confirmations')
    parser.add_argument('--confirmations', type=int, default=64)
    parser.add_argument('--chunk-size', type=int, default=50000)
    parser.add_argument('--result-cap', type=int, default=1000, help='Split at this many logs; set to provider cap or lower')
    parser.add_argument('--trades-json', help='Optional local trades JSON for market names; never filters records')
    parser.add_argument('--output', default='predict-redemptions.json')
    args = parser.parse_args(argv)
    url = os.environ.get(args.rpc_env, '')
    if not url.startswith(('https://', 'http://')):
        parser.error(f'Set {args.rpc_env} to a BSC RPC URL supporting historical eth_getLogs')
    if args.from_block < 0 or args.confirmations < 0 or args.chunk_size <= 0 or args.result_cap <= 0:
        parser.error('Invalid block range, confirmations, chunk size or result cap')
    output = Path(args.output).resolve()
    if any(p.exists() for p in (output, output.with_suffix('.partial.jsonl'), output.with_suffix('.progress.json'))):
        parser.error('Output or progress files already exist; choose a new --output path')
    try:
        account = address(args.account)
        markets = load_markets(args.trades_json)
        rpc = Rpc(url)
        if int(rpc.call('eth_chainId', []), 16) != 56:
            raise RpcError('RPC must be BNB Smart Chain mainnet (chain ID 56)')
        latest = int(rpc.call('eth_blockNumber', []), 16)
        end = args.to_block if args.to_block is not None else max(0, latest - args.confirmations)
        if not args.from_block <= end <= latest:
            raise ValueError('Invalid scan bounds; end must not exceed latest block')
        output.parent.mkdir(parents=True, exist_ok=True)
        records = collect(rpc, account, args.from_block, end, args.chunk_size, output, markets, args.result_cap)
        print(f'Complete: {len(records)} redemption events. Output: {output}')
        return 0
    except (RpcError, ValueError, OSError, KeyError) as exc:
        # Do not include network URLs or user credentials in error output.
        detail = str(exc) if isinstance(exc, (RpcError, ValueError)) else type(exc).__name__
        print(f'Scan failed: {detail}. Partial/progress files are NOT a complete export.', file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print('Interrupted. Partial/progress files are NOT a complete export.', file=sys.stderr)
        return 130


if __name__ == '__main__':
    sys.exit(main())
