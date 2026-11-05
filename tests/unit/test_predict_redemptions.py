import importlib.util
import json
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    'predict_redemptions', Path(__file__).resolve().parents[2] / 'scripts/predict_redemptions.py'
)
scanner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(scanner)


def word(n):
    return f'{n:064x}'


def event(adapter=False, payout=0):
    condition = 'ab' * 32
    account = scanner.DEFAULT_ACCOUNT
    if adapter:
        contract = list(scanner.CONTRACTS)[3]
        topics = [scanner.ADAPTER_TOPIC, '0x' + account[2:].rjust(64, '0'), '0x' + condition]
        data = word(64) + word(payout) + word(2) + word(10**18) + word(0)
    else:
        contract = list(scanner.CONTRACTS)[0]
        topics = [scanner.CTF_TOPIC, '0x' + account[2:].rjust(64, '0'),
                  '0x' + scanner.USDT[2:].rjust(64, '0'), '0x' + '00' * 32]
        data = condition + word(96) + word(payout) + word(2) + word(1) + word(2)
    return dict(address=contract, topics=topics, data='0x' + data,
                transactionHash='0x' + '12' * 32, blockHash='0x' + '34' * 32,
                blockNumber='0xa', transactionIndex='0x0', logIndex='0x2')


def test_ctf_zero_payout_is_retained_and_quantity_is_unknown():
    row = scanner.decode_redemption(event(), scanner.DEFAULT_ACCOUNT)
    assert row['payout_usdt'] == '0'
    assert row['index_sets'] == ['1', '2']
    assert row['redeemed_amounts'] is None


def test_adapter_precise_amount_and_different_abi():
    row = scanner.decode_redemption(event(True, 10**18 + 1), scanner.DEFAULT_ACCOUNT)
    assert row['payout_usdt'] == '1.000000000000000001'
    assert row['redeemed_amounts'] == ['1', '0']
    assert row['condition_id'] == '0x' + 'ab' * 32
    assert row['index_sets'] is None


def test_reject_wrong_account_and_truncated_data():
    with pytest.raises(ValueError, match='another redeemer'):
        scanner.decode_redemption(event(), '0x' + '11' * 20)
    log = event()
    log['data'] = log['data'][:-2]
    with pytest.raises(ValueError):
        scanner.decode_redemption(log, scanner.DEFAULT_ACCOUNT)


def test_range_shrink_has_no_gaps():
    class Rpc:
        def call(self, method, params):
            query = params[0]
            start, end = int(query['fromBlock'], 16), int(query['toBlock'], 16)
            assert query['topics'][1].endswith(scanner.DEFAULT_ACCOUNT[2:])
            if end - start + 1 > 2:
                raise scanner.RangeLimit()
            return []
    assert [upper for upper, _ in scanner.scan_logs(Rpc(), scanner.DEFAULT_ACCOUNT, 0, 6, 8)] == [0, 1, 2, 3, 4, 5, 6]


def test_result_cap_never_silently_truncates():
    class Rpc:
        def call(self, method, params):
            return [event()]
    with pytest.raises(scanner.RpcError, match='complete single block'):
        list(scanner.scan_logs(Rpc(), scanner.DEFAULT_ACCOUNT, 10, 10, 1, result_cap=1))


class FakeRpc:
    def __init__(self, status='0x1'):
        self.status = status

    def call(self, method, params):
        log = event(True, 10**18)
        if method == 'eth_getLogs':
            return [log, log]  # deduplicate RPC repeats
        if method == 'eth_getTransactionReceipt':
            return {'status': self.status, 'blockHash': log['blockHash'], 'logs': [log]}
        if method == 'eth_getBlockByNumber':
            return {'hash': log['blockHash'], 'timestamp': '0x0'}
        raise AssertionError(method)


def test_collect_checks_receipt_and_writes_complete_export(tmp_path):
    path = tmp_path / 'result.json'
    rows = scanner.collect(FakeRpc(), scanner.DEFAULT_ACCOUNT, 10, 10, 1, path, {})
    assert len(rows) == 1
    saved = json.loads(path.read_text())
    assert saved['metadata']['complete'] is True
    assert saved['metadata']['last_scanned_block'] == 10
    assert rows[0]['market_name'] is None
    assert rows[0]['timestamp_utc'] == '1970-01-01T00:00:00+00:00'


def test_failed_receipt_does_not_write_success(tmp_path):
    path = tmp_path / 'result.json'
    with pytest.raises(scanner.RpcError, match='Receipt'):
        scanner.collect(FakeRpc('0x0'), scanner.DEFAULT_ACCOUNT, 10, 10, 1, path, {})
    assert not path.exists()
    assert json.loads(path.with_suffix('.progress.json').read_text())['complete'] is False


def test_market_names_do_not_filter_records(tmp_path):
    path = tmp_path / 'trades.json'
    path.write_text(json.dumps([{'market': {'id': 12, 'question': 'Example',
                                          'conditionId': '0xAB'}}]), encoding='utf-8-sig')
    assert scanner.load_markets(path)['0xab']['market_name'] == 'Example'
    assert scanner.units(2**256 - 1).replace('.', '').isdigit()


@pytest.mark.parametrize('failure,expected', [
    (scanner.HTTPError('https://rpc/SECRET', 429, 'SECRET', {}, None), 'HTTP 429'),
    (scanner.URLError(scanner.socket.gaierror(-2, 'SECRET')), 'DNS resolution failed'),
    (TimeoutError('SECRET'), 'Request timeout'),
    (ValueError('SECRET'), 'Invalid JSON response'),
])
def test_rpc_diagnostic_preserves_category_without_secrets(monkeypatch, capsys, failure, expected):
    def fail(*args, **kwargs):
        raise failure
    monkeypatch.setattr(scanner, 'urlopen', fail)
    monkeypatch.setattr(scanner.time, 'sleep', lambda _: None)
    with pytest.raises(scanner.RpcError, match=expected) as exc:
        scanner.Rpc('https://rpc/SECRET', retries=2).call('eth_getLogs', [])
    assert 'SECRET' not in str(exc.value) + capsys.readouterr().err


def test_diagnose_writes_no_files(monkeypatch, tmp_path):
    class ProbeRpc:
        def __init__(self, *args, **kwargs):
            pass
        def call(self, method, params):
            return {'eth_chainId': '0x38', 'eth_blockNumber': '0x100', 'eth_getLogs': []}[method]
    monkeypatch.setattr(scanner, 'Rpc', ProbeRpc)
    monkeypatch.setenv('BSC_RPC_URL', 'https://rpc/SECRET')
    assert scanner.main(['--diagnose', '--output', str(tmp_path / 'out.json')]) == 0
    assert list(tmp_path.iterdir()) == []


def test_413_is_range_limit_only_for_getlogs(monkeypatch):
    def fail(*args, **kwargs):
        raise scanner.HTTPError('https://rpc/SECRET', 413, 'SECRET', {}, None)
    monkeypatch.setattr(scanner, 'urlopen', fail)
    monkeypatch.setattr(scanner.time, 'sleep', lambda _: None)
    with pytest.raises(scanner.RangeLimit, match='HTTP 413'):
        scanner.Rpc('https://rpc/SECRET').call('eth_getLogs', [])
    with pytest.raises(scanner.RpcError, match='HTTP 413'):
        scanner.Rpc('https://rpc/SECRET').call('eth_call', [])
