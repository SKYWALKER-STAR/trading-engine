import importlib.util
from pathlib import Path
import sys
from fractions import Fraction

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / 'scripts'
sys.path.insert(0, str(SCRIPTS))
spec = importlib.util.spec_from_file_location('predict_account_ledger', SCRIPTS / 'predict_account_ledger.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

ACCOUNT = m.DEFAULT_ACCOUNT
CTF = m.CTFS[0]
EXCHANGE = next(iter(m.EXCHANGES))
TOKEN = 42
UNIT = 10**18
CONDITION = '0x' + 'ab'*32


def log(contract, topics, values, index=1):
    return {'address': contract, 'topics': topics, 'data': '0x'+''.join(m.word(v) for v in values),
            'logIndex': hex(index), 'transactionHash': '0x'+'11'*32,
            'blockNumber': '0x64', 'transactionIndex': '0x0', 'blockHash': '0x'+'22'*32}


def shares(sender, receiver, qty, index=1, token=TOKEN):
    return log(CTF, [m.SINGLE, m.topic_address(EXCHANGE), m.topic_address(sender),
                     m.topic_address(receiver)], [token, qty], index)


def cash(sender, receiver, qty, index=2):
    return log(m.USDT, [m.TRANSFER, m.topic_address(sender), m.topic_address(receiver)], [qty], index)


def fill(buy, qty, value, fee=0, token=TOKEN, index=3):
    fields = [0, token, value, qty, fee] if buy else [token, 0, qty, value, fee]
    return log(EXCHANGE, [m.FILLED, '0x'+'33'*32, m.topic_address(ACCOUNT),
                          m.topic_address(EXCHANGE)], fields, index)


def receipt(logs):
    return {'logs': logs, 'transactionHash': '0x'+'11'*32, 'blockNumber': '0x64'}


class Chain:
    end = 100
    def __init__(self, opening=0, numerator=1, denominator=1):
        self.opening = opening
        self.numerator, self.denominator = numerator, denominator
    def balance(self, key, account, block):
        return self.opening
    def redemption_positions(self, row):
        return [((CTF, TOKEN), 1, None, self.numerator, self.denominator)]


def redeem(qty, payout):
    r = log(CTF, [m.CTF_TOPIC, m.topic_address(ACCOUNT), m.topic_address(m.USDT), m.ZERO32],
            [CONDITION, 96, payout, 1, 1], 5)
    return receipt(([shares(ACCOUNT, m.ZERO, qty)] if qty else [])+[r])


def buy(ledger, qty, value):
    ledger.process(receipt([shares(EXCHANGE, ACCOUNT, qty), cash(ACCOUNT, EXCHANGE, value),
                            fill(True, qty, value)]), '2026-10-01T00:00:00+00:00')


def test_average_cost_partial_sale_then_claim():
    ledger = m.Ledger(Chain(), ACCOUNT, 0)
    buy(ledger, 10*UNIT, 4*UNIT)
    buy(ledger, 10*UNIT, 8*UNIT)
    ledger.process(receipt([shares(ACCOUNT, EXCHANGE, 5*UNIT), cash(EXCHANGE, ACCOUNT, 4*UNIT),
                            fill(False, 5*UNIT, 4*UNIT)]), 'sale')
    ledger.process(redeem(15*UNIT, 15*UNIT), 'claim')
    row = ledger.rows[0]
    assert row['shares'] == '15'
    assert row['holding_cost_usdt'] == '9'
    assert row['average_cost_price'] == '0.6'
    assert row['pnl_usdt'] == '6'
    assert ledger.positions[(CTF, TOKEN)].quantity == 0


@pytest.mark.parametrize('numerator,denominator,payout,result,pnl', [
    (0, 1, 0, 'lost', '-6'), (1, 2, 5*UNIT, 'partial_payout', '-1'),
])
def test_losing_and_half_payout(numerator, denominator, payout, result, pnl):
    ledger = m.Ledger(Chain(numerator=numerator, denominator=denominator), ACCOUNT, 0)
    buy(ledger, 10*UNIT, 6*UNIT)
    ledger.process(redeem(10*UNIT, payout), 'claim')
    assert ledger.rows[0]['result'] == result
    assert ledger.rows[0]['pnl_usdt'] == pnl


def test_external_transfer_and_opening_cost_are_not_zero():
    ledger = m.Ledger(Chain(opening=2*UNIT), ACCOUNT, 10)
    ledger.process(receipt([shares('0x'+'99'*20, ACCOUNT, 3*UNIT)]), 'in')
    buy(ledger, 5*UNIT, UNIT)
    ledger.process(redeem(10*UNIT, 10*UNIT), 'claim')
    assert ledger.rows[0]['holding_cost_usdt'] is None
    assert ledger.rows[0]['pnl_usdt'] is None
    assert ledger.rows[0]['shares'] == '10'


def test_noop_redemption_is_not_a_losing_position():
    ledger = m.Ledger(Chain(), ACCOUNT, 0)
    ledger.process(redeem(0, 0), 'claim')
    assert ledger.rows[0]['zero_quantity_event'] is True
    assert ledger.rows[0]['average_cost_price'] is None


def test_buy_fee_in_shares_uses_net_quantity():
    ledger = m.Ledger(Chain(), ACCOUNT, 0)
    ledger.process(receipt([shares(EXCHANGE, ACCOUNT, 10*UNIT),
                            shares(ACCOUNT, '0x'+'88'*20, UNIT, index=2),
                            cash(ACCOUNT, EXCHANGE, 5*UNIT, index=3),
                            fill(True, 10*UNIT, 5*UNIT, fee=UNIT, index=4)]), 'buy')
    assert ledger.positions[(CTF, TOKEN)].quantity == 9*UNIT
    assert ledger.positions[(CTF, TOKEN)].cost == 5*UNIT


def test_multi_position_order_fills_reconcile_cash_and_shares():
    ledger = m.Ledger(Chain(), ACCOUNT, 0)
    ledger.process(receipt([shares(EXCHANGE, ACCOUNT, 10*UNIT),
                            shares(EXCHANGE, ACCOUNT, 5*UNIT, index=2, token=43),
                            cash(ACCOUNT, EXCHANGE, 7*UNIT, index=3),
                            fill(True, 10*UNIT, 4*UNIT, index=4),
                            fill(True, 5*UNIT, 3*UNIT, token=43, index=5)]), 'buy')
    assert ledger.positions[(CTF, TOKEN)].cost == 4*UNIT
    assert ledger.positions[(CTF, 43)].cost == 3*UNIT


def test_mismatched_payout_is_not_allocated():
    ledger = m.Ledger(Chain(), ACCOUNT, 0)
    buy(ledger, UNIT, UNIT//2)
    ledger.process(redeem(UNIT, 2*UNIT), 'claim')
    assert ledger.rows[0]['payout_usdt'] is None
    assert ledger.rows[0]['pnl_usdt'] is None
    assert ledger.rows[0]['issues']


def test_underflow_fails_instead_of_inventing_position():
    p = m.Position()
    with pytest.raises(m.RpcError, match='underflow'):
        p.remove(1)


def test_transfer_batch_and_self_transfer():
    event = log(CTF, [m.BATCH, m.topic_address(EXCHANGE), m.topic_address(EXCHANGE),
                      m.topic_address(ACCOUNT)], [64, 160, 2, 42, 43, 2, UNIT, 2*UNIT])
    moves = m.transfer_rows(event, ACCOUNT)
    assert [x['quantity'] for x in moves] == [UNIT, 2*UNIT]
    assert not m.transfer_rows(shares(ACCOUNT, ACCOUNT, UNIT), ACCOUNT)


def test_fraction_cost_preserves_precision_and_clears_unknown_on_full_exit():
    p = m.Position()
    p.add(3, Fraction(1))
    cost, _ = p.remove(1)
    assert cost == Fraction(1, 3)
    assert p.cost == Fraction(2, 3)
    p.add(1, None, 'external')
    assert p.cost is None
    p.remove(3)
    p.add(2, Fraction(7))
    assert p.cost == 7 and p.reason is None


def test_extra_cash_flow_does_not_become_position_cost():
    ledger = m.Ledger(Chain(), ACCOUNT, 0)
    ledger.process(receipt([shares(EXCHANGE, ACCOUNT, 10*UNIT),
                            cash(ACCOUNT, EXCHANGE, 5*UNIT),
                            fill(True, 10*UNIT, 5*UNIT),
                            cash('0x'+'88'*20, ACCOUNT, UNIT, index=4)]), 'buy plus gift')
    assert ledger.positions[(CTF, TOKEN)].cost is None


def test_ending_balance_mismatch_is_fatal():
    ledger = m.Ledger(Chain(), ACCOUNT, 0)
    buy(ledger, UNIT, UNIT//2)
    with pytest.raises(m.RpcError, match='Ending share balance mismatch'):
        ledger.verify()


def test_http_413_retries_smaller_ranges_without_skipping():
    class Rpc:
        def __init__(self):
            self.successful_ranges = []
        def call(self, method, params):
            a = int(params[0]['fromBlock'], 16)
            b = int(params[0]['toBlock'], 16)
            if b-a >= 2:
                raise m.RangeLimit('HTTP 413')
            self.successful_ranges.append((a, b))
            return []
    rpc = Rpc()
    result = list(m.scan(rpc, ACCOUNT, 10, 15, 4, 1000))
    assert [upper for upper, logs in result] == [11, 13, 15]
    assert rpc.successful_ranges == [(10, 11)]*3+[(12, 13)]*3+[(14, 15)]*3


def test_chain_position_uses_abi_derived_token_and_historical_resolution():
    class Rpc:
        def call(self, method, params):
            assert method == 'eth_call'
            selector = params[0]['data'][2:10]
            result = {'856296f7': 55, '39dd7530': TOKEN, 'dd34de67': 2,
                      'd42dc0c2': 2, '0504c814': 1}[selector]
            if selector in ('dd34de67', 'd42dc0c2', '0504c814'):
                assert params[1] == '0x64'
            return '0x'+m.word(result)
    chain = m.Chain(Rpc(), 200)
    event = m.decode_redemption(redeem(UNIT, UNIT//2)['logs'][-1], ACCOUNT)
    assert chain.redemption_positions(event) == [((CTF, TOKEN), 1, None, 1, 2)]
