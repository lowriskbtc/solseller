import pytest

import curve_sim as cs
from flow_source import b58encode, decode_trade_event, ws_url_from_rpc
from kol_source import score, wilson_low


# ---------------- pump.fun bonding curve ----------------

def test_reserves_satisfy_the_invariant_and_price():
    vsol, vtok = cs.reserves_from_mcap_sol(60.0)
    assert vsol * vtok == pytest.approx(cs.K)
    assert vsol / vtok == pytest.approx(60.0 / cs.TOTAL_SUPPLY)
    assert cs.reserves_from_price(0) is None


def test_launch_state_is_recovered():
    launch_mcap = cs.INIT_VIRTUAL_SOL / cs.INIT_VIRTUAL_TOKENS * cs.TOTAL_SUPPLY
    vsol, vtok = cs.reserves_from_mcap_sol(launch_mcap)
    assert vsol == pytest.approx(cs.INIT_VIRTUAL_SOL) and vtok == pytest.approx(cs.INIT_VIRTUAL_TOKENS)


def test_fee_free_round_trip_returns_exactly_the_input():
    tokens = cs.buy(40.0, 1.0, fee=0.0)
    vsol, vtok = cs.reserves_from_mcap_sol(40.0)
    mcap_after_buy = (vsol + 1.0) / (vtok - tokens) * cs.TOTAL_SUPPLY
    sol_back, mcap_after_sell = cs.sell(mcap_after_buy, tokens, fee=0.0)
    assert sol_back == pytest.approx(1.0) and mcap_after_sell == pytest.approx(40.0)


def test_fees_and_impact_cost_money():
    assert cs.simulate_position(40.0, 1.0, [40.0]) < 0          # flat exit loses the fees
    small = cs.buy(40.0, 0.1) / 0.1
    large = cs.buy(40.0, 10.0) / 10.0
    assert large < small                                         # bigger buys get a worse price


def test_buy_fee_is_taken_off_the_sol_going_in():
    assert cs.buy(40.0, 1.0, fee=0.01) == pytest.approx(cs.buy(40.0, 0.99, fee=0.0))
    sol, _ = cs.sell(40.0, 1_000_000.0, fee=0.01)
    gross, _ = cs.sell(40.0, 1_000_000.0, fee=0.0)
    assert sol == pytest.approx(gross * 0.99)


def test_chunked_sale_equals_one_sale_at_a_flat_mark():
    tokens = cs.buy(40.0, 2.0)
    one, _ = cs.sell(40.0, tokens)
    assert cs.sell_chunks([40.0] * 4, tokens) == pytest.approx(one, rel=1e-6)


def test_selling_into_a_pump_profits():
    assert cs.simulate_position(40.0, 1.0, [120.0]) > 0


@pytest.mark.parametrize("args", [(0, 1.0, [50]), (40, 0, [50]), (40, 1.0, []), (40, 1.0, [0, None])])
def test_unusable_inputs_return_none(args):
    assert cs.simulate_position(*args) is None


# ---------------- wallet ranking ----------------

def test_wilson_lower_bound_known_values():
    assert wilson_low(0, 0) == 0.0
    assert wilson_low(1, 1) == pytest.approx(0.2065, abs=1e-3)
    assert wilson_low(114, 237) == pytest.approx(0.418, abs=2e-3)


def test_thin_perfect_record_never_outranks_a_real_one():
    fluke = {"trades": 1, "wins": 1, "pnl_sol": 50}
    real = {"trades": 237, "wins": 114, "pnl_sol": 50}
    assert score(real) > score(fluke)


def test_score_zero_for_losers_and_persistence_counts():
    assert score({"trades": 10, "wins": 9, "pnl_sol": -5}) == 0.0
    k = {"trades": 100, "wins": 55, "pnl_sol": 100}
    assert score({**k, "days_seen": 5}, days_total=5) > score({**k, "days_seen": 1}, days_total=5)


# ---------------- pump.fun event decoding ----------------

@pytest.mark.parametrize("raw,expected", [
    (b"hello world", "StV1DL6CwTryKyV"),
    (b"\x00\x00\x01", "112"),
    (bytes(32), "1" * 32),                     # the System Program address
])
def test_b58encode_vectors(raw, expected):
    assert b58encode(raw) == expected


def test_decode_trade_event_round_trip():
    import struct
    import flow_source as fs
    mint, user = bytes(range(32)), bytes(range(32, 64))
    blob = b"\x00" * 8 + mint + struct.pack("<QQ", 2_500_000_000, 123) + b"\x01" + user
    blob = blob.ljust(max(len(blob), fs.TRADE_EVENT_MIN_LEN), b"\x00")
    ev = decode_trade_event(blob)
    assert ev == {"mint": b58encode(mint), "txType": "buy", "solAmount": 2.5, "traderPublicKey": b58encode(user)}


def test_decode_rejects_short_or_absurd_events():
    import struct
    import flow_source as fs
    assert decode_trade_event(b"\x00" * 10) is None
    blob = (b"\x00" * 40 + struct.pack("<QQ", 0, 0) + b"\x00" * 33).ljust(fs.TRADE_EVENT_MIN_LEN, b"\x00")
    assert decode_trade_event(blob) is None                    # zero-SOL trade


def test_ws_url():
    assert ws_url_from_rpc("https://rpc.example/x") == "wss://rpc.example/x"
    assert ws_url_from_rpc("http://localhost:8899") == "ws://localhost:8899"
