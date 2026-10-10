"""Exact pump.fun bonding-curve fill simulation.

The paper sleeve marks exits with `sol_in * (exit_mcap/entry_mcap) * haircut`.
That is a fantasy: it prices an infinitely deep book, so it says a position is
worth its mark no matter how large the position is relative to the curve. It is
what booked "+124% TP" while the wallet bled, and on live trades where both
numbers exist the mcap mark and the real on-chain result barely correlate.

A pump.fun curve is a constant product over VIRTUAL reserves:

    k = vSOL * vTOKEN                       (invariant until graduation)
    buy :  tokens_out = vTOKEN - k / (vSOL + sol_in_after_fee)
    sell:  sol_out    = (vSOL - k / (vTOKEN + tokens_in)) * (1 - fee)

so the exact proceeds of dumping a specific number of tokens - including our own
price impact, which is the entire point - are computable.

The useful trick: the curve state never has to be fetched. Price per token is
vSOL/vTOKEN and k is invariant, so from a market cap alone

    price = mcap_sol / TOTAL_SUPPLY,  vSOL = sqrt(k*price),  vTOKEN = sqrt(k/price)

recovers the whole state. Marking a position therefore costs no extra RPC - it
runs off the mcap the mark loop already polls.

Everything here is pure arithmetic; validate against real fills with
_curve_sim_validate.py before trusting it to size anything.
"""
import math

# pump.fun launch constants (lamports / raw token units are NOT used here - all
# quantities are in whole SOL and whole tokens for readability)
INIT_VIRTUAL_SOL = 30.0
INIT_VIRTUAL_TOKENS = 1_073_000_000.0
TOTAL_SUPPLY = 1_000_000_000.0
K = INIT_VIRTUAL_SOL * INIT_VIRTUAL_TOKENS       # 3.219e10
DEFAULT_FEE = 0.01                               # 1% each side


def reserves_from_price(price_sol_per_token):
    """(vSOL, vTOKEN) for a given price, using the constant product invariant."""
    if price_sol_per_token <= 0:
        return None
    return math.sqrt(K * price_sol_per_token), math.sqrt(K / price_sol_per_token)


def reserves_from_mcap_sol(mcap_sol):
    return reserves_from_price(mcap_sol / TOTAL_SUPPLY)


def buy(mcap_sol, sol_in, fee=DEFAULT_FEE):
    """Tokens received for `sol_in` at this mcap, after fee and own impact."""
    r = reserves_from_mcap_sol(mcap_sol)
    if not r or sol_in <= 0:
        return 0.0
    vsol, vtok = r
    eff = sol_in * (1 - fee)
    return vtok - K / (vsol + eff)


def sell(mcap_sol, tokens_in, fee=DEFAULT_FEE):
    """(sol_out, mcap_after) for dumping `tokens_in` at this mcap."""
    r = reserves_from_mcap_sol(mcap_sol)
    if not r or tokens_in <= 0:
        return 0.0, mcap_sol
    vsol, vtok = r
    gross = vsol - K / (vtok + tokens_in)
    vsol2, vtok2 = vsol - gross, vtok + tokens_in
    return gross * (1 - fee), (vsol2 / vtok2) * TOTAL_SUPPLY


def sell_chunks(marks, tokens_total, fee=DEFAULT_FEE):
    """Proceeds of selling `tokens_total` in equal chunks at successive marks.

    Each chunk walks the curve down for the next one, which is exactly what the
    fast-exit sleeve does on chain and what a single mcap ratio cannot express.
    `marks` is the mcap at each chunk (len(marks) chunks).
    """
    if not marks or tokens_total <= 0:
        return 0.0
    per = tokens_total / len(marks)
    out, drift = 0.0, 0.0
    for m in marks:
        # carry our own impact forward: a later chunk sells into the curve our
        # earlier chunk already pushed down
        eff = max(m - drift, 1e-9)
        sol, after = sell(eff, per, fee)
        drift += eff - after
        out += sol
    return out


def simulate_position(entry_mcap_sol, sol_in, exit_marks_sol, fee=DEFAULT_FEE):
    """Realistic PnL for a paper position, in SOL.

    entry_mcap_sol / exit_marks_sol are market caps denominated in SOL. Returns
    None when inputs are unusable rather than a misleading zero.
    """
    if not entry_mcap_sol or not exit_marks_sol or sol_in <= 0:
        return None
    marks = [m for m in exit_marks_sol if m and m > 0]
    if not marks:
        return None
    tokens = buy(entry_mcap_sol, sol_in, fee)
    if tokens <= 0:
        return None
    return round(sell_chunks(marks, tokens, fee) - sol_in, 6)
