"""Correct on-chain PnL attribution for a single sniper position.

Replaces the "sum every SOL delta on the position's ATA" approach, which was
wrong in two independent ways (both confirmed on-chain 2026-07-24):

  1. RACE. It ran seconds after the close, summed whatever signatures were
     indexed at that instant, stamped the result final, and the
     `real_pnl_sol is not None` guard stopped it ever being retried. Bear
     summed 3 of 4 txs and missed the +0.832 sell -> booked -0.988 on a 0.04
     SOL stake. kcat summed 4 buys and no sells -> booked -1.272 on 0.05.

  2. CROSS-POSITION CONTAMINATION. The ATA is per-(wallet, mint), not
     per-trade. Any other purchase of the same mint - a manual buy from the
     UI, a re-entry - lands in the same account, and the naive sum swallowed
     those whole positions. Bear's real snipe was buy -0.041772 / sell
     +0.067507 = +0.0257, a WIN; the -1.014 buy next to it was a separate
     ~1 SOL trade that the old code charged to the snipe.

The fix is lot attribution. Our snipe bought a specific lot; later sells
liquidate from a pooled balance that may contain other lots. Proceeds are
attributed pro-rata by quantity (standard average-cost treatment for fungible
units), so a sell that dumps the whole pooled balance credits our lot only its
share.

Attribution itself does no I/O - callers supply already-fetched transactions. The
account-resolution helpers at the bottom take an `rpc` callable for the same
reason, so the live bot and the offline reconciler share ONE implementation
rather than drifting apart (they already did once: the reconciler learned about
Token-2022 and fast-ack phantom signatures while jup_trader did not, which left
exactly the largest positions permanently unbooked).
"""
from solders.pubkey import Pubkey

# a position is considered fully liquidated when this little of the lot remains
DUST_FRACTION = 0.005          # 0.5% of the original lot
# a long-only spot position cannot lose more than its cost plus some fee margin
FEE_MARGIN_SOL = 0.05


class TxView:
    """One transaction, reduced to what attribution needs.

    sol   : signed SOL change to OUR wallet (fees included)
    token : signed change in OUR token balance for this mint (raw units)
    ts    : block time, used to match a position to the buy that really landed
    """

    __slots__ = ("sig", "sol", "token", "err", "ts")

    def __init__(self, sig, sol, token, err=False, ts=None):
        self.sig = sig
        self.sol = sol
        self.token = token
        self.err = err
        self.ts = ts


def attribute(txs, buy_sig, sell_sig=None):
    """Attribute realized PnL to one position's lot.

    `txs` must be EVERY transaction touching the position's ATA, oldest first,
    with none missing - a gap silently changes the pro-rata denominator. The
    caller is responsible for that completeness and must pass `complete=False`
    to the guard below if it could not guarantee it.

    Returns a dict with the verdict. `pnl` is None whenever the position cannot
    be scored honestly, with `reason` naming what is missing.
    """
    lot = 0             # our position's remaining tokens
    pool = 0            # all tokens sitting in the ATA (ours, all lots)
    cost = None         # SOL paid by our buy, positive
    proceeds = 0.0      # SOL credited back to our lot
    lot_start = 0
    saw_our_sell = False
    foreign_lots = 0

    for t in txs:
        if t.err:
            # A failed tx still burns a fee. Charge it only when it is one of
            # OUR logged signatures (e.g. a sell that reverted); other lots'
            # failures are not this position's cost.
            if t.sig in (buy_sig, sell_sig):
                proceeds += t.sol
            continue

        if t.token > 0:                                   # a buy landed
            if t.sig == buy_sig:
                cost = -t.sol                             # sol is negative here
                lot += t.token
                lot_start = lot
            elif cost is not None and lot > 0:
                foreign_lots += 1                         # co-mingled with ours
            pool += t.token

        elif t.token < 0:                                 # a sell landed
            sold = -t.token
            if pool <= 0:
                continue
            share = lot / pool if pool else 0.0
            if lot > 0 and cost is not None:
                proceeds += t.sol * share
                lot -= sold * share
                if lot < 0:
                    lot = 0
                if t.sig == sell_sig:
                    saw_our_sell = True
            pool -= sold
            if pool < 0:
                pool = 0

        else:
            # No token movement: ATA rent reclaim, etc. Rent is recovered at the
            # wallet level and is not this trade's PnL, so it is only counted
            # when it is one of our own signatures.
            if t.sig in (buy_sig, sell_sig):
                proceeds += t.sol

    if cost is None:
        return {"pnl": None, "reason": "buy tx not found in ATA history"}

    closed = lot <= lot_start * DUST_FRACTION
    pnl = round(proceeds - cost, 6)
    return {
        "pnl": pnl if closed else None,
        "reason": None if closed else "lot not fully liquidated yet",
        "cost": round(cost, 6),
        "proceeds": round(proceeds, 6),
        "closed": closed,
        "residual_fraction": round(lot / lot_start, 6) if lot_start else 0.0,
        "saw_our_sell": saw_our_sell,
        "foreign_lots": foreign_lots,
    }


def guard(result, sell_sig=None, sell_sig_present=None, complete=True):
    """Decide whether a result is safe to persist as ground truth.

    Refusing to write is always the right failure mode here: a `None` gets
    retried by the reconciler, whereas a wrong number is stamped final and
    silently poisons every downstream edge decision - which is exactly how
    -0.988 got booked for a winning trade.
    """
    if result.get("pnl") is None:
        return False, result.get("reason") or "not scoreable"
    if not complete:
        return False, "tx history incomplete (RPC gap)"
    # A logged sell that isn't in the history means one of two very different
    # things. If the lot is still open, we are looking at a stale or truncated
    # signature list and must not score off it - that is the race that booked
    # -0.988 for a winning trade. If the lot is fully liquidated over a complete
    # history, the logged sell simply never landed and something else (a manual
    # sell, a retry) closed the position; the attribution is still sound, so
    # score it and flag the discrepancy rather than leaving it unscoreable.
    if sell_sig and sell_sig_present is False:
        if not result.get("closed"):
            return False, "logged sell_sig missing and lot still open"
        result["sell_sig_unlanded"] = True
    bound = -(result["cost"] + FEE_MARGIN_SOL)
    if result["pnl"] < bound:
        return False, (f"loss {result['pnl']:.4f} exceeds -(cost+fees) "
                       f"{bound:.4f} - attribution is wrong")
    return True, "ok"


# ---------------------------------------------------------------------------
# account resolution — shared so the bot and the reconciler cannot drift apart
# ---------------------------------------------------------------------------

SPL_TOKEN = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
ATOKEN_PROG = Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
_MINT_PROG = {}


def mint_program(rpc, mint):
    """The token program owning this mint. pump.fun issues BOTH classic SPL and
    Token-2022 mints and the ATA seeds differ, so assuming Tokenkeg derives an
    address that has never existed (PIPI KIWI is Token-2022: its real account is
    4V6KMV..., the SPL guess was 8kzA8d...)."""
    if mint in _MINT_PROG:
        return _MINT_PROG[mint]
    prog = SPL_TOKEN
    try:
        info = rpc("getAccountInfo", [mint, {"encoding": "jsonParsed"}])
        prog = ((info or {}).get("value") or {}).get("owner") or SPL_TOKEN
    except Exception:
        pass
    _MINT_PROG[mint] = prog
    return prog


def derive_ata(rpc, owner, mint):
    """The associated token account, computed rather than read off the buy tx.

    Reading it off the buy tx fails whenever the recorded signature is not the
    one that landed. Since /api/buy went fast-ack (`wait=False`, 07-14) the bot
    stores the SUBMITTED signature and never reconciles it against what
    confirmed, so a retried buy leaves a `sig` that does not exist on chain.
    Deriving also keeps working after the account is closed for rent, because
    signature history outlives the account."""
    return str(Pubkey.find_program_address(
        [bytes(Pubkey.from_string(owner)),
         bytes(Pubkey.from_string(mint_program(rpc, mint))),
         bytes(Pubkey.from_string(mint))], ATOKEN_PROG)[0])


def pick_buy_sig(views, recorded, open_ts, window_s=3600):
    """The buy that actually landed for this position.

    Trusts `recorded` when it is present in the history; otherwise falls back to
    the landed buy closest in time to when the position was opened. Returns None
    if nothing is within `window_s`, so an unrelated later buy of the same mint
    is never adopted as ours."""
    buys = [v for v in views if v.token > 0 and not v.err]
    if not buys:
        return None
    if recorded and any(v.sig == recorded for v in buys):
        return recorded
    if not open_ts:
        return None
    near = min(buys, key=lambda v: abs((v.ts or 0) - open_ts))
    if near.ts and abs(near.ts - open_ts) <= window_s:
        return near.sig
    return None
