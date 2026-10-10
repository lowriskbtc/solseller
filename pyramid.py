"""
pyramid.py — clock-driven scale-in policy (probe → cut-or-pyramid → tranche out).

WHY THIS EXISTS
---------------
Teardown of `<target wallet>` (2026-07-28,
25,999 txs / 4,906 closed positions over 4 days, +328.8 SOL realized and
318.16 SOL of it swept to a treasury wallet, so the number is confirmed
off our own math). Full writeup: SNIPER_TEARDOWN_64hP97.md.

That wallet is not fast. Median entry is 42s / 100 slots AFTER mint creation —
it buys tokens that already have flow. Its entire edge is position management:

    probe ~0.86 SOL (hard ceiling 1.50, never exceeded in 4,906 positions)
    cut at median 29s if the launch is dead (median -7.8%)
    otherwise add ~1.0x the probe every ~60-70s (48s, 111s, 183s, 249s, 326s…)
    exit in even ~19% tranches, 4+ of them

Split by how many buys a position ended up with:

    1 buy  (never added)  n=2160  win 29.5%  ROC -5.91%  net -102.3 SOL
    5 buys                n= 223  win 71.3%  ROC +6.62%  net  +81.4 SOL
    8+ buys               n= 255  win 79.2%  ROC +6.07%  net +189.0 SOL

Their single-shot book LOSES 102 SOL. The add ladder makes +431 SOL. Our sniper
is a single-shot book: `sol_per_snipe` fires once and never adds. We are, by
construction, their losing cohort. That is what this module is for.

WHAT IS AND IS NOT MEASURED
---------------------------
Measured from their chain history: probe size and its ceiling, the add cadence,
the add multiple (~0.98x the probe), tranche shape (~19% each), and that adds are
NOT price-gated (add price is p25 -29%, p75 +31% vs first entry — they add into
weakness as readily as strength).

INFERRED, and flagged as such everywhere it matters: the *cut predicate*. We only
observe that un-added probes were sold at median 29s / -7.8%, and that p75 of
those exits is +2.4% — so at least a quarter of their cuts fire while flat or UP.
A pure "-8% at 30s" price rule cannot reproduce that, which is why the alive-test
below is primarily FLOW (is a crowd still trading this), with price only as a
crash brake. See `_alive`.

DEPLOYMENT CONTRACT (house rule: nothing enforces without forward evidence)
--------------------------------------------------------------------------
Phase 1 — GHOST. This engine simulates the ladder against the same mark stream
`_mark_and_exit` already produces for every position, and journals the result to
pyramid_compare.jsonl next to the real (fixed/fast-exit) outcome on the SAME
trade. It executes nothing, changes no position, and costs no throughput.
`pyramid_live` is hard-gated false and is checked independently of the top-level
`live` flag, the same way the graduating sleeve is pinned to paper.

Phase 2 — LIVE, only after the scoreboard says so ON REAL FILLS.

A paper win is NOT sufficient to promote, and the reason is specific: `entry_mcap`
is stamped from the detection event and sits a median 1.58x BELOW the curve state
a buy actually lands in (measured n=22, 2026-07-24; see `entry_slip_x` and
curve_sim.py, which was built for exactly this and is explicitly not trusted to
mark from). The fixed arm books ONE optimistic entry per position. A pyramid
books up to nine. Same-trade ranking cancels a constant bias, not a bias that
scales with leg count — so this engine is *structurally* flattered by paper marks
and the ghost scoreboard must be read as an upper bound, never as a green light.

The gate for going live is real fills at small size, not this scoreboard.

Sizing: `pyr_probe_sol` defaults to 0.02 against today's `sol_per_snipe` 0.1. A
ladder that fully develops (probe + 8 adds) reaches ~0.18 SOL, so a fully-
developed winner costs roughly what one flat snipe costs today — the point is not
to spend more, it is to spend it on the positions that survived 30 seconds.

This module never opens sockets, never touches files, and never blocks. Flow
evidence is READ from smart_exit's engine (`flow_snapshot`) so we reuse the
existing per-mint Helius subscription rather than opening a second one.
"""

from __future__ import annotations

import threading
import time
from collections import deque

TRADE_KEEP_S = 120       # flow memory per mint; rules read <= flow_window_s
TRADE_MAXLEN = 600       # hard cap so a hot mint cannot grow unbounded

# Every knob is overridable through sniper_config.json (see build_cfg).
# Defaults are their measured values, scaled to our capital where noted.
DEFAULTS = {
    # --- entry -----------------------------------------------------------
    "probe_sol": 0.02,        # scaled from their 0.86 (see header)
    "probe_max_sol": 0.03,    # mirrors their hard 1.50 ceiling, scaled
    "entry_delay_s": 0.0,     # their median is 42s. Deliberately 0 here: the
                              # delayed entry and the ladder are two different
                              # hypotheses and must not be tested at once.
    # --- the cut (INFERRED — see header) ----------------------------------
    "cut_s": 30.0,            # their un-added probes exit at median 29s
    "cut_ret": -8.0,          # % — their median un-added exit is -7.8%
    "min_prints": 3,          # crowd evidence required in the flow window
    "flow_window_s": 20.0,
    # --- the ladder -------------------------------------------------------
    "first_add_s": 48.0,      # their measured median first add
    "add_gap_s": 63.0,        # measured spacing: 48,111,183,249,326,412,508
    "add_mult": 1.0,          # their median add is 0.98x the probe
    "max_adds": 8,
    "add_abort_ret": -35.0,   # % — stop adding (do not average into a crater)
    # --- the exit ---------------------------------------------------------
    "tranches": 5,            # ~19% each, matching their measured ladder
    "tranche_gap_s": 20.0,
    "max_age_s": 900.0,       # hard backstop, shared with timeout_min
}

# Same haircuts _close_snipe uses, so the ghost's PnL is comparable to the
# real book's mark rather than being optimistic in a second, different way.
HAIRCUT_GRAD = 0.97
HAIRCUT_CURVE = 0.90


def _f(v, d=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


class Position:
    """Simulated multi-leg book for one mint.

    Accounting note: token units are arbitrary but consistent — a leg of `sol`
    at market cap `mcap` buys `sol/mcap` units, so `units * mcap` is always SOL
    and the effective entry is the harmonic-weighted `cost / units`. That is the
    generalisation of the single-leg math in _close_snipe, which means a promoted
    live arm can hand `sol_in=cost` and `entry_mcap=effective_entry` straight to
    the existing close path without changing it.
    """

    __slots__ = ("mint", "t0", "legs", "sells", "cut", "done", "note",
                 "next_add_ts", "next_tranche_ts", "sold_frac", "peak_mcap",
                 "armed_ts", "opened", "last_mcap", "graduated", "pos",
                 "fixed_side", "real_closed_ts")

    def __init__(self, pos, now):
        self.mint = pos.get("mint")
        self.pos = {k: pos.get(k) for k in
                    ("mint", "symbol", "source", "mode", "sol_in",
                     "entry_mcap", "detected_ts")}
        self.t0 = _f(pos.get("detected_ts"), now)
        self.legs = []          # [(ts, mcap, sol)]
        self.sells = []         # [(ts, mcap, frac_of_total_units)]
        self.cut = False
        self.done = False
        self.note = None
        self.opened = False
        self.next_add_ts = None
        self.next_tranche_ts = None
        self.sold_frac = 0.0
        self.peak_mcap = _f(pos.get("entry_mcap"))
        self.last_mcap = pos.get("entry_mcap")
        self.graduated = False
        self.armed_ts = None    # first tick where we saw crowd evidence
        self.fixed_side = None  # real arm's outcome, captured when IT closed
        self.real_closed_ts = None

    # -- accounting --------------------------------------------------------

    @property
    def cost(self):
        return sum(l[2] for l in self.legs)

    @property
    def units(self):
        return sum(l[2] / l[1] for l in self.legs if l[1])

    @property
    def entry_eff(self):
        u = self.units
        return (self.cost / u) if u else None

    def proceeds(self):
        u = self.units
        hc = HAIRCUT_GRAD if self.graduated else HAIRCUT_CURVE
        return sum(frac * u * mcap * hc for _ts, mcap, frac in self.sells)

    def pnl(self):
        return self.proceeds() - self.cost

    def ret_pct(self, mcap):
        e = self.entry_eff
        return ((mcap / e - 1) * 100) if (e and mcap) else None

    # -- mutation ----------------------------------------------------------

    def buy(self, ts, mcap, sol):
        self.legs.append((ts, mcap, sol))

    def sell(self, ts, mcap, frac):
        frac = max(0.0, min(frac, 1.0 - self.sold_frac))
        if frac <= 0:
            return
        self.sells.append((ts, mcap, frac))
        self.sold_frac += frac

    def close(self, ts, mcap, note):
        self.sell(ts, mcap, 1.0 - self.sold_frac)
        self.done = True
        self.note = note


class Engine:
    def __init__(self):
        self._lock = threading.Lock()
        self._book = {}       # mint -> Position
        self._done = []       # finished rows awaiting journalling
        self._stats = {"opened": 0, "cut": 0, "laddered": 0, "closed": 0,
                       "adds": 0, "no_flow": 0}
        # Own flow memory. This engine originally read smart_exit's, but
        # smart_exit drops a mint's deque and print count in pop_compares() —
        # which lands at ~35s for a copy-target position, i.e. BEFORE the 48s
        # first add. The ghost went blind precisely when it needed evidence, so
        # it holds its own copy and its own subscription lifetime.
        self._trades = {}     # mint -> deque[(ts, side, sol, trader)]
        self._prints = {}     # mint -> cumulative count
        self._ws_out = deque()

    # ------------------------------------------------------- flow plumbing --
    # Same interface FlowSource drives on smart_exit.Engine, so both can be fed
    # from one websocket via the fanout in jup_trader.

    def record_trade(self, ev, ignore_trader=None):
        mint = ev.get("mint")
        if not mint:
            return
        trader = ev.get("traderPublicKey")
        if ignore_trader and trader == ignore_trader:
            return
        row = (time.time(), ev.get("txType"), _f(ev.get("solAmount")), trader)
        with self._lock:
            dq = self._trades.get(mint)
            if dq is None:
                return
            dq.append(row)
            self._prints[mint] = self._prints.get(mint, 0) + 1

    def drain_ws(self):
        with self._lock:
            out = []
            while self._ws_out:
                out.append(self._ws_out.popleft())
            return out

    def active_mints(self):
        with self._lock:
            return list(self._trades.keys())

    def flow(self, mint, now, window_s):
        """(buy_sol, sell_sol, n_sells, biggest_sell, last_ts, seen, prints)."""
        with self._lock:
            dq = self._trades.get(mint)
            prints = self._prints.get(mint, 0)
            if not dq:
                return 0.0, 0.0, 0, 0.0, None, False, prints
            while dq and now - dq[0][0] > TRADE_KEEP_S:
                dq.popleft()
            buy = sell = big = 0.0
            n_sells = 0
            last_ts = dq[-1][0] if dq else None
            for ts, side, sol, _t in dq:
                if now - ts > window_s:
                    continue
                if side == "sell":
                    sell += sol
                    n_sells += 1
                    big = max(big, sol)
                elif side == "buy":
                    buy += sol
            return buy, sell, n_sells, big, last_ts, bool(dq), prints

    # ---------------------------------------------------------------- intake

    def register(self, pos, now=None):
        mint = pos.get("mint")
        if not mint or not pos.get("entry_mcap"):
            return
        now = now or time.time()
        with self._lock:
            if mint in self._book:
                return
            self._book[mint] = Position(pos, now)
            self._trades.setdefault(mint, deque(maxlen=TRADE_MAXLEN))
            self._prints.setdefault(mint, 0)
            self._ws_out.append(("subscribeTokenTrade", mint))

    def tracked(self, mint):
        with self._lock:
            return mint in self._book

    # ------------------------------------------------------------ decisions

    def _alive(self, p, mcap, now, cfg, flow):
        """Is a crowd still trading this? (reason_dead or None)

        INFERRED RULE — the one part of their behaviour we cannot observe
        directly (header). Flow is primary because a pure price cut cannot
        reproduce their exits: p75 of un-added-probe exit price is +2.4%, so a
        quarter of their cuts fire while flat or up. Price stays as a brake only.

        `prints == 0` means the feed never armed for this mint, NOT that the mint
        is dead. Treating those as dead is exactly what produced 36 bogus verdicts
        in the 07-24→07-27 blind window (see solseller flow-feed notes), so a
        never-armed position falls through to price-and-time rules alone.
        """
        buy, sell, n_sells, big, last_ts, seen, prints = flow
        ret = p.ret_pct(mcap)
        if ret is not None and ret <= -abs(cfg["add_abort_ret"]):
            return f"crater {ret:.0f}%"
        if prints <= 0:
            # no evidence either way — only the crash brake above applies.
            # NB: callers must not read this None as "alive"; `p.armed_ts is
            # None` is the separate test that blocks escalation (see on_mark).
            return None
        if not seen:
            return "flow-dead (no prints in window)"
        if prints < cfg["min_prints"]:
            return f"thin ({prints} prints)"
        if n_sells >= 2 and sell > 1.5 * max(buy, 1e-9):
            return f"sell-dominated {sell:.2f}v{buy:.2f}S"
        return None

    def on_mark(self, pos, mcap, graduated, now, cfg, flow):
        """Advance the simulated ladder one tick.

        Returns the list of actions a LIVE pyramid arm would have taken this
        tick: [("buy", sol), ("sell", frac), ("close", note)]. In ghost mode the
        caller ignores them; the simulated book is the product. Kept as a return
        value so promoting to live is a call-site change, not a rewrite.
        """
        mint = pos.get("mint")
        acts = []
        with self._lock:
            p = self._book.get(mint)
            if p is None or p.done:
                return acts
            if mcap:
                p.last_mcap = mcap
                p.peak_mcap = max(p.peak_mcap or 0, mcap)
            p.graduated = bool(graduated)
            if not mcap:
                if now - p.t0 >= cfg["max_age_s"]:
                    p.close(now, p.last_mcap or 0, "pyr-timeout (no price)")
                    self._finish(p)
                return acts
            age = now - p.t0

            # ---- entry (optionally delayed; default immediate) -------------
            if not p.opened:
                if age < cfg["entry_delay_s"]:
                    return acts
                sol = min(cfg["probe_sol"], cfg["probe_max_sol"])
                p.buy(now, mcap, sol)
                p.opened = True
                self._stats["opened"] += 1
                # first add is scheduled from the PROBE, matching their
                # measured 48s median gap from first buy to first add
                p.next_add_ts = now + cfg["first_add_s"]
                acts.append(("buy", sol))
                return acts

            # `armed` = this mint's feed has ever delivered a print. It gates
            # ESCALATION only: absence of evidence must never add capital, but
            # it must never trigger a cut either. Conflating those two is what
            # the 07-24 blind window did, and a pyramid gets it wrong 9x louder.
            _, _, _, _, _, _, prints = flow
            if prints > 0 and p.armed_ts is None:
                p.armed_ts = now
            dead = self._alive(p, mcap, now, cfg, flow)
            ret = p.ret_pct(mcap) or 0.0
            n_adds = len(p.legs) - 1

            # ---- crash brake: always first, pre-empts every ladder ----------
            if ret <= -abs(cfg["sl_pct"]):
                p.close(now, mcap, f"pyr-brake {ret:.0f}%")
                self._finish(p)
                acts.append(("close", p.note))
                return acts

            # ---- lone-probe phase -------------------------------------------
            # Until the probe has earned an add it is never tranched — a dead
            # 0.02 SOL probe is CUT, in one action. Their un-added probes exit
            # at median 29s; drip-feeding one out over 100s is not that trade,
            # and letting the exit ladder open here pre-empted the cut entirely
            # (caught by the dead-cut unit test).
            if n_adds == 0 and not p.sells:
                if age >= cfg["cut_s"] and (dead or ret <= cfg["cut_ret"]):
                    p.cut = True
                    p.close(now, mcap, f"pyr-cut @{age:.0f}s ({dead or f'{ret:.0f}%'})")
                    self._stats["cut"] += 1
                    self._finish(p)
                    acts.append(("close", p.note))
                    return acts

            # ---- the ladder: add on the clock while it lives -----------------
            if (not p.sells and n_adds < cfg["max_adds"]
                    and p.next_add_ts and now >= p.next_add_ts):
                if dead or p.armed_ts is None:
                    # hold the slot: re-check next tick rather than burning an
                    # add on a mint we have no positive evidence for
                    p.next_add_ts = now + cfg["add_gap_s"]
                else:
                    sol = min(cfg["probe_sol"] * cfg["add_mult"], cfg["probe_max_sol"])
                    p.buy(now, mcap, sol)
                    p.next_add_ts = now + cfg["add_gap_s"]
                    self._stats["adds"] += 1
                    if n_adds == 0:
                        self._stats["laddered"] += 1
                    acts.append(("buy", sol))
                    return acts

            # ---- the exit: even tranches --------------------------------------
            # Opens when the ladder is spent, the crowd left, or we age out. A
            # position that never got an add exits in ONE action, not N.
            n_tr = 1 if n_adds == 0 else max(1, int(cfg["tranches"]))
            if p.next_tranche_ts is None:
                if (n_adds and dead) or n_adds >= cfg["max_adds"] or age >= cfg["max_age_s"]:
                    p.next_tranche_ts = now
            if p.next_tranche_ts is not None and now >= p.next_tranche_ts:
                if n_tr - len(p.sells) <= 1:
                    p.close(now, mcap, f"pyr-flat @{age:.0f}s ({ret:+.0f}%) "
                                       f"{len(p.legs)}L")
                    self._finish(p)
                    acts.append(("close", p.note))
                else:
                    frac = (1.0 - p.sold_frac) / (n_tr - len(p.sells))
                    p.sell(now, mcap, frac)
                    p.next_tranche_ts = now + cfg["tranche_gap_s"]
                    acts.append(("sell", frac))
            return acts

    def note_real_close(self, mint, mcap, note, fixed=None, now=None):
        """The real (fixed) arm closed. Capture its outcome — but DO NOT close
        the ghost.

        This used to force-close the ghost "so the comparison is on the same
        trade". That was wrong, and measurably so: on 2026-07-28 the paper book
        was being closed by smart-exit's creator-sell rule after a median 1.4s,
        so every one of the first 12 ghosts was settled at 1-2.3s — before the
        48s first add. The ladder fired 0/12 times. Settling the ghost on the
        real close forces the pyramid to hold exactly as long as the arm it is
        supposed to be tested against, which erases the only variable under test.

        The ghost now runs to ITS own exit, exactly as smart_exit's ghost side
        does (see ghost_mints/ghost_mark there). What must be captured HERE is
        the fixed arm's PnL, because this is the only moment it is known — the
        comparison is same-ENTRY, not same-hold, which is the honest framing:
        two policies given the same candidate, each exiting on its own rules.
        """
        now = now or time.time()
        with self._lock:
            p = self._book.get(mint)
            if p is None or p.done:
                return
            p.fixed_side = fixed
            p.real_closed_ts = now
            if not p.opened:
                # the ghost never even probed (delayed entry, or closed inside
                # the first tick) — there is nothing to run on, so end it here
                p.done = True
                p.note = f"pyr-void ({note})"
                self._finish(p, fixed)

    def ghost_mints(self):
        """Mints whose ghost is still running and therefore still needs marks.

        The real position may already be closed and gone from `_sniper`'s open
        list, so jup_trader has to keep pricing these for the ladder to mean
        anything. Bounded by max_age_s and by reap()."""
        with self._lock:
            return [m for m, p in self._book.items() if not p.done and p.opened]

    def ghost_mark(self, mint, mcap, graduated, now, cfg, flow):
        """Advance a ghost whose real position has already closed."""
        with self._lock:
            p = self._book.get(mint)
            if p is None or p.done:
                return
        pos = dict(p.pos)
        self.on_mark(pos, mcap, graduated, now, cfg, flow)

    def _finish(self, p, fixed=None):
        """Caller holds the lock. `fixed` falls back to whatever the real arm
        reported when IT closed (note_real_close); a row that never got one is
        quarantined by the caller rather than scored against a missing arm."""
        if fixed is None:
            fixed = p.fixed_side
        self._stats["closed"] += 1
        if p.armed_ts is None:
            self._stats["no_flow"] += 1
        self._done.append({
            "ts": round(time.time(), 3),
            **p.pos,
            "fixed": fixed,
            "pyr": {
                "legs": len(p.legs),
                "adds": max(0, len(p.legs) - 1),
                "cost_sol": round(p.cost, 6),
                "entry_eff": p.entry_eff,
                "tranches": len(p.sells),
                "pnl_sol": round(p.pnl(), 6),
                "ret_pct": (round(100 * p.pnl() / p.cost, 2) if p.cost else None),
                "note": p.note,
                "cut": p.cut,
                # seconds the ghost outlived the real arm. 0 across the board
                # means the ghost is being settled on the real close again and
                # the ladder can never fire — the 0/12 bug.
                "outlived_s": (round(max(0.0, (p.sells[-1][0] if p.sells else 0)
                                         - p.real_closed_ts), 1)
                               if p.real_closed_ts and p.sells else None),
                # max(0, …): t0 comes from the position's detected_ts while the
                # settle timestamp can come from force_close's own clock, so a
                # clock skew (or a synthetic t0 in tests) must not emit a
                # nonsense hold that then poisons the hold-time stats.
                "hold_s": round(max(0.0, (p.sells[-1][0] if p.sells
                                          else time.time()) - p.t0), 1),
                # 0 => flow never armed for this mint, so the alive-test never
                # really ran. Journalling quarantines these exactly like
                # exit_compare's flow_prints==0 rows.
                "armed": p.armed_ts is not None,
                "peak_mcap": p.peak_mcap,
                "flow_prints": self._prints.get(p.mint, 0),
            },
        })
        self._book.pop(p.mint, None)
        self._trades.pop(p.mint, None)
        self._prints.pop(p.mint, None)
        self._ws_out.append(("unsubscribeTokenTrade", p.mint))

    # ------------------------------------------------------------ journalling

    def pop_done(self):
        with self._lock:
            out, self._done = self._done, []
            return out

    def reap(self, now, max_age_s):
        """Backstop for ghosts whose real position never reported a close —
        same leak class smart_exit.Engine.reap exists for."""
        out = []
        with self._lock:
            for mint, p in list(self._book.items()):
                if now - p.t0 < max_age_s:
                    continue
                if p.opened:
                    p.close(now, p.last_mcap or 0, "pyr-reaped")
                else:
                    p.done = True
                    p.note = "pyr-void (reaped)"
                self._finish(p)
                out.append(mint)
        return out

    def open_book(self):
        """In-flight simulated positions, for the dashboard.

        These are SIMULATED legs — no SOL moved and no position exists in the
        sniper's book, which is why none of this appears in the positions table.
        Exposed so the ladder is observable while it runs instead of only after
        it lands in pyramid_compare.jsonl.
        """
        now = time.time()
        with self._lock:
            out = []
            for m, p in self._book.items():
                if p.done or not p.opened:
                    continue
                mc = p.last_mcap
                out.append({
                    "mint": m,
                    "symbol": p.pos.get("symbol"),
                    "legs": len(p.legs),
                    "adds": max(0, len(p.legs) - 1),
                    "cost_sol": round(p.cost, 6),
                    "entry_eff": p.entry_eff,
                    "last_mcap": mc,
                    "ret_pct": (round(p.ret_pct(mc), 1) if mc and p.entry_eff else None),
                    "age_s": round(now - p.t0, 1),
                    "next_add_in_s": (round(p.next_add_ts - now, 1)
                                      if p.next_add_ts and not p.sells else None),
                    "tranches_done": len(p.sells),
                    "sold_pct": round(100 * p.sold_frac, 1),
                    "armed": p.armed_ts is not None,
                    "prints": self._prints.get(m, 0),
                    "real_closed": p.real_closed_ts is not None,
                })
            out.sort(key=lambda r: -r["age_s"])
            return out

    def status(self):
        with self._lock:
            return {"tracked": len(self._book), "pending_rows": len(self._done),
                    **self._stats}


def build_cfg(sniper_cfg):
    """Snapshot every knob once per sweep; the engine never reads files."""
    g = sniper_cfg.get
    d = DEFAULTS
    return {
        "enabled": bool(g("pyramid_enabled", True)),
        # Hard-gated and checked INDEPENDENTLY of the top-level `live` flag,
        # the same way the graduating sleeve is pinned to paper. Flipping
        # sniper_config's `live` must never arm this by itself.
        "live": bool(g("pyramid_live", False)),
        "probe_sol": _f(g("pyr_probe_sol"), d["probe_sol"]),
        "probe_max_sol": _f(g("pyr_probe_max_sol"), d["probe_max_sol"]),
        "entry_delay_s": _f(g("pyr_entry_delay_s"), d["entry_delay_s"]),
        "cut_s": _f(g("pyr_cut_s"), d["cut_s"]),
        "cut_ret": _f(g("pyr_cut_ret"), d["cut_ret"]),
        "min_prints": int(_f(g("pyr_min_prints"), d["min_prints"])),
        "flow_window_s": _f(g("pyr_flow_window_s"), d["flow_window_s"]),
        "first_add_s": _f(g("pyr_first_add_s"), d["first_add_s"]),
        "add_gap_s": _f(g("pyr_add_gap_s"), d["add_gap_s"]),
        "add_mult": _f(g("pyr_add_mult"), d["add_mult"]),
        "max_adds": int(_f(g("pyr_max_adds"), d["max_adds"])),
        "add_abort_ret": _f(g("pyr_add_abort_ret"), d["add_abort_ret"]),
        "tranches": int(_f(g("pyr_tranches"), d["tranches"])),
        "tranche_gap_s": _f(g("pyr_tranche_gap_s"), d["tranche_gap_s"]),
        "max_age_s": _f(g("pyr_max_age_s"), d["max_age_s"]),
        "sl_pct": _f(g("sl_pct"), 40.0),
    }


ENGINE = Engine()


# ------------------- pyramid-vs-fixed scoreboard (read-only) -------------------
# Same contract as smart_exit.scoreboard: rank the two policies on IDENTICAL
# entries so per-trade luck cancels. Caller owns file I/O.

def scoreboard(rows, min_cost=1e-9):
    """`rows` are parsed pyramid_compare.jsonl records, each carrying the real
    (fixed) outcome and the pyramid ghost for the same mint.

    Quarantines rows where flow never armed — the alive-test never ran on those,
    so they are not a test of this policy (same rule exit_compare uses for
    flow_prints==0). Reported separately rather than silently dropped."""
    scored, unarmed, skipped = [], 0, 0
    for r in rows:
        p = r.get("pyr") or {}
        f = r.get("fixed") or {}
        # `armed` is tested FIRST and deliberately: a blind-feed row is also
        # missing other fields, so checking those first would file it under
        # "skipped" and hide the one number that says the flow feed is dead.
        # That mis-attribution is the same class of bug as the 07-24 blind
        # window — the failure has to stay loud.
        if not p.get("armed"):
            unarmed += 1
            continue
        if not p or p.get("pnl_sol") is None or f.get("pnl_sol") is None:
            skipped += 1
            continue
        if not p.get("cost_sol", 0) > min_cost:
            skipped += 1
            continue
        scored.append((r, _f(p["pnl_sol"]), _f(f["pnl_sol"])))
    n = len(scored)
    if not n:
        return {"n": 0, "unarmed": unarmed, "skipped": skipped}

    pp = [t[1] for t in scored]
    fp = [t[2] for t in scored]
    # Normalised comparison: the arms deploy different capital per position, so
    # raw SOL alone would reward the pyramid for merely betting more. Report
    # both, and return-on-cost as the honest per-unit number.
    pcost = sum(_f((t[0].get("pyr") or {}).get("cost_sol")) for t in scored)
    fcost = sum(_f(t[0].get("sol_in")) for t in scored)
    ladder = [t for t in scored if (t[0].get("pyr") or {}).get("adds", 0) > 0]
    cutonly = [t for t in scored if (t[0].get("pyr") or {}).get("cut")]
    return {
        "n": n, "unarmed": unarmed, "skipped": skipped,
        "pyr_pnl": round(sum(pp), 4), "fixed_pnl": round(sum(fp), 4),
        "delta": round(sum(pp) - sum(fp), 4),
        "pyr_cost": round(pcost, 4), "fixed_cost": round(fcost, 4),
        "pyr_roc": round(100 * sum(pp) / pcost, 2) if pcost else None,
        "fixed_roc": round(100 * sum(fp) / fcost, 2) if fcost else None,
        "pyr_wr": round(100 * sum(1 for x in pp if x > 0) / n, 1),
        "fixed_wr": round(100 * sum(1 for x in fp if x > 0) / n, 1),
        "better": sum(1 for a, b in zip(pp, fp) if a > b + 1e-9),
        # The whole thesis in one line: does the laddered cohort separate from
        # the cut-only cohort? If it does not, this policy has nothing.
        "laddered_n": len(ladder),
        "laddered_pnl": round(sum(t[1] for t in ladder), 4),
        "laddered_wr": (round(100 * sum(1 for t in ladder if t[1] > 0) / len(ladder), 1)
                        if ladder else None),
        "cut_n": len(cutonly),
        "cut_pnl": round(sum(t[1] for t in cutonly), 4),
    }
