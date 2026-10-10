"""tpsl.py — server-side TP/SL executor for the pop-out trade box.

Replaces the popout's alert-only watcher. The window being open cannot be a
precondition for a stop-loss, so the decision loop lives here, in the trader
process, and the popout becomes a control surface over this state.

WHY THIS FILE IS SHAPED LIKE THIS
---------------------------------
Audited against the real journal (1054 rows, 192 mints, 2026-08-09):

  * 57.3% of mints had a cost basis that was provably wrong at some point.
  * 64 of 81 over-sells were SMALL (median 1.026x) -> the journal was recording
    Jupiter's pre-trade QUOTE, not the landed fill. That 2.6% median drift is
    larger than the 1% corroboration tolerance get_holdings() allowed, so real
    positions were being flagged "incomplete" by the ledger's own noise.
  * 17 were LARGE (up to 25.2x) and 47 mints were sold with no buy row at all
    -> whole fills missing (sniper buys journal elsewhere; a wait=False buy
    whose confirm straddled a restart is gone forever).

So the executor's first rule is: DO NOT FIRE ON A BASIS YOU CANNOT PROVE.
jup_trader now journals chain-verified fills (fill="chain"); only lots built
entirely from those, and corroborated against the on-chain balance, are
eligible for autonomous selling. Everything else still shows a PnL and still
raises a one-click alert exactly like before — it just never sells by itself.
An unprovable basis costs nothing. A confident wrong one sells at the wrong
price, which is the failure this whole file exists to prevent.

DECIDE() IS PURE
----------------
No clock, no network, no files: every input is an argument. That is what makes
_tpsl_test.py able to replay multi-buy/multi-sell sequences at chosen
timestamps and assert on fire/no-fire, and what makes the mutation runs
meaningful (see the test file's header).

THE FOUR RACES THIS GUARDS
--------------------------
1. Double-fire. /api/sell sizes from a live token_balance() read, so two sells
   in flight against one mint both size off the pre-first-sell balance and the
   second is rejected (or, worse, lands after a partial and sells the wrong
   fraction). Every sell -- manual OR automatic -- now takes the same per-mint
   lock, held until the fill settles.
2. Re-fire after a partial. A TP that sells 50% usually leaves the position
   still above the TP threshold, so a naive watcher sells the remainder on the
   very next tick. A fired side stays fired until price returns INSIDE the
   band; cooldown is a second, independent guard.
3. One bad price print. A single bogus Jupiter tick would otherwise dump a
   position at a price that never existed. A threshold must hold for
   confirm_ticks consecutive polls before it fires.
4. A frozen feed. The popout swallowed price errors (`catch(e){}`), so a dead
   Jupiter looked identical to a flat market and the stop-loss silently stopped
   watching. Price age is now an explicit input and a stale price BLOCKS the
   fire and RESETS the streak, rather than firing off a remembered number.

WHERE THE FIRES ARE RECORDED
----------------------------
tpsl_fires.jsonl, next to the config, one row per event and three events per
crossing (fire -> sent|send_fail -> settled) correlated by `jid`. Added
2026-08-15, when "have we gathered enough TP/SL data yet?" turned out to be
unanswerable: a fire lived only in the 60-slot self._events ring and one stdout
line, and trades.jsonl carries no source tag (`grep -c tpsl trades.jsonl` is 0),
so the sample reset to zero on every restart and the five fires of 2026-08-13
are gone for good. Each `fire` row snapshots the limits AND the trigger source
in force at that moment, because rows written either side of a config change
are otherwise silently incomparable. See Executor._journal().
"""

from __future__ import annotations

import json
import os
import threading
import time
import time as _time          # thread-join deadlines: real time, not deps.now()

# ---------------------------------------------------------------- config ----

DEFAULTS = {
    "armed": False,          # master switch. Disarmed on every start, on purpose.
    "tp_pct": 55.0,          # default take-profit, % over entry
    "sl_pct": 40.0,          # default stop-loss, % under entry
    "tp_sell_pct": 100.0,    # how much of the position each side sells
    "sl_sell_pct": 100.0,
    "confirm_ticks": 2,      # consecutive confirming polls before firing
    "max_price_age_s": 20.0, # older than this and nothing may fire
    "cooldown_s": 45.0,      # after a fire, this mint is inert for this long
    "max_fires_per_day": 40, # runaway guard on the whole executor
    "slippage_bps": None,    # None -> trader default
    "fee_mode": "mev",
    "per": {},               # mint -> {"tp":x,"sl":y,"off":bool}
    # -- trigger source (see Executor.refresh_quotes) --
    "quote_trigger": True,   # promote near-threshold mints to live quote pricing
    "quote_band_frac": 0.7,  # "near" = this fraction of the way to the threshold
    "retry_after_fail_s": 6.0,  # re-arm this soon after a sell fails to land
}

# state values a position can hold, per side
IDLE, PENDING, FIRED = "idle", "pending", "fired"

# what decide() can return
HOLD, FIRE = "hold", "fire"


def _num(v, dflt=None):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return dflt
    return f if f == f else dflt          # NaN -> default


class PosState:
    """Per-mint state carried between ticks. Plain data; decide() mutates it."""

    __slots__ = ("mint", "side", "state", "streak", "fired_ts", "fired_sig",
                 "fired_kind", "last_reason", "unprotected", "last_px_ts",
                 "fired_jid")

    def __init__(self, mint):
        self.mint = mint
        self.side = None          # which side ('tp'/'sl') the streak is counting
        self.state = IDLE
        self.streak = 0
        self.fired_ts = 0.0
        self.fired_sig = None
        self.fired_kind = None    # side that fired and has not re-armed yet
        # Journal id of the fire currently in flight. sell_settled() is handed
        # only (mint, status, err), so without this the settle row cannot be
        # tied back to the fire it resolves -- and two fires on one mint inside
        # a cooldown window become indistinguishable in the file.
        self.fired_jid = None
        self.last_reason = ""
        self.unprotected = None   # why this position cannot auto-sell, or None
        # Origin stamp of the price that last ADVANCED the streak. See decide():
        # confirmation has to mean two independent observations, and only the
        # origin stamp can tell a genuine second look from a re-read of one.
        self.last_px_ts = None

    def snapshot(self):
        return {"mint": self.mint, "state": self.state, "side": self.side,
                "streak": self.streak, "fired_kind": self.fired_kind,
                "fired_sig": self.fired_sig, "reason": self.last_reason,
                "unprotected": self.unprotected}


def _qty(row):
    """The size a quote is asked about. Prefers raw base units -- that is what
    the swap program actually takes -- and falls back to the UI amount for deps
    that do not carry a raw figure."""
    return row.get("raw") or row.get("amount")


def limits_for(cfg, mint):
    """Effective TP/SL for one mint. Per-mint overrides fall back to global.

    0 is a REAL value here. The popout used `parseFloat(v)||undefined`, which
    silently turned a 0% stop into "use the global" -- so the one setting that
    means "get me out at break-even" was the one setting you could not express.
    """
    per = (cfg.get("per") or {}).get(mint) or {}
    tp = _num(per.get("tp"), None)
    sl = _num(per.get("sl"), None)
    if tp is None:
        tp = _num(cfg.get("tp_pct"), DEFAULTS["tp_pct"])
    if sl is None:
        sl = _num(cfg.get("sl_pct"), DEFAULTS["sl_pct"])
    return {"tp": abs(tp), "sl": abs(sl), "off": bool(per.get("off"))}


def decide(st, *, pnl_pct, limits, now, price_age_s, basis, in_flight,
           armed, cfg, fires_today=0, price_ts=None):
    """PURE. Decide what to do with one position on one tick.

    Returns (action, reason, kind) where action is HOLD or FIRE and kind is
    'tp'/'sl' when firing. Mutates only `st`, never reads a clock or the network
    -- every input that could vary is an argument, which is what lets the stress
    harness replay a whole trading day in-process and deterministically.

    Ordering matters: each gate below is a reason NOT to sell, and the cheap,
    most-dangerous-if-missed ones come first. In particular a stale price RESETS
    the streak instead of merely blocking, so a feed that dies mid-crossing
    cannot resume from a half-built streak on one fresh tick.

    `price_ts` is the ORIGIN stamp of the price behind pnl_pct -- when the feed
    computed it, not when we read it. Passing it turns confirm_ticks into "N
    independent observations" instead of "N polls"; see the streak block below
    for why that distinction was the whole ballgame. None keeps the old
    poll-counting behaviour for callers that cannot supply a stamp.
    """
    st.unprotected = None

    # -- gates that block firing but are not the position's fault --------
    if in_flight:
        st.last_reason = "sell in flight"
        return HOLD, st.last_reason, None

    if basis != "ok":
        # The audit case. Show it, alert on it, never sell on it.
        st.unprotected = {
            "estimate": "entry price not chain-verified",
            "incomplete": "you hold more than the journal explains",
            "stale": "exited outside this app — old lot still on the books",
        }.get(basis, "no confirmed buy on record")
        st.streak, st.side, st.state = 0, None, IDLE
        st.last_px_ts = None
        st.last_reason = "unprotected: " + st.unprotected
        return HOLD, st.last_reason, None

    p = _num(pnl_pct, None)
    if p is None:
        st.unprotected = "no live PnL"
        st.streak, st.side, st.state = 0, None, IDLE
        st.last_px_ts = None
        st.last_reason = "no pnl"
        return HOLD, st.last_reason, None

    if price_age_s is None or price_age_s > _num(cfg.get("max_price_age_s"),
                                                 DEFAULTS["max_price_age_s"]):
        # A frozen feed must never look like a flat market. Reset, do not carry
        # a partial streak across the outage.
        st.unprotected = "price feed stale"
        st.streak, st.side, st.state = 0, None, IDLE
        st.last_px_ts = None
        st.last_reason = "stale price (%.0fs)" % (price_age_s or -1)
        return HOLD, st.last_reason, None

    if limits.get("off"):
        st.streak, st.side, st.state = 0, None, IDLE
        st.last_px_ts = None
        st.last_reason = "disabled for this mint"
        return HOLD, st.last_reason, None

    # -- where are we relative to the band? --------------------------------
    tp, sl = limits["tp"], limits["sl"]
    hit = "tp" if p >= tp else ("sl" if p <= -sl else None)

    if hit is None:
        # Back inside the band: this is the ONLY thing that re-arms a side that
        # already fired. Without it a TP that sells 50% leaves the position
        # above the threshold and the next tick sells the rest.
        st.streak, st.side, st.state = 0, None, IDLE
        st.last_px_ts = None
        st.fired_kind = None
        st.last_reason = "in band (%.1f%%)" % p
        return HOLD, st.last_reason, None

    if st.fired_kind == hit:
        st.last_reason = "%s already fired; waiting to re-enter band" % hit.upper()
        st.state = FIRED
        return HOLD, st.last_reason, None

    # -- streak accounting --------------------------------------------------
    # COUNT DISTINCT PRICES, NOT POLLS. This used to be `st.streak += 1` on
    # every tick, which quietly voided the guard it exists to be: the poll
    # interval (3s) was shorter than the price cache TTL (4s) and shorter still
    # than Jupiter price/v3's ~6s server-side refresh, so consecutive ticks
    # routinely read the SAME number. confirm_ticks=2 was therefore satisfied by
    # one price print observed twice -- no protection against a bad tick, and
    # ~3-12s of latency charged for it. Measured 2026-08-13: price/v3 returned a
    # byte-identical value for 40 consecutive 1s polls on a $370k/h pump mint.
    #
    # A repeat stamp is not evidence against the crossing either, so it must not
    # RESET the streak -- it simply carries no information. Hold and wait for a
    # genuinely new observation.
    if st.side != hit:
        st.side, st.streak = hit, 1
        st.last_px_ts = price_ts
    elif price_ts is not None and price_ts == st.last_px_ts:
        st.state = PENDING
        st.last_reason = "%s %.1f%% — awaiting a new price print" % (hit.upper(), p)
        return HOLD, st.last_reason, None
    else:
        st.streak += 1
        st.last_px_ts = price_ts
    st.state = PENDING

    if not armed:
        st.last_reason = "%s %.1f%% vs %s%% — DISARMED" % (
            hit.upper(), p, tp if hit == "tp" else -sl)
        return HOLD, st.last_reason, None

    cd = _num(cfg.get("cooldown_s"), DEFAULTS["cooldown_s"])
    if st.fired_ts and (now - st.fired_ts) < cd:
        st.last_reason = "cooldown %.0fs" % (cd - (now - st.fired_ts))
        return HOLD, st.last_reason, None

    cap = int(_num(cfg.get("max_fires_per_day"), DEFAULTS["max_fires_per_day"]))
    if fires_today >= cap:
        st.unprotected = "daily fire cap reached (%d)" % cap
        st.last_reason = st.unprotected
        return HOLD, st.last_reason, None

    need = max(1, int(_num(cfg.get("confirm_ticks"), DEFAULTS["confirm_ticks"])))
    if st.streak < need:
        st.last_reason = "%s confirming %d/%d" % (hit.upper(), st.streak, need)
        return HOLD, st.last_reason, None

    # LATCH HERE, not in the caller. decide() must be safe to call twice in a
    # row on the same state: if the "already fired" flag were only set by the
    # executor after a successful send, any path that called decide() again
    # first -- a retry, a second consumer, a test -- would get a second FIRE for
    # the same crossing. The executor clears these again if the send throws.
    st.state = FIRED
    st.fired_kind = hit
    st.fired_ts = now
    st.last_reason = "%s %.1f%% — FIRE" % (hit.upper(), p)
    return FIRE, st.last_reason, hit


def sell_pct_for(cfg, kind):
    key = "tp_sell_pct" if kind == "tp" else "sl_sell_pct"
    v = _num(cfg.get(key), DEFAULTS[key])
    return min(100.0, max(0.0, v))


# ---------------------------------------------------------- cost basis ----

def basis_from_rows(rows, pubkey, tol=0.005):
    """{mint: {"sol", "raw", "verified"}} — running average cost of the lot
    that is STILL HELD, rebuilt from trade-journal rows.

    Pure and order-explicit so the stress harness can drive it. jup_trader's
    cost_basis() is a thin wrapper that tails the file into this.

    `verified` is the whole point. It is True only when every row that built
    the live lot carried fill="chain" (a real pre/post token-balance delta read
    off the landed transaction). Rows without it hold Jupiter's pre-trade quote,
    which the 2026-08-09 audit measured drifting a median 2.6% from the actual
    fill -- and 2.6% is already wider than any tolerance worth calling accurate.
    Only verified lots may auto-sell.

    Rows are sorted by ts, not consumed in file order: a wait=False buy journals
    from a background confirm thread, so the file genuinely interleaves (3 mints
    in the audit). It changed no outcome there, but a ledger that depends on
    write order is one restart away from doing so.

    A sell larger than the tracked lot proves the ledger is missing buys, so the
    lot is marked unverified rather than silently clamped into looking healthy.
    """
    pos = {}
    for d in sorted(rows, key=lambda r: _num(r.get("ts"), 0.0) or 0.0):
        if d.get("status") != "confirmed" or d.get("wallet") != pubkey:
            continue
        mint = d.get("mint")
        try:
            raw = int(d.get("token_raw") or 0)
        except (TypeError, ValueError):
            continue
        sol = _num(d.get("sol"), 0.0) or 0.0
        if not mint or raw <= 0:
            continue
        p = pos.setdefault(mint, {"sol": 0.0, "raw": 0, "verified": True})
        chain = (d.get("fill") == "chain")
        if d.get("side") == "buy":
            p["sol"] += sol
            p["raw"] += raw
            p["verified"] = p["verified"] and chain
        else:
            if p["raw"] <= 0:
                # Sold something the ledger never saw bought. Whatever lot this
                # mint later shows cannot be trusted until it goes flat again.
                p["verified"] = False
                continue
            dirty = raw > p["raw"] * (1 + tol)    # sold more than we ever bought
            if dirty:
                p["verified"] = False
            frac = min(1.0, raw / p["raw"])
            p["sol"] -= p["sol"] * frac
            p["raw"] -= raw
            p["verified"] = p["verified"] and chain
            if p["raw"] <= 0:
                # Flat -> the lot is gone and so is its history, so a re-buy
                # months later starts clean and starts verifiable again.
                #
                # UNLESS this sell was itself proof the ledger is broken. An
                # oversell always drives raw <= 0, so resetting unconditionally
                # would erase the taint in the very step that earned it, and the
                # next lot would look pristine. A dirty flat stays suspect for
                # one more lot; the next CLEAN round-trip clears it. Same rule
                # the naked-sell branch above already follows.
                pos[mint] = {"sol": 0.0, "raw": 0, "verified": not dirty}
    return {m: p for m, p in pos.items() if p["raw"] > 0 and p["sol"] > 0}


def classify_basis(lot, held_raw, tol=0.01):
    """Turn a lot + the on-chain balance into the basis grade get_holdings ships.

      ok        chain-verified AND the chain agrees with the ledger -> may sell
      incomplete you hold materially MORE than the ledger explains
      stale     you hold materially LESS -- the ledger missed an exit
      estimate  arithmetic is sound but built on quoted, not landed, fills
      none      nothing on record

    Returns (grade, cost_sol_scaled). Anything but "ok" is display-only.

    THE LEDGER MUST AGREE WITH THE CHAIN IN BOTH DIRECTIONS. The upper bound
    was here from the start; the lower one was added 2026-08-09 after FROGGY
    (ESoQUJ4i…) displayed -86% on a position that was near flat.

    What happened: 14 buys that morning (1.4 SOL) left the wallet without a
    journaled sell -- exited somewhere other than this app. The ledger still
    believed all 1.4 SOL was invested. When he re-entered that evening with
    0.1 SOL at a ~15x lower price, average-cost blended the dead morning lot
    into the new one and scaled it by held/lot = 0.29, reporting 0.41 SOL of
    cost against a 0.1 SOL entry.

    Scaling by held_frac is only sound when the un-journaled exit happened at
    the SAME price as the rest of the lot. Across a 15x move it is nonsense,
    and -- this is the trap -- it is INTERNALLY CONSISTENT nonsense: every
    number checks out against every other number, so nothing looks wrong. The
    only thing that can catch it is the chain balance, which is why holding
    less than the ledger claims is now a hard stop rather than a scale factor.
    """
    if not lot or not held_raw or lot["raw"] <= 0:
        return "none", None
    if held_raw > lot["raw"] * (1 + tol):
        return "incomplete", None
    if held_raw < lot["raw"] * (1 - tol):
        return "stale", None
    cost = lot["sol"] * min(1.0, held_raw / lot["raw"])
    if cost <= 0:
        return "none", None
    return ("ok" if lot.get("verified") else "estimate"), cost


# ------------------------------------------------------------- executor ----

class Executor:
    """Owns the tick loop. All I/O is injected so this is testable offline.

    deps:
      holdings()          -> {"holdings":[{mint,amount,cost_sol,pnl_pct,basis,
                                           symbol,usdPrice}], "sol_price":x}
      prices(mints)       -> ({mint: usd or None}, sol_price)
      sell(mint, pct, slippage_bps, fee_mode) -> {"signature":..., "status":...}
      lock(mint)          -> a plain lock object (acquire(False)/release),
                             the SAME one the manual sell path takes
      now()               -> float seconds
      log(str)            -> None
    """

    def __init__(self, deps, cfg_path, hold_every_s=60.0, price_every_s=3.0,
                 tick_every_s=1.0, book="live", journal_path=None):
        self.d = deps
        self.cfg_path = cfg_path
        self.cfg = dict(DEFAULTS)
        self.load()
        # THE ONLY DURABLE RECORD THIS EXECUTOR LEAVES. Before this, a fire
        # existed as one entry in the 60-slot self._events ring plus one stdout
        # line, and trades.jsonl carries no source tag -- so a TP/SL sell was
        # indistinguishable from any other sell the moment the process
        # restarted. "Have we gathered enough TP/SL data yet" was therefore
        # unanswerable across sessions: the sample reset to zero every restart.
        # Derived from cfg_path so the stress harness (which builds Executors on
        # a tempdir config) journals to tempdir, not next to the live one.
        self.book = book
        self.journal_path = journal_path or os.path.join(
            os.path.dirname(os.path.abspath(cfg_path)), "tpsl_fires.jsonl")
        self._jid_n = 0
        self.hold_every_s = hold_every_s
        self.price_every_s = price_every_s
        # 2026-10-05: DISARMED = IDLE. Disarmed, nothing can fire, so the 3s
        # Jupiter price screen was pure rate-limit burn. The box still reads
        # its holdings and USD values from this executor, so prices keep
        # coming, just slowly. Arming snaps straight back to full speed.
        self.idle_price_every_s = float(os.environ.get("TPSL_IDLE_PRICE_S", "20"))
        self._was_armed = False
        # The DECISION cadence, decoupled from the price-feed cadence. Jupiter's
        # price/v3 has a ~6s server-side refresh floor, so polling it faster
        # only burns rate budget -- but a quote is recomputed per request, so a
        # near-threshold position can be re-evaluated every second off a live
        # number. Screen slowly, decide fast.
        self.tick_every_s = tick_every_s

        self._lock = threading.Lock()
        self._pos = {}            # mint -> PosState
        self._rows = []           # last holdings snapshot
        self._px = {}             # mint -> (usd, origin_ts)
        self._qx = {}             # mint -> (sol_out, origin_ts, size) from a quote
        self._hot = set()         # mints close enough to a threshold to quote
        self._q_inflight = set()  # mints with a quote request outstanding
        self._sol_px = None
        self._sol_px_ts = 0.0
        self._in_flight = set()
        self._fires = []          # timestamps of fires, for the daily cap
        self._events = []         # recent executor events, for the UI
        self._stop = threading.Event()
        self._thread = None
        self._hold_due = 0.0
        self._px_due = 0.0
        self._holds_running = False
        self._px_running = False
        self._last_err = None
        self._total_usd = None
        self._sol_balance = None

    # ---- config ----
    def load(self):
        try:
            with open(self.cfg_path, encoding="utf-8") as f:
                saved = json.load(f)
            if isinstance(saved, dict):
                self.cfg.update(saved)
        except Exception:
            pass
        # ARMED IS NEVER RESTORED FROM DISK. A trader restart is exactly when
        # you have least context about what the wallet holds, so the executor
        # must not come back firing because it was armed a week ago.
        self.cfg["armed"] = False
        return self.cfg

    def save(self):
        tmp = self.cfg_path + ".tmp"
        body = dict(self.cfg)
        body.pop("armed", None)           # see load()
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(body, f, indent=1)
        os.replace(tmp, self.cfg_path)    # atomic; a torn config is a dead SL

    # ---- lifecycle ----
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True,
                                        name="tpsl")
        self._thread.start()

    def stop(self):
        self._stop.set()

    def event(self, msg, kind="info"):
        with self._lock:
            self._events.insert(0, {"ts": self.d.now(), "msg": msg, "kind": kind})
            del self._events[60:]
        try:
            self.d.log("[tpsl] " + msg)
        except Exception:
            pass

    # ---- durable journal ----
    def _next_jid(self):
        with self._lock:
            self._jid_n += 1
            n = self._jid_n
        return "%d-%d" % (int(self.d.now() * 1000), n)

    def _journal(self, event, **fields):
        """Append one row to tpsl_fires.jsonl. NEVER raises.

        Three rows describe one crossing -- 'fire', then 'sent' or 'send_fail',
        then 'settled' -- correlated by `jid`, because sell_settled() is handed
        only (mint, status, err) and has no other handle on which fire it
        resolves. Rows are self-describing on purpose: `cfg` snapshots the
        limits and the trigger source IN FORCE AT THE FIRE, so rows written
        either side of a config change stay comparable instead of silently
        averaging two different experiments.

        Wrapped whole, json.dumps included, and called only AFTER the latch,
        the mint lock and the _in_flight mark are set -- a journal that cannot
        write must never be able to stall or skip a stop-loss.
        """
        try:
            row = {"ts": self.d.now(), "event": event, "book": self.book}
            row.update(fields)
            line = json.dumps(row, ensure_ascii=False, default=str)
            with open(self.journal_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception as e:
            # NOT via _last_err alone: refresh_prices() clears that on every
            # successful batch (3s), so a sink that cannot write would announce
            # itself for one tick and then look exactly like "no fires yet" --
            # the same invisibility this file was added to end. The event ring
            # does not self-clear and is what the popout renders. Safe from
            # recursion: event() never calls back into _journal().
            self._last_err = "journal: %s" % e
            self.event("JOURNAL WRITE FAILED: %s" % e, "error")

    def _cfg_snapshot(self, mint):
        """The knobs that decide WHETHER and HOW FAST a fire happens."""
        lim = limits_for(self.cfg, mint)
        return {"tp_pct": lim["tp"], "sl_pct": lim["sl"],
                "confirm_ticks": _num(self.cfg.get("confirm_ticks"),
                                      DEFAULTS["confirm_ticks"]),
                "quote_trigger": bool(self.cfg.get("quote_trigger")),
                "max_price_age_s": _num(self.cfg.get("max_price_age_s"),
                                        DEFAULTS["max_price_age_s"]),
                "cooldown_s": _num(self.cfg.get("cooldown_s"),
                                   DEFAULTS["cooldown_s"]),
                "slippage_bps": self.cfg.get("slippage_bps"),
                "fee_mode": self.cfg.get("fee_mode")}

    # ---- data refresh ----
    def refresh_holdings(self, reason="timer"):
        """Trade-class RPC — deliberately SLOW (60s) and event-driven.

        Amounts only change when you trade, so the fast loop does not need
        them; prices are what move. jup_trader calls this straight after any
        fill so a new position is protected within a second of landing rather
        than up to a minute later.
        """
        try:
            h = self.d.holdings()
        except Exception as e:
            self._last_err = "holdings: %s" % e
            return False
        rows = h.get("holdings") or []
        with self._lock:
            self._rows = rows
            self._total_usd = h.get("total_usd")
            self._sol_balance = h.get("sol_balance")
            self._sol_px = h.get("sol_price") or self._sol_px
            if h.get("sol_price"):
                self._sol_px_ts = self.d.now()
            held = {r["mint"] for r in rows}
            for m in list(self._pos):
                if m not in held:
                    del self._pos[m]      # position closed -> forget its state
            # ...and stop paying to quote it, and drop its last quote so a
            # re-buy can never be priced off the previous position's number.
            self._hot &= held
            for m in list(self._qx):
                if m not in held:
                    del self._qx[m]
            for m in held:
                self._pos.setdefault(m, PosState(m))
        self._hold_due = self.d.now() + self.hold_every_s
        return True

    def refresh_prices(self):
        """The cheap batched screen. One HTTP call for every held mint.

        STAMPS ARE ORIGIN STAMPS, NOT FETCH STAMPS. This used to record
        `self.d.now()`, i.e. the moment WE read the value -- so a price served
        out of the trader's 4s cache, or out of its 90s stale-fallback during a
        429 cooldown, was recorded as age 0.0. max_price_age_s could therefore
        never fire, and the "a frozen feed must never look like a flat market"
        guarantee in this file's header was not being kept. deps.prices() may
        now return {mint: (value, origin_ts)}; the bare {mint: value} form is
        still accepted and falls back to fetch time.
        """
        with self._lock:
            mints = [r["mint"] for r in self._rows]
        if not mints:
            return
        try:
            px, sol = self.d.prices(mints)
        except Exception as e:
            # Do NOT touch self._px: leaving the old values with their old
            # timestamps is what makes them age out and block firing. Zeroing
            # them would look like a price of 0, i.e. -100%, i.e. a stop-loss.
            self._last_err = "prices: %s" % e
            return
        t = self.d.now()

        def _split(v):
            if isinstance(v, (tuple, list)) and len(v) == 2:
                return _num(v[0], None), _num(v[1], t)
            return _num(v, None), t

        with self._lock:
            for m in mints:
                val, ts = _split(px.get(m))
                if val:
                    self._px[m] = (float(val), ts)
            sval, sts = _split(sol)
            if sval:
                self._sol_px, self._sol_px_ts = float(sval), sts
            self._last_err = None

    def refresh_quotes(self):
        """Live executable price for near-threshold mints only.

        WHY THIS EXISTS. Measured 2026-08-13 against lite-api, 40 polls at 1s on
        a $370k/h pump.fun mint: /price/v3 changed 6 times (median gap 6.0s) and
        on a large-cap pair it did not change ONCE in 40s, while /swap/v1/quote
        changed 13 times (median gap 2.0s, min 1.0s). price/v3 is a cached
        aggregate; a quote is computed off pool state per request. Triggering a
        stop-loss off the cached one means firing on a number that can be tens
        of seconds old -- which is most of the "TP/SL lands off track" gap, far
        more than the poll interval ever was.

        A quote is also the RIGHT number: it is the routed price for this exact
        size, net of price impact, i.e. what the sell will actually get. The
        cached mid-price is not.

        Quotes are NOT free -- one request per mint, no batching, competing with
        the budget real buys and sells need. So this is gated: only mints the
        cheap screen already puts within quote_band_frac of a threshold get one.
        A cold book costs nothing extra; a hot one costs ~1 rps per position.
        """
        if not self.cfg.get("quote_trigger"):
            return
        # DISARMED means it cannot fire, so a live quote buys nothing and costs
        # ~1 rps per hot mint. A position sitting far past its threshold (e.g.
        # -51% against a -10% SL) is permanently "hot", so this quoted forever
        # on a decision it was structurally forbidden to act on -- and that,
        # not the price loop, was what pinned the Jupiter rate limit.
        # Gating here rather than turning quote_trigger off means arming
        # restores full quote accuracy automatically, with nothing to remember.
        if not self.cfg.get("armed"):
            return
        quote = getattr(self.d, "quote_sol", None)
        if quote is None:            # deps predates the quote path
            return
        with self._lock:
            # ONE OUTSTANDING QUOTE PER MINT. Without this, a request slower
            # than the tick interval means the next tick starts another for the
            # same mint -- at 1s ticks against a multi-second timeout that
            # compounds into a burst of duplicate requests per position, which
            # is precisely the 429 storm the gating exists to prevent.
            hot = [(r["mint"], _qty(r)) for r in self._rows
                   if r["mint"] in self._hot and r["mint"] not in self._q_inflight]
            for m, _a in hot:
                self._q_inflight.add(m)
        if not hot:
            return

        def one(mint, amount):
            try:
                v = _num(quote(mint, amount), None)
                if v and v > 0:
                    # Stamped at completion: a quote is computed when it is
                    # served, so this IS its origin time, unlike a cached price.
                    # The size is stored with it because a quote is only valid
                    # for the amount it was asked about -- after a partial sell,
                    # proceeds for the old size would read as a fake profit.
                    with self._lock:
                        self._qx[mint] = (v, self.d.now(), amount)
            except Exception as e:
                self._last_err = "quote %s: %s" % (mint[:6], e)
            finally:
                with self._lock:
                    self._q_inflight.discard(mint)

        # ALWAYS off-thread, including the single-mint case -- which is the
        # common one. A quote is a network call; running it inline would put it
        # on the decision loop and stall the tick for the very position that is
        # about to fire, reintroducing the block that moving _fire off-thread
        # just removed.
        ts = [threading.Thread(target=one, args=h, daemon=True,
                               name="tpsl-q-" + h[0][:6]) for h in hot]
        for t_ in ts:
            t_.start()
        # ONE shared deadline, not one per thread: joining each in turn for the
        # full budget would let three hot mints cost three tick intervals. A
        # quote that misses the window is simply abandoned -- its mint keeps the
        # previous stamp and ages out through the normal staleness gate. Real
        # time, not the injected clock, because this is an actual thread join.
        deadline = _time.monotonic() + max(0.5, self.tick_every_s)
        for t_ in ts:
            t_.join(timeout=max(0.0, deadline - _time.monotonic()))

    def _mark_hot(self, row, pnl):
        """Should this mint be paying for live quotes? Hysteresis-free on
        purpose: the band is wide enough (default 70% of the way to the
        threshold) that flapping across it costs one quote, not a wrong trade."""
        m = row["mint"]
        if not self.cfg.get("quote_trigger") or pnl is None:
            self._hot.discard(m)
            return
        lim = limits_for(self.cfg, m)
        if lim["off"] or row.get("basis") != "ok":
            self._hot.discard(m)      # cannot auto-sell it, do not pay to watch it
            return
        frac = _num(self.cfg.get("quote_band_frac"),
                    DEFAULTS["quote_band_frac"]) or 0.7
        if pnl >= lim["tp"] * frac or pnl <= -lim["sl"] * frac:
            self._hot.add(m)
        else:
            self._hot.discard(m)

    # ---- pnl ----
    def live_pnl(self, row):
        """(pnl_pct, price_age_s, price_ts, source).

        Prefers a live quote when one is current: it is both fresher (measured
        ~2s vs ~6s+) and truer, being the routed proceeds for this exact size
        rather than a mid-price that ignores impact. The displayed PnL is the
        same number that fires, deliberately -- showing +20% from the cached
        mid while selling at +18% is how a TP comes to look "off track".

        Falls back to the batched USD price, and the returned age is that of the
        STALEST leg, since a fresh token price against a stale SOL price is not
        a fresh PnL.
        """
        cost = _num(row.get("cost_sol"), 0.0) or 0.0
        if cost <= 0:
            return None, None, None, None
        amt = _num(row.get("amount"), 0.0) or 0.0
        now = self.d.now()

        hit = self._px.get(row["mint"])
        q = self._qx.get(row["mint"])
        # PREFER THE QUOTE ONLY WHILE IT IS ACTUALLY THE FRESHER NUMBER.
        #
        # The first cut of this preferred the quote whenever the size matched,
        # full stop. That is fine until quotes start failing: Jupiter began
        # 429ing the quote path on 2026-08-13, `_qx` kept its last good value
        # (deliberately -- clearing it would read as a price of 0), live_pnl
        # went on returning that value, and its age grew without bound. The
        # stale-price gate then blocked every decision on a position whose
        # BATCHED feed was perfectly healthy the whole time -- a stop-loss
        # switched off by the very guard meant to protect it. Observed live at
        # age 1597s with price/v3 still answering normally.
        #
        # Comparing origin stamps makes the degradation automatic: while quotes
        # flow they are newer and they win; the moment they stall the batched
        # price overtakes them and takes back over.
        if (q and amt and q[2] == _qty(row)
                and (hit is None or q[1] >= hit[1])):
            # Quote path: proceeds are already SOL, so there is no sol_price leg
            # to go stale underneath us.
            return round((q[0] / cost - 1) * 100, 2), now - q[1], q[1], "quote"

        if hit:
            px, ts = hit
            age = now - ts
        else:
            px, ts, age = _num(row.get("usdPrice"), None), None, None
        sol_px = self._sol_px
        if not px or not sol_px:
            return None, age, ts, None
        # SOL in vs SOL out, both scaled by the tokens still held -- a partial
        # sell reduces cost_sol proportionally, so the percentage survives it.
        sol_age = now - self._sol_px_ts if self._sol_px_ts else None
        if age is None or (sol_age is not None and sol_age > age):
            age = sol_age          # the pair is only as fresh as its stalest leg
            if self._sol_px_ts:
                ts = self._sol_px_ts
        now_sol = (amt * px) / sol_px
        return round((now_sol / cost - 1) * 100, 2), age, ts, "price"

    def fires_today(self):
        cut = self.d.now() - 86400
        self._fires = [t for t in self._fires if t >= cut]
        return len(self._fires)

    # ---- one tick ----
    def tick(self):
        with self._lock:
            rows = list(self._rows)
            armed = bool(self.cfg.get("armed"))
        to_fire = []
        for r in rows:
            m = r["mint"]
            st = self._pos.setdefault(m, PosState(m))
            pnl, age, px_ts, src = self.live_pnl(r)
            self._mark_hot(r, pnl)
            action, reason, kind = decide(
                st, pnl_pct=pnl, limits=limits_for(self.cfg, m),
                now=self.d.now(), price_age_s=age, basis=r.get("basis"),
                in_flight=(m in self._in_flight), armed=armed, cfg=self.cfg,
                fires_today=self.fires_today(), price_ts=px_ts)
            if action == FIRE:
                # src/age travel with the fire: which feed the decision came off
                # and how old that number was is the whole latency question, and
                # it is not recoverable after the fact.
                to_fire.append((r, st, kind, pnl, src, age))
        for r, st, kind, pnl, src, age in to_fire:
            self._fire(r, st, kind, pnl, src=src, age=age)
        return to_fire

    def _fire(self, row, st, kind, pnl, src=None, age=None):
        mint = row["mint"]
        pct = sell_pct_for(self.cfg, kind)
        if pct <= 0:
            return
        sym = row.get("symbol") or mint[:4]

        # NON-BLOCKING, and that is load-bearing. _fire runs synchronously on
        # the single tick thread, and api_sell holds this same lock across a
        # Jupiter quote + build + send (up to ~15s on a timeout). Waiting here
        # would stall the decision loop for EVERY OTHER POSITION behind one slow
        # manual swap -- reintroducing exactly the cross-position blocking that
        # per-mint _in_flight exists to avoid.
        #
        # A failed acquire means someone is already selling this precise mint,
        # which is the one moment standing down is right. Unlatch so the next
        # tick re-decides from live numbers instead of leaving the side latched
        # as fired with no sell behind it.
        lk = self.d.lock(mint)
        if not lk.acquire(False):
            st.fired_kind, st.fired_ts, st.state = None, 0.0, PENDING
            self.event("%s %s deferred — a sell is already in flight on it"
                       % (kind.upper(), sym), "warn")
            return

        # Mark BEFORE the sell: _in_flight is what stops the next tick (3s away)
        # from firing the same position again while this one is still building.
        with self._lock:
            self._in_flight.add(mint)
            self._fires.append(self.d.now())
        st.state = FIRED
        st.fired_kind = kind
        st.fired_ts = self.d.now()
        self.event("%s %s %.1f%% -> SELL %g%%" % (kind.upper(), sym, pnl, pct),
                   kind)

        # Journalled only now, with every double-fire guard above already set.
        jid = self._next_jid()
        st.fired_jid = jid
        self._journal("fire", jid=jid, mint=mint, symbol=sym, kind=kind,
                      pnl_pct=pnl, sell_pct=pct,
                      price_source=src, price_age_s=age, streak=st.streak,
                      cost_sol=row.get("cost_sol"), amount=row.get("amount"),
                      basis=row.get("basis"), cfg=self._cfg_snapshot(mint))

        def _send():
            t0 = self.d.now()
            try:
                res = self.d.sell(mint, pct, self.cfg.get("slippage_bps"),
                                  self.cfg.get("fee_mode") or "mev")
                st.fired_sig = (res or {}).get("signature")
                self.event("%s %s sent %s" % (kind.upper(), sym,
                                              (st.fired_sig or "?")[:16]), kind)
                self._journal("sent", jid=jid, mint=mint, kind=kind,
                              sig=st.fired_sig, send_s=self.d.now() - t0)
            except Exception as e:
                # A failed send must NOT leave the side latched as fired -- the
                # position is still exposed and the next tick has to try again.
                st.fired_kind = None
                st.fired_ts = 0.0
                self.event("%s %s FAILED: %s" % (kind.upper(), sym, e), "error")
                self._journal("send_fail", jid=jid, mint=mint, kind=kind,
                              err=str(e), send_s=self.d.now() - t0)
            finally:
                lk.release()
                with self._lock:
                    self._in_flight.discard(mint)
            self.refresh_holdings("post-fire")

        # Everything that protects against a double-fire (the mint lock, the
        # _in_flight mark, the latch) is already set ABOVE, synchronously. Only
        # the slow part -- quote, build, sign, send, ~1.2s measured -- is handed
        # off, so a second position crossing in the same second is decided
        # immediately instead of queueing behind this one's network round-trip.
        # Inline when no loop is running so the stress harness stays
        # deterministic; see start().
        if self._thread and self._thread.is_alive():
            threading.Thread(target=_send, daemon=True,
                             name="tpsl-sell-" + mint[:6]).start()
        else:
            _send()

    def sell_settled(self, mint, status, err=None):
        """A sell this executor sent has resolved on chain. THE MISSING HALF.

        execute_swap(wait=False) returns at "submitted", so _fire only ever saw
        a send succeed -- the revert arrived ~30s later on the confirm thread,
        which journalled it and told nobody. Consequences, measured over the
        five fires of 2026-08-13: three reverted (two 6001 SlippageToleranceExceeded,
        one 6024) and in every case the side stayed LATCHED as fired with the
        position still fully held. The stop-loss had, in effect, disarmed itself
        at the exact moment it was needed, and the UI said "sent".

        failed  -> nothing moved. Unlatch and let the next tick re-decide.
        timeout -> may yet land. Also unlatch: the sell path re-reads the live
                   token balance to size, so if it did land the retry finds no
                   balance and raises rather than double-selling.
        """
        st = self._pos.get(mint)
        sym = mint[:4]
        for r in self._rows:
            if r.get("mint") == mint:
                sym = r.get("symbol") or sym
                break
        jid = getattr(st, "fired_jid", None) if st is not None else None
        self._journal("settled", jid=jid, mint=mint, symbol=sym,
                      status=status, err=(str(err) if err else None),
                      sig=(st.fired_sig if st is not None else None),
                      latency_s=((self.d.now() - st.fired_ts)
                                 if st is not None and st.fired_ts else None))
        if status == "confirmed":
            if st:
                st.last_reason = "sell confirmed"
            return
        if st is not None:
            st.fired_kind = None
            st.state = PENDING
            st.streak, st.side, st.last_px_ts = 0, None, None
            # Re-arm after a short breather rather than the full cooldown: an
            # exit that did not happen is still an open exposure, but hammering
            # a mint that keeps reverting just burns tips. The rebuilt
            # confirm_ticks streak adds a second or two of its own on top.
            cd = _num(self.cfg.get("cooldown_s"), DEFAULTS["cooldown_s"]) or 0.0
            retry = _num(self.cfg.get("retry_after_fail_s"),
                         DEFAULTS["retry_after_fail_s"]) or 0.0
            st.fired_ts = self.d.now() - max(0.0, cd - retry)
        self.event("SELL %s did not land (%s%s) — re-arming, still holding"
                   % (sym, status, (": %s" % err) if err else ""), "error")
        self.refresh_holdings("post-fail")

    # ---- loop ----
    def _bg_holdings(self):
        """get_holdings() is a trade-class account scan and can take seconds.
        Run inline it stalled EVERY position's decision -- including the price
        refresh -- for its whole duration, once a minute and again after every
        fill. It only reports amounts, which change when you trade and not
        otherwise, so nothing needs it to be synchronous with a tick."""
        if self._holds_running:
            return
        self._holds_running = True

        def run():
            try:
                self.refresh_holdings()
            finally:
                self._holds_running = False

        threading.Thread(target=run, daemon=True, name="tpsl-holdings").start()

    def _bg_prices(self):
        """The batched screen is one HTTP call with a 10s ceiling. Inline, that
        ceiling was a 10s freeze of the whole decision loop -- including the
        quote-driven positions that do not even use this feed. Single-flight for
        the same reason quotes are: a slow response must not stack up behind
        itself once per tick."""
        if self._px_running:
            return
        self._px_running = True

        def run():
            try:
                self.refresh_prices()
            finally:
                self._px_running = False

        threading.Thread(target=run, daemon=True, name="tpsl-prices").start()

    def _loop(self):
        self.refresh_holdings("start")
        nxt = self.d.now()
        while not self._stop.is_set():
            try:
                now = self.d.now()
                armed = bool(self.cfg.get("armed"))
                if armed and not self._was_armed:
                    # just armed: refresh everything now, not in up to 20s
                    self._px_due = now
                    self._hold_due = now
                self._was_armed = armed
                if now >= self._hold_due:
                    self._hold_due = now + self.hold_every_s
                    self._bg_holdings()
                if now >= self._px_due:
                    self._px_due = now + (self.price_every_s if armed
                                          else self.idle_price_every_s)
                    self._bg_prices()
                # Hot mints re-quote every tick; the batched screen above runs
                # at its own slower cadence. See refresh_quotes().
                self.refresh_quotes()
                self.tick()
            except Exception as e:
                self._last_err = "loop: %s" % e
                try:
                    self.d.log("[tpsl] loop error: %s" % e)
                except Exception:
                    pass
            # FIXED RATE, not fixed delay. `wait(period)` after the work meant
            # the true period was period + however long the work took, so the
            # loop silently ran slower than configured exactly when the network
            # was slow -- i.e. when it mattered.
            nxt += self.tick_every_s
            delay = nxt - self.d.now()
            if delay <= 0:                 # fell behind; resync, do not spiral
                nxt = self.d.now()
                delay = 0.05
            self._stop.wait(delay)

    # ---- for the UI ----
    def status(self):
        with self._lock:
            rows = list(self._rows)
            events = list(self._events[:25])
        out = []
        for r in rows:
            m = r["mint"]
            st = self._pos.get(m) or PosState(m)
            pnl, age, _ts, src = self.live_pnl(r)
            lim = limits_for(self.cfg, m)
            out.append({"mint": m, "symbol": r.get("symbol"),
                        "amount": r.get("amount"), "usd": r.get("usd"),
                        "priced": r.get("priced"),
                        "basis": r.get("basis"), "cost_sol": r.get("cost_sol"),
                        "pnl_pct": pnl,
                        # which feed this PnL came from, and whether the mint is
                        # currently worth paying for live quotes on
                        "pnl_src": src, "hot": m in self._hot,
                        "price_age_s": round(age, 1) if age is not None else None,
                        "tp": lim["tp"], "sl": lim["sl"], "off": lim["off"],
                        "protected": bool(r.get("basis") == "ok" and not lim["off"]),
                        **st.snapshot()})
        return {"ok": True, "armed": bool(self.cfg.get("armed")),
                "cfg": {k: v for k, v in self.cfg.items() if k != "per"},
                "per": self.cfg.get("per") or {},
                "positions": out, "events": events,
                "fires_today": self.fires_today(),
                "last_error": self._last_err,
                # the pop-out renders entirely off this payload, so it carries
                # the wallet totals too -- that keeps the trade box on ZERO
                # trade-class RPC of its own instead of a /api/holdings every 6s.
                "total_usd": self._total_usd, "sol_balance": self._sol_balance,
                "running": bool(self._thread and self._thread.is_alive())}
