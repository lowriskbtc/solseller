"""
smart_exit.py — flow-aware exit engine for the sniper (replaces fixed-% exits
with market-evidence exits; dual-policy scored, paper-enforced, live-shadow).

Why fixed % is the wrong shape (measured 2026-07-24, own logs):
- Grad-sleeve flow slice (n=1292): volume features move WR (sell-dominated 5m
  flow 6.3% WR vs 22.4% base) but the volume-PASS bucket loses MORE per trade
  (-1.80c vs -1.47c): high-volume coins gap THROUGH the -40% stop (184/349
  losses deeper than a clean SL), thin coins bleed to timeout. The bleed is in
  the exit, not the pick.
- Exit sweep (n=1866): every TP below +100% LOSES money (winners that run pay
  for everything) -> smart exits must NOT cap winners earlier; SL -25% beats
  -40% by +5.7 SOL -> losers must be cut before the crater.
Conclusion: exit on CROWD EVIDENCE (who is selling, is anyone still buying),
not on price thresholds. Price thresholds survive only as crash brakes.

Signals (first match wins, evaluated every mark tick ~1s):
  crash-brake   ret <= -sl_pct                      (unchanged safety net)
  creator-sell  the copy target sells his own launch (he IS the whale)
  whale-dump    any single sell >= whale_sol in the flow window
  flow-flip     >=2 sells, sell_sol >= flip_min_sol and > flip_ratio x buy_sol
  gap-guard     ret <= gap_ret (~-22%) while 10s net flow is not positive —
                grounded by the SL sweep: out before -40% becomes -80% on a gap
  trail-lock    peak >= trail_arm% and price gave back to trail_keep x peak —
                wide on purpose; flow-flip is the primary winner exit
  stall         no prints for stall_s and ret < stall_max_ret (dead launch;
                do not wait 15 min to book the same loss)
  smart-timeout age >= timeout_min                  (shared with fixed policy)
Flow rules arm only after the mint's first trade print — a dead websocket must
degrade to crash-brake/timeout behaviour, never fire stall on missing data.

Deployment contract (house rule: nothing enforces without forward evidence):
- PAPER positions: smart policy actually closes the position ("smart-exit …"
  notes). Paper is the test bed; the cost of being wrong is zero and the
  forward comparison is the product.
- LIVE positions: fast-exit ladder stays authoritative. Smart verdicts are
  STAMPED (smart_trigger_*) — enforcement is a config flip only after the
  scoreboard says smart beats fixed on forward data.
- EVERY position gets a ghost of the policy it did NOT run (fixed sim under
  paper-smart, smart sim under live-fixed), marked until both sides resolve;
  jup_trader appends the pair to exit_compare.jsonl. _smart_exit_report.py
  ranks the policies on identical trades — same-trade ranking cancels most of
  the paper-mark optimism, same trick as _snipe_exit_sweep.py.

Flow source: PumpPortal subscribeTokenTrade for held mints only (subscribe on
open, unsubscribe once the comparison row is written). record_trade() is fed
by jup_trader's existing websocket thread; this module never opens sockets and
never touches files.
"""

from __future__ import annotations

import threading
import time
from collections import deque

# safe fallbacks; every knob is overridable through sniper_config.json
DEFAULTS = {
    "flow_window_s": 20.0,
    "whale_sol": 1.0,
    "flip_ratio": 1.5,
    "flip_min_sol": 0.4,
    "stall_s": 40.0,
    "stall_max_ret": 10.0,   # % — a stalled coin still up more than this keeps riding
    "gap_ret": -22.0,        # % — evidence-backed early stop (SL sweep: -25 > -40)
    "trail_arm": 60.0,       # % peak return before the trail arms
    "trail_keep": 0.55,      # exit when ret <= peak_ret * keep
}

TRADE_KEEP_S = 120          # flow memory per mint; rules look at <= flow_window_s
TRADE_MAXLEN = 600          # hard cap so a hot mint can't grow unbounded


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


class Engine:
    def __init__(self):
        self._lock = threading.Lock()
        self._trades = {}       # mint -> deque[(ts, side, sol, trader)]
        self._state = {}        # mint -> dual-policy position state
        self._ws_out = deque()  # ("subscribeTokenTrade"|"unsubscribeTokenTrade", mint)
        # cumulative prints per mint. _trades is a trailing window (and is popped
        # on compare), so it cannot answer "did this position EVER see flow?" —
        # and that is the question that matters: from 2026-07-24 to 07-27 the
        # feed delivered zero prints and every "smart" verdict was really a
        # blind timeout, which the scoreboard scored as a policy loss. See
        # flow_source.py. The journaling side quarantines rows where this is 0.
        self._prints = {}       # mint -> int

    # ---------------------------------------------------------------- intake --

    def register(self, pos, enforce_smart):
        """Track a just-opened position. enforce_smart=True means the smart
        policy drives the real close (paper book); False means it shadows."""
        mint = pos.get("mint")
        if not mint:
            return
        with self._lock:
            if mint in self._state:
                return
            self._state[mint] = {
                "pos": {k: pos.get(k) for k in
                        ("mint", "symbol", "source", "mode", "sol_in",
                         "entry_mcap", "detected_ts")},
                "enforced": "smart" if enforce_smart else "fixed",
                "smart": {"done": False},
                "fixed": {"done": False, "chunks": 0, "marks": []},
                "peak_mcap": pos.get("entry_mcap") or 0.0,
                "last_mcap": pos.get("entry_mcap"),
                "stamped": False,
                "closed": False,
            }
            self._state[mint]["registered_ts"] = time.time()
            self._trades.setdefault(mint, deque(maxlen=TRADE_MAXLEN))
            self._prints.setdefault(mint, 0)
            self._ws_out.append(("subscribeTokenTrade", mint))

    def record_trade(self, ev, ignore_trader=None):
        """PumpPortal buy/sell event for a subscribed mint (websocket thread).
        Our own live sells must not count as crowd flow."""
        mint = ev.get("mint")
        if not mint:
            return
        trader = ev.get("traderPublicKey")
        if ignore_trader and trader == ignore_trader:
            return
        row = (time.time(), ev.get("txType"), _f(ev.get("solAmount")), trader)
        with self._lock:
            dq = self._trades.get(mint)
            if dq is None:      # not ours (or already comp­ared-and-dropped)
                return
            dq.append(row)
            self._prints[mint] = self._prints.get(mint, 0) + 1

    # ------------------------------------------------------------------ flow --

    def _flow(self, mint, now, window_s):
        """(buy_sol, sell_sol, n_sells, biggest_sell, last_trade_ts, seen_any)
        over the trailing window. Caller holds the lock."""
        dq = self._trades.get(mint)
        if not dq:
            return 0.0, 0.0, 0, 0.0, None, False
        while dq and now - dq[0][0] > TRADE_KEEP_S:
            dq.popleft()
        buy = sell = big = 0.0
        n_sells = 0
        last_ts = dq[-1][0] if dq else None
        for ts, side, sol, _ in dq:
            if now - ts > window_s:
                continue
            if side == "sell":
                sell += sol
                n_sells += 1
                big = max(big, sol)
            elif side == "buy":
                buy += sol
        return buy, sell, n_sells, big, last_ts, bool(dq)

    def flow_snapshot(self, mint, now, window_s):
        """Public read of the trailing flow window, for other policy engines
        (pyramid.py) that need the same evidence without opening a second
        subscription. Returns the _flow() tuple plus the cumulative print count:
        (buy_sol, sell_sol, n_sells, biggest_sell, last_trade_ts, seen, prints).

        `prints` is cumulative and `seen` is window-local — a position that had
        flow but has gone quiet reads seen=False, prints>0. Callers must treat
        prints==0 as "never armed" (no evidence) and NOT as "dead", which is the
        distinction the 07-24→07-27 blind window turned into 36 bad verdicts."""
        with self._lock:
            return (*self._flow(mint, now, window_s), self._prints.get(mint, 0))

    # ------------------------------------------------------------- decisions --

    def _decide_smart(self, st, mcap, now, cfg):
        """Reason string when the smart policy wants out, else None."""
        p = st["pos"]
        entry = p.get("entry_mcap")
        if not entry or not mcap:
            # unpriceable: only the shared timeout applies
            if now - (p.get("detected_ts") or now) >= cfg["timeout_min"] * 60:
                return "smart-timeout (no price)"
            return None
        ret = (mcap / entry - 1) * 100
        peak_ret = (st["peak_mcap"] / entry - 1) * 100 if st["peak_mcap"] else ret
        age = now - (p.get("detected_ts") or now)

        if ret <= -abs(cfg["sl_pct"]):
            return f"crash-brake {ret:.0f}%"

        buy, sell, n_sells, big, last_ts, seen = self._flow(
            p["mint"], now, cfg["flow_window_s"])
        if seen:
            dq = self._trades.get(p["mint"]) or ()
            tgt = cfg.get("target")
            if tgt and any(s == "sell" and t == tgt for _, s, _sol, t in dq):
                return "creator-sell"
            if big >= cfg["whale_sol"]:
                return f"whale-dump {big:.2f}S"
            if (n_sells >= 2 and sell >= cfg["flip_min_sol"]
                    and sell > cfg["flip_ratio"] * max(buy, 1e-9)):
                return f"flow-flip {sell:.2f}v{buy:.2f}S"
            b10, s10, _, _, _, _ = self._flow(p["mint"], now, 10.0)
            if ret <= cfg["gap_ret"] and b10 - s10 <= 0:
                return f"gap-guard {ret:.0f}%"
            if (last_ts and now - last_ts >= cfg["stall_s"]
                    and ret < cfg["stall_max_ret"]):
                return f"stall {now - last_ts:.0f}s ({ret:+.0f}%)"
        if peak_ret >= cfg["trail_arm"] and ret <= peak_ret * cfg["trail_keep"]:
            return f"trail-lock peak+{peak_ret:.0f}%→{ret:+.0f}%"
        if age >= cfg["timeout_min"] * 60:
            return f"smart-timeout ({ret:+.0f}%)"
        return None

    def _advance_fixed(self, st, mcap, graduated, now, cfg):
        """Ghost of today's fixed policy, replicated from _mark_and_exit:
        copy-target = fast-exit ladder (close at the AVERAGE of chunk marks)
        with the SL crash brake; everything else = TP/SL/timeout."""
        fx, p = st["fixed"], st["pos"]
        if fx["done"]:
            return
        entry = p.get("entry_mcap")
        age = now - (p.get("detected_ts") or now)
        if not entry or not mcap:
            if age >= cfg["timeout_min"] * 60:
                self._resolve(fx, st.get("last_mcap") or entry,
                              "timeout (no price)", graduated, entry)
            return
        ret = (mcap / entry - 1) * 100
        if p.get("source") == "copy_target" and cfg["fast_exit"]:
            if ret <= -abs(cfg["sl_pct"]):
                self._resolve(fx, mcap, f"fast-exit stop {ret:.0f}%",
                              graduated, entry)
                return
            start, gap = cfg["fast_exit_start_s"], max(1.0, cfg["fast_exit_gap_s"])
            chunks = max(1, int(cfg["fast_exit_chunks"]))
            due = (min(chunks, 1 + int((age - start) // gap))
                   if age >= start else 0)
            if due > fx["chunks"]:
                fx["marks"].append(mcap)
                fx["chunks"] = due
                if due >= chunks:
                    marks = fx["marks"] or [mcap]
                    self._resolve(fx, sum(marks) / len(marks),
                                  f"fast-exit flat @{age:.0f}s ({ret:+.0f}%)",
                                  graduated, entry)
            return
        if ret >= cfg["tp_pct"]:
            self._resolve(fx, mcap, f"take-profit +{ret:.0f}%", graduated, entry)
        elif ret <= -abs(cfg["sl_pct"]):
            self._resolve(fx, mcap, f"stop {ret:.0f}%", graduated, entry)
        elif age >= cfg["timeout_min"] * 60:
            self._resolve(fx, mcap, f"timeout ({ret:+.0f}%)", graduated, entry)

    @staticmethod
    def _resolve(side, exit_mcap, note, graduated, entry):
        side.update(done=True, exit_mcap=exit_mcap, note=note,
                    graduated=bool(graduated), ts=round(time.time(), 3),
                    ret_pct=round((exit_mcap / entry - 1) * 100, 2)
                    if (entry and exit_mcap) else None)

    # ------------------------------------------------------------- mark hook --

    def on_mark(self, pos, mcap, graduated, now, cfg):
        """Advance both policies for an OPEN position. Returns
        {"enforced": bool, "close": note|None, "stamp": note|None}."""
        mint = pos.get("mint")
        with self._lock:
            known = mint in self._state
        if not known:
            # restart survivor: adopt it mid-flight (flow starts now)
            self.register(pos, cfg["enforce"] and pos.get("mode") != "live")
        with self._lock:
            st = self._state.get(mint)
            if st is None:
                return {"enforced": False, "close": None, "stamp": None}
            if mcap:
                st["peak_mcap"] = max(st["peak_mcap"] or 0, mcap)
                st["last_mcap"] = mcap
            self._advance_fixed(st, mcap, graduated, now, cfg)
            out = {"enforced": st["enforced"] == "smart",
                   "close": None, "stamp": None}
            if not st["smart"]["done"]:
                reason = self._decide_smart(st, mcap, now, cfg)
                if reason:
                    self._resolve(st["smart"], mcap or st.get("last_mcap"),
                                  reason, graduated, st["pos"].get("entry_mcap"))
                    if st["enforced"] == "smart":
                        out["close"] = f"smart-exit {reason}"
                    elif not st["stamped"]:
                        st["stamped"] = True
                        out["stamp"] = reason
            return out

    def actual_closed(self, pos):
        """Called from _close_snipe: the REAL close overwrites the enforced
        side, so the comparison always scores actual outcome vs ghost."""
        with self._lock:
            st = self._state.get(pos.get("mint"))
            if st is None:
                return
            st["closed"] = True
            side = st[st["enforced"]]
            side.update(done=True, exit_mcap=pos.get("exit_mcap"),
                        note=pos.get("note"), graduated=bool(pos.get("graduated")),
                        ts=round(time.time(), 3))
            entry = st["pos"].get("entry_mcap")
            side["ret_pct"] = (round((pos["exit_mcap"] / entry - 1) * 100, 2)
                               if entry and pos.get("exit_mcap") else None)
            if pos.get("pnl_sol") is not None:
                side["pnl_sol"] = pos["pnl_sol"]

    # ------------------------------------------------------------- ghost path --

    def ghost_mints(self):
        """Mints whose position is closed but whose ghost policy still needs
        marks. jup_trader keeps pricing these in the same 1s sweep."""
        with self._lock:
            return [m for m, st in self._state.items()
                    if st["closed"] and not (st["smart"]["done"]
                                             and st["fixed"]["done"])]

    def ghost_mark(self, mint, mcap, graduated, now, cfg):
        with self._lock:
            st = self._state.get(mint)
            if st is None:
                return
            if mcap:
                st["peak_mcap"] = max(st["peak_mcap"] or 0, mcap)
                st["last_mcap"] = mcap
            self._advance_fixed(st, mcap, graduated, now, cfg)
            if not st["smart"]["done"]:
                reason = self._decide_smart(st, mcap, now, cfg)
                if reason:
                    self._resolve(st["smart"], mcap or st.get("last_mcap"),
                                  reason, graduated, st["pos"].get("entry_mcap"))

    def reap(self, now, max_age_s):
        """Force-release states that can no longer finish on their own.

        Two leaks, both seen in production on 2026-07-28 (25 opens / 23 closes,
        engine left holding 3 states with `open: 0` on the sniper side):

        * **never-closed** — a position that never gets a close event (the buy
          failed, or it was abandoned) is registered by on_mark but
          `actual_closed` never runs. `ghost_mints()` requires `closed`, so the
          ghost sweep never touches it: it is tracked forever and its flow
          subscription is never released. That is the subscription leak.
        * **ghost-stuck** — a closed position whose ghost side never resolved,
          so `pop_compares` never pops it.

        Rather than chase every path that can strand a position, anything older
        than max_age_s is closed out unconditionally. Rows are stamped `reaped`
        so journaling quarantines them: a position that never closed has no
        outcome, and a ghost resolved half an hour late is not a verdict.
        Returns [(mint, why, age_s)] for logging.
        """
        out = []
        with self._lock:
            for mint, st in list(self._state.items()):
                if st.get("reaped"):
                    continue
                p = st["pos"]
                # detected_ts is the true birth; registered_ts is only a fallback
                # for a position that somehow arrived without one. Taking max()
                # here would make a restart-adopted state look newborn and it
                # would never be reaped.
                born = p.get("detected_ts") or st.get("registered_ts") or 0
                if not born:
                    continue
                age = now - born
                if age < max_age_s:
                    continue
                why = "ghost-stuck" if st["closed"] else "never-closed"
                st["reaped"] = why
                st["closed"] = True
                for name in ("smart", "fixed"):
                    if not st[name]["done"]:
                        self._resolve(st[name], st.get("last_mcap"),
                                      f"reaped ({why})", False,
                                      p.get("entry_mcap"))
                out.append((mint, why, age))
        return out

    def pop_compares(self):
        """Finished dual-policy rows; caller journals them. Cleans up state and
        queues the unsubscribe."""
        rows = []
        with self._lock:
            done = [m for m, st in self._state.items()
                    if st["closed"] and st["smart"]["done"] and st["fixed"]["done"]]
            for m in done:
                st = self._state.pop(m)
                self._trades.pop(m, None)
                self._ws_out.append(("unsubscribeTokenTrade", m))
                rows.append({
                    "ts": round(time.time(), 3),
                    **st["pos"],
                    "enforced": st["enforced"],
                    "peak_mcap": st["peak_mcap"],
                    # 0 => the flow rules were never armed for this position, so
                    # the row is not a test of the policy. Journaling quarantines
                    # it instead of letting it into exit_compare.jsonl.
                    "flow_prints": self._prints.pop(m, 0),
                    # set when reap() force-closed this state; also quarantined.
                    "reaped": st.get("reaped"),
                    "smart": {k: st["smart"].get(k) for k in
                              ("exit_mcap", "note", "ts", "graduated",
                               "ret_pct", "pnl_sol")},
                    "fixed": {k: st["fixed"].get(k) for k in
                              ("exit_mcap", "note", "ts", "graduated",
                               "ret_pct", "pnl_sol")},
                })
        return rows

    # -------------------------------------------------------------- ws plumbing

    def drain_ws(self):
        """Pending (method, mint) subscribe ops — websocket thread only."""
        out = []
        with self._lock:
            while self._ws_out:
                out.append(self._ws_out.popleft())
        return out

    def active_mints(self):
        """For re-subscribing after a websocket reconnect."""
        with self._lock:
            return list(self._state.keys())

    def status(self):
        with self._lock:
            return {"tracked": len(self._state),
                    "ghosts": sum(1 for st in self._state.values()
                                  if st["closed"]),
                    "flow_mints": sum(1 for dq in self._trades.values() if dq),
                    "prints_total": sum(self._prints.values()),
                    "armed": sum(1 for m in self._state
                                 if self._prints.get(m, 0) > 0)}


def build_cfg(sniper_cfg):
    """Snapshot every knob the engine needs from sniper_cfg (with defaults),
    once per mark sweep — the engine never reads config files itself."""
    g = sniper_cfg.get
    return {
        "enforce": bool(g("smart_exit", True)),
        "sl_pct": float(g("sl_pct", 40.0)),
        "tp_pct": float(g("tp_pct", 100.0)),
        "timeout_min": float(g("timeout_min", 15.0)),
        "fast_exit": bool(g("fast_exit", True)),
        "fast_exit_start_s": float(g("fast_exit_start_s", 15)),
        "fast_exit_gap_s": float(g("fast_exit_gap_s", 10)),
        "fast_exit_chunks": int(g("fast_exit_chunks", 3)),
        "target": g("target"),
        "flow_window_s": float(g("smart_flow_window_s", DEFAULTS["flow_window_s"])),
        "whale_sol": float(g("smart_whale_sol", DEFAULTS["whale_sol"])),
        "flip_ratio": float(g("smart_flip_ratio", DEFAULTS["flip_ratio"])),
        "flip_min_sol": float(g("smart_flip_min_sol", DEFAULTS["flip_min_sol"])),
        "stall_s": float(g("smart_stall_s", DEFAULTS["stall_s"])),
        "stall_max_ret": float(g("smart_stall_max_ret", DEFAULTS["stall_max_ret"])),
        "gap_ret": float(g("smart_gap_ret", DEFAULTS["gap_ret"])),
        "trail_arm": float(g("smart_trail_arm", DEFAULTS["trail_arm"])),
        "trail_keep": float(g("smart_trail_keep", DEFAULTS["trail_keep"])),
    }


ENGINE = Engine()


# ---------------- smart-vs-fixed scoreboard (read-only) ----------------
# Same math _smart_exit_report.py prints to MemeBrain/scoreboard/smart-exit.md,
# exposed for the live /api/smart_exit panel. Kept here (not imported from the
# `_`-prefixed report) so jup_trader never pulls an offline analysis script into
# the reloader's watch set — editing one would restart the trading process.

def _num(x):
    """Coerce a journal field to float, or None if it isn't a number. The
    journal is appended by the live mark loop; a torn or hand-edited line must
    degrade to a skipped row, never a raise (this feeds /api/smart_exit)."""
    if isinstance(x, bool) or x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v == v and v not in (float("inf"), float("-inf")) else None


def pnl_of(side, sol_in, entry):
    """Booked pnl if the enforced side carried it; else the haircut model
    (0.97 graduated / 0.90 curve) so both columns share the same optimism."""
    if not isinstance(side, dict):
        return None
    booked = _num(side.get("pnl_sol"))
    if booked is not None:
        return booked
    xm, e, s = _num(side.get("exit_mcap")), _num(entry), _num(sol_in)
    if not (xm and e and s):
        return 0.0
    haircut = 0.97 if side.get("graduated") else 0.90
    return s * (xm / e) * haircut - s


def trigger_kind(note):
    n = (note or "").lower()
    for k in ("creator-sell", "whale-dump", "flow-flip", "gap-guard",
              "trail-lock", "stall", "crash-brake", "smart-timeout"):
        if k in n:
            return k
    return "other"


def wilson_low(w, n, z=1.96):
    if n == 0:
        return 0.0
    p = w / n
    d = 1 + z * z / n
    return (p + z * z / (2 * n) - z * ((p * (1 - p) + z * z / (4 * n)) / n) ** 0.5) / d


def scoreboard(rows, top_triggers=4):
    """Rank smart vs fixed on identical entries. `rows` are parsed
    exit_compare.jsonl records; the caller owns the file I/O."""
    scored, skipped = [], 0
    for r in rows:
        if not (isinstance(r, dict) and r.get("smart") and r.get("fixed")
                and r.get("entry_mcap")):
            continue
        s = pnl_of(r["smart"], r.get("sol_in"), r["entry_mcap"])
        f = pnl_of(r["fixed"], r.get("sol_in"), r["entry_mcap"])
        if s is None or f is None:      # unscorable row — drop it, keep the rest
            skipped += 1
            continue
        scored.append((r, s, f))
    n = len(scored)
    if not n:
        return {"n": 0, "skipped": skipped}
    rows = [t[0] for t in scored]
    s_pnls = [t[1] for t in scored]
    f_pnls = [t[2] for t in scored]
    ties = sum(1 for s, f in zip(s_pnls, f_pnls) if abs(s - f) <= 1e-9)
    better = sum(1 for s, f in zip(s_pnls, f_pnls) if s > f + 1e-9)
    decided = n - ties
    by_kind = {}
    for r, s, f in zip(rows, s_pnls, f_pnls):
        k = trigger_kind(r["smart"].get("note"))
        d = by_kind.setdefault(k, {"kind": k, "n": 0, "delta": 0.0})
        d["n"] += 1
        d["delta"] += s - f
    return {
        "n": n,
        "skipped": skipped,
        "smart_pnl": round(sum(s_pnls), 4),
        "fixed_pnl": round(sum(f_pnls), 4),
        "delta": round(sum(s_pnls) - sum(f_pnls), 4),
        "smart_wr": round(sum(1 for s in s_pnls if s > 0) / n * 100, 1),
        "fixed_wr": round(sum(1 for f in f_pnls if f > 0) / n * 100, 1),
        "better": better, "decided": decided, "ties": ties,
        "wilson_low": round(wilson_low(better, decided) * 100, 1),
        "triggers": sorted((dict(d, delta=round(d["delta"], 4))
                            for d in by_kind.values()),
                           key=lambda d: -d["n"])[:top_triggers],
    }
