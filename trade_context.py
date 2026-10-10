"""trade_context.py — record WHY a trade happened, not just that it did.

trades.jsonl is a receipt: ts, mint, sol, token_raw, sig, status. Enough to
compute PnL and cost basis, and useless for reconstructing a strategy. You
cannot look at `{"side":"buy","sol":0.1}` and recover "fresh pool, 5m volume
ramping, bought the retrace off the first dump". This module captures the
market state and the price action AROUND each fill so that decision is
recoverable later.

WHY THIS RUNS AT FILL TIME AND NOT AS A NIGHTLY BACKFILL
--------------------------------------------------------
The inputs are perishable, and the ones that matter most rot fastest:

  * GeckoTerminal /pools/{pool}/trades is a short rolling window (hours).
    The 15s candles are BUILT from it, so sub-minute price action is simply
    gone by tomorrow.
  * OHLCV comes back limit=150 — at 15s that is ~37 minutes of history, at
    1m about 2.5 hours. Ask a day later and the entry candle is off the end.
  * Dead pump.fun pools thin out and eventually stop returning useful data.

So a backfill job does not reconstruct this dataset, it produces a worse
different one. Everything here is snapshotted inline, off the trade thread.

WHAT IT WRITES
--------------
trade_context.jsonl   one record per (signature, phase). phase="fill" is the
                      state at the moment of the fill; the t+5m / t+30m / t+2h
                      phases are what happened next. Entries alone can only
                      score entries — without the follow-ups there is no way to
                      ask "should he have held", which is the question an exit
                      strategy is made of.
considered.jsonl      every CA pasted into the trade box that did NOT become a
                      trade. A filter learned only from taken trades has no
                      negative class; this is the other half, and it is
                      impossible to reconstruct after the fact.

WHAT IT DELIBERATELY DOES NOT DO
--------------------------------
No scoring, no classification, no inferred rules. It is a recorder. Deciding
what the strategy IS comes later, from the data, with enough trades to mean
something.
"""

from __future__ import annotations

import json
import os
import queue
import threading
import time

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CTX_PATH = os.path.join(HERE, "trade_context.jsonl")
CONSIDERED_PATH = os.path.join(HERE, "considered.jsonl")
PENDING_PATH = os.path.join(HERE, "_ctx_pending.json")

# how long after a fill to re-snapshot. The point of these is exit analysis:
# what the position did after he acted, on the horizons a manual trader is
# actually deciding over.
FOLLOWUPS = [(300, "t+5m"), (1800, "t+30m"), (7200, "t+2h")]

# candle history kept per timeframe. GT returns up to 150; trimming keeps the
# file from being mostly redundant tail while preserving the run-up into entry.
CANDLE_KEEP = {"15s": 120, "1m": 120, "5m": 96}

_HTTP = requests.Session()
_HTTP.headers.update({"User-Agent": "solseller-ctx/1"})

_write_lock = threading.Lock()


def _append(path, rec):
    line = json.dumps(rec) + "\n"
    with _write_lock, open(path, "a", encoding="utf-8") as f:
        f.write(line)


def _f(v, d=None):
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def dex_snapshot(mint):
    """Market state from dexscreener: the raw material for volume/freshness
    filters. Picks the deepest pool, which is the one the price is actually
    made on."""
    try:
        r = _HTTP.get("https://api.dexscreener.com/latest/dex/tokens/" + mint,
                      timeout=10)
        pairs = (r.json() or {}).get("pairs") or []
    except Exception:
        return None
    if not pairs:
        return None
    p = max(pairs, key=lambda x: _f((x.get("liquidity") or {}).get("usd"), 0.0))
    liq, vol = p.get("liquidity") or {}, p.get("volume") or {}
    chg, tx = p.get("priceChange") or {}, p.get("txns") or {}
    created = p.get("pairCreatedAt")

    def side(w, k):
        return (tx.get(w) or {}).get(k)

    return {
        "pool": p.get("pairAddress"), "dex": p.get("dexId"),
        "price_usd": _f(p.get("priceUsd")),
        "fdv": _f(p.get("fdv")), "mcap": _f(p.get("marketCap")),
        "liq_usd": _f(liq.get("usd")), "liq_base": _f(liq.get("base")),
        "liq_quote": _f(liq.get("quote")),
        "vol_m5": _f(vol.get("m5")), "vol_h1": _f(vol.get("h1")),
        "vol_h6": _f(vol.get("h6")), "vol_h24": _f(vol.get("h24")),
        "chg_m5": _f(chg.get("m5")), "chg_h1": _f(chg.get("h1")),
        "chg_h6": _f(chg.get("h6")), "chg_h24": _f(chg.get("h24")),
        "buys_m5": side("m5", "buys"), "sells_m5": side("m5", "sells"),
        "buys_h1": side("h1", "buys"), "sells_h1": side("h1", "sells"),
        "pair_created_ms": created,
        # freshness, which is filter #1's whole point, precomputed so nothing
        # downstream has to know dexscreener uses milliseconds
        "age_min": round((time.time() - created / 1000) / 60, 2) if created else None,
    }


class Recorder:
    """Bounded worker + scheduler. Never raises into the trade path.

    ONE worker, not a thread per fill. The confirm thread that calls this
    already does _chain_fill (up to 4 RPC with backoff) and a holdings refresh;
    adding pool resolution plus three candle fetches per fill on top of that,
    unbounded, is how a snipe burst turns into dozens of concurrent HTTP
    conversations against two rate-limited providers. The queue drops (loudly)
    rather than growing without limit.
    """

    def __init__(self, candles_fn=None, maxsize=256):
        # candles_fn(mint, tf) -> payload dict, injected from jup_trader so we
        # reuse its cache, single-flight and 429 handling instead of opening a
        # second uncoordinated path to the same provider.
        self.candles_fn = candles_fn
        self.q = queue.Queue(maxsize=maxsize)
        self._pending = []          # [(due_ts, job)] for follow-ups
        self._plock = threading.Lock()
        self._stop = threading.Event()
        self._worker = None
        self._sched = None
        self.dropped = 0
        self.written = 0
        self.last_error = None
        self._load_pending()

    # ---- lifecycle ----
    def start(self):
        if self._worker and self._worker.is_alive():
            return
        self._stop.clear()
        self._worker = threading.Thread(target=self._run, daemon=True,
                                        name="ctx-worker")
        self._sched = threading.Thread(target=self._run_sched, daemon=True,
                                       name="ctx-sched")
        self._worker.start()
        self._sched.start()

    def stop(self):
        self._stop.set()

    # ---- public API ----
    def record_fill(self, *, sig, mint, side, ts, sol, token_raw, symbol=None,
                    fee_mode=None, status="confirmed"):
        job = {"kind": "fill", "phase": "fill", "sig": sig, "mint": mint,
               "side": side, "trade_ts": ts, "sol": sol,
               "token_raw": token_raw, "symbol": symbol, "fee_mode": fee_mode,
               "status": status}
        self._enqueue(job)
        # queue the follow-ups now so a fill is never recorded without them
        for delay, phase in FOLLOWUPS:
            self._schedule(time.time() + delay,
                           dict(job, kind="followup", phase=phase))

    def record_considered(self, mint, symbol=None, source="paste"):
        """A CA he looked at. The negative class for filter learning."""
        self._enqueue({"kind": "considered", "phase": "considered",
                       "mint": mint, "symbol": symbol, "source": source,
                       "trade_ts": time.time()})

    def _enqueue(self, job):
        try:
            self.q.put_nowait(job)
        except queue.Full:
            self.dropped += 1
            # Loud on purpose: a silently dropped capture looks exactly like a
            # trade that was never made when the data is read back weeks later.
            print("[ctx] QUEUE FULL — dropped %s for %s (dropped=%d)"
                  % (job.get("phase"), (job.get("mint") or "?")[:10], self.dropped))

    # ---- follow-up scheduling (survives a restart) ----
    def _schedule(self, due, job):
        with self._plock:
            self._pending.append([due, job])
            self._save_pending()

    def _load_pending(self):
        try:
            with open(PENDING_PATH, encoding="utf-8") as f:
                self._pending = json.load(f) or []
        except Exception:
            self._pending = []

    def _save_pending(self):
        try:
            tmp = PENDING_PATH + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._pending, f)
            os.replace(tmp, PENDING_PATH)
        except Exception:
            pass

    def _run_sched(self):
        while not self._stop.is_set():
            now = time.time()
            due = []
            with self._plock:
                keep = []
                for row in self._pending:
                    (due if row[0] <= now else keep).append(row)
                if due:
                    self._pending = keep
                    self._save_pending()
            for _, job in due:
                self._enqueue(job)
            self._stop.wait(15)

    # ---- worker ----
    def _run(self):
        while not self._stop.is_set():
            try:
                job = self.q.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                self._capture(job)
            except Exception as e:
                self.last_error = str(e)[:200]
                print("[ctx] capture failed for %s: %s"
                      % ((job.get("mint") or "?")[:10], e))
            finally:
                self.q.task_done()

    def _candles(self, mint):
        """Multi-timeframe price action. 15s is the one that cannot be
        recovered later, so it is fetched first and never skipped."""
        out = {}
        if not self.candles_fn:
            return out
        for tf in ("15s", "1m", "5m"):
            try:
                p = self.candles_fn(mint, tf) or {}
                c = p.get("candles") or []
                if c:
                    out[tf] = c[-CANDLE_KEEP.get(tf, 120):]
            except Exception:
                pass                       # one timeframe failing is not fatal
        return out

    def _capture(self, job):
        t0 = time.time()
        mint = job["mint"]
        # A brand-new pump.fun pool is not indexed the instant the buy lands --
        # the same lag _recent_buy_mints() exists to work around. Those are
        # exactly the trades filter #1 cares about, so retry rather than write
        # an empty snapshot for the freshest entries.
        snap = None
        for attempt in range(4):
            snap = dex_snapshot(mint)
            if snap and snap.get("price_usd"):
                break
            if self._stop.wait(2 + attempt * 3):
                break
        rec = {
            "ts": round(time.time(), 3),
            "phase": job.get("phase"),
            "kind": job.get("kind"),
            "mint": mint,
            "symbol": job.get("symbol"),
            "market": snap,
        }
        if job.get("kind") in ("fill", "followup"):
            rec.update({
                "sig": job.get("sig"), "side": job.get("side"),
                "trade_ts": job.get("trade_ts"), "status": job.get("status"),
                "fee_mode": job.get("fee_mode"),
                "sol": job.get("sol"), "token_raw": job.get("token_raw"),
                # seconds since the fill, so follow-ups are self-describing
                # even if FOLLOWUPS is retuned later
                "age_s": round(time.time() - (job.get("trade_ts") or time.time()), 1),
            })
        rec["candles"] = self._candles(mint)
        rec["capture_ms"] = int((time.time() - t0) * 1000)
        _append(CTX_PATH if job.get("kind") != "considered" else CONSIDERED_PATH,
                rec)
        self.written += 1

    # ---- for the UI / debugging ----
    def status(self):
        with self._plock:
            pend = len(self._pending)
        return {"running": bool(self._worker and self._worker.is_alive()),
                "queued": self.q.qsize(), "pending_followups": pend,
                "written": self.written, "dropped": self.dropped,
                "last_error": self.last_error}
