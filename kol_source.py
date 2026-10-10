"""
kol_source.py — kolscan.io leaderboard -> ranked wallets, with persistence.

WHY THIS EXISTS (2026-08-02)
----------------------------
The copy_target sleeve earned +5.586 SOL between 07-11 and 07-21 and then
+0.017 SOL over the next 296 trades. The edge did not degrade, it stopped. The
open question from that finding was target selection, not exit tuning — and the
first thing this module answered is that **the current sniper target
(AufH68UF…) is not on the kolscan leaderboard at all.**

WHAT IS AND IS NOT TRUSTWORTHY HERE
-----------------------------------
kolscan's PnL is THEIR claim, scraped from their board. It is not verified
on-chain and nothing in this file verifies it. Treat the board as a *candidate
list*, never as evidence — the honest path from "on the board" to "worth copying"
runs through the bot's own shadow-and-score machinery, the same promotion
contract everything else here follows.

THE ONE-DAY-WONDER PROBLEM, AND WHY THE JOURNAL IS LOAD-BEARING
---------------------------------------------------------------
kolscan's timeframe switch is CLIENT-side: `?timeframe=1`, `=7`, `=30`,
`=weekly` all return byte-identical server HTML (verified 2026-08-02 — same 50
wallets, same order, same hash). So scraping can only ever see the DAILY board,
and a daily leaderboard is dominated by whoever caught one lucky launch.

There is no server-side weekly view to ask for, so we build one: every poll is
appended to kol_leaderboard.jsonl and `persistence()` derives days-seen, median
rank and trend from that history. A wallet that is rank 3 today means little; a
wallet on the board 12 days out of 14 is a different claim entirely.

That this works is not hypothetical — "Cented" was rank 1 when the old KOL
scraper last ran (2025-09-26, $13,470) and is rank 4 today ($13,921). Ten months
apart. That is the signal this module exists to capture, and a single snapshot
cannot express it.

SCRAPER ROT IS THE EXPECTED FAILURE MODE, SO IT IS LOUD
-------------------------------------------------------
The row selector is a hashed Next.js CSS-module name
(`leaderboard_leaderboardUser__8OZpJ`). It has survived since 2025-09 by luck
and WILL break on any redeploy. A parse that matches zero rows raises rather
than returning [], because an empty list reads as "no KOLs today" and would sit
undetected — which is precisely the silent-dead-feed failure flow_source.py was
written to fix, at a cost of 36 blind positions. Numbers are pulled by regex
from row TEXT rather than from inner class names, so only the outer row selector
is rot-exposed.

NOT IMPORTED FROM THE OLD SCANNER, DELIBERATELY
-----------------------------------------------
`a local wallet_scanner.db` is not used and should not
be. Measured 2026-08-02: 75 of its 133 wallet rows carry an identical
placeholder tuple (20 trades / 14 wins / $300 PnL / 70% win / score 181.0) under
`tokens_traded = 'ESTIMATED'`, and those fabricated rows occupy 16 of the top 20
slots when sorted by `performance_score` — i.e. ranking that table by its own
score metric returns mostly fiction. Its `transactions` and `tokens` tables are
empty, so even the 58 `REAL_DATA` rows have no retained evidence behind them.
Only the kolscan leaderboard piece is carried forward.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time

import requests
from bs4 import BeautifulSoup

HERE = os.path.dirname(os.path.abspath(__file__))
JOURNAL_PATH = os.path.join(HERE, "kol_leaderboard.jsonl")

LEADERBOARD_URL = "https://kolscan.io/leaderboard"
ROW_CLASS = "leaderboard_leaderboardUser__8OZpJ"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
           "Accept": "text/html,application/xhtml+xml"}

POLL_SECS = 3600.0        # daily board; hourly is already generous
STALE_AFTER_S = 7200.0    # status turns stale if no successful poll in 2h
MIN_TRADES_FOR_SIGNAL = 10   # below this, PnL is a coin flip — see `thin`

_WL_RE = re.compile(r"(\d+)\s*\|\s*/\s*\|\s*(\d+)")
_SOL_RE = re.compile(r"([+-]?[\d,]+\.?\d*)\s*\|\s*Sol", re.I)
_USD_RE = re.compile(r"\$\s*([\d,]+\.?\d*)")
_ADDR_RE = re.compile(r"/account/([1-9A-HJ-NP-Za-km-z]{32,44})")


def _num(s, default=None):
    try:
        return float(str(s).replace(",", "").replace("+", ""))
    except (TypeError, ValueError):
        return default


class KolParseError(RuntimeError):
    """Raised when the page loads but yields no rows — i.e. the selector rotted.
    Deliberately NOT swallowed into an empty list; see the module header."""


def fetch_leaderboard(timeout=20):
    """[{rank, wallet, name, wins, losses, win_ratio, pnl_sol, pnl_usd,
    profile_url}] for the current daily board. Raises KolParseError if the
    page parses to zero rows."""
    r = requests.get(LEADERBOARD_URL, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    rows = soup.find_all("div", class_=ROW_CLASS)
    if not rows:
        raise KolParseError(
            f"0 rows matched '{ROW_CLASS}' in {len(r.text)} bytes — kolscan "
            f"likely redeployed and the CSS-module hash changed. The selector "
            f"needs re-deriving; do NOT treat this as an empty board.")

    out = []
    for row in rows:
        try:
            link = row.find("a", href=_ADDR_RE)
            if not link:
                continue
            m = _ADDR_RE.search(link.get("href") or "")
            if not m:
                continue
            wallet = m.group(1)

            name_el = link.find("h1")
            name = name_el.get_text(strip=True) if name_el else wallet[:6]

            rank = None
            rank_el = row.find("h1")
            if rank_el is not None and rank_el is not name_el:
                rank = int(_num(rank_el.get_text(strip=True), 0) or 0) or None

            # Numbers come from row TEXT, not inner classes — inner class names
            # rot independently of the row selector and there is no reason to
            # depend on both.
            text = row.get_text(" | ", strip=True)
            wl = _WL_RE.search(text)
            wins = int(wl.group(1)) if wl else None
            losses = int(wl.group(2)) if wl else None
            total = (wins + losses) if (wins is not None and losses is not None) else 0

            sol = _SOL_RE.search(text)
            usd = _USD_RE.search(text)
            out.append({
                "rank": rank if rank is not None else len(out) + 1,
                "wallet": wallet,
                "name": name,
                "wins": wins,
                "losses": losses,
                "trades": total or None,
                # 18 of 50 on the 2026-08-02 board had <10 closed trades, and
                # rank 2 was 1W/0L on 259 SOL — one lucky launch, not a trader.
                # Ranking is by PnL alone, so the board systematically floats
                # tiny samples to the top. Anything flagged here is a candidate
                # to WATCH, never to copy on this evidence.
                "thin": bool(total < MIN_TRADES_FOR_SIGNAL),
                "win_ratio": round(100.0 * wins / total, 1) if total else None,
                "pnl_sol": _num(sol.group(1)) if sol else None,
                "pnl_usd": _num(usd.group(1)) if usd else None,
                "profile_url": f"https://kolscan.io/account/{wallet}",
            })
        except Exception:
            # One malformed row must not lose the other 49.
            continue

    if not out:
        raise KolParseError(
            f"{len(rows)} rows matched the selector but none yielded a wallet "
            f"— the row layout changed. Parser needs updating.")
    out.sort(key=lambda k: k["rank"])
    return out


def wilson_low(w, n, z=1.96):
    """Lower bound of the true win rate at 95%. Same function smart_exit.py
    ranks policies with — reused deliberately so 'is this real' is answered the
    same way everywhere in this repo.

    This is the whole defence against the thin-sample problem: it asks "what is
    the WORST this wallet's win rate plausibly is, given how few trades I have
    seen", so 1W/0L scores ~0.21 while 114W/237 scores ~0.42. A perfect record
    on one trade can never outrank a real record on hundreds.
    """
    if not n:
        return 0.0
    p = w / n
    d = 1 + z * z / n
    return max(0.0, (p + z * z / (2 * n)
                     - z * ((p * (1 - p) + z * z / (4 * n)) / n) ** 0.5) / d)


def score(k, days_total=1):
    """Composite rank: confidence-adjusted win rate x magnitude x persistence.

    kolscan ranks on PnL alone, which is why 17 of 50 rows on the 2026-08-02
    board were sub-10-trade flukes and rank 2 was 1W/0L. Three independent
    reads agreed the fix is the same — our own measurements (max real-sample WR
    54.2%, median 45.2%), the 64hP97 teardown (41.6% WR while earning
    $5,676/day), and grek.txt ("win rate filtered but not exclusively
    maximized... very high rates often come with tiny sample sizes").

    So the score deliberately does NOT reward a high win rate on its own:

      wilson_low(wins, trades)  confidence-adjusted WR — kills tiny samples
      log10(pnl_sol)            magnitude, compressed (PnL spans 4 orders)
      0.5 + 0.5 * persistence   days seen / days polled — one-day wonders halved

    Persistence contributes only once the journal has history; with a single
    day of polling every wallet scores the same on that term, which is correct
    rather than a bug (see the module header on the client-side timeframe).
    """
    trades = k.get("trades") or 0
    wins = k.get("wins") or 0
    pnl = k.get("pnl_sol") or 0.0
    if trades <= 0 or pnl <= 0:
        return 0.0
    import math
    wr_lb = wilson_low(wins, trades)
    mag = math.log10(max(pnl, 1.0))
    seen = k.get("days_seen") or 1
    persist = min(1.0, seen / max(days_total or 1, 1))
    return round(100.0 * wr_lb * mag * (0.5 + 0.5 * persist), 1)


def journal(board, path=JOURNAL_PATH):
    """Append one row per KOL per poll. This file IS the weekly view kolscan
    does not serve — never truncate it."""
    ts = round(time.time(), 3)
    day = time.strftime("%Y-%m-%d", time.localtime(ts))
    try:
        with open(path, "a", encoding="utf-8") as f:
            for k in board:
                f.write(json.dumps({"ts": ts, "day": day, **k},
                                   default=str) + "\n")
    except Exception:
        pass


def _read_journal(path=JOURNAL_PATH):
    out = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except Exception:
                    continue
    except FileNotFoundError:
        pass
    return out


def persistence(path=JOURNAL_PATH):
    """{wallet: {days_seen, polls, best_rank, median_rank, last_rank, trend}}.

    `days_seen` is the number of DISTINCT days a wallet appeared, not the poll
    count — polling hourly would otherwise make one good day look like 24.
    This is the column that separates a durable trader from today's lottery
    winner, and it is the only thing here that a kolscan snapshot cannot give.
    """
    rows = _read_journal(path)
    by = {}
    for r in rows:
        w = r.get("wallet")
        if not w:
            continue
        d = by.setdefault(w, {"days": set(), "ranks": [], "name": r.get("name"),
                              "last_ts": 0, "last_rank": None})
        d["days"].add(r.get("day"))
        if r.get("rank"):
            d["ranks"].append((r.get("ts") or 0, r["rank"]))
        if (r.get("ts") or 0) >= d["last_ts"]:
            d["last_ts"] = r.get("ts") or 0
            d["last_rank"] = r.get("rank")
            d["name"] = r.get("name") or d["name"]

    all_days = len({r.get("day") for r in rows if r.get("day")})
    out = {}
    for w, d in by.items():
        ranks = [rk for _, rk in sorted(d["ranks"])]
        if not ranks:
            continue
        n = len(ranks)
        med = (sorted(ranks)[n // 2] if n % 2
               else (sorted(ranks)[n // 2 - 1] + sorted(ranks)[n // 2]) / 2)
        # Trend over the most recent third vs the oldest third; needs a real
        # history before it means anything, hence the None below 6 samples.
        trend = None
        if n >= 6:
            k = max(2, n // 3)
            trend = round(sum(ranks[:k]) / k - sum(ranks[-k:]) / k, 1)
        out[w] = {
            "name": d["name"],
            "days_seen": len(d["days"]),
            "days_total": all_days,
            "polls": n,
            "best_rank": min(ranks),
            "median_rank": med,
            "last_rank": d["last_rank"],
            # positive => rank number falling => climbing the board
            "trend": trend,
        }
    return out


class KolSource:
    """Background poller. Owns no sockets and writes only its own journal."""

    def __init__(self, log=None):
        self._log = log or (lambda m: None)
        self._lock = threading.Lock()
        self._board = []
        self._stop = False
        self._thread = None
        self.last_ok_ts = None
        self.last_error = None
        self.polls = 0
        self.fails = 0

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop = False
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop = True

    def poll_once(self):
        try:
            board = fetch_leaderboard()
            with self._lock:
                self._board = board
            self.last_ok_ts = time.time()
            self.last_error = None
            self.polls += 1
            journal(board)
            return board
        except Exception as e:
            self.fails += 1
            self.last_error = f"{type(e).__name__}: {str(e)[:160]}"
            self._log(f"[KOL] {self.last_error}")
            raise

    def _run(self):
        while not self._stop:
            try:
                self.poll_once()
            except Exception:
                pass
            for _ in range(int(POLL_SECS)):
                if self._stop:
                    return
                time.sleep(1)

    def board(self):
        with self._lock:
            return list(self._board)

    def status(self):
        age = (time.time() - self.last_ok_ts) if self.last_ok_ts else None
        return {
            "rows": len(self.board()),
            "last_ok": self.last_ok_ts,
            "age_s": round(age, 1) if age is not None else None,
            # A board that stopped refreshing looks identical to a healthy one
            # in the UI unless this is surfaced.
            "stale": bool(age is None or age > STALE_AFTER_S),
            "polls": self.polls,
            "fails": self.fails,
            "last_error": self.last_error,
        }


def _ascii(s):
    """KOL display names carry emoji and the Windows console is cp1252, which
    raises UnicodeEncodeError on print. Only the CLI needs this — the journal
    and the API keep the real name."""
    return (str(s or "").encode("ascii", "replace").decode("ascii"))


if __name__ == "__main__":
    b = fetch_leaderboard()
    print(f"{len(b)} KOLs\n")
    print(f"{'#':<4}{'name':<18}{'wallet':<46}{'W/L':<10}"
          f"{'PnL SOL':>10}{'USD':>12}")
    for k in b[:15]:
        wl = f"{k['wins']}/{k['losses']}"
        print(f"{k['rank']:<4}{_ascii(k['name'])[:17]:<18}{k['wallet']:<46}"
              f"{wl:<10}{(k['pnl_sol'] or 0):>10.2f}"
              f"{(k['pnl_usd'] or 0):>12,.0f}")
