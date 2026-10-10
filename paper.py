"""paper.py — a paper-trading book for the pop-out trade box.

A second, parallel book. Buys and sells are priced off REAL Jupiter quotes at
the moment you click, charged REAL fee estimates, and written to their own
journal (paper_trades.jsonl). Nothing here signs, sends, or reads a wallet.

WHY THIS IS A SEPARATE FILE AND NOT A FLAG
------------------------------------------
The obvious implementation is `execute_swap(..., paper=True)`. That is the one
thing this must not be:

  * execute_swap's _journal writes TRADES_PATH. A simulated row in the real
    trade journal re-creates, by hand, the exact basis corruption the
    2026-08-09 audit existed to remove -- and cost_basis()/tpsl would then be
    computing a real position's entry price partly from fills that never
    happened.
  * it also calls CTX.record_fill, which is training data. Synthetic fills in
    there are indistinguishable from real ones a month later.
  * a boolean threaded through the live send path is one bad merge away from
    defaulting the wrong way.

So: separate module, separate journal, separate lock table, separate executor
instance, separate HTTP routes. The only thing shared with the live path is
read-only market data (quotes, prices) and the tested cost-basis arithmetic in
tpsl.basis_from_rows -- imported UNCHANGED.

WHY basis_from_rows AND NOT classify_basis
------------------------------------------
classify_basis exists because the real journal is an unreliable proxy for
on-chain truth: it grades a lot by whether every fill was chain-verified and
whether the chain balance agrees. A paper book has neither problem -- this file
IS the ledger of record, there is no chain to disagree with it, and held_raw is
the lot by construction. So paper reuses the running-average-cost arithmetic
(which is what carries partial sells correctly) and grades its own rows "ok".

Paper rows are written with fill="sim", never "chain". They are not lying about
their provenance to borrow a grade they did not earn.

WHAT "ACTS LIKE REAL MONEY" MEANS HERE, PRECISELY
-------------------------------------------------
  * FILL SIZE comes from a live Jupiter quote for the actual size, so price
    impact on a thin pool is real -- a 1 SOL paper buy on a $3k pool fills as
    badly as it would have.
  * FILL PRICE is the quote's expected `outAmount`, i.e. the OPTIMISTIC end.
    A real fill lands somewhere around it; this wallet's own journal measured
    quote-vs-landed drift at a median 2.6% (the `quote_drift` field). So a
    paper edge thinner than ~3% is inside the noise and means nothing.
    fill_haircut_bps applies a flat pessimism to both legs if you want it.
  * FEES are charged: the 5000-lamport signature fee always, plus the real
    Jito tip in mev mode (pulled from the same tip-floor feed the live path
    uses), plus a MODELLED priority fee in fast/eco. fee_cap_sol on the live
    path is a ceiling, not what lands, so charging the cap would overstate --
    these are estimates and are labelled as such.
  * ATA RENT is charged on the first buy of a mint (~0.00204 SOL) and never
    refunded, because Jupiter closes the wrapped-SOL account and leaves the
    token account open. On a 0.05 SOL buy that rent is 4% of the position --
    the single biggest reason a small-size paper book flatters itself.

Every one of those is a cost, so the modelling error runs pessimistic. A paper
book that overstates its edge is the failure worth avoiding.
"""

from __future__ import annotations

import json
import os
import secrets
import threading
import time

from tpsl import basis_from_rows

SOL_MINT = "So11111111111111111111111111111111111111112"

# The journal's wallet column. NOT derived from PUBKEY: paper is meant to work
# with the wallet locked, when PUBKEY is None, and a book keyed off None would
# silently merge into whatever wallet happened to be unlocked later.
PAPER_WALLET = "PAPER"

LAMPORTS = 1_000_000_000

DEFAULT_SETTINGS = {
    "start_sol": 5.0,
    # 5000 lamports/signature. Always charged, both legs.
    "base_fee_lamports": 5000,
    # fast/eco priority fees are MODELLED. The live path only knows its ceiling
    # (MAX_PRIORITY_LAMPORTS / ECO_PRIORITY_LAMPORTS) and usually pays far less,
    # so charging the ceiling would overstate. mev uses the real tip floor.
    "fast_prio_lamports": 200_000,     # 0.0002 ◎ against a 0.002 ◎ cap
    "eco_prio_lamports": 50_000,       # 0.00005 ◎ — the eco cap, often ~0 live
    # Rent-exempt deposit for a token account, charged once on the buy that
    # opens a position. MEASURED off this wallet's own live accounts
    # (2026-08-10, 24 of them, all 2_074_000) rather than the 2_039_280 quoted
    # for a bare SPL account -- Jupiter's ATAs carry an extension.
    #
    # Whether it is a real cost depends on you: it is refundable, and the rent
    # sweeper in jup_trader now reclaims it every 10 minutes. Charging it here
    # is the pessimistic default; set charge_ata_rent false to model the sweep.
    "ata_rent_lamports": 2_074_000,
    "charge_ata_rent": True,
    # flat adverse haircut on every fill, both legs, in basis points. 0 = fill
    # at the quote. See the module header on why 0 is optimistic.
    "fill_haircut_bps": 0.0,
}

# Settings a caller may change at runtime. start_sol is not here on purpose:
# changing the bankroll under a live book would silently restate every past
# balance. It moves only through reset().
MUTABLE_SETTINGS = ("base_fee_lamports", "fast_prio_lamports",
                    "eco_prio_lamports", "ata_rent_lamports",
                    "charge_ata_rent", "fill_haircut_bps")


def fmt_amount(ui, raw=None):
    """Token counts for human text. %g turns a million tokens into "1e+06",
    which is unreadable in a one-line fill report on a memecoin position."""
    if ui is None:
        return "%d raw" % (raw or 0)
    if ui >= 1000:
        return "{:,.0f}".format(ui)
    if ui >= 1:
        return "{:,.2f}".format(ui)
    return "%.4g" % ui


def _num(v, dflt=None):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return dflt
    return f if f == f else dflt


class PaperError(RuntimeError):
    """A paper trade that could not be simulated. Message reaches the UI."""


class PaperBook:
    """The book. All I/O injected, same discipline as tpsl.Executor.

    deps:
      quote(in_mint, out_mint, amount_raw, slippage_bps) -> Jupiter quote dict
      prices(mints)  -> ({mint: usd or None}, sol_usd)
      meta(mint)     -> {"symbol":..., "decimals":int or None, ...}
      tip_lamports() -> int   (the live Jito tip floor, for mev mode)
      now()          -> float
      log(str)       -> None
    """

    def __init__(self, deps, journal_path, state_path):
        self.d = deps
        self.journal_path = journal_path
        self.state_path = state_path
        self._wlock = threading.RLock()      # serialises read-book -> append
        self._append_lock = threading.Lock()
        self._mint_locks = {}
        self._mint_locks_guard = threading.Lock()
        self._rows_cache = None              # (mtime, size, rows)
        self._dec = {}                       # mint -> decimals
        self._sym = {}                       # mint -> symbol
        self.st = self._load_state()

    # ------------------------------------------------------------ state ----
    def _load_state(self):
        st = dict(DEFAULT_SETTINGS)
        st.update({"epoch": 0.0, "resets": 0, "created": self.d.now()})
        try:
            with open(self.state_path, encoding="utf-8") as f:
                saved = json.load(f)
            if isinstance(saved, dict):
                st.update(saved)
        except Exception:
            pass
        return st

    def _save_state(self):
        tmp = self.state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.st, f, indent=1)
        os.replace(tmp, self.state_path)     # atomic: a torn state file is a
                                             # book with no idea what it started with

    def settings(self, **kw):
        """Change fee/fill modelling. Unknown or unparseable keys are ignored."""
        with self._wlock:
            for k, v in kw.items():
                if k not in MUTABLE_SETTINGS:
                    continue
                if k == "charge_ata_rent":
                    self.st[k] = bool(v)
                    continue
                n = _num(v, None)
                if n is None or n < 0:
                    continue
                self.st[k] = n if k == "fill_haircut_bps" else int(n)
            self._save_state()
        return dict(self.st)

    def reset(self, start_sol=None):
        """New bankroll, new epoch. History is KEPT — the epoch just stops the
        old rows counting, so a reset can never be confused with a wipe and a
        past run stays auditable in the same file."""
        with self._wlock:
            if start_sol is not None:
                s = _num(start_sol, None)
                if s is None or s <= 0:
                    raise PaperError("starting balance must be > 0")
                self.st["start_sol"] = s
            self.st["epoch"] = self.d.now()
            self.st["resets"] = int(self.st.get("resets") or 0) + 1
            self._save_state()
            self._rows_cache = None
        self.d.log("[paper] book reset - %.4f SOL bankroll" % self.st["start_sol"])
        return dict(self.st)

    # ---------------------------------------------------------- journal ----
    def _rows(self):
        """Rows in the current epoch. mtime+size cached: holdings() runs on the
        executor's 60s timer and status() on every popout poll."""
        try:
            stt = os.stat(self.journal_path)
            key = (stt.st_mtime, stt.st_size)
        except OSError:
            self._rows_cache = None
            return []
        if self._rows_cache and self._rows_cache[0] == key:
            return self._rows_cache[1]
        rows = []
        try:
            with open(self.journal_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        continue
        except OSError:
            return []
        epoch = _num(self.st.get("epoch"), 0.0) or 0.0
        rows = [r for r in rows if (_num(r.get("ts"), 0.0) or 0.0) >= epoch]
        for r in rows:                       # decimals/symbol travel with the row
            m = r.get("mint")
            if m and r.get("decimals") is not None:
                self._dec.setdefault(m, r["decimals"])
            if m and r.get("symbol"):
                self._sym.setdefault(m, r["symbol"])
        self._rows_cache = (key, rows)
        return rows

    def _append(self, rec):
        line = json.dumps(rec) + "\n"
        with self._append_lock, open(self.journal_path, "a", encoding="utf-8") as f:
            f.write(line)
        self._rows_cache = None

    # ------------------------------------------------------------- book ----
    def lots(self):
        """{mint: {"sol": cost still held, "raw": tokens}} — tpsl's arithmetic,
        imported unchanged so a partial sell scales cost here exactly the way it
        does on the live book."""
        return basis_from_rows(self._rows(), PAPER_WALLET)

    def sol_balance(self):
        """Derived from the journal, never stored. A cached balance and a
        journal that disagree is the whole class of bug this book exists to be
        free of, so there is only one number and it is computed."""
        bal = _num(self.st.get("start_sol"), 0.0) or 0.0
        for r in self._rows():
            if r.get("status") != "confirmed":
                continue
            sol = _num(r.get("sol"), 0.0) or 0.0
            fee = _num(r.get("fee_sol"), 0.0) or 0.0
            bal += (-sol if r.get("side") == "buy" else sol) - fee
        return round(bal, 9)

    def _decimals(self, mint):
        if mint in self._dec:
            return self._dec[mint]
        try:
            m = self.d.meta(mint) or {}
        except Exception:
            m = {}
        if m.get("decimals") is not None:
            self._dec[mint] = int(m["decimals"])
        if m.get("symbol"):
            self._sym[mint] = m["symbol"]
        return self._dec.get(mint)

    def holdings(self):
        """Same shape tpsl.Executor expects from the live get_holdings()."""
        lots = self.lots()
        mints = list(lots)
        px, sol_px = {}, None
        if mints:
            try:
                px, sol_px = self.d.prices(mints)
            except Exception as e:
                # Degrade to unpriced rather than dropping the book. An unpriced
                # row shows no PnL, which makes the executor refuse to fire on
                # it — the correct behaviour for a dead feed.
                self.d.log("[paper] price fetch failed: %s" % e)
        rows = []
        for m, lot in lots.items():
            dec = self._decimals(m)
            amt = lot["raw"] / (10 ** dec) if dec is not None else None
            p = _num(px.get(m), None)
            usd = (amt * p) if (amt is not None and p) else 0.0
            cost = round(lot["sol"], 9)
            row = {
                "mint": m,
                "amount": amt if amt is not None else 0.0,
                "raw": lot["raw"],
                "usdPrice": p,
                "usd": round(usd, 2),
                "priced": bool(p and amt is not None),
                "cost_sol": cost,
                "pnl_pct": None,
                # The paper journal is the ledger of record and held == lot by
                # construction, so the basis is known-good whenever a cost
                # exists. See the module header.
                "basis": "ok" if cost > 0 else "none",
                "symbol": self._sym.get(m) or (m[:4] + "…"),
                "paper": True,
            }
            if row["priced"] and sol_px and cost > 0:
                row["pnl_pct"] = round((usd / sol_px / cost - 1) * 100, 1)
            rows.append(row)
        rows.sort(key=lambda r: -r["usd"])
        sol = self.sol_balance()
        return {"holdings": rows, "sol_balance": sol,
                "sol_price": sol_px,
                "sol_usd": round(sol * sol_px, 2) if sol_px else None,
                "total_usd": round(sum(r["usd"] for r in rows), 2)}

    # ------------------------------------------------------------ locks ----
    def mint_lock(self, mint):
        """Paper's OWN lock table. Injecting jup_trader.mint_lock() here would
        let a paper sell hold the live interlock and defer a real TP/SL fire on
        the same mint — a simulated trade must not be able to touch a real
        position's protection, however briefly."""
        with self._mint_locks_guard:
            lk = self._mint_locks.get(mint)
            if lk is None:
                lk = self._mint_locks[mint] = threading.Lock()
            return lk

    # -------------------------------------------------------------- fees ----
    def fee_lamports(self, fee_mode, first_buy=False):
        """(total, breakdown). Estimates, and labelled as such upstream."""
        st = self.st
        base = int(st.get("base_fee_lamports") or 0)
        mode = fee_mode if fee_mode in ("mev", "fast", "eco") else "mev"
        tip = 0
        prio = 0
        if mode == "mev":
            try:
                tip = int(self.d.tip_lamports() or 0)
            except Exception:
                tip = 0
        elif mode == "fast":
            prio = int(st.get("fast_prio_lamports") or 0)
        else:
            prio = int(st.get("eco_prio_lamports") or 0)
        rent = int(st.get("ata_rent_lamports") or 0) if (
            first_buy and st.get("charge_ata_rent")) else 0
        total = base + tip + prio + rent
        return total, {"base": base, "tip": tip, "prio": prio, "rent": rent,
                       "mode": mode}

    def _haircut(self, out_raw):
        bps = _num(self.st.get("fill_haircut_bps"), 0.0) or 0.0
        if bps <= 0:
            return int(out_raw), 0.0
        return int(out_raw * (1 - bps / 10000.0)), bps

    # -------------------------------------------------------------- buy ----
    def buy(self, mint, sol, slippage_bps, fee_mode="mev"):
        amt = _num(sol, None)
        if amt is None or amt <= 0:
            raise PaperError("amount must be > 0")
        lam_in = int(amt * LAMPORTS)
        if lam_in <= 0:
            raise PaperError("amount too small to simulate")
        with self._wlock:
            lot = self.lots().get(mint)
            first_buy = not (lot and lot["raw"] > 0)
            fee, fb = self.fee_lamports(fee_mode, first_buy=first_buy)
            have = int(round(self.sol_balance() * LAMPORTS))
            if lam_in + fee > have:
                # ASCII: a PaperError raised on the executor's sell path would
                # be printed by tpsl's event log. This one is buy-only, but the
                # whole class stays console-safe rather than relying on that.
                raise PaperError(
                    "insufficient paper SOL - have %.4f SOL, this costs %.4f SOL "
                    "including %.5f SOL of fees"
                    % (have / LAMPORTS, (lam_in + fee) / LAMPORTS, fee / LAMPORTS))
            # The quote is live and sized: price impact on a thin pool is real.
            q = self.d.quote(SOL_MINT, mint, lam_in, int(slippage_bps))
            out_raw = int(q.get("outAmount") or 0)
            if out_raw <= 0:
                raise PaperError("quote returned no tokens — no routable pool")
            out_raw, hc = self._haircut(out_raw)
            if out_raw <= 0:
                raise PaperError("fill haircut consumed the whole fill")
            dec = self._decimals(mint)
            sym = self._sym.get(mint) or (mint[:4] + "…")
            ts = round(self.d.now(), 3)
            sig = "paper-" + secrets.token_hex(16)
            self._append({
                "ts": ts, "wallet": PAPER_WALLET, "side": "buy",
                "mint": mint, "sol": round(lam_in / LAMPORTS, 9),
                "token_raw": out_raw, "sig": sig, "status": "confirmed",
                # NOT "chain". These are simulated and say so.
                "fill": "sim", "mode": "paper",
                "fee_sol": round(fee / LAMPORTS, 9), "fee_breakdown": fb,
                "fee_mode": fb["mode"], "slippage_bps": int(slippage_bps),
                "price_impact_pct": q.get("priceImpactPct"),
                "haircut_bps": hc, "decimals": dec, "symbol": sym,
                "src": "popout",
            })
            bal = self.sol_balance()
        ui = out_raw / (10 ** dec) if dec is not None else None
        # ASCII only on this path. d.log() is print() into a cp1252 Windows
        # console, and the U+25CE the UI uses raises UnicodeEncodeError there --
        # which would throw AFTER the row was appended, leaving a journaled
        # trade the window reports as failed. JSON-bound strings keep the glyph.
        self.d.log("[paper] BUY %s %.4f SOL -> %s (fee %.5f SOL)"
                   % (sym, lam_in / LAMPORTS, fmt_amount(ui, out_raw),
                      fee / LAMPORTS))
        return {
            "ok": True, "signature": sig, "status": "confirmed",
            "paper": True, "mint": mint, "symbol": sym,
            "outAmount": str(out_raw), "amount_ui": ui,
            "priceImpactPct": q.get("priceImpactPct"),
            "sent_via": "paper", "fee_mode": fb["mode"],
            "fee_sol": round(fee / LAMPORTS, 6), "fee_breakdown": fb,
            "sol_balance": bal, "solscan": None,
            "note": "filled %s at the quote%s · fee %.5f ◎%s"
                    % (fmt_amount(ui, out_raw),
                       "" if not hc else " less %.2f%% haircut" % (hc / 100.0),
                       fee / LAMPORTS,
                       " (incl. %.5f ◎ new-token rent)" % (fb["rent"] / LAMPORTS)
                       if fb["rent"] else ""),
        }

    # ------------------------------------------------------------- sell ----
    def sell(self, mint, pct, slippage_bps=None, fee_mode="mev",
             default_slippage_bps=300):
        p = _num(pct, None)
        if p is None or not 0 < p <= 100:
            raise PaperError("pct must be in (0, 100]")
        slip = int(slippage_bps or default_slippage_bps)
        lk = self.mint_lock(mint)
        with lk, self._wlock:
            lot = self.lots().get(mint)
            if not lot or lot["raw"] <= 0:
                raise PaperError("no paper balance to sell")
            raw = lot["raw"]
            amount = raw if p >= 100 else int(raw * p / 100)
            if amount <= 0:
                raise PaperError("no paper balance to sell")
            fee, fb = self.fee_lamports(fee_mode, first_buy=False)
            q = self.d.quote(mint, SOL_MINT, amount, slip)
            lam_out = int(q.get("outAmount") or 0)
            if lam_out <= 0:
                raise PaperError("quote returned no SOL — no routable pool")
            lam_out, hc = self._haircut(lam_out)
            # cost of exactly the fraction being sold, so a 10% scalp books 10%
            # of the basis and leaves the rest of the lot priced correctly.
            frac = min(1.0, amount / raw)
            cost = lot["sol"] * frac
            gross = lam_out / LAMPORTS
            realized = gross - (fee / LAMPORTS) - cost
            dec = self._decimals(mint)
            sym = self._sym.get(mint) or (mint[:4] + "…")
            ts = round(self.d.now(), 3)
            sig = "paper-" + secrets.token_hex(16)
            self._append({
                "ts": ts, "wallet": PAPER_WALLET, "side": "sell",
                "mint": mint, "sol": round(gross, 9), "token_raw": amount,
                "sig": sig, "status": "confirmed", "fill": "sim",
                "mode": "paper",
                "fee_sol": round(fee / LAMPORTS, 9), "fee_breakdown": fb,
                "fee_mode": fb["mode"], "slippage_bps": slip,
                "price_impact_pct": q.get("priceImpactPct"),
                "haircut_bps": hc, "decimals": dec, "symbol": sym,
                "sell_pct": p,
                # realized is NET of this leg's fee but not of the buy's — the
                # buy already paid its own out of the balance.
                "cost_sol": round(cost, 9),
                "realized_sol": round(realized, 9),
                "realized_pct": round((realized / cost) * 100, 2) if cost > 0 else None,
                "src": "popout",
            })
            bal = self.sol_balance()
        self.d.log("[paper] SELL %g%% %s -> %.4f SOL (pnl %+.4f SOL)"   # ASCII: see buy()
                   % (p, sym, gross, realized))
        return {
            "ok": True, "signature": sig, "status": "confirmed",
            "paper": True, "mint": mint, "symbol": sym,
            "outAmount": str(lam_out), "sol_out": round(gross, 6),
            "realized_sol": round(realized, 6),
            "realized_pct": round((realized / cost) * 100, 2) if cost > 0 else None,
            "priceImpactPct": q.get("priceImpactPct"),
            "sent_via": "paper", "fee_mode": fb["mode"],
            "fee_sol": round(fee / LAMPORTS, 6), "fee_breakdown": fb,
            "sol_balance": bal, "solscan": None,
            "note": "%+.4f ◎ realized (%.4f ◎ out, %.5f ◎ fee)"
                    % (realized, gross, fee / LAMPORTS),
        }

    # ------------------------------------------------------------ stats ----
    def stats(self):
        """Closed-trade summary for the current epoch. Realized only —
        unrealized is already on screen as the per-row PnL."""
        rows = [r for r in self._rows() if r.get("status") == "confirmed"]
        sells = [r for r in rows if r.get("side") == "sell"]
        realized = sum(_num(r.get("realized_sol"), 0.0) or 0.0 for r in sells)
        fees = sum(_num(r.get("fee_sol"), 0.0) or 0.0 for r in rows)
        wins = sum(1 for r in sells if (_num(r.get("realized_sol"), 0.0) or 0.0) > 0)
        start = _num(self.st.get("start_sol"), 0.0) or 0.0
        bal = self.sol_balance()
        # THE HEADLINE NUMBER, and it is deliberately not the sum of the
        # per-trade `realized_sol`. Each of those is net of its own exit fee but
        # not of its ENTRY fee -- the buy paid that out of the balance before a
        # lot existed to charge it to. Summing them therefore overstates, and
        # overstating is the one direction a paper book must never run.
        #
        # cash + capital-still-at-cost - bankroll needs no per-lot fee
        # attribution and is exact by construction: every lamport that left the
        # balance for any reason (fees, tips, rent) is already in it. When the
        # book is flat it is simply balance - bankroll.
        capital = sum((l["sol"] for l in self.lots().values()), 0.0)
        return {
            # 9dp throughout: a lamport is 1e-9 ◎ and these are money. Rounding
            # to 6 here silently broke the exactness of net_sol against the
            # balance it is derived from — the UI can format, this must not.
            "start_sol": round(start, 9),
            "sol_balance": bal,
            "net_sol": round(bal + capital - start, 9),
            "capital_sol": round(capital, 9),
            # per-trade sum, kept for the win/loss split. NOT the headline.
            "realized_sol": round(realized, 9),
            "fees_sol": round(fees, 9),
            "trades": len(rows), "sells": len(sells), "wins": wins,
            "win_rate": round(wins / len(sells) * 100, 1) if sells else None,
            "open": len(self.lots()),
            "epoch": self.st.get("epoch"), "resets": self.st.get("resets"),
        }

    def meta(self):
        """Everything the UI needs to describe the book's own rules."""
        st = self.st
        return {
            "start_sol": st.get("start_sol"),
            "fill_haircut_bps": st.get("fill_haircut_bps"),
            "charge_ata_rent": bool(st.get("charge_ata_rent")),
            "ata_rent_sol": round(int(st.get("ata_rent_lamports") or 0) / LAMPORTS, 6),
            "base_fee_sol": round(int(st.get("base_fee_lamports") or 0) / LAMPORTS, 6),
            "fast_prio_sol": round(int(st.get("fast_prio_lamports") or 0) / LAMPORTS, 6),
            "eco_prio_sol": round(int(st.get("eco_prio_lamports") or 0) / LAMPORTS, 6),
            "journal": os.path.basename(self.journal_path),
        }
