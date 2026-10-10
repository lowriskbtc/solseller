"""evm_copy.py — EVM copy-watcher (Phase 0 paper + gated live path).

Watches target wallets on EVM chains via Blockscout and copies what they do:
their token LAUNCHES (dev-style targets) and/or their BUYS (trader-style
targets), with mirrored sells. Paper fills are priced at *detection* time with
a liquidity-based haircut, so the paper record carries the latency tax and the
spread instead of pretending they don't exist.

LIVE PATH (added 2026-07-15, DEFAULT OFF):
  When cfg['copy_live'] is armed AND evm_swap is itself live AND the EVM wallet
  is unlocked, entries and exits execute for real via evm_swap.swap() instead of
  paper math. It is TRIPLE-GATED (copy_live + evm_swap.live + unlocked wallet),
  separate from the manual swap button's own live flag, so arming one never arms
  the other. Both sides are real: a live buy is matched by a live sell on
  TP/SL/timeout/mirror-sell — a live buy with a paper exit would strand real
  tokens. A sell that cannot route (honeypot / no liquidity / not yet filled) is
  NEVER paper-closed; the position is flagged and retried, loudly.

  Why this is opt-in and defended: the paper record on the seed target is
  deeply negative and fills lag the target by minutes. The ONLY defensible use
  of the live path is a small, hard-capped probe to acquire REAL ground truth
  (there is none yet on this chain). Config knobs — live_native_per_trade,
  live_latency_max_s, live_max_open, live_daily_loss_stop_usd — exist so the
  operator, not this module, owns the risk. Nothing here fires unless a human
  arms copy_live in evm_copy_config.json (hot-reloaded, no restart needed).
"""
import calendar
import json
import os
import threading
import time

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CFG_PATH = os.path.join(HERE, "evm_copy_config.json")
POS_PATH = os.path.join(HERE, "evm_copy_positions.jsonl")
STATE_PATH = os.path.join(HERE, "evm_copy_state.json")

_HDRS = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}

SCAN_HOSTS = {
    "ethereum": "eth.blockscout.com",
    "base": "base.blockscout.com",
    "arbitrum": "arbitrum.blockscout.com",
    "optimism": "optimism.blockscout.com",
    "polygon": "polygon.blockscout.com",
    "gnosis": "gnosis.blockscout.com",
    "robinhood": "robinhoodchain.blockscout.com",
}
GT_NET = {"ethereum": "eth", "polygon": "polygon_pos", "avalanche": "avax"}
GOPLUS_CHAIN_ID = {"ethereum": "1", "base": "8453", "arbitrum": "42161",
                   "optimism": "10", "polygon": "137", "gnosis": "100"}
# quote/wrapped-native tokens per chain — receiving one of these is proceeds,
# not a position; sending one is payment, not a sell
QUOTES = {
    "ethereum": {"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",   # WETH
                 "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",   # USDC
                 "0xdac17f958d2ee523a2206206994597c13d831ec7"},  # USDT
    "base": {"0x4200000000000000000000000000000000000006",
             "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"},
    "arbitrum": {"0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                 "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                 "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9"},
    "optimism": {"0x4200000000000000000000000000000000000006"},
    "polygon": {"0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",
                "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"},
    "gnosis": {"0xe91d153e0b41518a2ce8dd3d7944fa863463a97d"},
    "robinhood": {"0x0bd7d308f8e1639fab988df18a8011f41eacad73"},  # WETH
}
POOL_FEE = 0.01          # assume 1% fee tier (Robinhood memepools) — conservative
PENDING_PRICE_MAX_S = 600  # give a brand-new launch 10 min to get a price feed

_DEFAULT_CFG = {
    "enabled": True,
    "poll_secs": 20,
    "usd_per_copy": 25.0,
    "tp_pct": 100.0,
    "sl_pct": 50.0,
    "timeout_min": 240,
    "copy_launches": True,
    "copy_buys": True,
    "mirror_sells": True,
    "targets": [],
    # ---- live path (all OFF/permissive by default; operator owns the risk) ----
    "copy_live": False,            # master arm for AUTONOMOUS real execution
    "live_native_per_trade": 0.0,  # ETH spent per live buy (0 => nothing to spend)
    "live_latency_max_s": 60,      # skip a LIVE buy if detection is later than this
    "live_max_open": 0,            # cap concurrent live positions (0 => unlimited)
    "live_daily_loss_stop_native": 0,  # PRIMARY kill-switch: today's realized live
                                    # loss in ETH. Reliable (no price feed needed).
    "live_daily_loss_stop_usd": 0,  # secondary, best-effort (needs a WETH price;
                                    # goes blind when the feed drops — prefer native)
}

cfg = dict(_DEFAULT_CFG)
_state = {"thread": None, "stop": False, "last_check": None, "last_error": None,
          "positions": [], "events": [], "seen": {}, "pending": []}
_lock = threading.Lock()


# ---------------- persistence ----------------
def _load_cfg():
    global cfg
    try:
        cfg = {**_DEFAULT_CFG, **json.load(open(CFG_PATH, encoding="utf-8"))}
    except Exception:
        cfg = dict(_DEFAULT_CFG)
    return cfg


def save_cfg():
    with open(CFG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=1)


def _persist(rec):
    with _lock, open(POS_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")


def _state_load():
    try:
        d = json.load(open(STATE_PATH, encoding="utf-8"))
        _state["seen"] = {k: set(v) for k, v in (d.get("seen") or {}).items()}
    except Exception:
        _state["seen"] = {}


def _state_save():
    try:
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump({"seen": {k: sorted(v)[-400:]
                                for k, v in _state["seen"].items()}}, f)
    except Exception:
        pass


def _reconstruct():
    """Rebuild open positions from the event log after a restart."""
    by_key = {}
    if os.path.exists(POS_PATH):
        for line in open(POS_PATH, encoding="utf-8"):
            try:
                r = json.loads(line)
            except Exception:
                continue
            if r.get("event") == "open":
                by_key[(r["target"], r["token"])] = {k: v for k, v in r.items()
                                                     if k != "event"}
            elif r.get("event") == "close":
                p = by_key.get((r["target"], r["token"]))
                if p:
                    p.update({k: v for k, v in r.items() if k != "event"})
    return list(by_key.values())


# ---------------- market data ----------------
def _ts(iso):
    try:
        return calendar.timegm(time.strptime(iso[:19], "%Y-%m-%dT%H:%M:%S"))
    except Exception:
        return None


def _ds_pair(chain, token):
    """Best dexscreener pair for token on chain -> (price, liq_usd, pool) or None."""
    try:
        r = requests.get("https://api.dexscreener.com/latest/dex/tokens/" + token,
                         timeout=8, headers=_HDRS)
        best = None
        for p in (r.json() or {}).get("pairs") or []:
            if p.get("chainId") != chain:
                continue
            if (p.get("baseToken") or {}).get("address", "").lower() != token.lower():
                continue
            liq = (p.get("liquidity") or {}).get("usd") or 0
            if best is None or liq > best[1]:
                best = (float(p.get("priceUsd") or 0), liq, p.get("pairAddress"))
        return best if best and best[0] > 0 else None
    except Exception:
        return None


def _gt_prices(chain, tokens):
    """{addr_lower: price} via GeckoTerminal simple price, batched."""
    out = {}
    net = GT_NET.get(chain, chain)
    for i in range(0, len(tokens), 25):
        chunk = tokens[i:i + 25]
        try:
            r = requests.get("https://api.geckoterminal.com/api/v2/simple/networks/"
                             f"{net}/token_price/{','.join(chunk)}",
                             timeout=8, headers=_HDRS)
            prices = ((((r.json() or {}).get("data") or {})
                       .get("attributes") or {}).get("token_prices") or {})
            for k, v in prices.items():
                if v is not None:
                    out[k.lower()] = float(v)
        except Exception:
            pass
    return out


def _security(chain, token):
    """GoPlus token_security flags; 'unavailable' where the chain isn't covered
    (Robinhood Chain isn't) — recorded, never silently dropped."""
    cid = GOPLUS_CHAIN_ID.get(chain)
    if not cid:
        return {"status": "unavailable", "reason": f"goplus has no {chain} coverage"}
    try:
        r = requests.get(f"https://api.gopluslabs.io/api/v1/token_security/{cid}",
                         params={"contract_addresses": token}, timeout=8, headers=_HDRS)
        d = ((r.json() or {}).get("result") or {}).get(token.lower()) or {}
        if not d:
            return {"status": "unavailable", "reason": "no data yet"}
        flags = {k: d.get(k) for k in ("is_honeypot", "cannot_sell_all",
                                       "sell_tax", "buy_tax", "is_blacklisted",
                                       "owner_change_balance", "is_proxy")}
        bad = (d.get("is_honeypot") == "1" or d.get("cannot_sell_all") == "1"
               or float(d.get("sell_tax") or 0) >= 0.2)
        return {"status": "refuse" if bad else "ok", **flags}
    except Exception as e:
        return {"status": "unavailable", "reason": str(e)}


# ---------------- detection ----------------
ZERO = "0x0000000000000000000000000000000000000000"


def _fetch_transfers(chain, addr):
    host = SCAN_HOSTS[chain]
    r = requests.get(f"https://{host}/api/v2/addresses/{addr}/token-transfers",
                     timeout=15, headers=_HDRS)
    return (r.json() or {}).get("items") or []


def _classify(target, items):
    """Group one poll's new transfer items by tx and classify each tx as
    launch/buy/sell events. Returns a list of event dicts."""
    tgt = target.lower()
    by_tx = {}
    for it in items:
        by_tx.setdefault(it.get("transaction_hash") or it.get("tx_hash") or "?",
                         []).append(it)
    events = []
    for tx, legs in by_tx.items():
        chain_quotes = QUOTES.get(legs[0].get("_chain"), set())
        block_ts, minted, t_in, t_out = None, set(), {}, {}
        for it in legs:
            tok = it.get("token") or {}
            ta = (tok.get("address") or tok.get("address_hash") or "").lower()
            if not ta or (tok.get("type") or "ERC-20") != "ERC-20":
                continue
            block_ts = block_ts or _ts(it.get("timestamp") or "")
            frm = ((it.get("from") or {}).get("hash") or "").lower()
            to = ((it.get("to") or {}).get("hash") or "").lower()
            if frm == ZERO or it.get("type") == "token_minting":
                minted.add(ta)
            meta = {"symbol": tok.get("symbol") or "?", "name": tok.get("name") or ""}
            if to == tgt:
                t_in[ta] = meta
            if frm == tgt:
                t_out[ta] = meta
        for ta, meta in t_in.items():
            if ta in chain_quotes:
                continue
            kind = "launch" if ta in minted else "buy"
            events.append({"tx": tx, "kind": kind, "token": ta,
                           "block_ts": block_ts, **meta})
        for ta, meta in t_out.items():
            if ta in chain_quotes or ta in t_in:
                continue
            events.append({"tx": tx, "kind": "sell", "token": ta,
                           "block_ts": block_ts, **meta})
    return events


# ---------------- live execution (gated) ----------------
# evm_swap pulls in web3; import it lazily so a paper-only environment that
# never arms copy_live still loads this module.
_swap_mod = None
_live_state = {"day": None, "realized_usd": 0.0, "realized_native": 0.0}


def _swap():
    global _swap_mod
    if _swap_mod is None:
        import evm_swap as _m
        _swap_mod = _m
    return _swap_mod


def _swap_or_none():
    try:
        return _swap()
    except Exception:
        return None


def _wallet_or_none():
    try:
        import evm_wallet
        return evm_wallet
    except Exception:
        return None


def _native_usd(chain):
    """Best-effort native (ETH) price in USD, for logging only. Live PnL truth
    is denominated in native; USD is convenience and may be None."""
    weth = next(iter(QUOTES.get(chain, set())), None)
    if not weth:
        return None
    pair = _ds_pair(chain, weth)
    return pair[0] if pair else None


def _utc_day():
    return time.strftime("%Y-%m-%d", time.gmtime())


def _record_live_pnl(pnl_native, pnl_usd=None):
    """Accumulate today's realized live PnL and auto-disarm copy_live on a
    breached daily loss stop. The NATIVE stop is the real guard — it never
    depends on a price feed. The USD stop is best-effort and undercounts when
    the WETH feed is down, so it is secondary."""
    day = _utc_day()
    if _live_state["day"] != day:
        _live_state.update({"day": day, "realized_usd": 0.0, "realized_native": 0.0})
    if pnl_native is not None:
        _live_state["realized_native"] += pnl_native
    if pnl_usd is not None:
        _live_state["realized_usd"] += pnl_usd
    stop_n = float(cfg.get("live_daily_loss_stop_native") or 0)
    stop_u = float(cfg.get("live_daily_loss_stop_usd") or 0)
    tripped = None
    if stop_n > 0 and _live_state["realized_native"] <= -abs(stop_n):
        tripped = f"{_live_state['realized_native']:+.5f} ETH ≤ -{stop_n:g} ETH"
    elif stop_u > 0 and _live_state["realized_usd"] <= -abs(stop_u):
        tripped = f"{_live_state['realized_usd']:+.2f} USD ≤ -{stop_u:g} USD"
    if tripped:
        cfg["copy_live"] = False
        save_cfg()
        _log(f"🛑 daily live loss {tripped} — copy_live AUTO-DISARMED")


def _live_ready(latency_s):
    """Triple gate + operator caps. Returns (ok, reason). When not ok the caller
    falls back to a PAPER fill (no real money moves)."""
    if not cfg.get("copy_live"):
        return False, "copy_live off"
    if float(cfg.get("live_native_per_trade") or 0) <= 0:
        return False, "live_native_per_trade is 0"
    sw = _swap_or_none()
    if sw is None:
        return False, "evm_swap unavailable"
    try:
        if not sw.load_cfg().get("live"):
            return False, "evm_swap live off"
    except Exception as e:
        return False, f"evm_swap cfg err: {e}"
    w = _wallet_or_none()
    if w is None or getattr(w, "ACCOUNT", None) is None:
        return False, "wallet locked"
    lm = float(cfg.get("live_latency_max_s") or 0)
    if lm and latency_s is not None and latency_s > lm:
        return False, f"latency {latency_s:.0f}s > {lm:g}s"
    mo = int(cfg.get("live_max_open") or 0)
    if mo:
        live_open = sum(1 for p in _state["positions"]
                        if p.get("status") == "open" and p.get("mode") == "live")
        if live_open >= mo:
            return False, f"live_max_open {mo} reached"
    return True, "ok"


# ---------------- paper positions ----------------
def _impact(usd, liq):
    return usd / (2 * max(liq, 1.0))


def _open_copy(t, ev, price, liq, pool):
    now = time.time()
    latency = round(now - ev["block_ts"], 1) if ev.get("block_ts") else None
    sec = _security(t["chain"], ev["token"])
    if sec.get("status") == "refuse":
        _persist({"event": "refused", "target": t["address"], "token": ev["token"],
                  "symbol": ev["symbol"], "chain": t["chain"], "kind": ev["kind"],
                  "security": sec, "ts": now})
        _log(f"REFUSED {ev['symbol']} ({ev['kind']}) — security flags")
        return
    live_ok, live_reason = _live_ready(latency)
    if live_ok:
        _open_live(t, ev, price, liq, pool, sec, latency)
        return
    usd_in = float(cfg["usd_per_copy"])
    eff = price * (1 + POOL_FEE + _impact(usd_in, liq))
    # when copy_live is armed but this fill was blocked (e.g. latency), record WHY
    # it stayed paper so the operator can see what the gate is filtering out.
    note = None if not cfg.get("copy_live") else f"paper (live blocked: {live_reason})"
    pos = {"target": t["address"], "label": t.get("label") or "",
           "chain": t["chain"], "token": ev["token"], "symbol": ev["symbol"],
           "name": ev["name"], "kind": ev["kind"], "tx": ev["tx"],
           "mode": "paper", "status": "open",
           "detected_ts": round(now, 1), "block_ts": ev.get("block_ts"),
           "latency_s": latency,
           "usd_in": usd_in, "entry_price": price, "entry_eff": eff,
           "liq_usd": liq, "pool": pool, "fee_assumed": POOL_FEE,
           "security": sec, "pnl_usd": None, "exit_price": None, "note": note}
    with _lock:
        _state["positions"].append(pos)
    _persist({**pos, "event": "open"})
    _log(f"COPY {ev['kind'].upper()} {ev['symbol']} @ ${price:.3g} "
         f"(liq ${liq:,.0f}, latency {pos['latency_s']}s)")


def _open_live(t, ev, price, liq, pool, sec, latency):
    """Attempt a REAL buy via evm_swap. On any failure, log loudly and open
    NOTHING — a paper shadow of a trade we could not actually make is not ground
    truth. A live position tracks native ETH in/out, not paper mcap-marks."""
    now = time.time()
    native_in = float(cfg.get("live_native_per_trade") or 0)
    try:
        res = _swap().swap(t["chain"], ev["token"], "buy", native_in, confirm=True)
    except Exception as e:
        _persist({"event": "live_failed", "phase": "buy", "target": t["address"],
                  "token": ev["token"], "symbol": ev["symbol"], "chain": t["chain"],
                  "kind": ev["kind"], "native_in": native_in, "error": str(e),
                  "ts": round(now, 1)})
        _log(f"⛔ LIVE BUY FAILED {ev['symbol']}: {e}")
        return
    usd_px = _native_usd(t["chain"])
    usd_in = round(native_in * usd_px, 2) if usd_px else None
    pos = {"target": t["address"], "label": t.get("label") or "",
           "chain": t["chain"], "token": ev["token"], "symbol": ev["symbol"],
           "name": ev["name"], "kind": ev["kind"], "tx": res.get("tx"),
           "mode": "live", "status": "open",
           "detected_ts": round(now, 1), "block_ts": ev.get("block_ts"),
           "latency_s": latency,
           "native_in": native_in, "tokens_amt": res.get("expected_out"),
           "entry_price": price, "usd_in": usd_in, "liq_usd": liq, "pool": pool,
           "fee_assumed": POOL_FEE, "security": sec, "entry_tx": res.get("tx"),
           "entry_explorer": res.get("explorer"), "pnl_usd": None,
           "real_pnl_native": None, "exit_price": None, "note": None}
    with _lock:
        _state["positions"].append(pos)
    _persist({**pos, "event": "open"})
    _log(f"🟢 LIVE BUY {ev['symbol']} {native_in:g} ETH "
         f"tx {str(res.get('tx'))[:12]} (latency {latency}s)")


def _close(pos, price, reason):
    eff_exit = (price or 0) * (1 - POOL_FEE - _impact(pos["usd_in"], pos["liq_usd"]))
    pnl = pos["usd_in"] * (eff_exit / pos["entry_eff"] - 1) if price else -pos["usd_in"]
    pos.update({"status": "closed", "closed_ts": round(time.time(), 1),
                "exit_price": price, "exit_eff": eff_exit if price else None,
                "pnl_usd": round(pnl, 2), "note": reason})
    _persist({**pos, "event": "close"})
    _log(f"CLOSE {pos['symbol']} {reason} → {pos['pnl_usd']:+.2f} USD (paper)")


def _route_close(pos, price, reason):
    """A live position must never be paper-closed. Route by mode."""
    if pos.get("mode") == "live":
        _close_live(pos, reason)
    else:
        _close(pos, price, reason)


SELL_DUST_SHAVE = 0.999   # sell 99.9% so float→raw rounding never trips "exceeds balance"


def _close_live(pos, reason):
    """Sell a live position for REAL, selling the wallet's actual on-chain
    balance. A sell that cannot route (buy not yet mined, honeypot, no
    liquidity) does NOT close the position — it is marked pending_sell and
    retried each loop, and flagged STUCK + loud on a hard failure. evm_swap
    simulates before sending, so a reverting sell burns no gas beyond a
    one-time approve."""
    chain, token = pos["chain"], pos["token"]
    sw = _swap()
    try:
        bal = sw.balance_of(chain, token)
    except Exception as e:
        pos["pending_sell"], pos["stuck"] = reason, True
        pos["note"] = f"SELL BLOCKED (balance read): {e}"
        _persist({**pos, "event": "sell_blocked", "error": str(e),
                  "ts": round(time.time(), 1)})
        _log(f"🚨 LIVE SELL BLOCKED {pos['symbol']}: {e} — STUCK, holding tokens")
        return
    if bal <= 0:
        pos["pending_sell"] = reason               # buy not mined yet — retry
        pos["note"] = "awaiting fill before sell"
        _log(f"… {pos['symbol']} live sell deferred ({reason}) — no balance yet")
        return
    try:
        res = sw.swap(chain, token, "sell", bal * SELL_DUST_SHAVE, confirm=True)
    except Exception as e:
        pos["pending_sell"], pos["stuck"] = reason, True
        pos["note"] = f"SELL FAILED: {e}"
        _persist({**pos, "event": "sell_failed", "error": str(e),
                  "ts": round(time.time(), 1)})
        _log(f"🚨 LIVE SELL FAILED {pos['symbol']}: {e} — STUCK, holding tokens")
        return
    native_out = res.get("expected_out")           # sell proceeds in native ETH
    real_native = (native_out - pos["native_in"]) if native_out is not None else None
    usd_px = _native_usd(chain)
    real_usd = (round(real_native * usd_px, 2)
                if real_native is not None and usd_px else None)
    pos.pop("pending_sell", None)
    pos.pop("stuck", None)
    pos.update({"status": "closed", "closed_ts": round(time.time(), 1),
                "native_out": native_out, "real_pnl_native": real_native,
                "pnl_usd": real_usd, "exit_tx": res.get("tx"),
                "exit_explorer": res.get("explorer"), "note": reason,
                # honesty flag: native_out is the pre-send QUOTE, and gas on the
                # buy/sell/approve is NOT deducted → this PnL is gross-optimistic.
                "pnl_basis": "quote_gross_no_gas"})
    _record_live_pnl(real_native, real_usd)
    _persist({**pos, "event": "close"})
    rn = f"{real_native:+.5f}" if real_native is not None else "?"
    _log(f"🔴 LIVE SELL {pos['symbol']} {reason} → {rn} ETH real "
         f"tx {str(res.get('tx'))[:12]}")


def _log(msg):
    with _lock:
        _state["events"] = ([{"ts": round(time.time(), 1), "msg": msg}]
                            + _state["events"])[:60]


# ---------------- main loop ----------------
def _poll_target(t):
    key = t["address"].lower()
    items = _fetch_transfers(t["chain"], t["address"])
    for it in items:
        it["_chain"] = t["chain"]
    seen = _state["seen"].get(key)
    hashes = {it.get("transaction_hash") or "?" for it in items}
    if seen is None:                      # first sight: index history, copy nothing
        _state["seen"][key] = hashes
        _log(f"target {t.get('label') or key[:8]} indexed ({len(hashes)} recent txs)")
        return
    new_items = [it for it in items
                 if (it.get("transaction_hash") or "?") not in seen]
    if not new_items:
        return
    seen.update(hashes)
    open_tokens = {p["token"] for p in _state["positions"]
                   if p["status"] == "open" and p["target"] == t["address"]}
    for ev in _classify(t["address"], new_items):
        if ev["kind"] == "sell":
            if cfg.get("mirror_sells"):
                for p in list(_state["positions"]):
                    if (p["status"] == "open" and p["token"] == ev["token"]
                            and p["target"] == t["address"]):
                        px = _gt_prices(p["chain"], [p["token"]]).get(p["token"])
                        _route_close(p, px, "target_sold")
            continue
        if ev["kind"] == "launch" and not cfg.get("copy_launches"):
            continue
        if ev["kind"] == "buy" and not cfg.get("copy_buys"):
            continue
        if ev["token"] in open_tokens:
            continue
        pair = _ds_pair(t["chain"], ev["token"])
        if pair:
            _open_copy(t, ev, pair[0], pair[1], pair[2])
            open_tokens.add(ev["token"])
        else:                             # brand-new launch: price feed lags
            _state["pending"].append({"t": t, "ev": ev,
                                      "first_seen": time.time()})
            _log(f"detected {ev['kind']} {ev['symbol']} — waiting for price feed")


def _drain_pending():
    still = []
    for p in _state["pending"]:
        pair = _ds_pair(p["t"]["chain"], p["ev"]["token"])
        if pair:
            _open_copy(p["t"], p["ev"], pair[0], pair[1], pair[2])
        elif time.time() - p["first_seen"] < PENDING_PRICE_MAX_S:
            still.append(p)
        else:
            _persist({"event": "refused", "target": p["t"]["address"],
                      "token": p["ev"]["token"], "symbol": p["ev"]["symbol"],
                      "chain": p["t"]["chain"], "kind": p["ev"]["kind"],
                      "security": {"status": "no_price_feed"}, "ts": time.time()})
            _log(f"DROPPED {p['ev']['symbol']} — no price feed within 10 min")
    _state["pending"] = still


def _check_exits():
    # first, retry any live position whose sell was deferred (buy not yet mined)
    # or stuck (honeypot) — a real sell that could not route earlier.
    for p in [p for p in _state["positions"]
              if p.get("status") == "open" and p.get("mode") == "live"
              and p.get("pending_sell")]:
        _close_live(p, p["pending_sell"])
    opens = [p for p in _state["positions"]
             if p["status"] == "open" and not p.get("pending_sell")]
    by_chain = {}
    for p in opens:
        by_chain.setdefault(p["chain"], []).append(p)
    for chain, group in by_chain.items():
        prices = _gt_prices(chain, [p["token"] for p in group])
        for p in group:
            px = prices.get(p["token"])
            timed_out = time.time() - p["detected_ts"] > cfg["timeout_min"] * 60
            if px is None:
                if timed_out:
                    _route_close(p, None, "timeout (no price)")
                continue
            # trigger uses raw price for live (execution is real anyway) and the
            # haircut effective price for paper (matches its entry basis)
            if p.get("mode") == "live":
                chg = (px / p["entry_price"] - 1) if p.get("entry_price") else 0.0
            else:
                eff = px * (1 - POOL_FEE - _impact(p["usd_in"], p["liq_usd"]))
                chg = eff / p["entry_eff"] - 1
            if chg * 100 >= cfg["tp_pct"]:
                _route_close(p, px, f"tp +{cfg['tp_pct']:g}%")
            elif chg * 100 <= -cfg["sl_pct"]:
                _route_close(p, px, f"sl -{cfg['sl_pct']:g}%")
            elif timed_out:
                _route_close(p, px, "timeout")


def _loop():
    while not _state["stop"]:
        try:
            _load_cfg()          # hot-reload: targets + live controls, no restart
            for t in cfg.get("targets") or []:
                if not t.get("enabled", True):
                    continue
                if t.get("chain") not in SCAN_HOSTS:
                    continue
                _poll_target(t)
            _drain_pending()
            _check_exits()
            _state_save()
            _state["last_check"] = time.time()
            _state["last_error"] = None
        except Exception as e:
            _state["last_error"] = str(e)
        time.sleep(max(10, int(cfg.get("poll_secs") or 20)))


# ---------------- public API ----------------
def start():
    _load_cfg()
    if _state["thread"] and _state["thread"].is_alive():
        return
    _state["stop"] = False
    _state_load()
    all_pos = _reconstruct()
    opens = [p for p in all_pos if p.get("status") == "open"]
    closed = sorted((p for p in all_pos if p.get("status") == "closed"),
                    key=lambda p: p.get("closed_ts") or 0, reverse=True)[:40]
    _state["positions"] = opens + closed
    th = threading.Thread(target=_loop, daemon=True)
    _state["thread"] = th
    th.start()


def stop():
    _state["stop"] = True


def status():
    pos = sorted(_state["positions"], key=lambda p: -(p.get("detected_ts") or 0))
    closed = [p for p in pos if p["status"] == "closed"]
    wins = sum(1 for p in closed if (p.get("pnl_usd") or 0) > 0)
    live_closed = [p for p in closed if p.get("mode") == "live"]
    live_open = [p for p in pos if p["status"] == "open" and p.get("mode") == "live"]
    ready, reason = _live_ready(0)        # latency 0 => pure arming/gate check
    if not cfg.get("copy_live"):
        mode = "PAPER (copy_live off)"
    elif ready:
        mode = "LIVE ARMED"
    else:
        mode = f"copy_live set but BLOCKED: {reason}"
    return {
        "config": cfg,
        "running": bool(_state["thread"] and _state["thread"].is_alive()),
        "last_check": _state["last_check"], "last_error": _state["last_error"],
        "mode": mode,
        "live_ready": ready,
        "live_block_reason": None if ready else reason,
        "live_open": live_open[:20],
        "n_live_closed": len(live_closed),
        "live_real_pnl_native": round(sum(p.get("real_pnl_native") or 0
                                          for p in live_closed), 6),
        "live_stuck": [p for p in live_open if p.get("stuck")],
        "live_realized_native_today": (round(_live_state["realized_native"], 6)
                                       if _live_state["day"] == _utc_day() else 0.0),
        "live_realized_usd_today": (round(_live_state["realized_usd"], 2)
                                    if _live_state["day"] == _utc_day() else 0.0),
        "open": [p for p in pos if p["status"] == "open"][:20],
        "recent_closed": closed[:15],
        "n_closed": len(closed), "wins": wins,
        "paper_pnl_usd": round(sum(p.get("pnl_usd") or 0
                                   for p in closed if p.get("mode") != "live"), 2),
        "events": _state["events"][:25],
        "pending": len(_state["pending"]),
    }
