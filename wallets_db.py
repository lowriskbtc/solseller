"""
wallets_db.py — one registry for every wallet this bot tracks, across chains.

WHY (2026-08-02)
----------------
"Which wallets are we watching?" had four different answers living in four
files, and none of them recorded *why* a wallet was there:

    sniper_config.json  -> "target"          one Solana copy target
    evm_copy_config.json-> "targets"[]       EVM copy targets (robinhood)
    evm_wallets.json    -> []                EVM view-only holdings watch
    kol_leaderboard.jsonl                    50 scraped Solana KOLs

This module is the unified read surface over all four, plus a curated store of
its own for wallets that belong to none of them (an ETH/Robinhood whale you
just want on the board, say).

CURATED vs DERIVED — the distinction the whole design rests on
--------------------------------------------------------------
`wallets_db.json` holds ONLY hand-added wallets. Everything else is merged in
read-only at request time and tagged with its `source`.

Scraped KOLs are deliberately NOT copied into the registry. kolscan reranks
continuously; a copy would be a second truth that drifts from the first the
moment it does, which is the same class of bug as the pyramid scoreboard
mislabelling its own baseline. Derived rows are rendered, never stored.

`evm_copy_config.json` and `evm_wallets.json` are likewise READ, never written
here — `evm_copy.py` reads that config directly and is running right now with
live state. A registry that is purely additive cannot break a running watcher.
Each subsystem keeps its own write path; this module only unifies the view.

NO BALANCE ENRICHMENT IN THIS MODULE
------------------------------------
Deliberate. `/api/evm/wallets` scans 7 chains per address and needed a 90s
timeout for ONE wallet. A unified panel enriching 50+ rows on every poll would
be unusable, so rows here render instantly from JSON — address, chain, kind,
label — and holdings stay on the existing on-demand EVM endpoint.

`kind` is the field none of the four sources recorded, and it is the reason the
panel is worth having: it answers "why am I tracking this wallet".
"""

from __future__ import annotations

import json
import os
import re
import time

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, "wallets_db.json")

EVM_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
SOL_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

KINDS = ("kol", "whale", "copy-target", "watch", "other")
CHAINS = ("solana", "ethereum", "robinhood", "base", "arbitrum",
          "optimism", "polygon", "gnosis", "unknown")


def detect_chain(addr):
    """Best-effort only. An 0x address is valid on every EVM chain at once, so
    this returns the FAMILY, not a claim about where the wallet is active —
    the caller should pass an explicit chain when it knows one."""
    a = (addr or "").strip()
    if EVM_RE.match(a):
        return "evm"
    if SOL_RE.match(a):
        return "solana"
    return "unknown"


def valid(addr):
    a = (addr or "").strip()
    return bool(EVM_RE.match(a) or SOL_RE.match(a))


def load(path=DB_PATH):
    try:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f) or {}
        return d.get("wallets") or []
    except FileNotFoundError:
        return []
    except Exception:
        return []


def save(wallets, path=DB_PATH):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"wallets": wallets}, f, indent=1)
    os.replace(tmp, path)


def add(addr, chain=None, kind="watch", label="", notes="", path=DB_PATH):
    a = (addr or "").strip()
    if not valid(a):
        raise ValueError("not a valid EVM (0x…) or Solana address")
    ws = load(path)
    if any((w.get("address") or "").lower() == a.lower() for w in ws):
        raise ValueError("already in the registry")
    ws.append({
        "address": a,
        "chain": (chain or detect_chain(a)),
        "kind": kind if kind in KINDS else "other",
        "label": (label or "").strip(),
        "notes": (notes or "").strip(),
        "source": "manual",
        "enabled": True,
        "added_ts": round(time.time(), 3),
    })
    save(ws, path)
    return ws[-1]


def remove(addr, path=DB_PATH):
    a = (addr or "").strip().lower()
    ws = load(path)
    out = [w for w in ws if (w.get("address") or "").lower() != a]
    save(out, path)
    return len(ws) - len(out)


def _derived(sniper_cfg=None, kol_board=None):
    """Read-only rows from the subsystems that own their own stores."""
    rows = []

    # Solana copy target (sniper_config.json)
    tgt = ((sniper_cfg or {}).get("target") or "").strip()
    if tgt:
        rows.append({"address": tgt, "chain": "solana", "kind": "copy-target",
                     "label": "sniper target", "source": "sniper_config",
                     "enabled": True, "readonly": True})

    # EVM copy targets (evm_copy_config.json) — evm_copy.py owns this file
    try:
        with open(os.path.join(HERE, "evm_copy_config.json"),
                  encoding="utf-8") as f:
            cfg = json.load(f) or {}
        for t in cfg.get("targets") or []:
            rows.append({"address": t.get("address"),
                         "chain": t.get("chain") or "unknown",
                         "kind": "copy-target",
                         "label": t.get("label") or "",
                         "source": "evm_copy_config",
                         "enabled": bool(t.get("enabled", True)),
                         "readonly": True})
    except Exception:
        pass

    # EVM view-only watch (evm_wallets.json)
    try:
        with open(os.path.join(HERE, "evm_wallets.json"), encoding="utf-8") as f:
            for w in json.load(f) or []:
                rows.append({"address": w.get("address"), "chain": "evm",
                             "kind": "watch", "label": w.get("label") or "",
                             "source": "evm_wallets", "enabled": True,
                             "readonly": True})
    except Exception:
        pass

    # Scraped KOLs — rendered, never stored (see module header)
    for k in (kol_board or []):
        rows.append({"address": k.get("wallet"), "chain": "solana",
                     "kind": "kol", "label": k.get("name") or "",
                     "source": "kolscan", "enabled": True, "readonly": True,
                     "rank": k.get("rank"), "pnl_sol": k.get("pnl_sol"),
                     "thin": k.get("thin"), "days_seen": k.get("days_seen"),
                     "unverified": True})
    return rows


def merged(sniper_cfg=None, kol_board=None, path=DB_PATH):
    """Curated + derived, de-duplicated by address.

    A wallet present in both keeps its CURATED row (that one carries the user's
    label, kind and notes) but accumulates every source that mentions it, so
    the panel can show that e.g. an address is both a copy target and a KOL.
    """
    out, by = [], {}
    for w in load(path):
        r = dict(w, readonly=False, sources=["manual"])
        by[(w.get("address") or "").lower()] = r
        out.append(r)
    for d in _derived(sniper_cfg, kol_board):
        a = (d.get("address") or "").lower()
        if not a:
            continue
        cur = by.get(a)
        if cur:
            src = d.get("source")
            if src and src not in cur["sources"]:
                cur["sources"].append(src)
            # a curated row keeps its own label/kind, but inherits the live
            # facts the derived source is authoritative for
            for k in ("rank", "pnl_sol", "thin", "days_seen"):
                if d.get(k) is not None:
                    cur[k] = d[k]
            continue
        r = dict(d, sources=[d.get("source")])
        by[a] = r
        out.append(r)
    return out


def summary(rows):
    by_chain, by_kind = {}, {}
    for r in rows:
        by_chain[r.get("chain") or "unknown"] = by_chain.get(r.get("chain") or "unknown", 0) + 1
        by_kind[r.get("kind") or "other"] = by_kind.get(r.get("kind") or "other", 0) + 1
    return {"total": len(rows), "by_chain": by_chain, "by_kind": by_kind,
            "curated": len([r for r in rows if not r.get("readonly")])}
