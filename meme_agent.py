"""
meme_agent.py — LLM entry judge for the sniper (Phase 1: playbook-driven, two sleeves).

Pattern ported from HLPRO2/scalp_agent.py (proven live there): compact JSON
snapshot in, strict JSON verdict out, FAIL-CLOSED — any failure (no provider,
unparseable output, bad types) returns enter=False tagged with the error, so a
dead router can never masquerade as a real veto in the stats.

Phase 1 additions:
- The system prompt is ASSEMBLED per call: base role + sleeve context + the
  active rules from MemeBrain/playbook/. This is the learning surface — the
  nightly reflection (Phase 2) proposes candidate rules; only rules promoted to
  `status: active` (by beating the control book, Wilson-95 n>=30) reach the
  prompt. The judge never edits its own playbook.
- Two sleeves: "copy_target" (seconds-old creations by the watched spam
  launcher) and "graduating" (coins near completing the bonding curve).

Still shadow: jup_trader.py opens every candidate in the CONTROL book exactly
as before and journals the verdict to brain_decisions.jsonl; the brain book is
virtual (_brain_report.py).

Design laws carried over from HLPRO2:
- Small local models cannot threshold raw floats (2026-06-09 finding), so the
  snapshot pairs every important number with a categorical label. The verdict
  must survive a silent fallback to the local 7B.
- max_tokens 320: reasoning models (gpt-oss, gemini-2.5) burn budget thinking
  before the JSON; a small cap leaves an empty 'content'.
- Validate boundaries against REAL trade history, never synthetic ideals.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Optional

import llm_router

logger = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
PLAYBOOK_DIR = os.path.join(HERE, "MemeBrain", "playbook")

# ---------------------------------------------------------------- playbook ----

def _parse_note(text: str) -> dict:
    """Tolerant frontmatter + RULE-line parse. Returns {} on any malformation —
    a broken note must degrade to 'not injected', never to a crash."""
    out: dict = {}
    try:
        if text.startswith("---"):
            end = text.index("---", 3)
            for line in text[3:end].splitlines():
                if ":" in line:
                    k, v = line.split(":", 1)
                    out[k.strip()] = v.strip()
        m = re.search(r"^RULE:\s*(.+?)(?:\n\s*\n|\Z)", text, re.MULTILINE | re.DOTALL)
        if m:
            out["rule"] = " ".join(m.group(1).split())
    except Exception:
        return {}
    return out


def load_playbook(sleeve: str) -> list[str]:
    """Active rules for a sleeve, as prompt-ready lines. Re-read per call — the
    volume is ~tens of decisions/day, and freshness beats caching here."""
    rules = []
    try:
        for fn in sorted(os.listdir(PLAYBOOK_DIR)):
            if not fn.endswith(".md"):
                continue
            note = _parse_note(open(os.path.join(PLAYBOOK_DIR, fn),
                                    encoding="utf-8").read())
            if note.get("status") != "active" or not note.get("rule"):
                continue
            if note.get("sleeve", "all") not in ("all", sleeve):
                continue
            rules.append(f"- [{note.get('id', fn[:8])}] {note['rule']}")
    except Exception as exc:
        logger.warning("meme_agent: playbook load failed (%s)", exc)
    return rules


# ---------------------------------------------------------------- rugcheck ----

def rug_check(mint: str) -> Optional[dict]:
    """RugCheck free summary: normalized risk score + named risks. Added
    2026-07-11 after the worst graduating losses (-80..-91% gap-stops) showed
    CLEAN top-10 metrics (13-17%) — the wallet-split-insider signature that
    Jupiter's audit fields cannot see. Shadow enrichment only: it rides in the
    snapshot and the decision journal so its predictive value can be tested
    against outcomes BEFORE anything gates on it. Returns None on any failure."""
    try:
        r = requests.get(
            f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary",
            timeout=8, headers={"Accept": "application/json"})
        if r.status_code != 200:
            return None
        d = r.json() or {}
        score = d.get("score_normalised")
        if score is None:
            score = d.get("score")
        return {"score": score,
                "risks": [str(x.get("name")) for x in (d.get("risks") or [])
                          if x.get("name")][:8]}
    except Exception:
        return None


# ---------------------------------------------------------------- snapshot ----

def _band(v, cuts, labels):
    """Map a number onto categorical labels; None => 'unknown'."""
    if v is None:
        return "unknown"
    for cut, lab in zip(cuts, labels):
        if v < cut:
            return lab
    return labels[-1]


def build_snapshot(coin: dict, meta: Optional[dict] = None,
                   social: Optional[dict] = None,
                   source: str = "copy_target",
                   rug: Optional[dict] = None) -> dict:
    """Compact judge view of a candidate.

    coin   = pump.fun coin dict. For copy_target: the raw /coins creation entry.
             For graduating: the _fetch_graduating() row (carries progress,
             sol_in_curve, replies, live, koth; mcap under "mcap"/"usd_market_cap",
             created under "created"/"created_timestamp").
    meta   = jup_trader.token_meta() result (may be None)
    social = jup_trader.dex_social() result (may be None)

    Every number that matters carries a categorical twin so the local 7B
    backstop can reason about it (raw floats stay in for the 70B-class models).
    """
    now = time.time()
    created = (coin.get("created_timestamp") or coin.get("created") or 0) / 1000
    age_s = round(now - created, 1) if created else None
    mcap = coin.get("usd_market_cap") or coin.get("mcap")

    intel = (meta or {}).get("intel") or {}
    stats5m = (intel.get("stats") or {}).get("5m") or {}
    socials = (social or {}).get("socials") or []
    websites = (social or {}).get("websites") or []

    # how much off-chain data resolved — seconds-old mints are usually thin
    if intel:
        coverage = "full" if intel.get("holders") is not None else "partial"
    elif social and (socials or websites):
        coverage = "social_only"
    else:
        coverage = "minimal"

    snap = {
        "source": ("graduating_curve" if source == "graduating"
                   else "copy_target_creation"),
        "symbol": coin.get("symbol") or "?",
        "name": (coin.get("name") or "")[:48],
        "age_seconds": age_s,
        "age_band": _band(age_s, [120, 3600, 21600],
                          ["seconds_old", "minutes_old", "hours_old", "old"]),
        "mcap_usd": mcap,
        "mcap_band": _band(mcap, [8_000, 15_000, 40_000, 69_000],
                           ["sub_default", "early_curve", "mid_curve",
                            "near_graduation", "graduated_range"]),
        "data_coverage": coverage,
        "has_socials": bool(socials),
        "n_websites": len(websites),
        "rug_check": (rug if rug else "unavailable"),
    }
    if source == "graduating":
        prog = coin.get("progress")
        snap.update({
            "curve_progress_pct": prog,
            "progress_band": _band(prog, [90, 97], ["approaching", "close",
                                                    "imminent"]),
            "sol_in_curve": coin.get("sol_in_curve"),
            "reply_count": coin.get("replies"),
            "replies_band": _band(coin.get("replies"), [10, 50],
                                  ["quiet", "some_chatter", "busy"]),
            "live_stream": bool(coin.get("live")),
            "king_of_the_hill": bool(coin.get("koth")),
        })
    if intel:
        snap.update({
            "liquidity_usd": intel.get("liquidity"),
            "holders": intel.get("holders"),
            "holders_band": _band(intel.get("holders"), [10, 50, 200],
                                  ["very_few", "few", "growing", "many"]),
            "top10_holder_pct": intel.get("top10pct"),
            "concentration": _band(intel.get("top10pct"), [30, 60],
                                   ["distributed", "concentrated",
                                    "very_concentrated"]),
            "dev_holding_pct": intel.get("dev_pct"),
            "mint_authority_disabled": intel.get("mint_off"),
            "freeze_authority_disabled": intel.get("freeze_off"),
            "organic_score": intel.get("organic"),
            "flow_5m": {"net_buyers": stats5m.get("nb"),
                        "buy_vol": stats5m.get("bv"),
                        "sell_vol": stats5m.get("sv")},
        })
    return snap


# ------------------------------------------------------------------ prompt ----

BASE_PROMPT = """You are the entry gate for sniping pump.fun meme coins.

Strategy if entered: take-profit +100%, stop-loss -40%, hard timeout 15 min.

{context}

rug_check is an independent risk scan (higher score = riskier; risk names are
literal). The catastrophic failure mode on this venue is insider wallet-split
dumps that clean top-10 metrics miss — weight insider/holder-related rug risks
heavily. "unavailable" means the scan failed, not that the token is safe.

PLAYBOOK (learned + seeded rules — follow them; cite rule ids in your reason
when one drives the decision):
{rules}

Respond with ONLY this JSON object:
{{"enter": true|false, "confidence": <0.0-1.0>, "reason": "<short factual reason>"}}

confidence is your probability that entering beats skipping under the strategy
above. Be calibrated. When unsure, refuse with low confidence."""

COPY_TARGET_CONTEXT = """SLEEVE: brand-new creations by one watched serial creator, judged seconds after
launch. data_coverage is often thin at this age. Approve only launches that look
meaningfully better than this creator's typical dead-on-arrival output: elevated
mcap_band for the age, has_socials true, organic_score not "low", growing
holders, positive net_buyers, distributed concentration."""

GRADUATING_CONTEXT = """SLEEVE: coins nearing completion of the bonding curve (graduation ~$69k mcap,
then migration to PumpSwap). These are minutes-to-hours old with real flow data
available — judge the evidence, not the story. Momentum continuation is the
thesis; you are looking for sustained demand INTO graduation, not a finished
ride."""


def _system_for(sleeve: str) -> str:
    rules = load_playbook(sleeve)
    context = GRADUATING_CONTEXT if sleeve == "graduating" else COPY_TARGET_CONTEXT
    return BASE_PROMPT.format(
        context=context,
        rules="\n".join(rules) if rules else "- (no playbook rules loaded)")


# -------------------------------------------------------------- validation ----

_JSON_BLOCK = re.compile(r"\{.*\}", re.DOTALL)


def _extract_json(text: str) -> Any:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        m = _JSON_BLOCK.search(text)
        if not m:
            return None
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            return None


_ENTER_RE = re.compile(r'"enter"\s*:\s*(true|false)', re.IGNORECASE)
_CONF_RE = re.compile(r'"confidence"\s*:\s*([0-9.]+)')
_REASON_RE = re.compile(r'"reason"\s*:\s*"([^"]*)')


def _salvage(text: str) -> Any:
    """Last-ditch parse of a token-truncated JSON verdict (e.g. gemini cutting
    off mid-string). Requires BOTH enter and confidence to be present — anything
    less stays an error rather than becoming a fabricated verdict."""
    m_enter, m_conf = _ENTER_RE.search(text), _CONF_RE.search(text)
    if not (m_enter and m_conf):
        return None
    m_reason = _REASON_RE.search(text)
    return {"enter": m_enter.group(1).lower() == "true",
            "confidence": float(m_conf.group(1)),
            "reason": (m_reason.group(1) if m_reason else "") + " [salvaged]"}


def _refuse(error: str) -> dict:
    """Fail-closed verdict, explicitly tagged so router failures are never
    counted as real vetoes by the learning loop."""
    return {"enter": False, "confidence": 0.0, "reason": "",
            "model": "", "latency_ms": 0.0, "error": error}


_TRUE_STRINGS = {"true", "yes", "y", "1"}


def _as_bool(value: Any) -> bool:
    """bool("false") is True in Python, so a model replying "enter": "false" used to be
    recorded as an approval. Only explicit true values count."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value == 1
    if isinstance(value, str):
        return value.strip().lower() in _TRUE_STRINGS
    return False


def _coerce(raw: Any) -> dict:
    if not isinstance(raw, dict):
        return _refuse("non-object response")
    try:
        enter = _as_bool(raw.get("enter", False))
        confidence = float(raw.get("confidence", 0.0))
        reason = str(raw.get("reason", ""))[:140]
    except (TypeError, ValueError) as exc:
        return _refuse(f"bad types ({exc})")
    confidence = min(1.0, max(0.0, confidence))
    return {"enter": enter, "confidence": confidence, "reason": reason,
            "model": "", "latency_ms": 0.0, "error": None}


# ------------------------------------------------------------------- judge ----

def decide_snipe(snapshot: dict) -> dict:
    """Judge one candidate. Returns {enter, confidence, reason, model,
    latency_ms, error}. Never raises; fail-closed with the error field set."""
    sleeve = ("graduating" if snapshot.get("source") == "graduating_curve"
              else "copy_target")
    user = json.dumps(snapshot, separators=(",", ":"))
    # 1200, not 320: reasoning models (gpt-oss, gemini-2.5) spend their thinking
    # tokens against max_tokens, and with the playbook injected the prompt is
    # long enough that 320/640 left gpt-oss mid-reasoning and truncated gemini's
    # JSON mid-string (observed 2026-07-11). Volume is tens of calls/day on free
    # tiers; the extra budget costs nothing.
    res = llm_router.chat(_system_for(sleeve), user, temperature=0.2,
                          max_tokens=1200, force_json=True)
    if res is None:
        return _refuse("no provider answered")
    verdict = _extract_json(res.text)
    if verdict is None:
        verdict = _salvage(res.text)
    out = _coerce(verdict) if verdict is not None else _refuse("unparseable response")
    out["model"] = f"{res.provider}/{res.model}"
    out["latency_ms"] = round(res.latency_ms, 0)
    logger.info("meme_agent: %s [%s] enter=%s conf=%.2f (%.0fms) %s",
                out["model"], sleeve, out["enter"], out["confidence"],
                out["latency_ms"], out.get("error") or out["reason"])
    return out


def health_check() -> dict:
    return {"router": llm_router.status(),
            "playbook": {"copy_target": len(load_playbook("copy_target")),
                         "graduating": len(load_playbook("graduating"))}}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _envp = os.path.join(HERE, ".env")
    if os.path.exists(_envp):
        for _line in open(_envp, encoding="utf-8"):
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())

    print("playbook:", json.dumps(health_check()["playbook"]))

    # Boundary probes per sleeve: base cases should refuse, strong cases enter
    # (or at least score materially higher).
    spam = build_snapshot({"mint": "T1", "symbol": "WOOF", "name": "woof dog",
                           "usd_market_cap": 6300,
                           "created_timestamp": (time.time() - 20) * 1000})
    strong_new = build_snapshot(
        {"mint": "T2", "symbol": "GIGA", "name": "gigachad reborn",
         "usd_market_cap": 34_000,
         "created_timestamp": (time.time() - 90) * 1000},
        meta={"intel": {"liquidity": 21_000, "holders": 240, "top10pct": 22,
                        "dev_pct": 1.5, "mint_off": True, "freeze_off": True,
                        "organic": "high",
                        "stats": {"5m": {"nb": 85, "bv": 30_000, "sv": 9_000}}}},
        social={"socials": [{"type": "twitter"}], "websites": [{"url": "x"}]})
    stale_grad = build_snapshot(
        {"mint": "T3", "symbol": "TIRED", "name": "tired coin",
         "mcap": 62_000, "created": (time.time() - 5 * 3600) * 1000,
         "progress": 98.5, "sol_in_curve": 84.1, "replies": 4,
         "live": False, "koth": False},
        meta={"intel": {"liquidity": 30_000, "holders": 150, "top10pct": 45,
                        "dev_pct": 0.0, "mint_off": True, "freeze_off": True,
                        "organic": "low",
                        "stats": {"5m": {"nb": -12, "bv": 800, "sv": 4_100}}}},
        source="graduating")
    hot_grad = build_snapshot(
        {"mint": "T4", "symbol": "PUMP", "name": "pump city",
         "mcap": 58_000, "created": (time.time() - 40 * 60) * 1000,
         "progress": 91.0, "sol_in_curve": 71.0, "replies": 130,
         "live": True, "koth": True},
        meta={"intel": {"liquidity": 26_000, "holders": 410, "top10pct": 18,
                        "dev_pct": 0.5, "mint_off": True, "freeze_off": True,
                        "organic": "high",
                        "stats": {"5m": {"nb": 96, "bv": 41_000, "sv": 12_000}}}},
        social={"socials": [{"type": "twitter"}, {"type": "telegram"}],
                "websites": [{"url": "x"}]},
        source="graduating")
    for label, snap in (("spam-base-case", spam), ("strong-launch", strong_new),
                        ("stale-graduation", stale_grad), ("hot-graduation", hot_grad)):
        d = decide_snipe(snap)
        print(f"\n[{label}] enter={d['enter']} conf={d['confidence']:.2f} "
              f"model={d['model']} err={d['error']}\n  reason: {d['reason']}")
