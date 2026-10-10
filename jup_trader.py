"""
Minimal Solana buy/sell terminal via Jupiter.
Paste a token CA -> one-click buy (SOL amount) / sell (% of holdings).

Setup: put your wallet key in .env next to this file:
    SOL_PRIVATE_KEY=<base58 private key, e.g. Phantom export>
    RPC_URL=https://api.mainnet-beta.solana.com   (optional, default shown)
Run:  python jup_trader.py   ->  http://127.0.0.1:5010
"""
import base64
import json
import os
import re
import time
import threading
from datetime import datetime

import base58
import requests
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from flask import Flask, jsonify, request
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction

import curve_sim           # exact pump.fun bonding-curve fill math (shadow marks)
import pnl_attrib          # per-lot on-chain PnL attribution (see module docstring)
import smart_exit as _smx_mod   # flow-evidence exits: paper-enforced, live-shadow
import flow_source as _flow_mod  # per-mint trade flow (RPC logs; PumpPortal key-gated it)
import pyramid as _pyr_mod      # scale-in ladder policy — GHOST ONLY (see its header)
import tpsl as _tpsl_mod        # server-side TP/SL executor for the pop-out box
import paper as _paper_mod      # simulated book for the pop-out box (its own journal)
import trade_context as _ctx_mod   # records WHY a trade happened (see its header)

# Decision-context recorder. Built in main(); None-safe everywhere because the
# journal hook fires long before and after it exists.
CTX = None

# The live executor, built in main() once the wallet is known. Module-level and
# None-by-default because execute_swap()'s journal hook pokes it after every
# fill, and that hook has to be safe long before (and after) the executor runs.
TPSL = None

# The paper book and ITS OWN executor instance. Deliberately a second Executor
# rather than a mode on the first: they must be able to be armed independently,
# and a paper position must never share fire-caps, cooldowns, or the mint
# interlock with a real one. Built in main() alongside TPSL.
PAPER = None
PAPER_TPSL = None
try:
    import kol_source as _kol_mod   # kolscan.io leaderboard — CANDIDATES, not evidence
except Exception as _kol_import_err:   # a scraper dep must never cost the sniper
    _kol_mod = None
    print(f"[KOL] disabled — kol_source import failed: {_kol_import_err}")

HERE = os.path.dirname(os.path.abspath(__file__))

# ---------------- config ----------------
def _load_env():
    path = os.path.join(HERE, ".env")
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

_load_env()

RPC_URL = os.environ.get("RPC_URL", "https://api.mainnet-beta.solana.com")
SLIPPAGE_BPS = int(os.environ.get("SLIPPAGE_BPS", "300"))          # 3% default
MAX_PRIORITY_LAMPORTS = int(os.environ.get("MAX_PRIORITY_LAMPORTS", "2000000"))  # 0.002 SOL cap
PORT = int(os.environ.get("PORT", "5010"))

SOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_PROG = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROG = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
MIN_HOLDING_USD = float(os.environ.get("MIN_HOLDING_USD", "1"))
# api.jup.ag, not lite-api. Same paths, same data, same latency - the only
# difference is that this host honours an x-api-key. Keyless is the 0.5 RPS /
# 30 RPM bucket; a FREE key doubles it to 1 RPS / 60 RPM, and price+quotes+token
# lookups all share that one bucket. Jupiter's grandfathering of old portal
# limits ended 2026-06-30, which is why the 429s got noticeably worse.
# NOTE: the limit is per ORGANISATION, not per key - minting extra keys in the
# portal buys nothing.
JUP = "https://api.jup.ag"
JUP_API_KEY = os.environ.get("JUP_API_KEY", "").strip()
JITO_SEND = "https://mainnet.block-engine.jito.wtf/api/v1/transactions"
JITO_TIP_FLOOR = "https://bundles.jito.wtf/api/v1/bundles/tip_floor"
# 0 = dynamic (live 50th-pct landed tip); otherwise fixed lamports
JITO_TIP_LAMPORTS = int(os.environ.get("JITO_TIP_LAMPORTS", "0"))
JITO_TIP_MIN, JITO_TIP_MAX = 2000, 300000  # clamp dynamic tip (0.000002–0.0003 SOL)

# ---------------- fee modes ----------------
# "no jito = cheaper" is backwards and it cost real SOL to learn: the MEV path
# tips Jito at most JITO_TIP_MAX (0.0003 SOL), while MEV-off asks Jupiter for a
# "high" priority fee capped at MAX_PRIORITY_LAMPORTS (0.002 SOL) — a ceiling
# ~7x higher. So the old boolean had no cheap setting at all. Three modes now:
#   mev   jito tip, no priority fee        (unchanged default; sandwich-safe)
#   fast  priority "high", 0.002 cap       (what mev=false used to mean)
#   eco   priority "medium", small cap     (cheapest; lands slower, may miss)
# ECO is deliberately its OWN constant and does NOT lower MAX_PRIORITY_LAMPORTS
# — _pumpportal_tx() feeds that same constant to the live sniper's priorityFee,
# so shrinking it to get cheap dashboard sells would silently under-fee snipes.
ECO_PRIORITY_LAMPORTS = int(os.environ.get("ECO_PRIORITY_LAMPORTS", "50000"))   # 0.00005 SOL
ECO_PRIORITY_LEVEL = os.environ.get("ECO_PRIORITY_LEVEL", "medium")             # medium|high|veryHigh
FEE_MODES = ("mev", "fast", "eco")

# ---------------- pooled HTTP session ----------------
# Every trade-flow call (quote, swap-build, jito send/tip, RPC confirm-poll) used
# a fresh requests call = a new TCP+TLS handshake each time (~600-700ms wasted).
# One keep-alive Session pools connections per host: measured the repeated
# getSignatureStatuses poll drop from ~800ms to ~160ms (5x). Thread-safe.
from requests.adapters import HTTPAdapter
_HTTP = requests.Session()
_http_adapter = HTTPAdapter(pool_connections=16, pool_maxsize=32, max_retries=0)
_HTTP.mount("https://", _http_adapter)
_HTTP.mount("http://", _http_adapter)

# A SEPARATE session for Jupiter, carrying the api key.
#
# The key deliberately does NOT go on _HTTP. That session is shared with
# pumpportal.fun, frontend-api-v3.pump.fun, api.geckoterminal.com and
# api.dexscreener.com - a default header there would hand our paid-tier
# credential to four unrelated third parties on every keepalive ping. Same
# pooling settings, so the connection-reuse win is unchanged.
#
# Falls back to working keyless if JUP_API_KEY is unset: api.jup.ag serves
# unauthenticated requests too, just at the 0.5 RPS bucket. No key must never
# mean no trading.
# ---- sell-box priority on Jupiter (2026-10-05) ----
# Jupiter's limit is ONE 60/min bucket for everything: dashboard price ticks,
# holdings valuation, TP/SL screen, brain lookups, and the box's own quote +
# swap. Background traffic could use it all up, so a box buy got 429'd.
# Now (with SELLBOX_ONLY_RPC=1) background callers share a capped slice
# (JUP_BG_RPM, default 20/min), and are paused completely for JUP_BOX_HOLD_S
# after any sell-box Jupiter call (pasting a CA, buy, sell) so the box's
# quote + swap never queue behind them. A held-back call gets a local 429,
# which every caller here already handles (cached prices, retry later).
# Prints a one-line usage summary each minute so you can see who uses it.
JUP_BG_RPM = int(os.environ.get("JUP_BG_RPM", "20"))
JUP_BOX_HOLD_S = float(os.environ.get("JUP_BOX_HOLD_S", "15"))
import collections as _collections


class _JupGateSession(requests.Session):
    def __init__(self):
        super().__init__()
        self._g_lock = threading.Lock()
        self._g_bg = _collections.deque()
        self._g_hold_until = 0.0
        self._g_stats = {"box": 0, "bg": 0, "held": 0, "429": 0}

    def _is_box(self):
        try:
            return (not SELLBOX_ONLY_RPC) or bool(getattr(_RPC_CTX, "sellbox", False))
        except NameError:            # during module init, before those exist
            return True

    def request(self, method, url, *a, **kw):
        box = self._is_box()
        now = time.time()
        with self._g_lock:
            if box:
                self._g_stats["box"] += 1
                self._g_hold_until = now + JUP_BOX_HOLD_S
            else:
                while self._g_bg and now - self._g_bg[0] > 60:
                    self._g_bg.popleft()
                if now < self._g_hold_until or len(self._g_bg) >= JUP_BG_RPM:
                    self._g_stats["held"] += 1
                    r = requests.Response()
                    r.status_code = 429
                    r.reason = "Held for sell box"
                    r.url = url
                    r._content = (b"background Jupiter call held back locally "
                                  b"to keep budget for the sell box")
                    return r
                self._g_bg.append(now)
                self._g_stats["bg"] += 1
        r = super().request(method, url, *a, **kw)
        if r.status_code == 429:
            with self._g_lock:
                self._g_stats["429"] += 1
        return r

    def take_stats(self):
        with self._g_lock:
            st = dict(self._g_stats)
            for k in self._g_stats:
                self._g_stats[k] = 0
        return st


def _jup_stats_loop():
    while True:
        time.sleep(60)
        try:
            st = _JUP_HTTP.take_stats()
            if any(st.values()):
                print(f"[JUP] last 60s: box {st['box']}, background {st['bg']} "
                      f"(cap {JUP_BG_RPM}/min), held back {st['held']}, "
                      f"real 429s from Jupiter {st['429']}")
        except Exception:
            pass


_JUP_HTTP = _JupGateSession()
threading.Thread(target=_jup_stats_loop, daemon=True, name="jup-stats").start()
_JUP_HTTP.mount("https://", _http_adapter)
_JUP_HTTP.mount("http://", _http_adapter)
if JUP_API_KEY:
    _JUP_HTTP.headers.update({"x-api-key": JUP_API_KEY})

# ---------------- connection keep-alive heartbeat ----------------
# Snipes are minutes apart, so the pooled connections above go idle and the
# far end closes them — the NEXT buy then pays a fresh TCP+TLS handshake
# (measured 300ms-12s cold vs ~140ms warm, 2026-07-17). A tiny request to each
# trade-critical host every 25s keeps a live socket in the pool so the hot path
# never pays the cold tail. Responses are irrelevant (404s are fine); only the
# handshake reuse matters.
def _keepalive_loop():
    # Every rotation member gets warmed, not just RPC_URL: a peer that only
    # sees traffic when the primary 429s would pay the cold 300ms-12s handshake
    # at exactly the moment the primary is already failing.
    def _ping(u):
        return lambda: _HTTP.post(u, json={"jsonrpc": "2.0", "id": 1,
                                           "method": "getHealth", "params": []},
                                  timeout=5)
    # TRADE_RPC_URL is warmed on purpose even though it is the scarce key: one
    # getHealth per 25s is nothing against a per-mint socket, and it is exactly
    # what stops the first sell of a session paying the 300ms-12s cold
    # handshake this loop was written to eliminate.
    hosts = [_ping(u) for u in dict.fromkeys([TRADE_RPC_URL] + _BG_ROTATE)] + [
        lambda: _HTTP.get("https://pumpportal.fun/", timeout=5),
        lambda: _HTTP.get("https://frontend-api-v3.pump.fun/coins/keepalive", timeout=5),
    ]
    while True:
        for ping in hosts:
            try:
                ping()
            except Exception:
                pass                     # a dead ping just means the next one reopens it
        time.sleep(25)

# NOTE: the thread is NOT started here any more. _keepalive_loop now reads
# _BG_ROTATE/TRADE_RPC_URL, defined further down with the rest of the RPC pool —
# starting it at this point raced module init and the thread would die on a
# NameError before its first ping, silently costing every cold handshake back.
# It is started immediately after those exist.

# ---------------- session persistence (Windows DPAPI) ----------------
# Keeps the unlocked wallet across server restarts without a plaintext key
# on disk: the key blob is encrypted with the Windows user's login secret.
import ctypes
import ctypes.wintypes

SESSION_PATH = os.path.join(HERE, ".session")

class _BLOB(ctypes.Structure):
    _fields_ = [("cbData", ctypes.wintypes.DWORD),
                ("pbData", ctypes.POINTER(ctypes.c_char))]

def _dpapi(data, protect):
    buf = ctypes.create_string_buffer(data, len(data))
    blob_in = _BLOB(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_char)))
    blob_out = _BLOB()
    fn = (ctypes.windll.crypt32.CryptProtectData if protect
          else ctypes.windll.crypt32.CryptUnprotectData)
    if not fn(ctypes.byref(blob_in), None, None, None, None, 0,
              ctypes.byref(blob_out)):
        raise RuntimeError("DPAPI call failed")
    try:
        return ctypes.string_at(blob_out.pbData, blob_out.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(blob_out.pbData)

def session_save(kp):
    try:
        with open(SESSION_PATH, "wb") as f:
            f.write(_dpapi(bytes(kp), protect=True))
    except Exception:
        pass  # persistence is best-effort; trading still works

def session_load():
    try:
        if os.path.exists(SESSION_PATH):
            with open(SESSION_PATH, "rb") as f:
                return Keypair.from_bytes(_dpapi(f.read(), protect=False))
    except Exception:
        pass
    return None

def session_clear():
    try:
        if os.path.exists(SESSION_PATH):
            os.remove(SESSION_PATH)
    except Exception:
        pass


# Active wallet: .env key > saved session > create/unlock in the UI.
_pk = os.environ.get("SOL_PRIVATE_KEY", "").strip()
KEYPAIR = Keypair.from_base58_string(_pk) if _pk else session_load()
PUBKEY = str(KEYPAIR.pubkey()) if KEYPAIR else None

def set_active(kp):
    global KEYPAIR, PUBKEY
    KEYPAIR = kp
    PUBKEY = str(kp.pubkey())
    session_save(kp)

def clear_active():
    global KEYPAIR, PUBKEY
    KEYPAIR = None
    PUBKEY = None
    session_clear()

def require_wallet():
    if KEYPAIR is None:
        raise RuntimeError("no wallet unlocked — create or unlock one first")

app = Flask(__name__)


# DNS-rebinding guard. The server binds to 127.0.0.1, but a malicious page can point its own
# domain at 127.0.0.1 and then call these wallet and trading routes as a "same-origin" page.
# Browsers always send the Host they think they are talking to, so only accept local names.
# Add names (comma-separated) in JUP_ALLOWED_HOSTS if you deliberately serve it elsewhere.
_ALLOWED_HOSTS = {"127.0.0.1", "localhost", "[::1]"} | {
    h.strip().lower() for h in os.getenv("JUP_ALLOWED_HOSTS", "").split(",") if h.strip()}


def _host_name(host_header):
    host = (host_header or "").strip().lower()
    if host.startswith("["):                      # IPv6 literal, e.g. [::1]:5010
        return host.split("]")[0] + "]"
    return host.rsplit(":", 1)[0] if ":" in host else host


@app.before_request
def _reject_foreign_host():
    from flask import request as _rq
    if _host_name(_rq.host) not in _ALLOWED_HOSTS:
        return jsonify({"error": "forbidden host"}), 403
    return None


@app.before_request
def _tag_sellbox_request():
    # Flask's dev server runs each request on its own thread, so a
    # thread-local set here covers every rpc() that request makes.
    from flask import request as _rq
    _RPC_CTX.sellbox = _rq.headers.get("X-Sellbox") == "1"


@app.teardown_request
def _untag_sellbox_request(_exc=None):
    _RPC_CTX.sellbox = False
_rpc_lock = threading.Lock()


# ---------------- encrypted wallet vault ----------------
VAULT_PATH = os.path.join(HERE, "wallets.enc.json")
_KDF_ITERS = 600_000

def _vault_load():
    if os.path.exists(VAULT_PATH):
        with open(VAULT_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"wallets": []}

def _vault_save(v):
    tmp = VAULT_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(v, f, indent=1)
    os.replace(tmp, VAULT_PATH)

def _fernet(password, salt):
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32,
                     salt=salt, iterations=_KDF_ITERS)
    return Fernet(base64.urlsafe_b64encode(kdf.derive(password.encode())))

def wallet_store(kp, password):
    """Encrypt a keypair into the vault. Returns its base58 private key."""
    priv = base58.b58encode(bytes(kp)).decode()
    v = _vault_load()
    if any(w["pubkey"] == str(kp.pubkey()) for w in v["wallets"]):
        raise RuntimeError("wallet already in vault")
    salt = os.urandom(16)
    token = _fernet(password, salt).encrypt(priv.encode()).decode()
    v["wallets"].append({
        "pubkey": str(kp.pubkey()),
        "created": time.strftime("%Y-%m-%d %H:%M:%S"),
        "salt": base64.b64encode(salt).decode(),
        "token": token,
    })
    _vault_save(v)
    return priv

def wallet_create(password):
    """Generate wallet, store encrypted. Returns (keypair, private_key_b58).
    The private key is returned ONCE and never persisted in plaintext."""
    kp = Keypair()
    return kp, wallet_store(kp, password)

_EVM_KEY_REDIRECT = (
    "that looks like an EVM / Ethereum private key (0x + 64 hex, or bare 64 "
    "hex) — this is the SOLANA vault. To sell an EVM token like bcat, scroll to "
    "the 'EVM WALLET // VAULT' card and import it there instead. (Pasting it "
    "here would build an unrelated Solana wallet that can't hold your token.)")

def parse_private_key(text):
    """Accept any common SOLANA private-key export format and return a Keypair.
    Handles: base58 64-byte keypair (Phantom), base58 32-byte seed, 128-hex full
    keypair, and JSON byte arrays (Solana CLI).

    ⚠️ 32-byte hex (0x-prefixed or bare 64 chars) is REJECTED: that is the EVM
    private-key shape, and silently turning it into a Solana wallet is a footgun
    (it bit the user 07-14 — an EVM key became junk wallet 4jwP…). Real Solana
    keys come as base58 or JSON arrays, so nothing legitimate is lost."""
    s = (text or "").strip().strip('"').strip("'")
    if not s:
        raise ValueError("empty private key")
    if len(s.split()) >= 12:
        raise ValueError("that looks like a recovery phrase, not a private key — "
                         "in the wallet app export the individual wallet's "
                         "private key (Robinhood: wallet → settings → "
                         "back up / export private key)")
    if s.startswith("[") and s.endswith("]"):
        try:
            raw = bytes(json.loads(s))
        except Exception:
            raise ValueError("malformed JSON byte array")
        if len(raw) == 64:
            return Keypair.from_bytes(raw)
        if len(raw) == 32:
            return Keypair.from_seed(raw)
        raise ValueError(f"byte array is {len(raw)} bytes — expected 64 or 32")
    low = s.lower()
    hexs = s[2:] if low.startswith("0x") else s
    is_hex = hexs and all(c in "0123456789abcdefABCDEF" for c in hexs)
    # 0x-prefix is unambiguously EVM; bare 32-byte hex is the EVM key shape too.
    if low.startswith("0x") or (is_hex and len(hexs) == 64):
        raise ValueError(_EVM_KEY_REDIRECT)
    if is_hex and len(hexs) == 128:          # 64-byte full Solana keypair in hex
        return Keypair.from_bytes(bytes.fromhex(hexs))
    try:
        raw = base58.b58decode(s)
    except Exception:
        raise ValueError("unrecognized format — paste the key exactly as the "
                         "wallet app exported it (base58, hex, or [..] array)")
    if len(raw) == 64:
        return Keypair.from_base58_string(s)
    if len(raw) == 32:
        return Keypair.from_seed(raw)
    raise ValueError(f"base58 decodes to {len(raw)} bytes — expected 64 "
                     "(full keypair) or 32 (seed). A 12/24-word recovery "
                     "phrase is not a private key — export the per-wallet "
                     "private key from the app instead.")

def wallet_remove(pubkey):
    v = _vault_load()
    keep = [w for w in v["wallets"] if w["pubkey"] != pubkey]
    if len(keep) == len(v["wallets"]):
        raise RuntimeError("wallet not in vault")
    v["wallets"] = keep
    _vault_save(v)

def wallet_is_empty(pubkey):
    """(is_empty, sol) — empty means <0.001 SOL and no token balances."""
    sol = rpc("getBalance", [pubkey])["value"] / 1e9
    if sol >= 0.001:
        return False, sol
    for prog in (TOKEN_PROG, TOKEN_2022_PROG):
        res = rpc("getTokenAccountsByOwner",
                  [pubkey, {"programId": prog}, {"encoding": "jsonParsed"}])
        for acc in res.get("value", []):
            info = acc["account"]["data"]["parsed"]["info"]["tokenAmount"]
            if float(info["uiAmount"] or 0) > 0:
                return False, sol
    return True, sol

def wallet_unlock(pubkey, password):
    for w in _vault_load()["wallets"]:
        if w["pubkey"] == pubkey:
            try:
                priv = _fernet(password, base64.b64decode(w["salt"])) \
                    .decrypt(w["token"].encode()).decode()
            except InvalidToken:
                raise RuntimeError("wrong password")
            return Keypair.from_base58_string(priv)
    raise RuntimeError("wallet not in vault")


# ---------------- RPC helpers ----------------
# ---- HTTP RPC failover (2026-08-04) ------------------------------------------
# Helius free tier started returning 429 on plain HTTP RPC as well as on the
# logsSubscribe handshake, which took /api/holdings out entirely ("ok": false,
# "429 Client Error") — the holdings panel simply rendered empty, with the
# wallet looking like it held nothing.
#
# READS fail over to the public endpoint. SENDS NEVER DO: public mainnet-beta
# is unstaked and drops sendTransaction silently under load, so failing a buy
# over would lose real SOL instead of just a price refresh. That asymmetry is
# the whole point of this block — see also flow_source.py, same rule.
# CORRECTION (2026-08-04, same day): sends were originally pinned to the paid
# endpoint with no failover at all, on the reasoning that public mainnet-beta
# drops sendTransaction silently. That reasoning was half right and it cost a
# real sell — "SELL 100% RASMR FAILED: 429".
#
# The distinction that actually matters is not read-vs-send, it is
# REJECTED vs UNKNOWN:
#
#   HTTP 429/502/503  the server refused it. The transaction was definitively
#                     NOT broadcast, so retrying on another endpoint cannot
#                     double-send. Certain failure is worse than a probabilistic
#                     send, so we DO fail over.
#   timeout / conn    the request may have reached the validator and we simply
#                     did not hear back. Retrying elsewhere risks broadcasting
#                     the SAME signed transaction twice. We do NOT fail over —
#                     the confirm-poll resolves it.
#
# So sends fail over on explicit rejection only, and loudly.
PUBLIC_RPC = "https://api.mainnet-beta.solana.com"
_SEND_METHODS = {"sendTransaction"}
_HARD_REJECT = (429, 502, 503)
RPC_RETURN_HOME_S = 900.0
_rpc_state = {"on_fallback": False, "since": 0.0, "failovers": 0, "last_error": None}

# ---------------- two traffic classes (2026-08-09) ----------------
# Rotation alone was the wrong policy. A free Helius key is a CREDIT budget, and
# the things that drain it are not the things that need it: an always-on
# logsSubscribe socket and the PnL reconciler burn far more than the buy box
# ever will, and they do it while nobody is looking at the screen.
#
# So traffic is split by class, not spread evenly:
#
#   TRADE_RPC_URL   the scarce key. Sends, the sizing read, balances/holdings,
#                   the token lookup behind the buy box, and confirm polls.
#                   Nothing on a timer, nothing that runs while the UI is shut.
#   RPC_URL         the background default: everything else, and the URL that
#                   flow_source / pyramid_runner / insider_scan pick up (the
#                   last two parse .env for the literal "RPC_URL=" prefix, so
#                   pointing THIS one at public is what actually moves them).
#   RPC_URLS        extra free background endpoints, round-robin with a
#                   cooldown, so the background class does not choke either.
#
# TRADE_RPC_URL defaults to RPC_URL. With it unset the TRADE path is unchanged
# (paid key first, public behind it) — but background reads still move off the
# paid key onto public, because excluding it from _BG_ROTATE is the point. That
# is the one behaviour that differs on an unconfigured install, and it differs
# in the protective direction.
TRADE_RPC_URL = os.environ.get("TRADE_RPC_URL", "").strip() or RPC_URL
RPC_URLS = [u.strip() for u in os.environ.get("RPC_URLS", "").split(",") if u.strip()]
RPC_COOL_S = float(os.environ.get("RPC_COOLDOWN_S", "20"))
# The scarce key is deliberately NOT in the background pool — that exclusion is
# the whole feature. PUBLIC_RPC is appended as the last resort rather than
# rotated in, so the fallback banner keeps meaning something.
_BG_ROTATE = [u for u in dict.fromkeys([RPC_URL] + RPC_URLS)
              if u not in (TRADE_RPC_URL, PUBLIC_RPC)] or [PUBLIC_RPC]
_rpc_cool = {}          # url -> unix ts it may be used again
_rpc_rr = {"n": 0}      # round-robin cursor; a torn int just picks another peer

threading.Thread(target=_keepalive_loop, daemon=True).start()   # see note above


def _rpc_host(url):
    return url.split("/?")[0].split("//")[-1]


def _coolable(url):
    """Only background pool members are benched. Cooling PUBLIC_RPC would strand
    the last resort, and cooling TRADE_RPC_URL would do nothing but corrupt the
    pool/cooling counts the banner does arithmetic on — the scarce key's health
    is reported through on_fallback instead."""
    return url in _BG_ROTATE and url != PUBLIC_RPC


def _bg_urls():
    """Rotate the background pool, public last. Never returns an empty list."""
    now = time.time()
    live = [u for u in _BG_ROTATE if _rpc_cool.get(u, 0) <= now]
    if not live:
        # BINDING CASE: every background endpoint is cooling. Skipping them here
        # would leave nothing to serve the read, so ignore the cooldowns — a 429
        # costs one wasted retry, having no candidate costs the read. It must
        # never reach for TRADE_RPC_URL to escape this; that is the budget.
        live = list(_BG_ROTATE)
    if len(live) > 1:
        _rpc_rr["n"] += 1
        k = _rpc_rr["n"] % len(live)
        live = live[k:] + live[:k]
    return live + ([PUBLIC_RPC] if PUBLIC_RPC not in live else [])


def _trade_urls():
    """Scarce key first, background pool behind it as failover only."""
    if (_rpc_state["on_fallback"]
            and time.time() - _rpc_state["since"] <= RPC_RETURN_HOME_S):
        return list(dict.fromkeys(_bg_urls() + [TRADE_RPC_URL]))
    return list(dict.fromkeys([TRADE_RPC_URL] + _bg_urls()))


def rpc_status():
    return {"on_fallback": _rpc_state["on_fallback"],
            # on_fallback now means "TRADE traffic is off the scarce key".
            # Background traffic living on public is policy, not a fault, and
            # must never light the banner or it stops being believed.
            "endpoint": _rpc_host(PUBLIC_RPC if _rpc_state["on_fallback"]
                                  else TRADE_RPC_URL),
            "failovers": _rpc_state["failovers"],
            "last_error": _rpc_state["last_error"],
            "trade_endpoint": _rpc_host(TRADE_RPC_URL),
            "split": TRADE_RPC_URL != RPC_URL,
            "pool": [_rpc_host(u) for u in _BG_ROTATE],
            "cooling": [_rpc_host(u) for u, t in _rpc_cool.items()
                        if t > time.time()]}


# ---------------- sell-box-only key (2026-10-05) ----------------
# The scarce key kept getting rate limited because the WHOLE trader used the
# trade class: dashboard holdings polls, TP/SL's minutely holdings read, token
# lookups from the main page. Now only work done on behalf of the SELL BOX
# (popout.html, which tags every request with X-Sellbox: 1) and TP/SL fires
# (armed from the box) may touch TRADE_RPC_URL. Everything else, including the
# main dashboard's buys/sells, runs on the background pool (public).
# Set SELLBOX_ONLY_RPC=0 in .env to restore the old behaviour.
SELLBOX_ONLY_RPC = os.environ.get("SELLBOX_ONLY_RPC", "1") == "1"
_RPC_CTX = threading.local()


class sellbox_scope:
    """`with sellbox_scope():` lets rpc() inside it use the scarce key."""
    def __enter__(self):
        self._prev = getattr(_RPC_CTX, "sellbox", False)
        _RPC_CTX.sellbox = True
        return self

    def __exit__(self, *exc):
        _RPC_CTX.sellbox = self._prev
        return False


def _key_allowed():
    return (not SELLBOX_ONLY_RPC) or getattr(_RPC_CTX, "sellbox", False)


def rpc(method, params, trade=False):
    """trade=True routes to TRADE_RPC_URL — the scarce key. Default is background.

    Opt-IN, not opt-out: a new caller added later is background by default, so
    the failure mode of forgetting the flag is a slightly slower panel, never a
    silently drained key. Reserve it for the buy box, account lookups, and the
    trade path itself. Sends are always trade class regardless of the flag —
    routing a send to unstaked public is the documented way to lose real SOL.

    It also fixes a staleness hazard rotation introduced: peers sit at
    independent slots, so a lagging one can hand back a stale-high token
    balance, and a 100% sell sized off that number is rejected by the token
    program. The sizing read is trade class, so it always asks one node.
    """
    # No lock: requests.Session is thread-safe (urllib3 pools per-host), and the
    # old global _rpc_lock made a snipe's sendTransaction queue behind whatever
    # confirm-poll happened to hold it — worst case a full RTT of dead wait.
    is_send = method in _SEND_METHODS
    is_trade = (is_send or trade) and _key_allowed()
    if is_send and not is_trade:
        print("[RPC] send from outside the sell box -> background/public RPC "
              "(SELLBOX_ONLY_RPC=1). Unstaked: confirm on solscan.")
    if (_rpc_state["on_fallback"]
            and time.time() - _rpc_state["since"] > RPC_RETURN_HOME_S):
        _rpc_state["on_fallback"] = False          # periodically come home
        print("[RPC] retrying trade endpoint")
    # A send always NAMES the staked key first — it must never inherit the
    # background pool's public-first ordering, which is how a real trade ends up
    # on an unstaked endpoint that drops sendTransaction silently. Trade reads
    # get the same ordering; the background pool sits behind both as failover,
    # since a hard-rejected trade call is better retried than lost.
    urls = _trade_urls() if is_trade else _bg_urls()
    last = None
    for i, url in enumerate(urls):
        more = i + 1 < len(urls)
        try:
            r = _HTTP.post(url, json={"jsonrpc": "2.0", "id": 1,
                                      "method": method, "params": params},
                           timeout=15)
            if r.status_code in _HARD_REJECT and _coolable(url):
                # 429 is the rate-limit signal the pool exists for: bench this
                # endpoint so the next background call picks a peer instead of
                # walking into the same limit.
                _rpc_cool[url] = time.time() + RPC_COOL_S
            if r.status_code in _HARD_REJECT and more:
                # definitively not broadcast -> safe to retry elsewhere
                last = RuntimeError(f"{r.status_code} from {url.split('/?')[0]}")
                if is_send:
                    print(f"[RPC] send rejected {r.status_code} by "
                          f"{_rpc_host(url)} — retrying on {_rpc_host(urls[i + 1])} "
                          f"(tx was NOT broadcast)")
                continue
            r.raise_for_status()
            out = r.json()
            if "error" in out:
                raise RuntimeError(f"RPC {method}: {out['error'].get('message')}")
            # Only TRADE traffic landing off the scarce key is a fault. The
            # background class is SUPPOSED to be on public — if that lit the
            # banner it would be on permanently and he would stop reading it,
            # including the one time trade traffic really did fall off.
            if (is_trade and not is_send and url != TRADE_RPC_URL
                    and not _rpc_state["on_fallback"]):
                _rpc_state.update(on_fallback=True, since=time.time(),
                                  failovers=_rpc_state["failovers"] + 1)
                print(f"[RPC] *** TRADE READS FELL OFF {_rpc_host(TRADE_RPC_URL)}"
                      f" -> {_rpc_host(url)} ***")
            if url == PUBLIC_RPC and is_send:
                print("[RPC] *** SEND WENT OUT OVER PUBLIC RPC *** "
                      "unstaked — confirm before assuming it landed")
            return out["result"]
        except Exception as e:
            last = e
            _rpc_state["last_error"] = str(e)[:120]
            # Transport-level failure (timeout, conn reset, 5xx) -> bench it. A
            # RuntimeError here is the node ANSWERING with a JSON-RPC error,
            # which says nothing about its health, so it never cools.
            if _coolable(url) and isinstance(e, requests.exceptions.RequestException):
                _rpc_cool[url] = time.time() + RPC_COOL_S
            # UNKNOWN outcome (timeout / connection): a send may already be in
            # flight, so never re-broadcast it on another endpoint.
            if is_send and not isinstance(e, RuntimeError):
                raise
            if not more:
                raise
    raise last if last else RuntimeError("rpc: no endpoint reachable")


def sol_balance():
    # confirmed, not the finalized default — get_holdings() calls this, and a
    # SOL figure 13s stale next to live token rows just looks broken after a buy.
    return rpc("getBalance", [PUBKEY, {"commitment": "confirmed"}],
               trade=True)["value"] / 1e9


def token_balance(mint):
    """Return (raw_amount:int, ui_amount:float, decimals:int).

    trade=True: api_sell turns this raw amount straight into the swap size, so
    it must not come off a background peer sitting behind the trade node — a
    stale-high number gets the sell rejected outright. See rpc()."""
    # commitment=confirmed (2026-08-09). The JSON-RPC DEFAULT is "finalized",
    # measured ~32 slots / ~13s behind confirmed. confirm() returns the instant
    # it sees confirmationStatus=="confirmed", so for those 13s a just-filled
    # buy was real, on-chain, and invisible here — and because api_sell sizes
    # the swap from THIS call, a sell in that window died on "no balance to
    # sell". That is the whole "I can't sell it yet while it dumps" window.
    #
    # A confirmed slot can in principle be dropped, which would make this
    # balance stale-high and get the swap rejected. That is rare; 13 blind
    # seconds on a fresh position is not.
    res = rpc("getTokenAccountsByOwner",
              [PUBKEY, {"mint": mint},
               {"encoding": "jsonParsed", "commitment": "confirmed"}], trade=True)
    raw, ui, dec = 0, 0.0, None
    for acc in res.get("value", []):
        amt = acc["account"]["data"]["parsed"]["info"]["tokenAmount"]
        raw += int(amt["amount"])
        ui += float(amt["uiAmount"] or 0)
        dec = amt["decimals"]
    return raw, ui, dec


def _intel_from(t):
    """Meme-trading intel from a Jupiter tokens/v2 entry."""
    def win(s):
        s = s or {}
        return {"pc": s.get("priceChange"),
                "bv": s.get("buyVolume") or 0, "sv": s.get("sellVolume") or 0,
                "nb": s.get("numNetBuyers"), "traders": s.get("numTraders")}
    audit = t.get("audit") or {}
    return {
        "mcap": t.get("mcap"), "liquidity": t.get("liquidity"),
        "holders": t.get("holderCount"),
        "createdAt": t.get("createdAt")
                     or (t.get("firstPool") or {}).get("createdAt"),
        "launchpad": t.get("launchpad"), "verified": bool(t.get("isVerified")),
        "organic": t.get("organicScoreLabel"),
        "top10pct": audit.get("topHoldersPercentage"),
        "dev_pct": audit.get("devBalancePercentage"),
        "mint_off": audit.get("mintAuthorityDisabled"),
        "freeze_off": audit.get("freezeAuthorityDisabled"),
        "stats": {"5m": win(t.get("stats5m")), "1h": win(t.get("stats1h")),
                  "6h": win(t.get("stats6h")), "24h": win(t.get("stats24h"))},
    }


_meta_cache = {}                 # mint -> (ts, meta dict)
_meta_cache_lock = threading.Lock()
META_TTL = float(os.environ.get("META_TTL", "8"))


def token_meta(mint):
    """symbol/name/decimals/usdPrice/intel via Jupiter, decimals fallback via RPC.

    CACHED, and it has to be. This makes TWO Jupiter calls (tokens/v2/search +
    price/v3) and /api/token/<mint> calls it on every request, while the trade
    box UI ticks once a SECOND -- 2 req/s = ~120/min against a 60/min ceiling.
    The trade box was single-handedly eating the whole rate limit and then
    reporting that it was rate limited.

    It was invisible for the same reason it was expensive: both calls swallow
    their exceptions, so the 429s never reached /api/pxdiag (which only
    instruments the price-batch path) and the feed looked healthy while the
    budget was gone. Failures are counted now.

    None of this data moves second-to-second -- symbol, name, decimals, icon,
    liquidity intel. An 8s TTL is invisible in the UI and cuts this ~90%.
    """
    now = time.time()
    with _meta_cache_lock:
        hit = _meta_cache.get(mint)
        if hit and now - hit[0] < META_TTL:
            return dict(hit[1])          # copy: callers mutate (meta["social"])

    meta = {"symbol": mint[:4] + "…", "name": "Unknown", "decimals": None,
            "usdPrice": None, "intel": None, "icon": None, "dev": None, "tags": None}
    try:
        r = _JUP_HTTP.get(f"{JUP}/tokens/v2/search", params={"query": mint}, timeout=10)
        if r.status_code == 429:
            _px_fail.update(n=_px_fail["n"] + 1, at=time.time(),
                            last="token_meta search: HTTP 429")
        for t in r.json() or []:
            if t.get("id") == mint or t.get("address") == mint:
                meta["symbol"] = t.get("symbol") or meta["symbol"]
                meta["name"] = t.get("name") or meta["name"]
                meta["decimals"] = t.get("decimals")
                meta["usdPrice"] = t.get("usdPrice")
                meta["icon"] = t.get("icon")
                meta["dev"] = t.get("dev")
                meta["tags"] = t.get("tags")
                meta["intel"] = _intel_from(t)
                break
    except Exception:
        pass
    try:
        r = _JUP_HTTP.get(f"{JUP}/price/v3", params={"ids": mint}, timeout=10)
        if r.status_code == 429:
            _px_fail.update(n=_px_fail["n"] + 1, at=time.time(),
                            last="token_meta price: HTTP 429")
        p = (r.json() or {}).get(mint)
        if p and p.get("usdPrice") is not None:
            meta["usdPrice"] = p["usdPrice"]
    except Exception:
        pass
    if meta["decimals"] is None:
        try:
            meta["decimals"] = rpc("getTokenSupply", [mint], trade=True)["value"]["decimals"]
        except Exception:
            pass
    # Cache even a partial result: a miss that fell back to defaults still cost
    # two requests, and retrying it every 1s tick is exactly the loop this fixes.
    with _meta_cache_lock:
        _meta_cache[mint] = (time.time(), dict(meta))
    return meta


def dex_social(mint):
    """Logo + socials/websites from Dexscreener (Jupiter's data has no socials).
    Pooled _HTTP so we don't pay a fresh TLS handshake per token load."""
    out = {"imageUrl": None, "header": None, "websites": [], "socials": []}
    try:
        r = _HTTP.get(f"https://api.dexscreener.com/latest/dex/tokens/{mint}", timeout=8)
        pairs = (r.json() or {}).get("pairs") or []
        if pairs:
            info = pairs[0].get("info") or {}
            out["imageUrl"] = info.get("imageUrl")
            out["header"] = info.get("header")
            out["websites"] = info.get("websites") or []
            out["socials"] = info.get("socials") or []
    except Exception:
        pass
    return out


# ---------------- trade log + PnL ----------------
TRADES_PATH = os.path.join(HERE, "trades.jsonl")
EQUITY_PATH = os.path.join(HERE, "equity.jsonl")

_append_lock = threading.Lock()

def _append_jsonl(path, rec):
    # Locked: background confirm threads (wait=False sniper buys) and the Flask
    # request thread can now both write the trade journal at once.
    line = json.dumps(rec) + "\n"
    with _append_lock, open(path, "a", encoding="utf-8") as f:
        f.write(line)

def _read_jsonl(path):
    out = []
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except Exception:
                    pass
    return out


def trade_stats(wallet):
    """Average-cost PnL (in SOL) from trades this app executed."""
    trades = [t for t in _read_jsonl(TRADES_PATH)
              if t.get("wallet") == wallet and t.get("status") == "confirmed"]
    pos = {}      # mint -> {"raw": tokens held, "cost": SOL spent on them}
    per = {}      # mint -> aggregates
    events = []   # realized-PnL curve points
    realized = wins = sells = 0
    for t in sorted(trades, key=lambda x: x["ts"]):
        p = pos.setdefault(t["mint"], {"raw": 0, "cost": 0.0})
        a = per.setdefault(t["mint"], {"buys": 0, "sells": 0, "realized": 0.0})
        if t["side"] == "buy":
            p["raw"] += t["token_raw"]
            p["cost"] += t["sol"]
            a["buys"] += 1
        else:
            # tokens sold beyond what we bought here carry zero basis
            matched = min(t["token_raw"], p["raw"])
            avg = (p["cost"] / p["raw"]) if p["raw"] > 0 else 0.0
            basis = avg * matched
            pnl = t["sol"] - basis
            p["raw"] = max(0, p["raw"] - t["token_raw"])
            p["cost"] = max(0.0, p["cost"] - basis)
            a["sells"] += 1
            a["realized"] += pnl
            realized += pnl
            sells += 1
            wins += 1 if pnl > 0 else 0
            events.append({"ts": t["ts"], "cum": round(realized, 6),
                           "pnl": round(pnl, 6), "mint": t["mint"]})
    return {"per": per, "open": pos, "events": events,
            "realized": realized, "sells": sells, "count": len(trades),
            "win_rate": round(wins / sells * 100, 1) if sells else None}


# ---------------- portfolio ----------------
_sym_cache = {}

def _symbol_for(mint):
    if mint not in _sym_cache:
        meta = {"symbol": mint[:4] + "…", "name": ""}
        try:
            r = _JUP_HTTP.get(f"{JUP}/tokens/v2/search",
                             params={"query": mint}, timeout=8)
            for t in r.json() or []:
                if t.get("id") == mint or t.get("address") == mint:
                    meta = {"symbol": t.get("symbol") or meta["symbol"],
                            "name": t.get("name") or ""}
                    break
        except Exception:
            pass
        _sym_cache[mint] = meta
    return _sym_cache[mint]


_px_fail = {"n": 0, "last": None, "at": 0.0, "status": None}
_px_cache = {}            # mint -> (fetched_at, usdPrice)
_px_cache_lock = threading.Lock()
_px_cooldown_until = 0.0  # set on 429; price polling stands down until then
# 8.0, not 4.0. price/v3 is a CACHED AGGREGATE with a ~6s refresh floor (BONK
# measured unchanged for 40s), so a 4s TTL spent roughly a third to a half of
# the request budget re-fetching numbers that could not have moved. Under the
# 30-60 RPM ceiling that budget is the same one BUY/SELL quotes draw from, so
# the waste was directly costing fills. Do not lower this to "get fresher
# prices" - below ~6s there is no fresher price to get, only more 429s.
PX_TTL = 8.0              # a memecoin price moves, but not 3x per second
PX_COOLDOWN_S = 20.0
PX_STALE_MAX = 90.0       # serve a cached price this old rather than nothing

# How often a token we never bought (dust, airdropped spam) gets re-priced.
# See the provenance split in get_holdings(). Anything with a cost basis or a
# buy in the last 6h is exempt and priced every sweep.
DUST_RECHECK_S = float(os.environ.get("DUST_RECHECK_S", "600"))
_dust_checked = {}        # mint -> last time it was included in a price batch

def _batch_prices(mints, allow_stale=True):
    """{mint: usdPrice}, cached and rate-limit aware.

    NEVER FAIL SILENTLY HERE. This one function feeds the holdings panel, the
    trade box, and the TP/SL executor's entire notion of price. A bare
    `except: pass` meant a dead price feed was indistinguishable from a flat
    market -- SOL price came back null, every PnL went blank, and nothing said
    why. Failures are counted and surfaced through /api/pxdiag.

    RATE LIMITING (added 2026-08-09, after lite-api started 429ing): Jupiter's
    free tier throttles the whole process, and price polling is by far the
    chattiest caller -- so an unthrottled price loop does not just blank the
    PnL, it burns the budget that BUY and SELL quotes need. A failed quote is a
    missed trade; a slightly stale price is not. Hence:

      * a short shared TTL cache, so N callers wanting the same mint in the
        same few seconds cost one request;
      * only cache MISSES are requested, never the whole set again;
      * on a 429 the price path stands down for PX_COOLDOWN_S and serves cached
        values, rather than hammering the limit that trades also live under.

    Quotes deliberately do NOT go through any of this. They are the thing worth
    spending the budget on.
    """
    global _px_cooldown_until
    now = time.time()
    out, misses = {}, []
    with _px_cache_lock:
        for m in mints:
            hit = _px_cache.get(m)
            if hit and now - hit[0] < PX_TTL:
                out[m] = hit[1]
            else:
                misses.append(m)
    if misses and now < _px_cooldown_until:
        # throttled: fall back to anything recent enough to still be honest
        if allow_stale:
            with _px_cache_lock:
                for m in misses:
                    hit = _px_cache.get(m)
                    if hit and now - hit[0] < PX_STALE_MAX:
                        out[m] = hit[1]
        return out
    for i in range(0, len(misses), 50):
        chunk = misses[i:i + 50]
        try:
            r = _JUP_HTTP.get(f"{JUP}/price/v3",
                             params={"ids": ",".join(chunk)}, timeout=10)
            _px_fail["status"] = r.status_code
            if r.status_code == 429:
                _px_cooldown_until = time.time() + PX_COOLDOWN_S
                raise RuntimeError(
                    "HTTP 429 rate limit — price polling standing down %.0fs"
                    % PX_COOLDOWN_S)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:160]}")
            got = r.json() or {}
            stamp = time.time()
            with _px_cache_lock:
                for m, p in got.items():
                    if p and p.get("usdPrice") is not None:
                        v = float(p["usdPrice"])
                        out[m] = v
                        _px_cache[m] = (stamp, v)
        except Exception as e:
            _px_fail.update(n=_px_fail["n"] + 1, last=str(e)[:200],
                            at=time.time())
            print(f"[PRICE] batch failed ({len(chunk)} mints): {str(e)[:200]}")
            if allow_stale:
                with _px_cache_lock:
                    for m in chunk:
                        hit = _px_cache.get(m)
                        if hit and time.time() - hit[0] < PX_STALE_MAX:
                            out[m] = hit[1]
    return out


def _batch_prices_ts(mints, allow_stale=True):
    """{mint: (usdPrice, origin_ts)} — the same fetch, but each value carries
    WHEN IT ENTERED THE CACHE rather than when this call happened.

    The TP/SL executor needs the difference. _batch_prices serves values up to
    PX_TTL old normally and up to PX_STALE_MAX (90s) old while a 429 cooldown is
    in force; the executor was stamping all of them with its own read time, so
    its price-age gate saw 0.0s no matter how stale the number really was.
    Reading the stamps back out of the cache is the honest lower bound on
    staleness -- it is when WE learned the price, which is the most anything
    here can know.
    """
    _batch_prices(mints, allow_stale)          # populate/refresh, then stamp
    now = time.time()
    out = {}
    with _px_cache_lock:
        for m in mints:
            hit = _px_cache.get(m)
            if hit and now - hit[0] < PX_STALE_MAX:
                out[m] = (hit[1], hit[0])
    return out


@app.route("/api/pxdiag")
def api_pxdiag():
    """Why is there no price? Answers it directly instead of by inference."""
    sol = _batch_prices([SOL_MINT])
    return jsonify({"ok": True, "sol_price": sol.get(SOL_MINT),
                    "jup_base": JUP, "fails": _px_fail["n"],
                    "last_error": _px_fail["last"],
                    "last_status": _px_fail["status"],
                    "cached_mints": len(_px_cache),
                    "cooldown_s_left": max(0.0, round(_px_cooldown_until - time.time(), 1)),
                    "last_fail_age_s": (round(time.time() - _px_fail["at"], 1)
                                        if _px_fail["at"] else None)})


def _recent_buy_mints(max_age_s=21600, tail_bytes=262144):
    """Mints this wallet bought in the last 6h, from our own trade journal.

    Exists because commitment was only HALF of the invisible-position problem.
    A token bought seconds ago usually has no Jupiter price yet, so
    `usd = ui * 0 = 0` and the `usd > min_usd` filter dropped it — owned,
    confirmed, and still unsellable. Setting MIN_HOLDING_USD=0 does not fix
    that either, since `0 > 0` is false.

    Keyed off what we BOUGHT rather than "anything unpriced": a wallet
    accumulates spam airdrops, which are also unpriced, and flooding the sell
    list with scam tokens is its own hazard. Tail-read and bounded — the
    journal grows forever and this sits on the holdings path.

    The tail-read itself now lives in _recent_mints() so the rent sweeper,
    which needs the same window over BOTH sides, shares one implementation.
    """
    return _recent_mints(max_age_s, side="buy", tail_bytes=tail_bytes)


def cost_basis(tail_bytes=1048576):
    """{mint: {"sol": cost_of_what_is_still_held, "raw": raw_tokens}} from our
    own trade journal — the entry price TP/SL is measured against.

    Running average cost, not "the last buy": a sell reduces cost in proportion
    to the raw tokens it removed, and the lot RESETS once raw hits zero. Without
    that reset, a token bought, fully sold, and bought again months later would
    be priced against the old entry and show a nonsense PnL — and TP/SL would
    fire off that nonsense.

    Only status=="confirmed" rows count. The journal is full of "failed" buys
    (three in a row on 3CK6eMQw alone); counting those would inflate cost and
    make a winner look like a loser. "timeout" rows are excluded too, which can
    under-count a buy that did in fact land — deliberately the safer direction,
    since it understates cost rather than inventing it.

    Works in RAW units so no decimals lookup is needed anywhere.

    The arithmetic itself now lives in tpsl.basis_from_rows() so the executor
    and the stress harness (_tpsl_test.py, 101 checks + mutation runs) share ONE
    implementation with this panel -- the number you auto-sell against must be
    the same number you are looking at. This function is just the tail-read.

    TAIL LIMIT: at 1MB this holds roughly 3,000 rows; the journal is ~332KB /
    1,054 rows as of 2026-08-09, so nothing is being truncated yet. When it does
    start to bite, the oldest lots lose their buys and go `incomplete` -- which
    blocks auto-selling rather than mispricing it, but it will need a rollover.
    """
    rows = []
    try:
        size = os.path.getsize(TRADES_PATH)
        with open(TRADES_PATH, "rb") as f:
            if size > tail_bytes:
                f.seek(size - tail_bytes)
                f.readline()
            for line in f:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
    except Exception:
        return {}
    return _tpsl_mod.basis_from_rows(rows, PUBKEY)


def get_holdings(min_usd=MIN_HOLDING_USD):
    """Wallet tokens worth more than min_usd, plus anything bought in the last
    6h regardless of price — see _recent_buy_mints."""
    require_wallet()
    amounts = {}  # mint -> ui amount
    raws = {}     # mint -> raw amount (cost-basis math)
    for prog in (TOKEN_PROG, TOKEN_2022_PROG):
        # the account lookup itself — trade class, but only ever reached from a
        # live page: nothing on the server polls /api/holdings on a timer.
        # commitment=confirmed for the same reason as token_balance() above:
        # the finalized default hid a just-filled buy for ~13s.
        res = rpc("getTokenAccountsByOwner",
                  [PUBKEY, {"programId": prog},
                   {"encoding": "jsonParsed", "commitment": "confirmed"}],
                  trade=True)
        for acc in res.get("value", []):
            info = acc["account"]["data"]["parsed"]["info"]
            ui = float(info["tokenAmount"]["uiAmount"] or 0)
            if ui > 0:
                m = info["mint"]
                amounts[m] = amounts.get(m, 0) + ui
                # raw too: cost_basis() works in raw units, so PnL never needs
                # a decimals lookup to line the two sides up.
                raws[m] = raws.get(m, 0) + int(info["tokenAmount"]["amount"] or 0)
    recent = _recent_buy_mints()
    basis = cost_basis()
    # DON'T PRICE THE SPAM EVERY SWEEP.
    #
    # This used to be _batch_prices(list(amounts) + [SOL_MINT]) -- every token
    # with a nonzero balance, which on this wallet meant 10 mints priced every
    # 60s to surface ONE position. The rest is dust below MIN_HOLDING_USD and
    # airdropped spam, and it shares the Jupiter budget with the swap quotes,
    # which is what a failing SELL is competing against.
    #
    # There is a chicken-and-egg here -- you need a price to know a mint is
    # dust -- so the split is by PROVENANCE, not by value:
    #
    #   always   anything we have a cost basis in (we bought it and still hold
    #            it), anything bought in the last 6h, and SOL. These are the
    #            only mints that can become a position, so a real holding is
    #            never delayed by this.
    #   slowly   everything else, re-checked every DUST_RECHECK_S. Spam cannot
    #            become a position; an unbought token that somehow moons still
    #            appears, just within 10 minutes instead of 60 seconds.
    #
    # Priced-slowly mints keep their cached value in between (PX_STALE_MAX
    # bounds how old that can be), so they still render -- they just stop
    # costing a request per sweep.
    now_ts = time.time()
    price_set, deferred = [SOL_MINT], []
    for m in amounts:
        if m in basis or m in recent:
            price_set.append(m)
        elif now_ts - _dust_checked.get(m, 0.0) >= DUST_RECHECK_S:
            price_set.append(m)
            _dust_checked[m] = now_ts
        else:
            deferred.append(m)
    prices = _batch_prices(price_set)
    if deferred:
        # cached-but-not-refreshed, so a deferred mint still shows a number
        # rather than vanishing from the panel between re-checks.
        with _px_cache_lock:
            for m in deferred:
                hit = _px_cache.get(m)
                if hit and now_ts - hit[0] < PX_STALE_MAX:
                    prices.setdefault(m, hit[1])
    sol_px = prices.get(SOL_MINT) or 0.0
    rows = []
    for m, ui in amounts.items():
        px = prices.get(m) or 0.0
        usd = ui * px
        # "usd" stays a NUMBER even when unpriced — both UIs call .toFixed(2)
        # on it unguarded, so a null here would throw and blank the whole panel.
        # `priced` is the flag they render off instead.
        if usd > min_usd or m in recent:
            row = {"mint": m, "amount": ui,
                   # base units, not the UI figure: a quote has to be asked in
                   # the same units the swap program takes (tpsl._qty)
                   "raw": raws.get(m, 0),
                   "usdPrice": prices.get(m), "usd": round(usd, 2),
                   "priced": bool(px), "cost_sol": None, "pnl_pct": None,
                   "basis": "none"}
            # PnL in SOL terms, which is how the position was actually sized.
            # Both sides scale by the raw tokens still held, so a partial sell
            # leaves the percentage correct instead of halving it.
            #
            # CORROBORATE THE BASIS AGAINST CHAIN FIRST (2026-08-09). The trade
            # journal is NOT a reliable ledger of fills: buys with wait=False
            # journal from a background confirm thread, so every restart during
            # that window drops the row for good, and sniper buys journal
            # through the position store instead. Measured on a live wallet:
            # 43,544,894,749 raw held vs 12,726,126,689 journaled — the basis
            # knew about under a third of the position and reported +133% on
            # what was really a fraction of that.
            #
            # A PnL that is merely unknown costs nothing. A PnL that is
            # confidently wrong fires TP/SL at the wrong price, so holding more
            # than the journal explains means we publish NO number at all.
            #
            # As of 2026-08-09 the grade also carries FILL PROVENANCE, and only
            # "ok" (chain-verified fills that the chain balance agrees with) may
            # be auto-sold. "estimate" means the arithmetic is sound but was
            # built on Jupiter's pre-trade quotes -- the median 2.6% drift that
            # this whole rework exists to remove. It still shows a PnL and still
            # raises an alert; it just never sells by itself.
            grade, cost = _tpsl_mod.classify_basis(basis.get(m), raws.get(m, 0))
            row["basis"] = grade
            if cost and px and sol_px:
                row["cost_sol"] = round(cost, 6)
                row["pnl_pct"] = round((usd / sol_px / cost - 1) * 100, 1)
            elif grade in ("ok", "estimate"):
                # priced=False -> we know the cost but not the value. Say so
                # rather than shipping a grade that implies a live number.
                row["cost_sol"] = round(cost, 6) if cost else None
            rows.append(row)
    # symbol lookups only for rows that made the cut, in parallel
    from concurrent.futures import ThreadPoolExecutor
    if rows:
        with ThreadPoolExecutor(max_workers=8) as ex:
            metas = list(ex.map(lambda r: _symbol_for(r["mint"]), rows))
        for row, meta in zip(rows, metas):
            row.update(meta)
    rows.sort(key=lambda r: -r["usd"])
    _prewarm_pools([r["mint"] for r in rows[:5]])
    sol = sol_balance()
    return {"holdings": rows, "sol_balance": sol,
            "sol_price": prices.get(SOL_MINT),
            "sol_usd": round(sol * prices.get(SOL_MINT, 0), 2),
            "total_usd": round(sum(r["usd"] for r in rows), 2)}


# ---------------- Jito (MEV protect) ----------------
_tip_cache = {"t": 0.0, "v": 10000}

def jito_tip(max_age=60):
    """Current landed-tip 50th percentile in lamports, cached 60s, clamped.
    _tip_prewarm_loop refreshes ahead of expiry (max_age=45) so trades never
    pay the tip-floor fetch inline."""
    if JITO_TIP_LAMPORTS > 0:
        return JITO_TIP_LAMPORTS
    if time.time() - _tip_cache["t"] > max_age:
        try:
            r = _HTTP.get(JITO_TIP_FLOOR, timeout=5).json()
            sol = r[0]["landed_tips_50th_percentile"]
            _tip_cache["v"] = max(JITO_TIP_MIN, min(JITO_TIP_MAX, int(sol * 1e9)))
            _tip_cache["t"] = time.time()
        except Exception:
            pass  # keep last known tip
    return _tip_cache["v"]


def _tip_prewarm_loop():
    while True:
        try:
            jito_tip(max_age=45)
        except Exception:
            pass
        time.sleep(15)

threading.Thread(target=_tip_prewarm_loop, daemon=True).start()


def jito_send(signed_b64):
    r = _HTTP.post(JITO_SEND, json={
        "jsonrpc": "2.0", "id": 1, "method": "sendTransaction",
        "params": [signed_b64, {"encoding": "base64"}],
    }, timeout=10)
    out = r.json()
    if "error" in out:
        raise RuntimeError(f"jito send: {out['error'].get('message')}")
    return out["result"]


# ---------------- Jupiter swap ----------------
def _jup_json(r, what):
    """Parse a Jupiter response, or raise something a human can act on.

    `r.json()` on a non-JSON body raises "Expecting value: line 1 column 1
    (char 0)", which is what a rate-limited BUY actually showed in the trade box
    on 2026-08-09 -- a message that names neither the cause nor the fix. Jupiter
    returns 429 as PLAIN TEXT, so the JSON decoder is the first thing to trip
    and the real reason never surfaces. Read the status first, always.
    """
    if r.status_code == 429:
        raise RuntimeError(
            "Jupiter rate limit (429) — the free lite-api tier is throttling "
            "us. Nothing was sent and no SOL moved. Wait a few seconds and "
            "retry; see /api/pxdiag for how often this is happening.")
    try:
        return r.json()
    except ValueError:
        body = (r.text or "").strip()[:160] or "(empty body)"
        raise RuntimeError(
            f"{what}: Jupiter returned HTTP {r.status_code} with a non-JSON "
            f"body — {body}")


def jup_quote(input_mint, output_mint, amount, slippage_bps, timeout=15):
    # timeout defaults to the TRADE budget: a quote that backs a real swap is
    # worth waiting for, because losing it loses the trade. Pollers that merely
    # want a fresh price (TP/SL triggers) must pass something short -- a trigger
    # quote that misses its tick is worthless and holding the slot only stacks
    # up duplicate requests behind it.
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(int(amount)),
        "slippageBps": slippage_bps,
        "restrictIntermediateTokens": "true",
    }
    r = _JUP_HTTP.get(f"{JUP}/swap/v1/quote", params=params, timeout=timeout)
    q = _jup_json(r, "quote failed")
    # stonk.fun (Raydium LaunchLab) coins are paired with tokenized stocks,
    # DOGE, pre-IPO tokens or other stonk coins, never SOL, so every route is
    # SOL -> that asset -> coin. restrictIntermediateTokens keeps Jupiter to
    # well-known middle hops; an obscure pairing asset can fall outside that
    # list. On a no-route, ask once more without the restriction. Costs one
    # extra quote, only on failure.
    if (r.status_code != 200 or "error" in q) and \
            any(k in str(q.get("error") if isinstance(q, dict) else q).lower()
                for k in ("no route", "not tradable", "could not find")):
        params["restrictIntermediateTokens"] = "false"
        r2 = _JUP_HTTP.get(f"{JUP}/swap/v1/quote", params=params, timeout=timeout)
        q2 = _jup_json(r2, "quote failed")
        if r2.status_code == 200 and "error" not in q2:
            return q2
    if r.status_code != 200 or "error" in q:
        err = q.get("error") if isinstance(q, dict) else q
        # The common ones, said plainly. "quote failed: {...}" told you nothing.
        msg = str(err)
        if "not tradable" in msg.lower() or "no route" in msg.lower():
            msg += (" — no routable pool for this pair yet. A token seconds old "
                    "often has no route until its pool is indexed, and a "
                    "stonk.fun / launchpad coin that JUST graduated has no route "
                    "for a few seconds while its new pool is indexed. Retry.")
        raise RuntimeError(f"quote failed: {msg}")
    return q


def resolve_fee_mode(fee_mode=None, mev_protect=True):
    """fee_mode wins when given; otherwise derive the legacy boolean's meaning.
    Unset must map exactly onto the old behaviour or every existing caller
    (api_buy, the PumpPortal path, execute_swap defaults) changes fee silently."""
    if fee_mode in FEE_MODES:
        return fee_mode
    return "mev" if mev_protect else "fast"


def jup_swap_tx(quote, mev_protect=True, dynamic_slippage=False, fee_mode=None):
    """Build swap tx. mev: Jito tip. fast: high priority fee. eco: cheap priority fee."""
    mode = resolve_fee_mode(fee_mode, mev_protect)
    tip = jito_tip() if mode == "mev" else None
    if mode == "mev":
        prio = {"jitoTipLamports": tip}
    elif mode == "eco":
        prio = {"priorityLevelWithMaxLamports": {
            "maxLamports": ECO_PRIORITY_LAMPORTS,
            "priorityLevel": ECO_PRIORITY_LEVEL,
        }}
    else:
        prio = {"priorityLevelWithMaxLamports": {
            "maxLamports": MAX_PRIORITY_LAMPORTS,
            "priorityLevel": "high",
        }}
    body = {
        "quoteResponse": quote,
        "userPublicKey": PUBKEY,
        "wrapAndUnwrapSol": True,
        "dynamicComputeUnitLimit": True,
        "dynamicSlippage": bool(dynamic_slippage),
        "prioritizationFeeLamports": prio,
    }
    # "Missing token program for <mint>": the quote found a route, but
    # Jupiter's swap builder has not yet recorded whether this brand-new mint
    # is a Token or Token-2022 coin. It catches up within seconds, so retry
    # the build (same quote) a few times before giving up.
    for attempt, wait in enumerate((0, 0.6, 1.0, 1.5)):
        if wait:
            time.sleep(wait)
        r = _JUP_HTTP.post(f"{JUP}/swap/v1/swap", json=body, timeout=15)
        out = _jup_json(r, "swap build failed")
        err = str(out.get("error") if isinstance(out, dict) else out)
        if not (r.status_code != 200 and "missing token program" in err.lower()):
            break
    if r.status_code != 200 or "swapTransaction" not in out:
        if "missing token program" in err.lower():
            raise RuntimeError(
                "swap build failed: Jupiter hasn't indexed this brand-new token "
                "yet (missing token program), even after ~3s of retries. "
                "Nothing was signed or sent. Try again in a few seconds.")
        raise RuntimeError("swap build failed: %s (nothing was signed or sent)"
                           % (out.get("error") if isinstance(out, dict) else out))
    return out["swapTransaction"], tip


def sign_and_send(swap_tx_b64, mev_protect=True):
    tx = VersionedTransaction.from_bytes(base64.b64decode(swap_tx_b64))
    signed = VersionedTransaction(tx.message, [KEYPAIR])
    signed_b64 = base64.b64encode(bytes(signed)).decode()
    if mev_protect:
        try:
            return jito_send(signed_b64), True
        except Exception:
            pass  # Jito down/rate-limited -> public RPC still lands the trade
    sig = rpc("sendTransaction", [
        signed_b64,
        {"encoding": "base64", "skipPreflight": True, "maxRetries": 3},
    ])
    return sig, False


def confirm(sig, timeout_s=45):
    # Poll at 0.5s (Solana slots are ~0.4s) so we notice the landing fast instead
    # of sitting up to 1.5s past it. Each poll is a pooled ~160ms round-trip now.
    # A transient RPC hiccup (e.g. a public-RPC 429) must not abort the loop — it
    # would otherwise throw straight out and mislabel a landed tx, so swallow it
    # and keep polling until the deadline.
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            st = rpc("getSignatureStatuses", [[sig]], trade=True)["value"][0]
        except Exception:
            st = None
        if st:
            if st.get("err"):
                return "failed", st["err"]
            if st.get("confirmationStatus") in ("confirmed", "finalized"):
                return "confirmed", None
        time.sleep(0.3)   # slots are ~0.4s; 0.5s overshot a landed tx by half a slot
    return "timeout", None


# ---------------- per-mint sell interlock ----------------
# api_sell sizes the swap from a LIVE token_balance() read, so two sells racing
# on one mint both size off the pre-first-sell balance: the second either gets
# rejected by the token program or, if the first has partially settled, sells a
# different fraction than the button said. Manual clicks alone made this rare
# (the pop-out's global `busy` flag serialised them client-side); an autonomous
# executor firing while a click is in flight makes it routine.
#
# Every sell path -- api_sell AND tpsl -- takes the mint's lock around
# read-balance + swap, so the sizing read can never straddle another sell.
_mint_locks = {}
_mint_locks_guard = threading.Lock()

def mint_lock(mint):
    with _mint_locks_guard:
        return _mint_locks.setdefault(mint, threading.Lock())


def _chain_fill(sig, mint, side, tries=4):
    """What the transaction ACTUALLY moved, read off the landed tx.

    The trade journal used to record `quote.outAmount` -- Jupiter's estimate,
    computed BEFORE the swap executed. Audited over the real journal on
    2026-08-09 (1054 rows, 192 mints) that estimate drifts a median 2.6% from
    the fill, and 57.3% of mints ended up with a cost basis that was provably
    wrong at some point. A TP/SL measured against a 2.6%-wrong entry is not a
    TP/SL, so the executor refuses to auto-sell anything that is not sourced
    from here (tpsl.basis_from_rows -> `verified`).

    Only the ESTIMATED leg of each side needs correcting:
      buy  — we specify the lamports in, so `sol` is already exact; the tokens
             received are the estimate. Read the token delta.
      sell — we specify the tokens in, so `token_raw` is already exact; the SOL
             received is the estimate. Read the native delta.

    Native delta has the network fee added back (it is not a worse price, it is
    a separate cost) but is left net of any Jito tip and ATA rent, which really
    did come out of the proceeds.

    One trade-class RPC per fill, off the hot path -- never on a timer. Returns
    None on any doubt at all; the caller then journals the quote and marks the
    row unverified, which costs an alert, not money.
    """
    for i in range(tries):
        try:
            tx = rpc("getTransaction",
                     [sig, {"encoding": "jsonParsed",
                            "maxSupportedTransactionVersion": 0,
                            "commitment": "confirmed"}], trade=True)
        except Exception:
            tx = None
        if tx and tx.get("meta"):
            break
        time.sleep(0.6)          # the tx is confirmed but not yet indexed
    else:
        return None
    meta = tx.get("meta") or {}
    if meta.get("err"):
        return None
    try:
        if side == "buy":
            pre = post = 0
            for b in meta.get("preTokenBalances") or []:
                if b.get("owner") == PUBKEY and b.get("mint") == mint:
                    pre += int(b["uiTokenAmount"]["amount"])
            for b in meta.get("postTokenBalances") or []:
                if b.get("owner") == PUBKEY and b.get("mint") == mint:
                    post += int(b["uiTokenAmount"]["amount"])
            got = post - pre
            return {"token_raw": got} if got > 0 else None
        # sell: native lamports in minus out, fee added back
        keys = (tx.get("transaction") or {}).get("message", {}).get("accountKeys") or []
        if not keys:
            return None
        first = keys[0]
        payer = first.get("pubkey") if isinstance(first, dict) else first
        if payer != PUBKEY:                 # not the fee payer -> index 0 is not us
            return None
        pre_l = (meta.get("preBalances") or [None])[0]
        post_l = (meta.get("postBalances") or [None])[0]
        if pre_l is None or post_l is None:
            return None
        got = (post_l - pre_l) + int(meta.get("fee") or 0)
        return {"sol": got / 1e9} if got > 0 else None
    except Exception:
        return None


def execute_swap(input_mint, output_mint, amount_raw, slippage_bps,
                 mev_protect=True, dynamic_slippage=False, wait=True,
                 fee_mode=None, on_status=None):
    """Quote -> build -> sign+send -> confirm.

    wait=True  (default): block on confirm() and return the real confirmed/failed
               status. Used by the manual dashboard (api_buy/api_sell) whose "ok"
               flag and the PnL journal both key off status=="confirmed".
    wait=False: return the instant the tx is *sent* (status="submitted") and
               confirm in a background thread, which writes the trade-journal row
               with the true status once it lands. Used by the copy-sniper so its
               watcher thread is never frozen up to 45s waiting on confirmation
               (which would make it miss other launches).
    """
    t0 = time.time()
    mode = resolve_fee_mode(fee_mode, mev_protect)
    quote = jup_quote(input_mint, output_mint, amount_raw, slippage_bps)
    swap_tx, tip = jup_swap_tx(quote, mev_protect, dynamic_slippage, mode)
    # Only "mev" goes to the block engine — an eco tx carries a priority fee and
    # no tip instruction, so a Jito send would just be a bundle nobody picks up.
    sig, via_jito = sign_and_send(swap_tx, mode == "mev")
    send_ts = round(time.time(), 3)

    def _journal(status, err=None):
        try:  # trade journal for the PnL dashboard
            side = "buy" if input_mint == SOL_MINT else "sell"
            mint = output_mint if side == "buy" else input_mint
            sol = (amount_raw if side == "buy"
                   else int(quote.get("outAmount") or 0)) / 1e9
            token_raw = (int(quote.get("outAmount") or 0)
                         if side == "buy" else amount_raw)
            # Replace the estimated leg with what actually landed. Only a
            # confirmed tx has a fill to read; "failed"/"timeout" rows keep the
            # quote and stay marked unverified, which is what excludes them from
            # the basis anyway. See _chain_fill for why this matters.
            fill, drift = "quote", None
            if status == "confirmed":
                real = _chain_fill(sig, mint, side)
                if real:
                    if side == "buy" and real.get("token_raw"):
                        drift = real["token_raw"] / token_raw - 1 if token_raw else None
                        token_raw = real["token_raw"]
                    elif side == "sell" and real.get("sol"):
                        drift = real["sol"] / sol - 1 if sol else None
                        sol = real["sol"]
                    fill = "chain"
            _append_jsonl(TRADES_PATH, {
                "ts": send_ts, "wallet": PUBKEY, "side": side,
                "mint": mint, "sol": sol, "token_raw": token_raw,
                "sig": sig, "status": status, "fill": fill,
                # kept for observability: this is the number that was silently
                # wrong before, so it should be visible when it gets large.
                "quote_drift": round(drift, 5) if drift is not None else None,
                # WHY it did not land. This used to be dropped on the floor
                # (`_journal(confirm(sig)[0])`), so a swap that reverted for
                # slippage was journalled as a bare "failed" and the reason had
                # to be dug out of the chain by hand afterwards.
                "err": (json.dumps(err) if err else None),
            })
        except Exception:
            pass
        # A new/changed position must be protected within seconds, not at the
        # next slow holdings sweep. Safe to call even when the executor is
        # disarmed or absent.
        try:
            if TPSL is not None:
                TPSL.refresh_holdings("post-fill")
        except Exception:
            pass
        # Snapshot the market state and price action around this fill while it
        # still exists to be snapshotted. Enqueue only — the capture itself
        # runs on the recorder's own worker, so this never adds latency here.
        try:
            if CTX is not None and status == "confirmed":
                CTX.record_fill(sig=sig, mint=mint, side=side, ts=send_ts,
                                sol=sol, token_raw=token_raw,
                                symbol=(_sym_cache.get(mint) or {}).get("symbol"),
                                fee_mode=mode, status=status)
        except Exception:
            pass
        # Tell the caller how it actually ended. The TP/SL executor uses this to
        # un-latch a side whose sell reverted; without it a failed exit looked
        # exactly like a successful one and the position sat unprotected.
        try:
            if on_status is not None:
                on_status(status, err)
        except Exception:
            pass

    if wait:
        status, err = confirm(sig)
        _journal(status, err)
    else:
        # confirm off-thread; journal is written by the thread once it resolves,
        # so the row still lands exactly once with the true status (no duplicate).
        status, err = "submitted", None
        threading.Thread(
            target=lambda: _journal(*confirm(sig)), daemon=True
        ).start()

    return {
        "signature": sig,
        "status": status,
        "error": json.dumps(err) if err else None,
        "outAmount": quote.get("outAmount"),
        "priceImpactPct": quote.get("priceImpactPct"),
        "sent_via": "jito" if via_jito else "rpc",
        "fee_mode": mode,
        "tip_sol": round(tip / 1e9, 6) if tip else None,
        # what we ASKED for, not what landed — Jupiter's estimate is usually
        # well under the cap, so treat this as the ceiling for this trade.
        "fee_cap_sol": (round(tip / 1e9, 6) if mode == "mev" else
                        round((ECO_PRIORITY_LAMPORTS if mode == "eco"
                               else MAX_PRIORITY_LAMPORTS) / 1e9, 6)),
        "elapsed_s": round(time.time() - t0, 2),
        "solscan": f"https://solscan.io/tx/{sig}",
    }


# ---------------- copy-sniper (paper-first) ----------------
# Watches a target wallet's pump.fun token CREATIONS and opens a position on
# each new launch. Paper mode simulates the buy against real market-cap moves;
# live mode fires a real Jupiter buy (only once the token is routable), gated
# hard by per-snipe + daily caps and disarmed by default.
PUMP_API = "https://frontend-api-v3.pump.fun"
SNIPER_CFG_PATH = os.path.join(HERE, "sniper_config.json")
SNIPER_POS_PATH = os.path.join(HERE, "sniper_positions.jsonl")

DEFAULT_SNIPER_CFG = {
    "target": "SET_TARGET_WALLET_ADDRESS",
    "enabled": False,       # is the watcher running
    "live": False,          # False = paper, True = real SOL
    "sol_per_snipe": 0.03,
    # Snipe slippage %, decoupled from the dashboard's SLIPPAGE_BPS so widening
    # it for fresh curves never silently widens manual swaps too. Raised 10 -> 50
    # on 2026-07-16: pump.fun `Custom: 6002` (TooMuchSolRequired) was reverting
    # buys when rival snipers bought in the same block. NOTE this bounds BOTH
    # legs — on the buy it is adverse selection (you only get the worse fill when
    # the curve ran away), on the sell it is protective (a filled bad exit beats
    # a stranded token). It does NOT affect txs dropped before execution.
    "snipe_slippage_pct": 50.0,
    "tp_pct": 100,          # paper/live exit: take profit at +100%
    "sl_pct": 40,           # exit at -40%
    "timeout_min": 15,      # exit no matter what after N minutes
    # Competitor-style fast exit (copy-target sleeve ONLY): the dissected arb
    # wallet (2026-07-17) is fully flat within ~30-40s of entry, in chunks, no
    # take-profit holds — and our own 07-16 data showed TP holds are fantasy
    # (booked "+124%" while the wallet bled). Chunks avoid crater-ing the curve
    # with one 100% dump. Grad sleeve keeps the old TP/SL/timeout policy.
    "fast_exit": True,
    "fast_exit_start_s": 15,   # first chunk this many seconds after detection
    "fast_exit_gap_s": 10,     # subsequent chunks every N seconds
    "fast_exit_chunks": 3,     # flat after this many chunks (~35s total)
    "max_sol_per_day": 0.3,  # hard daily cap (live)
    "poll_secs": 30,        # HTTP reconciliation sweep only — the PumpPortal
                            # websocket is the primary (real-time) detection path
    "ws_enabled": True,     # PumpPortal wss new-token feed (~1s from block vs
                            # ~5-12s for frontend-api polling)
    "brain": True,          # shadow LLM judge on every detection (gates nothing)
    # second candidate sleeve: coins nearing bonding-curve graduation (paper,
    # judged with the same shadow brain; capped so the mark-to-market loop
    # never floods pump.fun with per-position price polls)
    # 2026-07-24: OFF. 623 paper closes in a week at a 23% hit rate, judged by a
    # brain already shown to have no edge, and no path to live — it was 78% of
    # all activity and contributed only noise. Re-enable only to A/B the exit
    # policy against the copy sleeve.
    "grad_enabled": False,
    "grad_min_progress": 85,   # % of curve bought
    "grad_poll_secs": 90,
    "grad_max_per_hour": 4,
    # measured median gap between the detection-event mcap we stamp as
    # `entry_mcap` and the curve state our buy actually lands in (n=22 live
    # fills, 2026-07-24). Used only for the shadow curve mark.
    "entry_slip_x": 1.58,
    # ---- pyramid scale-in (2026-07-28, pyramid.py) ----
    # GHOST ONLY. Simulates the probe -> cut-or-ladder -> tranche-out policy
    # reverse-engineered from 64hP97… (+77.5 SOL/day, 4,906 positions; see
    # SNIPER_TEARDOWN_64hP97.md) against the same marks the real book gets, and
    # journals both outcomes to pyramid_compare.jsonl. It executes nothing.
    # `pyramid_live` is deliberately separate from `live`: flipping the sniper
    # to live must never arm this arm too. Read pyramid.py's header before
    # promoting — the paper mark flatters a multi-leg book structurally.
    "pyramid_enabled": True,
    "pyramid_live": False,
    # ---- smart exits (2026-07-24, smart_exit.py) ----
    # True: the flow-evidence policy CLOSES paper positions; live positions keep
    # the fast-exit ladder and get smart_trigger_* stamps only. Either way every
    # position runs BOTH policies (the other as a ghost) -> exit_compare.jsonl,
    # scored by _smart_exit_report.py. Knobs (smart_flow_window_s, smart_whale_sol,
    # smart_flip_ratio, ...) default in smart_exit.DEFAULTS; override here.
    "smart_exit": True,
}

def _sniper_cfg_load():
    cfg = dict(DEFAULT_SNIPER_CFG)
    if os.path.exists(SNIPER_CFG_PATH):
        try:
            cfg.update(json.load(open(SNIPER_CFG_PATH, encoding="utf-8")))
        except Exception:
            pass
    return cfg

def _sniper_cfg_save(cfg):
    tmp = SNIPER_CFG_PATH + ".tmp"
    json.dump(cfg, open(tmp, "w", encoding="utf-8"), indent=1)
    os.replace(tmp, SNIPER_CFG_PATH)

sniper_cfg = _sniper_cfg_load()
_sniper = {
    "thread": None, "stop": False, "seen": set(),
    "positions": [], "started_ts": None, "last_error": None,
    "last_check": None, "day": None, "day_spent": 0.0,
    "grad_last_scan": 0.0, "grad_opens": [],
    "ws_thread": None, "ws_connected": False,
    "ws_last_event": None, "ws_detections": 0,
}
_sniper_lock = threading.Lock()


def _claim_new_mint(mint):
    """Atomic check-and-mark on the seen set, so the websocket feed and the
    reconciliation poller can never both open the same mint."""
    if not mint:
        return False
    with _sniper_lock:
        if mint in _sniper["seen"]:
            return False
        _sniper["seen"].add(mint)
        return True


# ---------------- LLM brain (Phase 0: shadow judge) ----------------
# Every detected creation is judged asynchronously by meme_agent (free-LLM
# rotation, local Ollama backstop) and the verdict journaled to
# brain_decisions.jsonl. Phase 0 gates NOTHING: the control book (all
# candidates) keeps trading exactly as before; the "brain book" is virtual —
# computed by joining approved decisions to control outcomes (_brain_report.py).
# The judge earns the right to actually gate only by beating the control book.
BRAIN_DECISIONS_PATH = os.path.join(HERE, "brain_decisions.jsonl")
try:
    import meme_agent as _meme_agent
except Exception as _brain_import_err:      # missing file/dep => brain off, sniper unharmed
    _meme_agent = None
    print(f"[BRAIN] disabled — meme_agent import failed: {_brain_import_err}")


def _brain_enabled():
    return _meme_agent is not None and bool(sniper_cfg.get("brain", True))


def _brain_judge(coin, pos):
    """Off-thread: enrich, judge, journal. Never touches the position — a judge
    crash or a 30s enrichment stall must not cost the control book anything."""
    try:
        mint = coin.get("mint")
        meta = social = None
        try:
            meta = token_meta(mint)          # usually unindexed seconds after launch
        except Exception:
            pass
        try:
            social = dex_social(mint)
        except Exception:
            pass
        rug = None
        try:
            rug = _meme_agent.rug_check(mint)
        except Exception:
            pass
        snap = _meme_agent.build_snapshot(coin, meta=meta, social=social,
                                          source=pos.get("source", "copy_target"),
                                          rug=rug)
        d = _meme_agent.decide_snipe(snap)
        _append_jsonl(BRAIN_DECISIONS_PATH, {
            "ts": round(time.time(), 1), "mint": mint,
            "symbol": pos.get("symbol"),
            "source": pos.get("source", "copy_target"),
            "entry_mcap": pos.get("entry_mcap"), "snapshot": snap, **d,
        })
    except Exception as e:
        try:
            _append_jsonl(BRAIN_DECISIONS_PATH, {
                "ts": round(time.time(), 1), "mint": coin.get("mint"),
                "enter": False, "confidence": 0.0, "reason": "", "model": "",
                "latency_ms": 0.0, "error": f"brain_judge crashed: {str(e)[:120]}"})
        except Exception:
            pass


def _brain_counts():
    """Cheap decision tally for the status endpoint (file is ~17 lines/day)."""
    recs = _read_jsonl(BRAIN_DECISIONS_PATH)
    judged = [r for r in recs if not r.get("error")]
    return {"decisions": len(recs), "judged": len(judged),
            "approved": sum(1 for r in judged if r.get("enter")),
            "errors": len(recs) - len(judged)}


def _pump_creations(target):
    r = _HTTP.get(f"{PUMP_API}/coins", params={"creator": target, "limit": 30},
                     timeout=12, headers=_GT_HEADERS)
    return r.json() or []

def _pump_mcap(mint):
    try:
        r = _HTTP.get(f"{PUMP_API}/coins/{mint}", timeout=10, headers=_GT_HEADERS)
        d = r.json() or {}
        return d.get("usd_market_cap"), bool(d.get("complete"))
    except Exception:
        return None, False


def _sniper_positions_all():
    return _read_jsonl(SNIPER_POS_PATH)

def _sniper_persist(pos):
    _append_jsonl(SNIPER_POS_PATH, pos)

def _reconstruct_positions():
    """Replay the append-only event log into one merged record per mint, so a
    restart (incl. the dev auto-reloader) keeps closed history AND the on-chain
    real PnL instead of dropping everything but currently-open positions."""
    by_mint = {}
    for e in _sniper_positions_all():          # chronological append order
        m = e.get("mint")
        if not m:
            continue
        by_mint.setdefault(m, {}).update({k: v for k, v in e.items() if k != "event"})
    return list(by_mint.values())


def _day_key():
    return time.strftime("%Y-%m-%d", time.gmtime())

def _reset_day_if_needed():
    if _sniper["day"] != _day_key():
        _sniper["day"] = _day_key()
        _sniper["day_spent"] = 0.0


def _open_snipe(coin, source="copy_target", via="poll", detected_ts=None):
    """Open a paper (or live) position for a detected candidate.
    source: "copy_target" (watched creator's creation) | "graduating".
    via: "ws" (real-time PumpPortal feed) | "poll" (HTTP sweep) — kept on the
    position so entry-quality can be compared across detection paths.
    detected_ts: ws recv time, passed in because this now runs off-thread."""
    mint = coin["mint"]
    entry_mcap = coin.get("usd_market_cap")
    sol_in = float(sniper_cfg["sol_per_snipe"])
    # the graduating sleeve is PAPER-ONLY no matter what the live flag says —
    # it is unvalidated; only the copy-target sleeve may ever route real SOL
    live = bool(sniper_cfg["live"]) and source == "copy_target"
    pos = {
        "mint": mint, "symbol": coin.get("symbol") or "?",
        "name": coin.get("name") or "",
        "detected_ts": round(detected_ts or time.time(), 3),
        "create_sig": coin.get("create_sig"),
        "entry_mcap": entry_mcap, "sol_in": sol_in,
        "mode": "live" if live else "paper",
        "source": source, "via": via,
        "status": "open", "exit_mcap": None, "pnl_sol": None,
        "peak_mcap": entry_mcap, "trough_mcap": entry_mcap,
        "sig": None, "note": None,
    }
    if live:
        _reset_day_if_needed()
        if KEYPAIR is None:
            pos["status"] = "skipped"; pos["note"] = "no wallet unlocked"
        elif _sniper["day_spent"] + sol_in > float(sniper_cfg["max_sol_per_day"]):
            pos["status"] = "skipped"; pos["note"] = "daily cap reached"
        else:
            res = _live_snipe_buy(mint, sol_in)
            pos.update(res)
            if res.get("status") == "open":
                _sniper["day_spent"] += sol_in
    if pos["status"] == "open":
        # dual-policy exit tracking + per-mint trade-feed subscribe. Paper books
        # are CLOSED by the smart policy (when smart_exit is on); live books
        # keep the ladder and only get shadow stamps. See smart_exit.py.
        try:
            _smx_mod.ENGINE.register(
                pos, enforce_smart=(pos["mode"] != "live"
                                    and bool(sniper_cfg.get("smart_exit", True))))
        except Exception:
            pass
        # pyramid ghost: copy-target sleeve only (the grad sleeve is unvalidated
        # and paper-pinned; laddering it would just multiply that noise).
        # Registration is free — the ghost buys nothing.
        try:
            if (sniper_cfg.get("pyramid_enabled", True)
                    and pos.get("source") == "copy_target"):
                _pyr_mod.ENGINE.register(pos)
        except Exception:
            pass
    with _sniper_lock:
        _sniper["positions"].append(pos)
    _sniper_persist({**pos, "event": "open"})
    # shadow judge: fire-and-forget so detection of the next launch never blocks
    if _brain_enabled():
        threading.Thread(target=lambda: _brain_judge(coin, pos), daemon=True).start()
    return pos


# ---- PumpPortal local-tx swaps (bonding curve OR PumpSwap, pool="auto") ----
# Jupiter only routes a token AFTER it graduates off the pump.fun curve (~$69k
# mcap → migrates to PumpSwap), so every sub-graduation launch logged "never
# became routable". PumpPortal fills against the live bonding curve directly, so
# a fresh mint is tradable the instant it exists. The API returns an UNSIGNED tx;
# we sign locally with KEYPAIR and push it through our own RPC path, so the key
# never leaves this process. NOTE: PumpPortal charges a 0.5% trade fee.
PUMPPORTAL_TRADE = "https://pumpportal.fun/api/trade-local"
SNIPE_RETRIES = 3          # curve buys fill instantly; retries only cover transient RPC/API blips
SNIPE_RETRY_SECS = 1       # was 3 — on a fresh curve every second of delay is price paid

# ---- creator_vault correction ----
# PumpPortal derives the pump.fun `creator_vault` PDA from the address that
# SIGNED the create tx. Our target is a launch service: it signs every create,
# but pump.fun's on-chain `creator` (who the creator fee is owed to) is sometimes
# a *different* wallet — measured 2 of its last 12 launches. On those, PumpPortal
# hands back a tx carrying the wrong vault and it lands-and-reverts with
# `Custom: 2006` (AnchorError ConstraintSeeds on creator_vault): 3 of 10 live buy
# failures on 2026-07-16, incl. Kibshi. The bonding-curve account is the only
# authoritative source, so we read it and swap the key ourselves.
#
# Cost: none. The read is fired in parallel with PumpPortal's ~700ms build (the
# read is ~160ms), and ANY failure falls through to sending PumpPortal's tx
# unmodified — i.e. degrades to exactly the previous behaviour, never to a delay.
PUMP_PROGRAM = Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")


def _pump_creator_vault(mint):
    """(correct_vault, on_chain_creator) read from the mint's bonding curve, or
    (None, None) if it can't be resolved — caller must treat that as 'don't touch
    the tx'. BondingCurve layout: 8 disc + 5x u64 + 1 bool, then creator:Pubkey."""
    try:
        bc, _ = Pubkey.find_program_address(
            [b"bonding-curve", bytes(Pubkey.from_string(mint))], PUMP_PROGRAM)
        acc = rpc("getAccountInfo", [str(bc), {"encoding": "base64"}])
        data = base64.b64decode(((acc or {}).get("value") or {})["data"][0])
        if len(data) < 81:
            return None, None
        creator = Pubkey.from_bytes(data[49:81])
        vault, _ = Pubkey.find_program_address(
            [b"creator-vault", bytes(creator)], PUMP_PROGRAM)
        return vault, creator
    except Exception:
        return None, None


def _fix_creator_vault(tx_bytes, correct_vault, assumed_creator):
    """Swap PumpPortal's wrong creator_vault for the on-chain-correct one.

    We only touch the tx when we can positively identify the bad key: the vault
    PumpPortal *would* derive from `assumed_creator` (the create-tx signer). If
    that key isn't present, the layout isn't what we think it is and we return
    the tx untouched rather than corrupting a working trade. Swapping in place
    preserves the account's index, so its writable/signer slot is unchanged and
    every instruction's account references stay valid."""
    if correct_vault is None:
        return tx_bytes
    try:
        tx = VersionedTransaction.from_bytes(tx_bytes)
        msg = tx.message
        keys = [str(k) for k in msg.account_keys]
        if str(correct_vault) in keys:
            return tx_bytes                     # PumpPortal already got it right
        wrong, _ = Pubkey.find_program_address(
            [b"creator-vault", bytes(Pubkey.from_string(str(assumed_creator)))],
            PUMP_PROGRAM)
        if str(wrong) not in keys:
            return tx_bytes                     # can't identify it — leave alone
        keys[keys.index(str(wrong))] = str(correct_vault)
        patched = MessageV0(msg.header, [Pubkey.from_string(k) for k in keys],
                            msg.recent_blockhash, msg.instructions,
                            msg.address_table_lookups)
        print(f"[SNIPE-VAULT] corrected creator_vault "
              f"{str(wrong)[:8]}... -> {str(correct_vault)[:8]}...")
        return bytes(VersionedTransaction.populate(patched, tx.signatures))
    except Exception as e:
        print(f"[SNIPE-VAULT] patch skipped ({str(e)[:60]}) — sending as-is")
        return tx_bytes


def _pumpportal_tx(action, mint, amount, denominated_in_sol, slippage_pct, pool="auto"):
    """Build + sign + send a pump.fun / PumpSwap trade. Returns the tx signature."""
    # Fire the vault resolve FIRST so its ~160ms overlaps the build below and
    # costs nothing; the target signs every create, so it's what PumpPortal
    # assumes the creator is.
    _vault_box = {}
    _vt = threading.Thread(
        target=lambda: _vault_box.update(
            zip(("vault", "creator"), _pump_creator_vault(mint))),
        daemon=True)
    _vt.start()
    r = _HTTP.post(PUMPPORTAL_TRADE, json={
        "publicKey": PUBKEY,
        "action": action,                                   # "buy" | "sell"
        "mint": mint,
        "denominatedInSol": "true" if denominated_in_sol else "false",
        "amount": amount,                                   # SOL (buy) or token-count / "100%" (sell)
        "slippage": slippage_pct,                           # percent
        "priorityFee": round(MAX_PRIORITY_LAMPORTS / 1e9, 6),
        "pool": pool,
    }, timeout=15)
    ctype = r.headers.get("content-type", "")
    if r.status_code != 200 or ctype.startswith("application/json") or not r.content:
        raise RuntimeError(f"pumpportal {action}: {r.text[:160]}")
    # The resolve was racing the build and the build is ~4x slower, so this join
    # is normally already satisfied. Cap it anyway: a slow RPC must never hold up
    # a snipe — we'd rather send PumpPortal's tx unpatched than arrive late.
    _vt.join(timeout=0.4)
    # assumed_creator MUST be what PumpPortal guessed — the create-tx signer,
    # i.e. our copy target — not the real on-chain creator we just read. Only
    # the sniper reaches this path (the dashboard trades via Jupiter), so the
    # target is always the signer here.
    raw = _fix_creator_vault(r.content, _vault_box.get("vault"),
                             sniper_cfg.get("target"))
    tx_b64 = base64.b64encode(raw).decode()
    # The tx already carries a priority fee; send over RPC (mev_protect=False) —
    # a Jito path would need a tip instruction PumpPortal did not add.
    sig, _ = sign_and_send(tx_b64, mev_protect=False)
    return sig

def _snipe_slippage():
    return float(sniper_cfg.get("snipe_slippage_pct", 50.0))

def _pumpportal_buy(mint, sol_in, pool="auto"):
    return _pumpportal_tx("buy", mint, sol_in, True, _snipe_slippage(), pool)

def _pumpportal_sell(mint, amount="100%", pool="auto"):
    return _pumpportal_tx("sell", mint, amount, False, _snipe_slippage(), pool)


def _confirm_snipe_fill(sig):
    """A live buy is marked 'open' the instant it's sent; downgrade it to
    'skipped' if the tx never actually lands, so a failed buy can't masquerade
    as a filled position."""
    t0 = time.time()
    status, _ = confirm(sig, timeout_s=40)
    if status == "confirmed":
        # Landing time is the half of the latency budget we do NOT control (it's
        # blockspace contention, not our code). Journal it so "the sniper is
        # slow" can be attributed instead of guessed at.
        with _sniper_lock:
            for p in _sniper["positions"]:
                if p.get("sig") == sig:
                    p["land_s"] = round(time.time() - t0, 2)
                    _sniper_persist({**p, "event": "buy_landed"})
                    break
        return
    with _sniper_lock:
        for p in _sniper["positions"]:
            if p.get("sig") == sig and p["status"] == "open":
                p["status"] = "skipped"
                p["note"] = f"buy tx did not land ({status})"
                p["land_s"] = round(time.time() - t0, 2)
                _sniper_persist({**p, "event": "buy_failed"})
                break


def _live_snipe_buy(mint, sol_in):
    """Real buy via PumpPortal (pool='auto' → bonding curve or PumpSwap).

    Replaces the old Jupiter path that could only fill post-graduation. We capture
    the real error so a genuine failure (low SOL, slippage, RPC) is distinguishable
    from a true miss — and confirm the fill off-thread so the watcher never blocks."""
    last_err = None
    for attempt in range(SNIPE_RETRIES):
        try:
            sig = _pumpportal_buy(mint, sol_in)
            # Stamp when the tx actually left. detected_ts alone can't tell a
            # slow build from a slow landing, and `blockTime` is a validator
            # ESTIMATE that lags ~2s — measuring against it (as
            # _snipe_latency_report.py does) overstates our lag by that much.
            # sent_ts - detected_ts is the only part we control; landed_slot vs
            # create slot is the part we don't.
            threading.Thread(target=lambda: _confirm_snipe_fill(sig), daemon=True).start()
            return {"status": "open", "sig": sig, "attempts": attempt + 1,
                    "last_error": None, "sent_ts": round(time.time(), 3),
                    "note": f"buy sent via pumpportal (try {attempt + 1})"}
        except Exception as e:
            last_err = str(e)
            if attempt < SNIPE_RETRIES - 1:
                time.sleep(SNIPE_RETRY_SECS)

    low = (last_err or "").lower()
    if "insufficient" in low or "lamports" in low or "balance" in low:
        note = f"insufficient SOL — {(last_err or '')[:70]}"
    elif "slippage" in low or "0x1771" in low:
        note = f"slippage exceeded — {(last_err or '')[:70]}"
    elif last_err:
        note = f"buy failed {SNIPE_RETRIES}x: {last_err[:90]}"
    else:
        note = "buy failed (missed)"
    return {"status": "skipped", "attempts": SNIPE_RETRIES,
            "last_error": last_err, "note": note}


def _mark_and_exit():
    """Mark open positions to market; apply TP/SL/timeout exits.

    Prices are fetched concurrently: the pump.fun poll costs ~160ms, so a serial
    sweep left the Nth open position N*160ms staler than the first before its
    exit was even considered. Only the I/O fans out — every exit decision still
    runs on this thread, so _close_snipe keeps its single-writer guarantee."""
    from concurrent.futures import ThreadPoolExecutor
    now = time.time()
    open_pos = [p for p in list(_sniper["positions"]) if p["status"] == "open"]
    smx_cfg = _smx_mod.build_cfg(sniper_cfg)
    try:
        pyr_cfg = _pyr_mod.build_cfg(sniper_cfg)
    except Exception as e:
        pyr_cfg = None
        _sniper["last_error"] = f"pyr-cfg: {str(e)[:80]}"
    # ghosts: closed positions whose OTHER exit policy hasn't resolved yet —
    # they keep getting priced (bounded by the shared 15-min timeout) so
    # smart-vs-fixed is always scored on the same trade. See smart_exit.py.
    # Reap BEFORE the early return: a leaked state is in neither open_pos nor
    # ghost_mints (never-closed states fail ghost_mints' `closed` test), so an
    # early return would strand it — and its flow subscription — forever.
    try:
        for mint, why, age in _smx_mod.ENGINE.reap(now, SMX_REAP_AFTER_S):
            _sniper["smx_reaped"] = _sniper.get("smx_reaped", 0) + 1
            print(f"[SMX] reaped {mint[:12]}… ({why}, age {age/60:.0f}m) — "
                  f"released its flow subscription; row quarantined")
    except Exception as e:
        _sniper["last_error"] = f"smx-reap: {str(e)[:80]}"
    try:
        ghost_mints = _smx_mod.ENGINE.ghost_mints()
    except Exception:
        ghost_mints = []
    # pyramid ghosts leak the same way smart_exit states do (a position whose
    # real close never fires), so reap on the same schedule and drain rows on
    # BOTH exits from this function — including the early return below.
    try:
        _pyr_mod.ENGINE.reap(now, SMX_REAP_AFTER_S)
        for row in _pyr_mod.ENGINE.pop_done():
            _journal_pyramid_row(row)
    except Exception as e:
        _sniper["last_error"] = f"pyr-drain: {str(e)[:80]}"
    # Pyramid ghosts outlive their real position on purpose (the ladder's first
    # add is at 48s; the paper book is often closed by creator-sell in ~1.4s), so
    # they must keep being priced after the position leaves open_pos.
    try:
        _open_mints = {p["mint"] for p in open_pos}
        pyr_ghosts = ([m for m in _pyr_mod.ENGINE.ghost_mints()
                       if m not in _open_mints and m not in ghost_mints]
                      if pyr_cfg and pyr_cfg["enabled"] else [])
    except Exception:
        pyr_ghosts = []
    if not open_pos and not ghost_mints and not pyr_ghosts:
        # still drain: reap() may have just made rows poppable
        try:
            for row in _smx_mod.ENGINE.pop_compares():
                _journal_compare_row(row)
        except Exception as e:
            _sniper["last_error"] = f"smx-compare: {str(e)[:80]}"
        return
    with ThreadPoolExecutor(max_workers=8) as ex:
        marks = list(ex.map(lambda p: _pump_mcap(p["mint"]), open_pos))
        gmarks = list(ex.map(_pump_mcap, ghost_mints))
        pmarks = list(ex.map(_pump_mcap, pyr_ghosts))
    for mint, (gm, ggrad) in zip(ghost_mints, gmarks):
        try:
            _smx_mod.ENGINE.ghost_mark(mint, gm, ggrad, now, smx_cfg)
        except Exception:
            pass
    for mint, (pm, pgrad) in zip(pyr_ghosts, pmarks):
        try:
            flow = _pyr_mod.ENGINE.flow(mint, now, pyr_cfg["flow_window_s"])
            _pyr_mod.ENGINE.ghost_mark(mint, pm, pgrad, now, pyr_cfg, flow)
        except Exception:
            pass
    try:
        for row in _smx_mod.ENGINE.pop_compares():
            _journal_compare_row(row)
    except Exception as e:
        _sniper["last_error"] = f"smx-compare: {str(e)[:80]}"
    for pos, (mcap, graduated) in zip(open_pos, marks):
        if pos["status"] != "open":          # closed elsewhere while we fetched
            continue
        if mcap is None or not pos.get("entry_mcap"):
            # can't price it; time out if very old
            if now - pos["detected_ts"] > sniper_cfg["timeout_min"] * 60:
                _close_snipe(pos, pos.get("entry_mcap"), "timeout (no price)")
            continue
        pos["peak_mcap"] = max(pos.get("peak_mcap") or 0, mcap)
        pos["trough_mcap"] = min(pos.get("trough_mcap") or mcap, mcap)
        pos["graduated"] = graduated
        ret = (mcap / pos["entry_mcap"] - 1) * 100
        age_min = (now - pos["detected_ts"]) / 60
        # ---- smart exit engine (flow evidence; smart_exit.py) ----
        # Paper: the engine's verdict closes the book. Live: verdict is stamped
        # once (smart_trigger_*) and the ladder below stays authoritative.
        try:
            act = _smx_mod.ENGINE.on_mark(pos, mcap, graduated, now, smx_cfg)
        except Exception as e:
            act = {"enforced": False, "close": None, "stamp": None}
            _sniper["last_error"] = f"smx: {str(e)[:80]}"
        # ---- pyramid ghost (pyramid.py) ----
        # Simulates the scale-in ladder on the SAME mark, reusing smart_exit's
        # per-mint flow subscription rather than opening a second one. Returns
        # the actions a live arm WOULD take; in ghost mode they are discarded.
        # Wrapped like every other engine call here: a bug in the ghost must
        # mean "no ghost", never "positions stop being marked".
        try:
            if pyr_cfg and pyr_cfg["enabled"]:
                flow = _pyr_mod.ENGINE.flow(
                    pos["mint"], now, pyr_cfg["flow_window_s"])
                _pyr_mod.ENGINE.on_mark(pos, mcap, graduated, now, pyr_cfg, flow)
        except Exception as e:
            _sniper["last_error"] = f"pyr: {str(e)[:80]}"
        if act["stamp"] and not pos.get("smart_trigger_ts"):
            pos["smart_trigger_ts"] = round(now, 3)
            pos["smart_trigger_mcap"] = mcap
            pos["smart_trigger_note"] = act["stamp"]
        if act["close"]:
            _close_snipe(pos, mcap, act["close"])
            continue
        if act["enforced"]:
            continue    # smart owns this book; fixed rules run as its ghost
        # ---- fast exit (copy-target sleeve): flat within ~35s, in chunks ----
        # No TP wait, no 15-min timeout: sell into the launch wave while it
        # exists. SL stays as a crash brake (dump everything at once). Paper
        # closes at the AVERAGE of the chunk marks, not the last print, so the
        # simulated exit matches how a chunked live dump would actually fill.
        if pos.get("source") == "copy_target" and sniper_cfg.get("fast_exit", True):
            age_s = now - pos["detected_ts"]
            start = float(sniper_cfg.get("fast_exit_start_s", 15))
            gap = max(1.0, float(sniper_cfg.get("fast_exit_gap_s", 10)))
            chunks = max(1, int(sniper_cfg.get("fast_exit_chunks", 3)))
            if ret <= -abs(sniper_cfg["sl_pct"]):
                _close_snipe(pos, mcap, f"fast-exit stop {ret:.0f}%")
                continue
            done = int(pos.get("chunks_sold") or 0)
            due = (min(chunks, 1 + int((age_s - start) // gap))
                   if age_s >= start else 0)
            if due > done:
                pos.setdefault("exit_marks", []).append(mcap)
                # live: sell an equal slice of what REMAINS (33% -> 50% -> the
                # final 100% is fired by _close_snipe below)
                #
                # Gated on land_s (buy CONFIRMED), not sig (buy merely SENT).
                # The ladder opens at 15s but _confirm_snipe_fill waits up to
                # 40s, so gating on sig fired sells at 15s and 25s against a
                # token account that did not exist yet: the ATA got created
                # empty and pump.fun rejected the sell with custom error 6022.
                # Measured 2026-07-28: 17 of 19 bot tx failures were exactly
                # this, in pairs one fast_exit_gap_s apart.
                if (pos.get("mode") == "live" and pos.get("land_s") is not None
                        and due < chunks):
                    try:
                        pct = round(100 / (chunks - done))
                        pos.setdefault("chunk_sigs", []).append(
                            _pumpportal_sell(pos["mint"], f"{pct}%"))
                    except Exception as e:
                        print(f"[SNIPE-EXIT] chunk sell failed: {str(e)[:60]}")
                pos["chunks_sold"] = due
                if due >= chunks:
                    # Same race at the final 100% sell, and worse: closing here
                    # sets status="closed", so when _confirm_snipe_fill finally
                    # times out its "downgrade to skipped" no-ops (it only acts
                    # on status=="open"). That is how a buy that never landed
                    # became a CLOSED position with a mark-derived pnl and no
                    # real_pnl_sol — 9 of 23 live closes overnight. Hold it open
                    # and let the confirm decide; the 15-min timeout bounds it.
                    if pos.get("mode") == "live" and pos.get("land_s") is None:
                        continue
                    marks = pos.get("exit_marks") or [mcap]
                    _close_snipe(pos, sum(marks) / len(marks),
                                 f"fast-exit flat @{age_s:.0f}s ({ret:+.0f}%)")
            continue
        if ret >= sniper_cfg["tp_pct"]:
            _close_snipe(pos, mcap, f"take-profit +{ret:.0f}%")
        elif ret <= -abs(sniper_cfg["sl_pct"]):
            _close_snipe(pos, mcap, f"stop {ret:.0f}%")
        elif age_min >= sniper_cfg["timeout_min"]:
            _close_snipe(pos, mcap, f"timeout ({ret:+.0f}%)")


def _wallet_sol_delta(sig):
    """Real lamport change to OUR wallet in a tx (fee payer = our key). Returns SOL
    float incl. all fees/slippage, or None if the tx isn't retrievable yet."""
    try:
        t = rpc("getTransaction", [sig, {"encoding": "jsonParsed",
                                         "maxSupportedTransactionVersion": 0}])
    except Exception:
        return None
    if not t or not t.get("meta"):
        return None
    keys = [k["pubkey"] for k in t["transaction"]["message"]["accountKeys"]]
    try:
        i = keys.index(PUBKEY)
    except ValueError:
        return None
    return (t["meta"]["postBalances"][i] - t["meta"]["preBalances"][i]) / 1e9


def _token_account_for(buy_sig, mint):
    """Our SPL token account (ATA) for `mint`, read straight from the buy tx's
    postTokenBalances. Used to enumerate the position's full on-chain tx history."""
    try:
        t = rpc("getTransaction", [buy_sig, {"encoding": "jsonParsed",
                                             "maxSupportedTransactionVersion": 0}])
    except Exception:
        return None
    if not t or not t.get("meta"):
        return None
    keys = [k["pubkey"] for k in t["transaction"]["message"]["accountKeys"]]
    for b in (t["meta"].get("postTokenBalances") or []):
        if b.get("owner") == PUBKEY and b.get("mint") == mint:
            return keys[b["accountIndex"]]
    return None


def _ata_signatures(ata):
    """Every signature touching the account, newest first, paginated. None on RPC
    failure — a truncated list silently corrupts the pro-rata denominator, so the
    caller must distinguish "none" from "couldn't tell"."""
    out, before = [], None
    while True:
        params = {"limit": 1000}
        if before:
            params["before"] = before
        try:
            page = rpc("getSignaturesForAddress", [ata, params])
        except Exception:
            return None
        if page is None:
            return None
        out.extend(page)
        if len(page) < 1000 or len(out) > 5000:
            return out
        before = page[-1]["signature"]


def _tx_views(sigs, mint):
    """(views oldest-first, complete). Each tx is fetched ONCE and yields both the
    wallet SOL delta and our token delta, which is what tells a buy from a sell."""
    views, complete = [], True
    for s in reversed(sigs):                     # RPC returns newest first
        t = None
        for _ in range(5):
            try:
                t = rpc("getTransaction", [s["signature"],
                                           {"encoding": "jsonParsed",
                                            "maxSupportedTransactionVersion": 0}])
            except Exception:
                t = None
            if t and t.get("meta"):
                break
            time.sleep(1.5)
        if not t or not t.get("meta"):
            complete = False
            continue
        keys = [k["pubkey"] for k in t["transaction"]["message"]["accountKeys"]]
        try:
            i = keys.index(PUBKEY)
        except ValueError:
            continue
        sol = (t["meta"]["postBalances"][i] - t["meta"]["preBalances"][i]) / 1e9

        def _bal(side):
            for b in (t["meta"].get(side) or []):
                if b.get("mint") == mint and b.get("owner") == PUBKEY:
                    return int(b["uiTokenAmount"]["amount"])
            return 0

        views.append(pnl_attrib.TxView(
            s["signature"], sol,
            _bal("postTokenBalances") - _bal("preTokenBalances"),
            err=bool(t["meta"].get("err")),
            ts=s.get("blockTime") or t.get("blockTime")))
    return views, complete


def _finalize_real_pnl(pos):
    """Book the TRUE realized PnL of a live snipe, stored alongside — not over —
    the mcap estimate so you can see both.

    This used to sum every SOL delta on the position's ATA. That was wrong twice
    over (both confirmed on-chain 2026-07-24): it raced the sell being indexed and
    stamped a partial sum as final (Bear: 3 of 4 txs, booked -0.988 on a 0.04 SOL
    stake — the trade was actually a +0.026 WIN), and the ATA is per-(wallet,mint)
    rather than per-trade, so any other buy of the same mint got charged to the
    snipe. Attribution is now per-lot; see pnl_attrib.py.

    Writes nothing unless the result passes the guards — an unscoreable position
    keeps real_pnl_sol=None and is retried by _pnl_reconcile_loop. A None gets
    fixed later; a wrong number is trusted forever."""
    buy_sig = pos.get("sig")
    if pos.get("real_pnl_sol") is not None:
        return
    mint = pos.get("mint")
    # DERIVE the ATA rather than reading it off the buy tx. The recorded sig is
    # whatever /api/buy submitted, not necessarily what landed (fast-ack), so for
    # PIPI KIWI, Sebastian, OPEN-CHAN and Ryzhik it does not exist on chain at
    # all — and those are the LARGEST positions, so resolving via the buy tx left
    # exactly the trades that matter permanently unbooked.
    try:
        ata = pnl_attrib.derive_ata(rpc, PUBKEY, mint)
    except Exception:
        ata = _token_account_for(buy_sig, mint) if buy_sig else None
    if not ata:
        return                                   # retried later; never guessed
    sigs = _ata_signatures(ata)
    if not sigs:
        return
    views, complete = _tx_views(sigs, mint)
    # recover the buy that actually landed when the recorded one is a phantom
    eff_buy = pnl_attrib.pick_buy_sig(views, buy_sig, pos.get("detected_ts"))
    if not eff_buy:
        return
    if eff_buy != buy_sig:
        pos["buy_sig_landed"] = eff_buy
    sell_sig = pos.get("sell_sig")
    present = any(v.sig == sell_sig for v in views) if sell_sig else None
    res = pnl_attrib.attribute(views, eff_buy, sell_sig)
    ok, why = pnl_attrib.guard(res, sell_sig, present, complete)
    if not ok:
        with _sniper_lock:
            pos["real_pnl_pending"] = why
        return
    with _sniper_lock:
        pos["real_pnl_sol"] = res["pnl"]
        pos["pnl_basis"] = "on-chain (lot-attributed)"
        pos["pnl_cost_sol"] = res["cost"]
        pos["pnl_proceeds_sol"] = res["proceeds"]
        pos["foreign_lots"] = res["foreign_lots"]
        pos["tx_count"] = len(sigs)
        pos.pop("real_pnl_pending", None)
    _sniper_persist({**pos, "event": "real_pnl"})


def _pnl_reconcile_loop():
    """Durability for the above. The booker used to be a fire-and-forget thread:
    anything that failed once — an RPC hiccup, a sell not yet indexed, the process
    restarting mid-close — stayed unbooked forever. 10 of 24 live closes in the
    week of 07-18 were stuck that way, and they skewed to the LARGE estimated
    winners, which is exactly what made the sleeve look profitable when it wasn't.

    Scans the FULL event log, not `_sniper["positions"]` — that list keeps only
    the 40 most recent closes, and paper closes share the window, so a live trade
    can age out of it within hours and never be retried. Results are persisted as
    `real_pnl` events, which is the source of truth; the in-memory copy picks them
    up on the next restart.

    Runs sequentially (parallel RPC bursts get rate-limited) and gives up on a
    position after MAX_TRIES attempts per process, so the genuinely unbookable —
    positions whose buy never landed at all — do not get re-fetched forever."""
    MAX_TRIES, PER_SWEEP = 3, 12
    tries = {}
    time.sleep(45)                               # let boot settle
    while True:
        try:
            todo = [p for p in _reconstruct_positions()
                    if p.get("mode") == "live" and p.get("status") == "closed"
                    and p.get("sig") and p.get("real_pnl_sol") is None
                    and tries.get(p["mint"], 0) < MAX_TRIES]
            todo.sort(key=lambda p: -(p.get("closed_ts") or 0))
            todo = todo[:PER_SWEEP]
            booked = 0
            for p in todo:
                tries[p["mint"]] = tries.get(p["mint"], 0) + 1
                _finalize_real_pnl(p)
                if p.get("real_pnl_sol") is not None:
                    booked += 1
                time.sleep(1)
            if todo:
                print(f"[SNIPE-PNL] reconcile swept {len(todo)} unbooked, "
                      f"booked {booked}")
        except Exception as e:
            print(f"[SNIPE-PNL] reconcile loop: {str(e)[:100]}")
        time.sleep(900)


threading.Thread(target=_pnl_reconcile_loop, daemon=True).start()


def _close_token_ata(mint, wait_s=90):
    """Reclaim the ~0.00204 SOL ATA rent once a live position is fully sold.

    The dissected competitor wallet (07-17) closes its token account after every
    dump; we never did — each live snipe left rent locked in a dead account.
    Polls until the balance is actually 0 (the sell has to land first), then
    sends a closeAccount ix signed locally. Best-effort: any failure just leaves
    the dust ATA exactly as before, so there is nothing to roll back."""
    if KEYPAIR is None:
        return
    deadline = time.time() + wait_s
    acct = None
    while time.time() < deadline:
        try:
            res = rpc("getTokenAccountsByOwner",
                      [PUBKEY, {"mint": mint}, {"encoding": "jsonParsed"}])
            vals = res.get("value") or []
            if not vals:
                return                     # no ATA at all — nothing to reclaim
            acct = vals[0]
            raw = int(acct["account"]["data"]["parsed"]["info"]
                      ["tokenAmount"]["amount"])
            if raw == 0:
                break
        except Exception:
            pass
        time.sleep(5)
    else:
        return                             # never emptied (sell failed?) — leave it
    try:
        from solders.instruction import AccountMeta, Instruction
        from solders.hash import Hash
        owner = KEYPAIR.pubkey()
        ata = Pubkey.from_string(acct["pubkey"])
        # read the owning token program off the account itself instead of
        # assuming Tokenkeg — pump.fun mints are classic SPL today, but this
        # keeps a Token-2022 mint from producing a wrong-program ix
        token_prog = Pubkey.from_string(acct["account"]["owner"])
        ix = Instruction(token_prog, bytes([9]), [   # 9 = CloseAccount
            AccountMeta(ata, is_signer=False, is_writable=True),
            AccountMeta(owner, is_signer=False, is_writable=True),
            AccountMeta(owner, is_signer=True, is_writable=False),
        ])
        bh = Hash.from_string(rpc("getLatestBlockhash", [])["value"]["blockhash"])
        msg = MessageV0.try_compile(owner, [ix], [], bh)
        tx = VersionedTransaction(msg, [KEYPAIR])
        sig = rpc("sendTransaction", [base64.b64encode(bytes(tx)).decode(),
                                      {"encoding": "base64", "skipPreflight": True}])
        print(f"[SNIPE-ATA] closed {mint[:8]}… rent reclaimed ({sig[:16]}…)")
    except Exception as e:
        print(f"[SNIPE-ATA] close failed {mint[:8]}…: {str(e)[:80]}")


# ---------------- ATA rent sweeper ----------------
# Every token you have ever bought left a token account behind holding
# ~0.00204 SOL of rent-exempt deposit. Jupiter closes the WRAPPED-SOL account
# on every swap but never the output token's, so the rent stays locked once the
# position is gone. _close_token_ata() above reclaims it for one mint right
# after a sniper exit; this sweeps everything the wallet has accumulated.
#
# THE ONLY WAY THIS COSTS MONEY IS THE CREATE-THEN-FILL RACE.
# A buy's transaction creates the ATA and fills it in the same tx, but between
# the create landing and the fill being visible on the node we read, the
# account looks exactly like a dead one worth reclaiming. Close it there and
# the pending swap is stranded. Every guard below exists for that window, in
# descending order of how much it actually discriminates:
#
#   1. mint_lock, non-blocking. The SAME interlock api_sell and tpsl._fire hold
#      across quote -> build -> send. Failing to take it means a trade is in
#      flight on this mint right now. This one CLOSES the window; the rest only
#      narrow it.
#   2. commitment=confirmed on the balance read. The JSON-RPC default is
#      finalized, ~13s behind -- a just-filled buy reads as zero there. This is
#      the same trap token_balance() documents.
#   3. no trade row for the mint in the last RENT_SWEEP_MIN_AGE_S.
#   4. not a position the TP/SL executor is currently watching.
#   5. never SOL_MINT: Jupiter's wrapAndUnwrapSol recreates and closes the
#      wrapped-SOL account on every single swap, so it is pure race surface for
#      one account's worth of rent. Excluded outright.
#
# A nonzero balance also makes the token program itself reject CloseAccount, so
# the worst case of a guard failing is a reverted transaction, not burnt tokens.
RENT_SWEEP_ENABLED = os.environ.get("RENT_SWEEP", "1") == "1"
RENT_SWEEP_EVERY_S = float(os.environ.get("RENT_SWEEP_EVERY_S", "600"))   # 10 min
RENT_SWEEP_MIN_AGE_S = float(os.environ.get("RENT_SWEEP_MIN_AGE_S", "900"))
RENT_SWEEP_BATCH = int(os.environ.get("RENT_SWEEP_BATCH", "6"))
_rent_last = {"ts": None, "closed": 0, "reclaimed_sol": 0.0, "sigs": [],
              "error": None, "runs": 0}


def _recent_mints(max_age_s, side=None, tail_bytes=262144):
    """Mints with a journal row newer than max_age_s. side=None means any.

    Tail-read and bounded: the journal grows forever and this sits on both the
    holdings path and the rent sweep.
    """
    out = set()
    try:
        size = os.path.getsize(TRADES_PATH)
        cutoff = time.time() - max_age_s
        with open(TRADES_PATH, "rb") as f:
            if size > tail_bytes:
                f.seek(size - tail_bytes)
                f.readline()               # drop the partial first line
            for line in f:
                try:
                    d = json.loads(line)
                except Exception:
                    continue
                if (d.get("wallet") == PUBKEY and d.get("mint")
                        and (side is None or d.get("side") == side)
                        and float(d.get("ts") or 0) >= cutoff):
                    out.add(d["mint"])
    except Exception:
        pass
    return out


def _rent_candidates():
    """(candidates, skipped) without sending anything. Drives both the dry-run
    endpoint and the sweep itself, so what you inspect is what would run."""
    require_wallet()
    traded = _recent_mints(RENT_SWEEP_MIN_AGE_S)
    watched = set()
    try:
        if TPSL is not None:
            with TPSL._lock:
                watched = {r["mint"] for r in TPSL._rows}
    except Exception:
        pass
    cands, skipped = [], []
    for prog in (TOKEN_PROG, TOKEN_2022_PROG):
        res = rpc("getTokenAccountsByOwner",
                  [PUBKEY, {"programId": prog},
                   # confirmed, NOT the finalized default -- see guard 2 above
                   {"encoding": "jsonParsed", "commitment": "confirmed"}],
                  trade=True)
        for acc in res.get("value") or []:
            info = acc["account"]["data"]["parsed"]["info"]
            mint = info["mint"]
            raw = int(info["tokenAmount"]["amount"] or 0)
            lam = int(acc["account"].get("lamports") or 0)
            row = {"mint": mint, "pubkey": acc["pubkey"],
                   "program": acc["account"]["owner"], "lamports": lam,
                   "sol": round(lam / 1e9, 6)}
            if mint == SOL_MINT:
                row["skip"] = "wrapped SOL — Jupiter recreates it every swap"
            elif raw != 0:
                row["skip"] = "holds %d raw tokens" % raw
            elif mint in watched:
                row["skip"] = "TP/SL is watching this position"
            elif mint in traded:
                row["skip"] = "traded in the last %.0f min" % (RENT_SWEEP_MIN_AGE_S / 60)
            else:
                cands.append(row)
                continue
            skipped.append(row)
    return cands, skipped


def _close_atas(rows):
    """Close a list of candidate accounts. Returns (closed, reclaimed_lamports,
    sigs). Batched, with a per-account retry: one poisoned account reverts its
    whole batch, and at ~0.00204 SOL reclaimed against a 0.000005 SOL fee the
    arithmetic never justifies letting it block the others."""
    from solders.instruction import AccountMeta, Instruction
    from solders.hash import Hash
    owner = KEYPAIR.pubkey()
    closed, reclaimed, sigs = 0, 0, []

    def send(group):
        ixs = []
        for r in group:
            ixs.append(Instruction(
                Pubkey.from_string(r["program"]), bytes([9]),   # 9 = CloseAccount
                [AccountMeta(Pubkey.from_string(r["pubkey"]), is_signer=False,
                             is_writable=True),
                 AccountMeta(owner, is_signer=False, is_writable=True),
                 AccountMeta(owner, is_signer=True, is_writable=False)]))
        bh = Hash.from_string(rpc("getLatestBlockhash", [])["value"]["blockhash"])
        msg = MessageV0.try_compile(owner, ixs, [], bh)
        tx = VersionedTransaction(msg, [KEYPAIR])
        # skipPreflight=False here, unlike the trade path: this is never racing
        # a price, and a simulation failure is exactly the signal that one of
        # these accounts is not safe to close after all.
        return rpc("sendTransaction", [base64.b64encode(bytes(tx)).decode(),
                                       {"encoding": "base64",
                                        "skipPreflight": False}])

    for i in range(0, len(rows), max(1, RENT_SWEEP_BATCH)):
        group = rows[i:i + max(1, RENT_SWEEP_BATCH)]
        # Re-take the interlock immediately before signing. The candidate list
        # may be seconds old by now, and a click in that gap is the whole race.
        held = []
        for r in group:
            lk = mint_lock(r["mint"])
            if lk.acquire(False):
                held.append((r, lk))
        try:
            batch = [r for r, _ in held]
            if not batch:
                continue
            try:
                sig = send(batch)
                # SENT IS NOT LANDED, and the first version of this counted it
                # as landed -- it added the whole batch to `reclaimed` the
                # moment sendTransaction returned a signature. A dropped tx
                # would then have been reported as SOL you got back, which is
                # the same "green for a trade that never landed" failure the
                # pop-out's header calls out. Confirm before counting.
                st, err = confirm(sig, timeout_s=30)
                if st == "confirmed":
                    closed += len(batch)
                    reclaimed += sum(r["lamports"] for r in batch)
                    sigs.append(sig)
                    print("[RENT] closed %d account(s), reclaimed %.5f SOL (%s)"
                          % (len(batch), sum(r["lamports"] for r in batch) / 1e9,
                             sig[:16]))
                else:
                    # Not counted. The accounts are untouched, so the next
                    # sweep simply picks them up again.
                    print("[RENT] batch of %d did not land (%s) — not counted"
                          % (len(batch), st))
                    raise RuntimeError("batch %s" % st)
            except Exception as e:
                print("[RENT] batch of %d failed (%s) — retrying singly"
                      % (len(batch), str(e)[:80]))
                for r in batch:
                    try:
                        sig = send([r])
                        st, _ = confirm(sig, timeout_s=30)
                        if st != "confirmed":
                            print("[RENT] %s did not land (%s)"
                                  % (r["mint"][:8], st))
                            continue
                        closed += 1
                        reclaimed += r["lamports"]
                        sigs.append(sig)
                    except Exception as e2:
                        print("[RENT] %s could not be closed: %s"
                              % (r["mint"][:8], str(e2)[:80]))
        finally:
            for _, lk in held:
                lk.release()
    return closed, reclaimed, sigs


def rent_sweep():
    """One pass. Safe to call with nothing to do."""
    cands, _ = _rent_candidates()
    if not cands:
        _rent_last.update(ts=time.time(), closed=0, reclaimed_sol=0.0, sigs=[],
                          error=None, runs=_rent_last["runs"] + 1)
        return _rent_last
    closed, lam, sigs = _close_atas(cands)
    _rent_last.update(ts=time.time(), closed=closed,
                      reclaimed_sol=round(lam / 1e9, 6), sigs=sigs[-10:],
                      error=None, runs=_rent_last["runs"] + 1)
    return _rent_last


def _rent_sweep_loop():
    # One interval before the first pass: a restart is the moment the wallet is
    # least settled, and there is never anything urgent about reclaiming rent.
    while True:
        time.sleep(RENT_SWEEP_EVERY_S)
        try:
            if KEYPAIR is None:
                continue               # locked wallet — nothing to sign with
            rent_sweep()
        except Exception as e:
            _rent_last["error"] = str(e)[:200]
            print("[RENT] sweep failed: %s" % str(e)[:160])


@app.route("/api/rent/sweep")
def api_rent_preview():
    """DRY RUN. Lists exactly what a sweep would close and what it would
    reclaim, plus every account it is skipping AND WHY -- the skip reasons are
    the guards, so this is how you check them without sending a transaction."""
    try:
        cands, skipped = _rent_candidates()
        return jsonify({
            "ok": True, "enabled": RENT_SWEEP_ENABLED,
            "every_s": RENT_SWEEP_EVERY_S, "min_age_s": RENT_SWEEP_MIN_AGE_S,
            "batch": RENT_SWEEP_BATCH,
            "closable": len(cands),
            "reclaimable_sol": round(sum(c["lamports"] for c in cands) / 1e9, 6),
            "candidates": cands, "skipped": skipped, "last_run": _rent_last})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/rent/sweep", methods=["POST"])
def api_rent_sweep():
    """Run one sweep now. Signs and sends, so it is a POST."""
    try:
        return jsonify({"ok": True, **rent_sweep()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


def _close_snipe(pos, exit_mcap, note):
    # A live position holds real tokens — actually SELL them (PumpPortal, curve or
    # AMM). Without this the wallet just accumulates un-sellable curve tokens while
    # we book a fictional PnL. Only when the buy really landed (has a sig).
    if pos.get("mode") == "live" and pos.get("sig") and not pos.get("sell_sig"):
        try:
            pos["sell_sig"] = _pumpportal_sell(pos["mint"], "100%")
            note += " · sold via pumpportal"
        except Exception as e:
            note += f" · SELL FAILED: {str(e)[:60]}"
    entry = pos.get("entry_mcap")
    if entry and exit_mcap:
        # PnL scales with mcap. This is an OPTIMISTIC UPPER BOUND, not realized
        # proceeds: a real curve exit walks the bonding curve down and slips well
        # past this haircut on thin liquidity. Un-graduated tokens get a heavier
        # haircut to reflect that. Judge live edge from the wallet, not this mark.
        haircut = 0.97 if pos.get("graduated") else 0.90
        gross = pos["sol_in"] * (exit_mcap / entry)
        pos["pnl_sol"] = round(gross * haircut - pos["sol_in"], 5)
        pos["pnl_basis"] = "mcap-estimate (upper bound)"
        # SHADOW (stamps only, gates nothing): the same position priced through
        # the real bonding curve, entered at the mcap we would ACTUALLY fill at.
        # Inverting 22 live buys on 2026-07-24 showed `entry_mcap` — stamped from
        # the PumpPortal detection event — sits a median 1.58x BELOW the curve
        # state the buy really landed in, so every paper trade is booked into
        # profit for free before it starts. Recorded, not enforced: on the same
        # 22 positions the curve model did NOT beat the naive mark against real
        # on-chain PnL (5/22), so it is not yet trustworthy enough to mark from.
        # Decide from forward data. See _curve_sim_validate.py.
        try:
            slip = float(sniper_cfg.get("entry_slip_x", 1.58))
            px = _sol_price_usd(max_age=300) or 0
            if px and slip > 0:
                marks = [m / px for m in (pos.get("exit_marks")
                                          or [exit_mcap]) if m]
                sim = curve_sim.simulate_position(
                    (entry / px) * slip, pos["sol_in"], marks)
                if sim is not None:
                    pos["pnl_sol_curve_shadow"] = sim
                    pos["entry_slip_x"] = slip
        except Exception:
            pass
    else:
        pos["pnl_sol"] = 0.0
    pos["exit_mcap"] = exit_mcap
    pos["status"] = "closed"
    pos["note"] = note
    pos["closed_ts"] = round(time.time(), 1)
    _sniper_persist({**pos, "event": "close"})
    # the REAL close (whatever fired it) becomes the enforced side of the
    # smart-vs-fixed comparison; the other policy keeps running as a ghost
    try:
        _smx_mod.ENGINE.actual_closed(pos)
    except Exception:
        pass
    # Hand the real arm's outcome to the pyramid ghost — this is the only moment
    # it is known. The ghost is deliberately NOT closed here: it runs to its own
    # exit (see note_real_close). Comparison is same-ENTRY, not same-hold.
    try:
        _pyr_mod.ENGINE.note_real_close(
            pos["mint"], exit_mcap, note,
            fixed={"pnl_sol": pos.get("pnl_sol"), "note": note,
                   "exit_mcap": exit_mcap, "sol_in": pos.get("sol_in"),
                   "mode": pos.get("mode")})
    except Exception:
        pass
    # book the true on-chain PnL for live positions (off-thread; waits on the sell)
    if pos.get("mode") == "live" and pos.get("sig"):
        threading.Thread(target=lambda: _finalize_real_pnl(pos), daemon=True).start()
        # reclaim the ATA rent once the sell has emptied the account (off-thread,
        # best-effort — polls balance so it can't race the sell landing)
        threading.Thread(target=lambda: _close_token_ata(pos["mint"]), daemon=True).start()


def _scan_graduating():
    """Second candidate sleeve: open paper positions on coins nearing curve
    completion. Capped per hour so _mark_and_exit's per-position price polls
    can't flood pump.fun, and hard-guarded against re-sniping a mint (the
    event log merges by mint — a re-entry would corrupt its history)."""
    cutoff = time.time() - 3600
    _sniper["grad_opens"] = [t for t in _sniper["grad_opens"] if t > cutoff]
    cap = int(sniper_cfg.get("grad_max_per_hour", 4))
    if len(_sniper["grad_opens"]) >= cap:
        return
    for c in _fetch_graduating(float(sniper_cfg.get("grad_min_progress", 85))):
        if len(_sniper["grad_opens"]) >= cap:
            break
        m = c.get("mint")
        if not _claim_new_mint(m):
            continue
        _open_snipe({
            "mint": m, "symbol": c.get("symbol"), "name": c.get("name"),
            "usd_market_cap": c.get("mcap"),
            "created_timestamp": c.get("created"),
            # graduating-only context the brain snapshot reads straight off the coin
            "progress": c.get("progress"), "sol_in_curve": c.get("sol_in_curve"),
            "replies": c.get("replies"), "live": c.get("live"),
            "koth": c.get("koth"),
        }, source="graduating")
        _sniper["grad_opens"].append(time.time())


# ---- PumpPortal real-time creation feed ----
# The pump.fun frontend API indexes new mints ~2-10s after the create tx; polling
# it every 4s put detection ~5-12s behind the block. PumpPortal's public
# websocket pushes every creation within ~1s, so it is now the primary detection
# path; _sniper_loop's HTTP poll survives only as a slow reconciliation sweep
# that catches anything the socket missed while down.
PUMPPORTAL_WS = "wss://pumpportal.fun/api/data"
_FLOW = None    # flow_source.FlowSource — per-mint trade prints for smart_exit
WS_RECONNECT_MIN, WS_RECONNECT_MAX = 2, 60

try:
    from websocket import create_connection, WebSocketTimeoutException
except Exception as _ws_import_err:     # missing dep => poller-only, sniper unharmed
    create_connection = None
    print(f"[SNIPER-WS] disabled — websocket-client import failed: {_ws_import_err}")


def _ws_feed_on():
    return create_connection is not None and bool(sniper_cfg.get("ws_enabled", True))


_sol_price_cache = {"t": 0.0, "v": None}

def _sol_price_usd(max_age=60):
    """SOL/USD via the Jupiter price API, cached 60s (PumpPortal reports market
    cap in SOL; positions store it in USD). _sniper_loop refreshes it ahead of
    expiry (max_age=40) so the detection→open path never pays the fetch."""
    if time.time() - _sol_price_cache["t"] > max_age:
        try:
            v = _batch_prices([SOL_MINT]).get(SOL_MINT)
            if v:
                _sol_price_cache.update(t=time.time(), v=v)
        except Exception:
            pass
    return _sol_price_cache["v"]


NOPREBUY_PATH = os.path.join(HERE, "noprebuy_launches.jsonl")
# dual-policy exit rows (smart vs fixed on the same trade) — smart_exit.py
EXIT_COMPARE_PATH = os.path.join(HERE, "exit_compare.jsonl")
# rows whose position never received a trade print: kept for forensics, kept OUT
# of the scoreboard — they measure a blind timeout, not the smart policy.
EXIT_COMPARE_QUARANTINE_PATH = os.path.join(HERE, "exit_compare_blind.jsonl")
# pyramid ghost vs the real (fixed) arm on the same trade — pyramid.py
PYRAMID_COMPARE_PATH = os.path.join(HERE, "pyramid_compare.jsonl")
PYRAMID_QUARANTINE_PATH = os.path.join(HERE, "pyramid_blind.jsonl")


class _FlowFanout:
    """One websocket, two consumers (smart_exit + pyramid).

    FlowSource drives a single engine via drain_ws / record_trade /
    active_mints. Both engines implement that interface, so this tees the feed
    instead of opening a second logsSubscribe connection.

    The unsubscribe rule is the important part: smart_exit releases a mint when
    its dual-policy row is complete (~35s for a copy-target position), but the
    pyramid ghost may still be laddering — its first add is at 48s. Forwarding
    smart_exit's unsubscribe would blind the ghost right before its first
    decision, which is the bug this class exists to prevent. So an unsubscribe
    is only passed through once NEITHER engine still wants the mint.
    """

    def __init__(self):
        self._engines = (_smx_mod.ENGINE, _pyr_mod.ENGINE)

    def record_trade(self, ev, ignore_trader=None):
        for e in self._engines:
            try:
                e.record_trade(ev, ignore_trader=ignore_trader)
            except Exception:
                pass

    def active_mints(self):
        out = set()
        for e in self._engines:
            try:
                out.update(e.active_mints())
            except Exception:
                pass
        return list(out)

    def drain_ws(self):
        ops, wanted = [], set(self.active_mints())
        for e in self._engines:
            try:
                ops.extend(e.drain_ws())
            except Exception:
                pass
        out = []
        for method, mint in ops:
            if method == "unsubscribeTokenTrade" and mint in wanted:
                continue        # the other engine is still using it
            out.append((method, mint))
        return out


def _journal_pyramid_row(row):
    """Route one pyramid ghost row to its scoreboard or to quarantine.

    Same quarantine rule as _journal_compare_row and for the same reason: the
    ladder's alive-test is gated on flow evidence, so a row whose mint never
    delivered a print did not test this policy — it tested the fall-through.
    Those rows would otherwise look like free wins, because a never-armed
    position holds its lone probe to the age backstop and books whatever the
    mark says. Keep them out of the scoreboard and make it visible.
    """
    pyr = row.get("pyr") or {}
    if not pyr.get("armed"):
        _sniper["pyr_blind"] = _sniper.get("pyr_blind", 0) + 1
        _append_jsonl(PYRAMID_QUARANTINE_PATH, row)
        return "quarantine"
    if not row.get("fixed"):
        # settled without the real arm reporting (reap) — no pair, no verdict
        _append_jsonl(PYRAMID_QUARANTINE_PATH, row)
        return "quarantine"
    _append_jsonl(PYRAMID_COMPARE_PATH, row)
    return "compare"


def _journal_compare_row(row):
    """Route one dual-policy row to the scoreboard or to quarantine.

    A position that saw zero trade prints never armed creator-sell, whale-dump,
    flow-flip, stall or gap-guard — `_decide_smart` gates all five behind
    `if seen:`. Scoring it ranks a blind 15-min timeout against the 35s ladder
    and files the result as a policy verdict; that is precisely how the
    07-24..07-27 scoreboard produced "smart loses to fixed" while the feed was
    dead. Keep those rows out of exit_compare.jsonl and make the failure loud.

    Returns "compare" or "quarantine" (callers ignore it; tests do not).
    """
    if row.get("reaped"):
        _sniper["last_error"] = (
            f"REAPED: {row.get('symbol') or (row.get('mint') or '?')[:8]} "
            f"force-closed ({row['reaped']}) — no usable outcome, quarantined")
        print(f"[SMX] {_sniper['last_error']}")
        _append_jsonl(EXIT_COMPARE_QUARANTINE_PATH, row)
        return "quarantine"
    if not row.get("flow_prints"):
        _sniper["flow_blind_closes"] = _sniper.get("flow_blind_closes", 0) + 1
        _sniper["last_error"] = (
            f"FLOW DEAD: {row.get('symbol') or (row.get('mint') or '?')[:8]} "
            f"closed with 0 trade prints — smart-exit is running blind, row "
            f"quarantined (n={_sniper['flow_blind_closes']})")
        print(f"[FLOW] {_sniper['last_error']}")
        _append_jsonl(EXIT_COMPARE_QUARANTINE_PATH, row)
        return "quarantine"
    _append_jsonl(EXIT_COMPARE_PATH, row)
    return "compare"
# /api/smart_exit scoreboard cache — invalidated on mtime or after 15s
_SMX_BOARD_CACHE = {"t": 0.0, "mtime": 0.0, "board": None}


def _log_noprebuy(ev):
    """Shadow-journal a creation that left a front-runnable gap.

    Measured 2026-07-16: 94% of pump.fun launches bundle create+buy into ONE
    atomic tx (our copy target: 14/14), so the dev owns ~9.75% of supply before
    the mint is visible and no amount of speed can put us first — atomicity is
    not a latency problem. The remaining ~6% (initialBuy == 0) leave a real gap,
    and are the ONLY launches where "be first" is mechanically possible.

    This records candidates and nothing else — no position, no SOL, no gating.
    _noprebuy_report.py scores them later against what the price actually did.
    Trading these is a separate decision that needs this data first."""
    # Require a real creation payload FIRST. `initialBuy` is absent on socket
    # acks/heartbeats and on non-create traffic, and `float(None or 0) == 0`
    # would file those as front-runnable launches — poisoning the one dataset
    # this exists to build. (Seen live: a USDC event and two empty acks.) The
    # caller's txType check is deliberately permissive for the sniper path, so
    # do NOT rely on it — demand txType == "create" explicitly here.
    if ev.get("txType") != "create":
        return
    if not ev.get("mint") or not ev.get("signature"):
        return
    if float(ev.get("initialBuy") or 0) > 0:
        return
    _append_jsonl(NOPREBUY_PATH, {
        "ts": round(time.time(), 3),
        "mint": ev.get("mint"),
        "symbol": ev.get("symbol"),
        "name": ev.get("name"),
        # the create-tx signer. NOT proof there is no dev — a creator that
        # skipped the bundled buy can still buy in the very next tx and dump;
        # the report has to check that before any of this counts as an edge.
        "creator": ev.get("traderPublicKey"),
        "create_sig": ev.get("signature"),
        "mcap_sol": ev.get("marketCapSol"),
        "sol_amount": ev.get("solAmount"),
        "bonding_curve": ev.get("bondingCurveKey"),
        "pool": ev.get("pool"),
    })


def _ws_event_coin(ev):
    """Map a PumpPortal create event onto the pump.fun coin shape _open_snipe
    and the brain snapshot expect. Metadata the frontend API would eventually
    carry (description, socials) is absent here — it was almost always still
    null this early anyway."""
    mcap_sol, px = ev.get("marketCapSol"), _sol_price_usd()
    return {
        "mint": ev.get("mint"),
        "symbol": ev.get("symbol"), "name": ev.get("name"),
        "usd_market_cap": round(mcap_sol * px, 2) if (mcap_sol and px) else None,
        "created_timestamp": time.time() * 1000,
        # the create tx itself — lets _snipe_latency_report.py compute the TRUE
        # block→detection lag from on-chain blockTime instead of guessing
        "create_sig": ev.get("signature"),
    }


def _pumpportal_ws_loop():
    """subscribeNewToken delivers EVERY pump.fun creation; we filter for the
    copy target client-side. Reconnects with capped exponential backoff; the
    recv timeout keeps stop requests responsive."""
    backoff = WS_RECONNECT_MIN
    while not _sniper["stop"]:
        ws = None
        try:
            ws = create_connection(PUMPPORTAL_WS, timeout=10)
            ws.send(json.dumps({"method": "subscribeNewToken"}))
            # NOTE (2026-07-27): subscribeTokenTrade used to be re-armed here.
            # PumpPortal now key-gates it ("only available when connecting with
            # an API key funded with at least 0.02 SOL") and answers with a
            # txType-less message that every branch below silently dropped —
            # the exit engine ran blind for 36 positions. Per-mint flow now
            # comes from the RPC via flow_source.py. subscribeNewToken is still
            # free and is still what detection runs on.
            _sniper["ws_connected"] = True
            backoff = WS_RECONNECT_MIN
            ws.settimeout(5)
            while not _sniper["stop"]:
                # engine (un)subscribes no longer ride this socket — FlowSource
                # owns them against the RPC now (see flow_source.py).
                try:
                    raw = ws.recv()
                except WebSocketTimeoutException:
                    continue
                if not raw:
                    raise ConnectionError("socket closed by server")
                ev = json.loads(raw)
                _sniper["ws_last_event"] = time.time()
                if ev.get("txType") not in (None, "create"):
                    continue
                # Runs BEFORE the target filter on purpose: these launches are by
                # definition not our target's (it bundles every time). Guarded so
                # a logging fault can never take down the detection feed, and it
                # only writes on ~6% of events.
                if sniper_cfg.get("noprebuy_log", True):
                    try:
                        _log_noprebuy(ev)
                    except Exception:
                        pass
                if ev.get("traderPublicKey") != sniper_cfg["target"]:
                    continue
                if not _claim_new_mint(ev.get("mint")):
                    continue
                _sniper["ws_detections"] += 1
                # Hand off IMMEDIATELY: _open_snipe does a price lookup, file
                # I/O and (live) the buy retries — inline it blocked this recv
                # loop for seconds, so a burst of launches (the target spams
                # them) queued behind the first. Detection→open is now µs.
                threading.Thread(target=lambda ev=ev, ts=time.time():
                                 _open_snipe(_ws_event_coin(ev), via="ws",
                                             detected_ts=ts),
                                 daemon=True).start()
        except Exception as e:
            if not _sniper["stop"]:
                _sniper["last_error"] = f"ws: {str(e)[:120]}"
        finally:
            _sniper["ws_connected"] = False
            try:
                if ws:
                    ws.close()
            except Exception:
                pass
        for _ in range(int(backoff * 2)):
            if _sniper["stop"]:
                break
            time.sleep(0.5)
        backoff = min(backoff * 2, WS_RECONNECT_MAX)


# Mark-to-market cadence, decoupled from the (now slow) poll. At 12s a token
# could round-trip an entire pump and dump inside one blind spot; 3s is the
# floor that keeps the per-position pump.fun polls under its rate limit (a 429
# makes _pump_mcap return None, which silently disables TP/SL and leaves the
# position to time out — faster is NOT free here).
# 2x the 15-min position timeout: anything the engine still holds this long
# after detection can never resolve normally. See Engine.reap().
SMX_REAP_AFTER_S = 1800
MARK_SECS = 1   # was 3 — price polls are pooled+off-VPN now (~120ms), so a 1s
                # cadence costs little and shaves up to 2s off every exit decision


def _sniper_loop():
    target = sniper_cfg["target"]
    # seed: treat all existing creations as already seen so we only fire on NEW ones
    try:
        for c in _pump_creations(target):
            _claim_new_mint(c["mint"])
    except Exception as e:
        _sniper["last_error"] = str(e)
    _sniper["started_ts"] = time.time()
    last_poll = last_mark = 0.0
    while not _sniper["stop"]:
        now = time.time()
        if now - last_poll >= float(sniper_cfg["poll_secs"]):
            last_poll = now
            try:
                for c in _pump_creations(sniper_cfg["target"]):
                    if not _claim_new_mint(c["mint"]):
                        continue
                    # only fire on creations after we started watching
                    if (c.get("created_timestamp") or 0) / 1000 >= _sniper["started_ts"] - 5:
                        _open_snipe(c)
                _sniper["last_check"] = time.time()
                _sniper["last_error"] = None
            except Exception as e:
                _sniper["last_error"] = str(e)
        if (sniper_cfg.get("grad_enabled", True)
                and now - _sniper["grad_last_scan"]
                    >= float(sniper_cfg.get("grad_poll_secs", 90))):
            _sniper["grad_last_scan"] = now
            try:
                _scan_graduating()
            except Exception as e:
                _sniper["last_error"] = f"grad: {e}"
        if now - last_mark >= MARK_SECS:
            last_mark = now
            try:
                _mark_and_exit()
            except Exception as e:
                _sniper["last_error"] = str(e)
            # pre-warm the SOL/USD cache here (refresh past 40s of age) so the
            # ws open path always hits a warm cache instead of a blocking fetch
            try:
                _sol_price_usd(max_age=40)
            except Exception:
                pass
        if not _sniper["stop"]:
            time.sleep(0.5)


def sniper_start():
    if _sniper["thread"] and _sniper["thread"].is_alive():
        return
    _sniper["stop"] = False
    all_pos = _reconstruct_positions()
    # never re-snipe a mint we ever traded: the event log merges by mint, so a
    # second entry (possible for graduating coins after a restart resets `seen`)
    # would corrupt that mint's history
    _sniper["seen"].update(p["mint"] for p in all_pos if p.get("mint"))
    opens = [p for p in all_pos if p.get("status") == "open"]
    closed = sorted((p for p in all_pos if p.get("status") == "closed"),
                    key=lambda p: p.get("closed_ts") or p.get("detected_ts") or 0,
                    reverse=True)[:40]           # keep recent closed for display + real-PnL
    _sniper["positions"] = opens + closed
    t = threading.Thread(target=_sniper_loop, daemon=True)
    _sniper["thread"] = t
    t.start()
    if _ws_feed_on():
        wt = threading.Thread(target=_pumpportal_ws_loop, daemon=True)
        _sniper["ws_thread"] = wt
        wt.start()
    # per-mint trade flow for the exit engine (its own socket, off the RPC)
    global _FLOW
    if _FLOW is None:
        _FLOW = _flow_mod.FlowSource(_FlowFanout(), RPC_URL,
                                     ignore_trader=PUBKEY, log=print)
    _FLOW.start()

def sniper_stop():
    _sniper["stop"] = True
    if _FLOW is not None:
        _FLOW.stop()


def sniper_status():
    _reset_day_if_needed()
    pos = sorted(_sniper["positions"], key=lambda p: -p["detected_ts"])
    closed = [p for p in pos if p["status"] == "closed"]
    realized = round(sum(p.get("pnl_sol") or 0 for p in closed), 5)
    wins = sum(1 for p in closed if (p.get("pnl_sol") or 0) > 0)
    # REAL money split: only live closes with an on-chain-booked PnL count here.
    real_closed = [p for p in closed
                   if p.get("mode") == "live" and p.get("real_pnl_sol") is not None]
    realized_real = round(sum(p["real_pnl_sol"] for p in real_closed), 6)
    real_wins = sum(1 for p in real_closed if p["real_pnl_sol"] > 0)
    running = bool(_sniper["thread"] and _sniper["thread"].is_alive())
    try:
        smx = _smx_mod.ENGINE.status()
    except Exception:
        smx = None
    # Pyramid ghost. Surfaced because it is otherwise INVISIBLE in the UI: it
    # creates no position, paper or live, so nothing about it shows up in the
    # positions table and "where are my pyramid buys" has no answer on screen.
    # `live` here is the real-money gate, and it is False unless pyramid_live
    # was set explicitly — the ladder's buys are simulated, not sent.
    try:
        pyr = _pyr_mod.ENGINE.status()
        pyr["live"] = bool(sniper_cfg.get("pyramid_live", False))
        pyr["enabled"] = bool(sniper_cfg.get("pyramid_enabled", True))
        pyr["open_book"] = _pyr_mod.ENGINE.open_book()
    except Exception:
        pyr = None
    return {
        "config": sniper_cfg, "running": running, "smart_exit": smx,
        "pyramid": pyr,
        "last_check": _sniper["last_check"], "last_error": _sniper["last_error"],
        "day_spent": round(_sniper["day_spent"], 4),
        "realized_sol": realized,
        "closed": len(closed), "wins": wins,
        "win_rate": round(wins / len(closed) * 100, 1) if closed else None,
        "realized_real_sol": realized_real,
        "real_closed": len(real_closed),
        "real_wins": real_wins,
        "real_win_rate": round(real_wins / len(real_closed) * 100, 1) if real_closed else None,
        "open": sum(1 for p in pos if p["status"] == "open"),
        "positions": pos[:60],
        "brain": {"enabled": _brain_enabled(), **_brain_counts()},
        "grad": {"enabled": bool(sniper_cfg.get("grad_enabled", True)),
                 "opens_last_hour": len(_sniper["grad_opens"])},
        "ws": {"enabled": _ws_feed_on(),
               "connected": _sniper["ws_connected"],
               "last_event": _sniper["ws_last_event"],
               "detections": _sniper["ws_detections"]},
        # per-mint trade flow (flow_source.py). blind_closes > 0 means positions
        # are resolving with no crowd evidence — smart exits are running dark.
        "flow": {**(_FLOW.status() if _FLOW is not None
                    else {"connected": False, "subscribed_mints": 0,
                          "prints": 0, "last_event": None,
                          "last_error": "not started"}),
                 "blind_closes": _sniper.get("flow_blind_closes", 0)},
        # Which RPC actually sends our buys/sells. Host only — never the api-key,
        # this endpoint is read by the browser. Public mainnet-beta is not a
        # staked connection and silently drops sendTransaction under load, so
        # "am I on the private RPC?" needs to be answerable at a glance.
        "rpc_host": RPC_URL.split("/?")[0].split("//")[-1],
        "rpc_private": "api.mainnet-beta.solana.com" not in RPC_URL,
        # HTTP-RPC failover state (reads only; sends never move). Separate from
        # flow.on_fallback, which is the websocket feed — they fail independently.
        "rpc": rpc_status(),
    }


# ---------------- API ----------------
@app.route("/api/wallet/list")
def api_wallet_list():
    v = _vault_load()
    pubs = [w["pubkey"] for w in v["wallets"]]
    query = pubs + ([PUBKEY] if PUBKEY and PUBKEY not in pubs else [])
    bals = {}
    if query:
        try:  # one batched call for every wallet's lamports
            res = rpc("getMultipleAccounts", [query, {"encoding": "base64"}])
            for p, acc in zip(query, res["value"]):
                bals[p] = (acc["lamports"] / 1e9) if acc else 0.0
        except Exception:
            pass  # balances just show as unknown
    return jsonify({
        "active": PUBKEY,
        "active_sol": bals.get(PUBKEY),
        "wallets": [{"pubkey": w["pubkey"], "created": w["created"],
                     "sol": bals.get(w["pubkey"])}
                    for w in v["wallets"]],
    })


@app.route("/api/wallet/create", methods=["POST"])
def api_wallet_create():
    d = request.json or {}
    try:
        pw = d.get("password", "")
        if len(pw) < 8:
            raise ValueError("password must be at least 8 characters")
        if pw != d.get("password2", ""):
            raise ValueError("passwords do not match — enter it twice")
        kp, priv = wallet_create(pw)
        set_active(kp)
        return jsonify({"ok": True, "pubkey": str(kp.pubkey()),
                        "private_key": priv})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/wallet/logout", methods=["POST"])
def api_wallet_logout():
    clear_active()
    return jsonify({"ok": True})


@app.route("/api/wallet/import", methods=["POST"])
def api_wallet_import():
    d = request.json or {}
    try:
        pw = d.get("password", "")
        if len(pw) < 8:
            raise ValueError("password must be at least 8 characters")
        if pw != d.get("password2", ""):
            raise ValueError("passwords do not match — enter it twice")
        try:
            kp = parse_private_key(d.get("private_key", ""))
        except ValueError as ve:
            raise ValueError(f"invalid private key — {ve}")
        except Exception:
            raise ValueError("invalid private key — expected base58 (Phantom/"
                             "Robinhood export), hex, or JSON byte array")
        wallet_store(kp, pw)
        set_active(kp)
        return jsonify({"ok": True, "pubkey": PUBKEY,
                        "sol_balance": sol_balance()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/wallet/remove", methods=["POST"])
def api_wallet_remove():
    d = request.json or {}
    try:
        pubkey = d.get("pubkey", "")
        if not any(w["pubkey"] == pubkey for w in _vault_load()["wallets"]):
            raise ValueError("wallet not in vault")
        empty, sol = wallet_is_empty(pubkey)
        if not empty:
            # funded wallet: require its password so a stray click can't
            # destroy the only copy of a recoverable key
            try:
                wallet_unlock(pubkey, d.get("password", ""))
            except Exception:
                raise ValueError(
                    f"wallet holds funds ({sol:.4f} SOL or tokens) — "
                    "enter its correct vault password to remove it")
        wallet_remove(pubkey)
        if PUBKEY == pubkey:   # removed wallet was the active signer
            clear_active()
        return jsonify({"ok": True, "removed": pubkey, "was_empty": empty})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/wallet/unlock", methods=["POST"])
def api_wallet_unlock():
    d = request.json or {}
    try:
        kp = wallet_unlock(d.get("pubkey", ""), d.get("password", ""))
        set_active(kp)
        return jsonify({"ok": True, "pubkey": PUBKEY,
                        "sol_balance": sol_balance()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


_search_cache = {}          # lower(query) -> (fetched_at, results)
SEARCH_TTL = 20             # incremental typing re-hits the same prefixes constantly

def _created_ts(iso):
    """ISO string or epoch -> sortable epoch seconds; missing -> -inf (sorts last)."""
    if not iso:
        return float("-inf")
    if isinstance(iso, (int, float)):
        return iso if iso < 1e12 else iso / 1000.0   # tolerate ms epochs
    try:
        return datetime.fromisoformat(str(iso).replace("Z", "+00:00")).timestamp()
    except Exception:
        return float("-inf")


@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify({"ok": True, "results": []})

    if _is_evm(q):
        # one result per chain the token trades on, best-liquidity pair each
        try:
            r = _HTTP.get(f"https://api.dexscreener.com/latest/dex/tokens/{q}",
                          timeout=8, headers=_GT_HEADERS)
            best = {}   # chain -> (liq, pair)
            for p in (r.json() or {}).get("pairs") or []:
                if (p.get("baseToken") or {}).get("address", "").lower() != q.lower():
                    continue
                liq = (p.get("liquidity") or {}).get("usd") or 0
                c = p.get("chainId")
                if c not in best or liq > best[c][0]:
                    best[c] = (liq, p)
            results = []
            for c, (liq, p) in sorted(best.items(), key=lambda kv: -kv[1][0]):
                bt = p.get("baseToken") or {}
                results.append({
                    "mint": q, "symbol": bt.get("symbol") or "?",
                    "name": (bt.get("name") or "") + "  [" + c + "]",
                    "icon": (p.get("info") or {}).get("imageUrl"),
                    "usdPrice": float(p.get("priceUsd") or 0) or None,
                    "mcap": p.get("marketCap") or p.get("fdv"),
                    "liquidity": liq, "holders": None,
                    "createdAt": p.get("pairCreatedAt"), "launchpad": c,
                    "verified": False, "organic": None,
                    "vol24h": (p.get("volume") or {}).get("h24"),
                    "top10pct": None, "mint_off": None, "freeze_off": None})
            return jsonify({"ok": True, "results": results[:8]})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 400

    key = q.lower()
    hit = _search_cache.get(key)
    if hit and time.time() - hit[0] < SEARCH_TTL:
        results = hit[1]
        _prewarm_pools([r["mint"] for r in results[:3]])
        return jsonify({"ok": True, "results": results})

    try:
        # keep-alive session (pooled TCP+TLS) instead of a cold connection per keystroke
        r = _JUP_HTTP.get(f"{JUP}/tokens/v2/search",
                      params={"query": q}, timeout=6)
        results = []
        for t in (r.json() or []):
            s24 = t.get("stats24h") or {}
            audit = t.get("audit") or {}
            results.append({
                "mint": t.get("id"),
                "symbol": t.get("symbol") or "?",
                "name": t.get("name") or "",
                "icon": t.get("icon"),
                "usdPrice": t.get("usdPrice"),
                "mcap": t.get("mcap"),
                "liquidity": t.get("liquidity"),
                "holders": t.get("holderCount"),
                "createdAt": t.get("createdAt")
                             or (t.get("firstPool") or {}).get("createdAt"),
                "launchpad": t.get("launchpad"),
                "verified": bool(t.get("isVerified")),
                "organic": t.get("organicScoreLabel"),
                "vol24h": (s24.get("buyVolume") or 0) + (s24.get("sellVolume") or 0),
                "top10pct": audit.get("topHoldersPercentage"),
                "mint_off": audit.get("mintAuthorityDisabled"),
                "freeze_off": audit.get("freezeAuthorityDisabled"),
            })
        # newest first, then keep the display list bounded
        results.sort(key=lambda x: _created_ts(x["createdAt"]), reverse=True)
        results = results[:8]
        _search_cache[key] = (time.time(), results)
        _prewarm_pools([r["mint"] for r in results[:3]])
        return jsonify({"ok": True, "results": results})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ---------------- discover: coins close to bonding-curve graduation ----------------
# A pump.fun curve holds 793.1M sellable tokens; progress = share already bought.
# Sorting the API by mcap returns stale/migrated artifacts, so instead we scan the
# most recently TRADED incomplete coins and rank by real_token_reserves.
CURVE_TOTAL = 793_100_000_000_000   # raw units, 6 decimals
_discover_cache = {"ts": 0.0, "coins": []}
DISCOVER_TTL = 10

def _fetch_graduating(min_progress=50.0):
    coins, seen = [], set()
    for off in (0, 50, 100):
        try:
            r = _HTTP.get(f"{PUMP_API}/coins",
                             params={"sort": "last_trade_timestamp", "order": "DESC",
                                     "complete": "false", "limit": 50, "offset": off,
                                     "includeNsfw": "false"},
                             timeout=12, headers=_GT_HEADERS)
            batch = r.json() or []
        except Exception:
            continue
        for c in batch:
            m, rt = c.get("mint"), c.get("real_token_reserves")
            if not m or m in seen or rt is None:
                continue
            seen.add(m)
            if c.get("complete") or c.get("pump_swap_pool"):
                continue          # already graduated / migrating to PumpSwap
            prog = (1 - rt / CURVE_TOTAL) * 100
            if prog < min_progress:
                continue
            coins.append({
                "mint": m,
                "symbol": c.get("symbol") or "?",
                "name": c.get("name") or "",
                "image": c.get("image_uri"),
                "progress": round(prog, 1),
                "mcap": c.get("usd_market_cap"),
                "sol_in_curve": round((c.get("real_sol_reserves") or 0) / 1e9, 2),
                "created": c.get("created_timestamp"),
                "last_trade": c.get("last_trade_timestamp"),
                "replies": c.get("reply_count"),
                "live": bool(c.get("is_currently_live")),
                "koth": bool(c.get("king_of_the_hill_timestamp")),
            })
    coins.sort(key=lambda x: -x["progress"])
    return coins

@app.route("/api/discover")
def api_discover():
    now = time.time()
    if now - _discover_cache["ts"] < DISCOVER_TTL:
        return jsonify({"ok": True, "coins": _discover_cache["coins"]})
    try:
        coins = _fetch_graduating()
        _discover_cache["ts"], _discover_cache["coins"] = now, coins
        return jsonify({"ok": True, "coins": coins})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ---------------- chart + order flow data ----------------
GT_ROOT = "https://api.geckoterminal.com/api/v2/networks"
GT = f"{GT_ROOT}/solana"
_GT_HEADERS = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}

# ---------------- EVM tokens (view-only, multi-chain) ----------------
# 0x addresses (Robinhood Chain, Base, Ethereum, …) resolve through
# Dexscreener + GeckoTerminal instead of Jupiter. Charts/flow/price reuse the
# same pool pipeline below; trading stays Solana-only (Jupiter can't route EVM).
import re as _evm_re_mod
EVM_RE = _evm_re_mod.compile(r"^0x[0-9a-fA-F]{40}$")

def _is_evm(addr):
    return bool(addr) and bool(EVM_RE.match(addr))

# dexscreener chainId -> GeckoTerminal network id (identical unless mapped)
GT_NET_MAP = {"ethereum": "eth", "polygon": "polygon_pos", "avalanche": "avax"}

_evm_tok_cache = {}   # addr(lower) -> (ts, info dict or None)
EVM_TOK_TTL = 180

def _evm_token_info(addr):
    """Highest-liquidity pair for a 0x token across all chains, via
    Dexscreener. Returns {chain, gt_net, pool, pair} or None."""
    key = addr.lower()
    hit = _evm_tok_cache.get(key)
    if hit and time.time() - hit[0] < EVM_TOK_TTL:
        return hit[1]
    info = None
    try:
        r = _HTTP.get(f"https://api.dexscreener.com/latest/dex/tokens/{addr}",
                      timeout=8, headers=_GT_HEADERS)
        pairs = (r.json() or {}).get("pairs") or []
        # only pairs where this token is the BASE (priceUsd refers to it)
        pairs = [p for p in pairs
                 if (p.get("baseToken") or {}).get("address", "").lower() == key]
        if pairs:
            best = max(pairs, key=lambda p:
                       (p.get("liquidity") or {}).get("usd") or 0)
            chain = best.get("chainId")
            info = {"chain": chain,
                    "gt_net": GT_NET_MAP.get(chain, chain),
                    "pool": best.get("pairAddress"), "pair": best}
    except Exception:
        pass
    # cache misses too, but never overwrite a previous good hit with None
    if info is not None or hit is None:
        _evm_tok_cache[key] = (time.time(), info)
    return info

def _gt_base(mint):
    """GeckoTerminal network base URL for this token (solana or EVM chain)."""
    if _is_evm(mint):
        info = _evm_token_info(mint)
        if not info:
            raise LookupError("no DEX pool yet")
        return f"{GT_ROOT}/{info['gt_net']}"
    return GT

_pool_cache = {}
_pool_locks = {}                 # per-mint single-flight locks
_pool_locks_guard = threading.Lock()

def _pool_lock(mint):
    with _pool_locks_guard:
        lk = _pool_locks.get(mint)
        if lk is None:
            lk = _pool_locks[mint] = threading.Lock()
        return lk

def _pool_gt(mint):
    r = _HTTP.get(f"{GT}/tokens/{mint}/pools", params={"page": 1},
                     timeout=6, headers=_GT_HEADERS)
    data = (r.json() or {}).get("data") or []
    if not data:
        return None
    best = max(data, key=lambda p:
               float(p["attributes"].get("reserve_in_usd") or 0))
    return best["attributes"]["address"]

def _pool_dex(mint):
    r = _HTTP.get(f"https://api.dexscreener.com/token-pairs/v1/solana/{mint}",
                     timeout=6, headers=_GT_HEADERS)
    pairs = r.json() or []
    if not pairs:
        return None
    return max(pairs, key=lambda p:
               (p.get("liquidity") or {}).get("usd") or 0)["pairAddress"]

def _pool_for(mint):
    """Highest-liquidity pool address for a mint. Races GeckoTerminal and
    dexscreener concurrently (preferring GT's higher-liquidity pick) and
    single-flights, so overlapping chart+flow requests resolve the pool once —
    and the dexscreener fallback adds ~no latency because it ran in parallel."""
    if _is_evm(mint):                            # EVM: dexscreener already picked
        info = _evm_token_info(mint)             # the pool; own cache handles TTL
        return info["pool"] if info else None
    if mint in _pool_cache:
        return _pool_cache[mint]
    with _pool_lock(mint):
        if mint in _pool_cache:                  # filled while we waited on the lock
            return _pool_cache[mint]
        out = {}
        def run(name, fn):
            try:
                out[name] = fn(mint)
            except Exception:
                out[name] = None
        tg = threading.Thread(target=run, args=("gt", _pool_gt))
        td = threading.Thread(target=run, args=("dx", _pool_dex))
        tg.start(); td.start()
        tg.join(6)                               # prefer GT when it answers in time
        pool = out.get("gt")
        if not pool:
            td.join(6)                           # dexscreener already in flight — cheap
            pool = out.get("dx")
        if pool:
            _pool_cache[mint] = pool
        return pool


def _prewarm_pools(mints):
    """Resolve pool addresses in the background so the chart opens instantly."""
    todo = [m for m in mints if m and m not in _pool_cache]
    if todo:
        threading.Thread(
            target=lambda: [_pool_for(m) for m in todo], daemon=True).start()


import calendar

_trades_cache = {}   # pool -> (fetched_at, data); shared by flow + 15s candles
_trades_locks = {}
_trades_locks_guard = threading.Lock()
TRADES_TTL = 12      # was 8; flow polls every 10s, so 12s makes every 2nd poll a hit

def _trades_lock(pool):
    with _trades_locks_guard:
        lk = _trades_locks.get(pool)
        if lk is None:
            lk = _trades_locks[pool] = threading.Lock()
        return lk

def _trades_for(pool, base=GT):
    cached = _trades_cache.get(pool)
    if cached and time.time() - cached[0] < TRADES_TTL:
        return cached[1]
    with _trades_lock(pool):                     # single-flight: coalesce concurrent misses
        cached = _trades_cache.get(pool)
        if cached and time.time() - cached[0] < TRADES_TTL:
            return cached[1]
        r = _HTTP.get(f"{base}/pools/{pool}/trades", timeout=6, headers=_GT_HEADERS)
        if r.status_code == 429:
            raise RuntimeError("rate limited by chart data provider — retrying shortly")
        data = (r.json() or {}).get("data") or []
        _trades_cache[pool] = (time.time(), data)
        return data


def _candles_from_trades(mint, pool, secs):
    """Sub-minute candles built from the raw trades feed."""
    data = _trades_for(pool, _gt_base(mint))
    # EVM addresses are checksummed in the UI but lowercase in GT's feed
    tgt = mint.lower() if _is_evm(mint) else mint
    buckets = {}
    for d in data:
        a = d["attributes"]
        try:
            ts = calendar.timegm(time.strptime(
                a["block_timestamp"], "%Y-%m-%dT%H:%M:%SZ"))
        except Exception:
            continue
        frm = a.get("from_token_address") or ""
        price = float((a.get("price_from_in_usd")
                       if (frm.lower() if _is_evm(mint) else frm) == tgt
                       else a.get("price_to_in_usd")) or 0)
        if price <= 0:
            continue
        usd = float(a.get("volume_in_usd") or 0)
        t = ts // secs * secs
        b = buckets.get(t)
        if b is None:
            # [t, o, h, l, c, vol] + first/last trade ts to order open/close
            buckets[t] = [t, price, price, price, price, usd, ts, ts]
        else:
            b[2] = max(b[2], price)
            b[3] = min(b[3], price)
            b[5] += usd
            if ts < b[6]:
                b[6] = ts; b[1] = price
            if ts >= b[7]:
                b[7] = ts; b[4] = price
    return [b[:6] for b in sorted(buckets.values())]


TF_MAP = {"1m": ("minute", 1, 60), "5m": ("minute", 5, 300),
          "15m": ("minute", 15, 900), "1h": ("hour", 1, 3600)}
# coarse timeframes barely move between polls — cache them longer so we don't
# burn the free-tier rate budget (and trip 429s) re-fetching unchanged data.
CHART_TTL = {"15s": 8, "1m": 10, "5m": 20, "15m": 30, "1h": 60}
_chart_cache = {}       # (mint, tf) -> (fetched_at, payload)   [non-empty only]
_chart_last_good = {}   # (mint, tf) -> last non-empty payload  [served stale]
_chart_refreshing = set()
_chart_refresh_guard = threading.Lock()


def _chart_fetch(mint, tf):
    """Fetch + cache candles for (mint, tf). Returns the payload or raises."""
    key = (mint, tf)
    pool = _pool_for(mint)
    if not pool:
        raise LookupError("no DEX pool yet")
    if tf == "15s":
        candles, secs = _candles_from_trades(mint, pool, 15), 15
    else:
        unit, agg, secs = TF_MAP.get(tf, TF_MAP["1m"])
        r = _HTTP.get(f"{_gt_base(mint)}/pools/{pool}/ohlcv/{unit}",
                         params={"aggregate": agg, "limit": 150},
                         timeout=6, headers=_GT_HEADERS)
        if r.status_code == 429:
            raise RuntimeError(
                "rate limited by chart data provider — retrying shortly")
        rows = (((r.json() or {}).get("data") or {})
                .get("attributes") or {}).get("ohlcv_list") or []
        candles = sorted([int(x[0])] + [float(v) for v in x[1:6]]
                         for x in rows)
    # An empty result is a soft failure (429 body, provider glitch): don't cache
    # it — the caller keeps serving last-good instead of blanking the chart.
    if not candles:
        raise RuntimeError("no candles returned by data provider")
    payload = {"ok": True, "pool": pool, "tf": tf, "secs": secs,
               "candles": candles}
    _chart_cache[key] = (time.time(), payload)
    _chart_last_good[key] = payload
    return payload


def _chart_refresh_bg(mint, tf):
    """Refresh (mint, tf) in the background, at most one refresh per key."""
    key = (mint, tf)
    with _chart_refresh_guard:
        if key in _chart_refreshing:
            return
        _chart_refreshing.add(key)
    def run():
        try:
            _chart_fetch(mint, tf)
        except Exception:
            pass
        finally:
            with _chart_refresh_guard:
                _chart_refreshing.discard(key)
    threading.Thread(target=run, daemon=True).start()


@app.route("/api/chart/<mint>")
def api_chart(mint):
    tf = request.args.get("tf", "1m")
    key = (mint, tf)
    cached = _chart_cache.get(key)
    if cached and time.time() - cached[0] < CHART_TTL.get(tf, 10):
        return jsonify(cached[1])
    # Stale-while-revalidate: once we have any good candles for this key, paint
    # them instantly and refresh in the background — the UI never blocks on a
    # slow or rate-limited provider. priceTick keeps the last price live.
    good = _chart_last_good.get(key)
    if good:
        _chart_refresh_bg(mint, tf)
        return jsonify(dict(good, stale=True))
    # First open for this key: nothing cached yet, so fetch synchronously.
    try:
        return jsonify(_chart_fetch(mint, tf))
    except LookupError:
        return jsonify({"ok": False, "error": "no DEX pool yet"}), 404
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/flow/<mint>")
def api_flow(mint):
    """Recent trades (order flow) for the token's top pool."""
    pool = _pool_for(mint)
    if not pool:
        return jsonify({"ok": False, "error": "no DEX pool yet"}), 404
    try:
        data = _trades_for(pool, _gt_base(mint))
        tgt = mint.lower() if _is_evm(mint) else mint
        trades, buy_usd, sell_usd, buys, sells = [], 0.0, 0.0, 0, 0
        for d in data[:120]:
            a = d["attributes"]
            usd = float(a.get("volume_in_usd") or 0)
            side = a.get("kind") or "?"
            frm = a.get("from_token_address") or ""
            price = float((a.get("price_from_in_usd")
                           if (frm.lower() if _is_evm(mint) else frm) == tgt
                           else a.get("price_to_in_usd")) or 0)
            trades.append({"ts": a.get("block_timestamp"), "side": side,
                           "usd": round(usd, 2), "price": price})
            if side == "buy":
                buy_usd += usd; buys += 1
            else:
                sell_usd += usd; sells += 1
        return jsonify({"ok": True, "trades": trades,
                        "buy_usd": round(buy_usd), "sell_usd": round(sell_usd),
                        "buys": buys, "sells": sells})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/price/<mint>")
def api_price(mint):
    if _is_evm(mint):
        try:
            info = _evm_token_info(mint)
            if not info:
                return jsonify({"ok": False, "error": "no price"}), 404
            r = _HTTP.get("https://api.geckoterminal.com/api/v2/simple/networks/"
                          f"{info['gt_net']}/token_price/{mint}",
                          timeout=8, headers=_GT_HEADERS)
            prices = ((((r.json() or {}).get("data") or {})
                       .get("attributes") or {}).get("token_prices") or {})
            v = prices.get(mint.lower())
            if v is None:
                return jsonify({"ok": False, "error": "no price"}), 404
            return jsonify({"ok": True, "price": float(v), "ts": time.time()})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 400
    try:
        r = _JUP_HTTP.get(f"{JUP}/price/v3", params={"ids": mint}, timeout=8)
        p = (r.json() or {}).get(mint) or {}
        if p.get("usdPrice") is None:
            return jsonify({"ok": False, "error": "no price"}), 404
        return jsonify({"ok": True, "price": float(p["usdPrice"]),
                        "ts": time.time()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/prices")
def api_prices():
    """usdPrice for ?mints=a,b,c — Jupiter only, ZERO RPC.

    Exists so the trade box can watch TP/SL at a few seconds' resolution
    without that cadence landing on the scarce Helius key: /api/holdings is a
    trade-class account scan, this is one batched HTTP call. Amounts change
    only when you trade; prices change constantly, so they poll separately.
    """
    mints = [m.strip() for m in (request.args.get("mints") or "").split(",") if m.strip()]
    if not mints:
        return jsonify({"ok": True, "prices": {}, "sol_price": None})
    px = _batch_prices(mints[:100] + [SOL_MINT])
    return jsonify({"ok": True,
                    "prices": {m: px.get(m) for m in mints},
                    "sol_price": px.get(SOL_MINT)})


@app.route("/api/holdings")
def api_holdings():
    try:
        return jsonify({"ok": True, **get_holdings()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ---------------- TP / SL executor ----------------
# The pop-out used to watch TP/SL in the browser and only ALERT. A stop-loss
# whose liveness depends on a Document-PiP window staying open is not a
# stop-loss, so the watching and the selling now happen here and the window is
# just a control surface. tpsl.py's header has the audit that motivated it.
TPSL_CFG_PATH = os.path.join(HERE, "tpsl_config.json")


class _TpslDeps:
    """The executor's whole I/O surface, so tpsl.py imports nothing from here
    and _tpsl_test.py can substitute a fake wallet wholesale."""

    def holdings(self):
        return get_holdings()

    def prices(self, mints):
        px = _batch_prices_ts(list(mints) + [SOL_MINT])
        return {m: px.get(m) for m in mints}, px.get(SOL_MINT)

    def quote_sol(self, mint, amount_raw):
        """SOL proceeds for dumping this exact size, right now.

        Jupiter's /price/v3 is a cached aggregate: measured 2026-08-13 it moved
        once every ~6s on an active pump.fun mint and not at all over 40s on a
        large cap. A quote is recomputed per request off pool state, so it is
        both far fresher AND the routed, impact-inclusive number the sell will
        actually get -- the right thing to compare a threshold against.

        Called only for mints the executor has already screened as close to
        firing (see tpsl.Executor.refresh_quotes), because unlike the batched
        price path this costs one request per mint and shares a rate limit with
        the quotes real trades need.
        """
        try:
            amount_raw = int(amount_raw)
        except (TypeError, ValueError):
            return None
        if amount_raw <= 0:
            return None
        q = jup_quote(mint, SOL_MINT, amount_raw, SLIPPAGE_BPS, timeout=2.0)
        out = int(q.get("outAmount") or 0)
        return (out / 1e9) if out > 0 else None

    def sell(self, mint, pct, slippage_bps, fee_mode):
        # A TP/SL fire is a sell the box armed: it gets the scarce key.
        with sellbox_scope():
            return self._sell(mint, pct, slippage_bps, fee_mode)

    def _sell(self, mint, pct, slippage_bps, fee_mode):
        raw, ui, _ = token_balance(mint)
        amount = raw if pct >= 100 else int(raw * pct / 100)
        if amount <= 0:
            raise ValueError("no balance to sell")

        def settled(status, err):
            # THE HALF THAT WAS MISSING. wait=False returns at "submitted", so
            # without this the executor never learned that a sell reverted --
            # it kept the side latched as fired while still holding the whole
            # position. Three of the five fires on 2026-08-13 went that way.
            try:
                if TPSL is not None:
                    TPSL.sell_settled(mint, status, err)
            except Exception:
                pass

        # wait=False for the same reason the sniper uses it: never block the
        # decision loop for up to 45s on confirmation. The journal row still
        # lands with the true status from the confirm thread.
        return execute_swap(mint, SOL_MINT, amount,
                            int(slippage_bps or SLIPPAGE_BPS),
                            mev_protect=(fee_mode == "mev"),
                            wait=False, fee_mode=fee_mode, on_status=settled)

    def lock(self, mint):
        return mint_lock(mint)

    def now(self):
        return time.time()

    def log(self, msg):
        print(msg)


def tpsl_start():
    global TPSL
    if TPSL is None:
        # price_every_s is the cheap batched screen; tick_every_s is how often a
        # near-threshold position is re-decided off a live quote. Jupiter's
        # price feed has a ~6s refresh floor, so screening faster than 3s buys
        # nothing -- the speed comes from the 1s quote path, not from polling
        # the cache harder.
        TPSL = _tpsl_mod.Executor(_TpslDeps(), TPSL_CFG_PATH,
                                  price_every_s=3.0, tick_every_s=1.0)
    TPSL.start()
    return TPSL


@app.route("/api/tpsl")
def api_tpsl():
    if TPSL is None:
        return jsonify({"ok": False, "error": "executor not running"}), 503
    # `book` is additive and exists so the pop-out paints its LIVE/PAPER skin
    # from the server's answer rather than from a local variable. The dangerous
    # confusion runs both ways: spending real SOL while believing it is paper,
    # and believing a stop-loss exists on a position that only paper holds.
    return jsonify({**TPSL.status(), "book": "live"})


# ---------------- paper book ----------------
# A second, simulated book behind the same pop-out window. See paper.py's header
# for why this is a separate module and not a `paper=True` flag on execute_swap.
#
# THE SEPARATION IS STRUCTURAL, NOT A CHECK. There is no branch anywhere that
# decides whether to send a real transaction: /api/paper/* reaches PaperBook,
# which has no keypair, no signer and no RPC, and /api/buy|/api/sell reach
# execute_swap, which never looks at the paper book. A mode mix-up therefore
# cannot spend real SOL — the worst it can do is simulate a trade you meant to
# make, or refuse one you did.
PAPER_JOURNAL_PATH = os.path.join(HERE, "paper_trades.jsonl")
PAPER_STATE_PATH = os.path.join(HERE, "paper_state.json")
PAPER_TPSL_CFG_PATH = os.path.join(HERE, "tpsl_paper_config.json")


class _PaperDeps:
    """Read-only market data plus a clock. No wallet, by construction."""

    def quote(self, in_mint, out_mint, amount_raw, slippage_bps):
        # The SAME quote call the live path uses, at the same size, so price
        # impact on a thin pool is simulated rather than assumed away.
        return jup_quote(in_mint, out_mint, amount_raw, slippage_bps)

    def prices(self, mints):
        px = _batch_prices(list(mints) + [SOL_MINT])
        return {m: px.get(m) for m in mints}, px.get(SOL_MINT)

    def meta(self, mint):
        return token_meta(mint)

    def tip_lamports(self):
        return jito_tip()

    def now(self):
        return time.time()

    def log(self, msg):
        print(msg)


class _PaperTpslDeps:
    """The paper executor's I/O. Every method is the paper analogue of
    _TpslDeps — including lock(), which MUST NOT be the live mint_lock: a
    simulated sell holding the real interlock could defer a real TP/SL fire on
    the same mint."""

    def holdings(self):
        return PAPER.holdings()

    def prices(self, mints):
        px = _batch_prices(list(mints) + [SOL_MINT])
        return {m: px.get(m) for m in mints}, px.get(SOL_MINT)

    def sell(self, mint, pct, slippage_bps, fee_mode):
        return PAPER.sell(mint, pct, slippage_bps, fee_mode,
                          default_slippage_bps=SLIPPAGE_BPS)

    def lock(self, mint):
        return PAPER.mint_lock(mint)

    def now(self):
        return time.time()

    def log(self, msg):
        print(msg)


def paper_start():
    global PAPER, PAPER_TPSL
    if PAPER is None:
        PAPER = _paper_mod.PaperBook(_PaperDeps(), PAPER_JOURNAL_PATH,
                                     PAPER_STATE_PATH)
    if PAPER_TPSL is None:
        # book="paper" tags this executor's rows in the shared tpsl_fires.jsonl
        # -- both books journal to the same file and paper fills are optimistic,
        # so anything scored off it has to filter on that field first.
        PAPER_TPSL = _tpsl_mod.Executor(_PaperTpslDeps(), PAPER_TPSL_CFG_PATH,
                                        book="paper")
    PAPER_TPSL.start()
    return PAPER


def _paper_ready():
    if PAPER is None or PAPER_TPSL is None:
        return jsonify({"ok": False, "error": "paper book not running"}), 503
    return None


def _paper_status():
    """The pop-out renders off this. Identical in shape to /api/tpsl plus a
    `book` discriminator and the book's own totals, so the window can paint its
    mode from the SERVER's answer instead of from what was last clicked —
    the same rule paintArm() already follows for ARMED."""
    st = PAPER_TPSL.status()
    st["book"] = "paper"
    st["paper"] = PAPER.meta()
    st["stats"] = PAPER.stats()
    return st


@app.route("/api/paper/tpsl")
def api_paper_tpsl():
    bad = _paper_ready()
    return bad or jsonify(_paper_status())


@app.route("/api/paper/tpsl/arm", methods=["POST"])
def api_paper_tpsl_arm():
    """No require_wallet(), unlike the live arm. Arming a book that cannot
    spend needs no key, and paper working with the wallet locked is the point.
    Note this touches PAPER_TPSL.cfg only — it cannot reach the live executor."""
    bad = _paper_ready()
    if bad:
        return bad
    want = bool((request.json or {}).get("armed"))
    PAPER_TPSL.cfg["armed"] = want
    PAPER_TPSL.event("ARMED — paper auto-sell is live" if want
                     else "DISARMED — paper alerts only", "warn")
    return jsonify({"ok": True, "armed": want, "book": "paper"})


@app.route("/api/paper/tpsl/config", methods=["POST"])
def api_paper_tpsl_config():
    bad = _paper_ready()
    if bad:
        return bad
    d = request.json or {}
    for k in ("tp_pct", "sl_pct", "tp_sell_pct", "sl_sell_pct",
              "confirm_ticks", "max_price_age_s", "cooldown_s",
              "max_fires_per_day", "slippage_bps", "fee_mode",
              # the quote-trigger knobs belong here too: if the live quote path
              # misbehaves it has to be switchable from the box, because the
              # only alternative is a restart and a restart disarms the executor
              "quote_trigger", "quote_band_frac", "retry_after_fail_s"):
        if k in d:
            PAPER_TPSL.cfg[k] = d[k]
    if "per" in d and isinstance(d["per"], dict):
        PAPER_TPSL.cfg["per"] = d["per"]
    for st in PAPER_TPSL._pos.values():        # see api_tpsl_config
        st.fired_kind, st.streak, st.side = None, 0, None
    PAPER_TPSL.save()
    return jsonify(_paper_status())


@app.route("/api/paper/token/<mint>")
def api_paper_token(mint):
    """Lookup for the paper quick-buy field. Same data as /api/token but with
    no require_wallet() and the PAPER holding rather than the on-chain one, so
    the box is usable with the wallet locked."""
    bad = _paper_ready()
    if bad:
        return bad
    if _is_evm(mint):
        return jsonify({"ok": False, "evm": True,
                        "error": "EVM token — the paper book is Solana-only"}), 400
    try:
        meta = token_meta(mint)
        lot = PAPER.lots().get(mint)
        # The BOOK's decimals first, not the lookup's. They were recorded on the
        # buy that opened the position, so they exist even when Jupiter has
        # since dropped the token (which is exactly what happens to a dying
        # memecoin -- see the /api/token dexscreener fallback). Falling back to
        # the lookup meant a held position reported "holding 0", and 0 in the
        # one field you check before sizing a trade is worse than blank.
        # _dec is read directly rather than through _decimals(): that helper
        # falls back to a token_meta() lookup, and we already did one above.
        # lots() has just populated the cache from the journal rows.
        dec = PAPER._dec.get(mint, meta.get("decimals"))
        bal = (lot["raw"] / (10 ** dec)) if (lot and dec is not None) else None
        return jsonify({"ok": True, "mint": mint, **meta, "paper": True,
                        "balance": bal,
                        "balance_raw": lot["raw"] if lot else 0,
                        "sol_balance": PAPER.sol_balance(),
                        "wallet": _paper_mod.PAPER_WALLET})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/paper/buy", methods=["POST"])
def api_paper_buy():
    bad = _paper_ready()
    if bad:
        return bad
    d = request.json or {}
    try:
        mint = d["mint"].strip()
        if _is_evm(mint):
            raise ValueError("EVM token — the paper book is Solana-only")
        res = PAPER.buy(mint, d.get("sol"),
                        int(d.get("slippageBps", SLIPPAGE_BPS)),
                        d.get("feeMode") or "mev")
        # Same reason the live journal hook calls this: a new position must be
        # visible to its executor within a second, not at the next 60s sweep.
        try:
            PAPER_TPSL.refresh_holdings("post-fill")
        except Exception:
            pass
        return jsonify(res)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/paper/sell", methods=["POST"])
def api_paper_sell():
    bad = _paper_ready()
    if bad:
        return bad
    d = request.json or {}
    try:
        mint = d["mint"].strip()
        res = PAPER.sell(mint, d.get("pct"),
                         int(d.get("slippageBps", SLIPPAGE_BPS)),
                         d.get("feeMode") or "mev",
                         default_slippage_bps=SLIPPAGE_BPS)
        try:
            PAPER_TPSL.refresh_holdings("post-fill")
        except Exception:
            pass
        return jsonify(res)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/paper/settings", methods=["POST"])
def api_paper_settings():
    """Fee/fill modelling only. The bankroll is NOT settable here — changing it
    under a live book would restate every past balance; that goes through
    /api/paper/reset, which starts a new epoch."""
    bad = _paper_ready()
    if bad:
        return bad
    try:
        PAPER.settings(**(request.json or {}))
        return jsonify(_paper_status())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/paper/reset", methods=["POST"])
def api_paper_reset():
    """New bankroll, new epoch. Rows are kept: the epoch stops old ones
    counting, so a past run stays auditable in the same file."""
    bad = _paper_ready()
    if bad:
        return bad
    try:
        PAPER.reset((request.json or {}).get("start_sol"))
        PAPER_TPSL.cfg["armed"] = False       # a fresh book starts disarmed
        for st in list(PAPER_TPSL._pos.values()):
            st.fired_kind, st.streak, st.side = None, 0, None
        PAPER_TPSL.refresh_holdings("reset")
        return jsonify(_paper_status())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/ctx")
def api_ctx():
    """Is the decision recorder actually keeping up? `dropped` is the number
    that matters — a dropped capture is indistinguishable from a trade that
    never happened once the data is read back."""
    if CTX is None:
        return jsonify({"ok": False, "error": "recorder not running"}), 503
    st = CTX.status()
    for name, path in (("context", _ctx_mod.CTX_PATH),
                       ("considered", _ctx_mod.CONSIDERED_PATH)):
        try:
            with open(path, "rb") as f:
                st[name + "_rows"] = sum(1 for _ in f)
            st[name + "_bytes"] = os.path.getsize(path)
        except Exception:
            st[name + "_rows"] = 0
    return jsonify({"ok": True, **st})


@app.route("/api/tpsl/arm", methods=["POST"])
def api_tpsl_arm():
    """The only switch that lets this thing spend money. Deliberately its own
    endpoint, and deliberately not persisted -- see Executor.load()."""
    if TPSL is None:
        return jsonify({"ok": False, "error": "executor not running"}), 503
    d = request.json or {}
    want = bool(d.get("armed"))
    if want:
        try:
            require_wallet()
        except Exception as e:
            return jsonify({"ok": False, "error": "wallet locked: %s" % e}), 400
    TPSL.cfg["armed"] = want
    TPSL.event("ARMED — auto-sell is live" if want
               else "DISARMED — alerts only", "warn")
    return jsonify({"ok": True, "armed": want})


@app.route("/api/tpsl/config", methods=["POST"])
def api_tpsl_config():
    if TPSL is None:
        return jsonify({"ok": False, "error": "executor not running"}), 503
    d = request.json or {}
    for k in ("tp_pct", "sl_pct", "tp_sell_pct", "sl_sell_pct",
              "confirm_ticks", "max_price_age_s", "cooldown_s",
              "max_fires_per_day", "slippage_bps", "fee_mode",
              # the quote-trigger knobs belong here too: if the live quote path
              # misbehaves it has to be switchable from the box, because the
              # only alternative is a restart and a restart disarms the executor
              "quote_trigger", "quote_band_frac", "retry_after_fail_s"):
        if k in d:
            TPSL.cfg[k] = d[k]
    if "per" in d and isinstance(d["per"], dict):
        TPSL.cfg["per"] = d["per"]
    # Limits moved -> every position re-evaluates from scratch, so a threshold
    # dragged past the current price does not stay latched from the old one.
    for st in TPSL._pos.values():
        st.fired_kind, st.streak, st.side = None, 0, None
    TPSL.save()
    return jsonify(TPSL.status())


# ---------------- EVM watch-wallets (view-only balance tracking) ----------------
# Watch any 0x address: balances come from each chain's public Blockscout
# instance (keyless), prices from Blockscout's exchange_rate with a
# dexscreener fallback. No keys are stored and nothing can be spent from here.
EVM_WALLETS_PATH = os.path.join(HERE, "evm_wallets.json")
EVM_SCAN_HOSTS = {   # dexscreener chainId -> Blockscout API host
    "ethereum": "eth.blockscout.com",
    "base": "base.blockscout.com",
    "arbitrum": "arbitrum.blockscout.com",
    "optimism": "optimism.blockscout.com",
    "polygon": "polygon.blockscout.com",
    "gnosis": "gnosis.blockscout.com",
    "robinhood": "robinhoodchain.blockscout.com",
}   # BSC has no public Blockscout — its tokens still chart, balances won't scan
EVM_NATIVE_SYM = {"polygon": "POL", "gnosis": "xDAI"}   # everything else: ETH
_evm_wallets_lock = threading.Lock()

def _evm_wallets_load():
    try:
        return json.load(open(EVM_WALLETS_PATH, encoding="utf-8"))
    except Exception:
        return []

def _evm_wallets_save(ws):
    with _evm_wallets_lock:
        with open(EVM_WALLETS_PATH, "w", encoding="utf-8") as f:
            json.dump(ws, f, indent=1)

_evm_coin_px = {}   # chain -> (ts, usd)

def _evm_coin_price(chain):
    hit = _evm_coin_px.get(chain)
    if hit and time.time() - hit[0] < 300:
        return hit[1]
    v = None
    try:
        r = _HTTP.get(f"https://{EVM_SCAN_HOSTS[chain]}/api/v2/stats", timeout=8)
        v = float((r.json() or {}).get("coin_price") or 0) or None
    except Exception:
        pass
    _evm_coin_px[chain] = (time.time(), v)
    return v

def _evm_scan_chain(chain, host, addr):
    """Native + ERC-20 balances for one wallet on one chain via Blockscout."""
    rows = []
    try:
        r = _HTTP.get(f"https://{host}/api/v2/addresses/{addr}", timeout=10)
        if r.status_code == 404:                 # address never seen on chain
            return rows
        cb = int((r.json() or {}).get("coin_balance") or 0)
        if cb > 0:
            ui, px = cb / 1e18, _evm_coin_price(chain)
            rows.append({"chain": chain, "address": None,
                         "symbol": EVM_NATIVE_SYM.get(chain, "ETH"),
                         "name": "native", "icon": None, "native": True,
                         "amount": ui,
                         "usd": round(ui * px, 2) if px else None})
    except Exception:
        pass
    try:
        r = _HTTP.get(f"https://{host}/api/v2/addresses/{addr}/token-balances",
                      timeout=15)
        for tb in (r.json() or []):
            tok = tb.get("token") or {}
            if (tok.get("type") or "ERC-20") != "ERC-20":
                continue                         # NFTs aren't priceable holdings
            try:
                dec = int(tok.get("decimals") or 18)
                ui = int(tb.get("value") or 0) / (10 ** dec)
            except Exception:
                continue
            if ui <= 0:
                continue
            rate = tok.get("exchange_rate")
            taddr = (tok.get("address") or tok.get("address_hash") or "").lower()
            rows.append({"chain": chain, "address": taddr,
                         "symbol": tok.get("symbol") or "?",
                         "name": tok.get("name") or "",
                         "icon": tok.get("icon_url"), "native": False,
                         "amount": ui,
                         "usd": round(ui * float(rate), 2) if rate else None})
    except Exception:
        pass
    return rows

def _evm_fill_prices(rows):
    """Dexscreener-batch price any ERC-20 rows Blockscout couldn't price."""
    todo = [r for r in rows if not r["native"] and r["usd"] is None and r["address"]]
    for i in range(0, len(todo), 20):
        chunk = todo[i:i + 20]
        try:
            r = _HTTP.get("https://api.dexscreener.com/latest/dex/tokens/"
                          + ",".join(x["address"] for x in chunk),
                          timeout=8, headers=_GT_HEADERS)
            pairs = (r.json() or {}).get("pairs") or []
        except Exception:
            continue
        best = {}   # (chain, addr) -> (liq, price)
        for p in pairs:
            k = (p.get("chainId"),
                 (p.get("baseToken") or {}).get("address", "").lower())
            liq = (p.get("liquidity") or {}).get("usd") or 0
            if k not in best or liq > best[k][0]:
                best[k] = (liq, float(p.get("priceUsd") or 0))
        for x in chunk:
            hit = best.get((x["chain"], x["address"]))
            if hit and hit[1]:
                x["usd"] = round(x["amount"] * hit[1], 2)

_evm_scan_cache = {}   # addr(lower) -> (ts, rows)
EVM_SCAN_TTL = 60

def _evm_wallet_scan(addr):
    key = addr.lower()
    hit = _evm_scan_cache.get(key)
    if hit and time.time() - hit[0] < EVM_SCAN_TTL:
        return hit[1]
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=len(EVM_SCAN_HOSTS)) as ex:
        futs = [ex.submit(_evm_scan_chain, c, h, addr)
                for c, h in EVM_SCAN_HOSTS.items()]
        rows = [row for f in futs for row in f.result()]
    _evm_fill_prices(rows)
    rows.sort(key=lambda r: -(r["usd"] or 0))
    _evm_scan_cache[key] = (time.time(), rows)
    return rows

@app.route("/api/evm/wallets")
def api_evm_wallets():
    out = []
    for w in _evm_wallets_load():
        try:
            rows = _evm_wallet_scan(w["address"])
        except Exception:
            rows = []
        out.append({"address": w["address"], "label": w.get("label") or "",
                    "tokens": rows,
                    "total_usd": round(sum(r["usd"] or 0 for r in rows), 2)})
    return jsonify({"ok": True, "wallets": out,
                    "chains": sorted(EVM_SCAN_HOSTS)})

@app.route("/api/evm/wallets/add", methods=["POST"])
def api_evm_wallet_add():
    d = request.json or {}
    addr = (d.get("address") or "").strip()
    if not _is_evm(addr):
        return jsonify({"ok": False, "error": "not a valid 0x address"}), 400
    ws = _evm_wallets_load()
    if any(w["address"].lower() == addr.lower() for w in ws):
        return jsonify({"ok": False, "error": "already watching that address"}), 400
    ws.append({"address": addr, "label": (d.get("label") or "").strip()})
    _evm_wallets_save(ws)
    return jsonify({"ok": True})

@app.route("/api/evm/wallets/remove", methods=["POST"])
def api_evm_wallet_remove():
    d = request.json or {}
    addr = (d.get("address") or "").strip().lower()
    ws = [w for w in _evm_wallets_load() if w["address"].lower() != addr]
    _evm_wallets_save(ws)
    return jsonify({"ok": True})


# ---------------- EVM copy-watcher (Phase 0 — PAPER ONLY) ----------------
import evm_copy

@app.route("/api/evm/copy")
def api_evm_copy():
    return jsonify({"ok": True, **evm_copy.status()})

@app.route("/api/evm/copy/targets", methods=["POST"])
def api_evm_copy_targets():
    d = request.json or {}
    addr = (d.get("address") or "").strip()
    if d.get("remove"):
        evm_copy.cfg["targets"] = [t for t in evm_copy.cfg["targets"]
                                   if t["address"].lower() != addr.lower()]
        evm_copy.save_cfg()
        return jsonify({"ok": True})
    if not _is_evm(addr):
        return jsonify({"ok": False, "error": "not a valid 0x address"}), 400
    if any(t["address"].lower() == addr.lower() for t in evm_copy.cfg["targets"]):
        return jsonify({"ok": False, "error": "already a target"}), 400
    chain = (d.get("chain") or "robinhood").strip()
    if chain not in evm_copy.SCAN_HOSTS:
        return jsonify({"ok": False, "error": f"unsupported chain {chain}"}), 400
    evm_copy.cfg["targets"].append({"address": addr, "chain": chain,
                                    "label": (d.get("label") or "").strip(),
                                    "enabled": True})
    evm_copy.save_cfg()
    return jsonify({"ok": True})

@app.route("/api/evm/copy/toggle", methods=["POST"])
def api_evm_copy_toggle():
    on = bool((request.json or {}).get("enabled"))
    evm_copy.cfg["enabled"] = on
    evm_copy.save_cfg()
    if on:
        evm_copy.start()
    else:
        evm_copy.stop()
    return jsonify({"ok": True, "enabled": on})


# ---------------- EVM trading wallet (encrypted vault) ----------------
# Key management. Storing/unlocking a key does NOT arm any spending: swaps live
# in evm_swap.py and refuse unless cfg['live'] is armed AND the request carries
# an explicit confirm flag. An unlocked wallet on its own spends nothing.
import evm_wallet
import evm_swap

@app.route("/api/evm/wallet")
def api_evm_wallet():
    st = evm_wallet.status()
    # enrich each saved wallet with its live cross-chain balances (60s cached
    # scan, reused from the watch-wallet path) so the vault card shows funds
    for w in st.get("vault") or []:
        try:
            rows = _evm_wallet_scan(w["address"])
            w["tokens"] = rows
            w["total_usd"] = round(sum(r["usd"] or 0 for r in rows), 2)
        except Exception:
            w["tokens"], w["total_usd"] = [], None
    return jsonify({"ok": True, **st})

@app.route("/api/evm/wallet/create", methods=["POST"])
def api_evm_wallet_create():
    pw = (request.json or {}).get("password") or ""
    if len(pw) < 8:
        return jsonify({"ok": False, "error": "password must be 8+ chars"}), 400
    try:
        addr, priv = evm_wallet.create(pw)
        # returned ONCE for backup (localhost only), never persisted in plaintext
        return jsonify({"ok": True, "address": addr, "private_key": priv})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

@app.route("/api/evm/wallet/import", methods=["POST"])
def api_evm_wallet_import():
    d = request.json or {}
    pw, priv = d.get("password") or "", d.get("private_key") or ""
    if len(pw) < 8:
        return jsonify({"ok": False, "error": "password must be 8+ chars"}), 400
    try:
        addr = evm_wallet.store(priv, pw)
        evm_wallet.unlock(addr, pw)
        return jsonify({"ok": True, "address": addr})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

@app.route("/api/evm/wallet/reveal", methods=["POST"])
def api_evm_wallet_reveal():
    d = request.json or {}
    try:
        priv = evm_wallet.reveal(d.get("address") or "", d.get("password") or "")
        return jsonify({"ok": True, "private_key": priv})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

@app.route("/api/evm/wallet/unlock", methods=["POST"])
def api_evm_wallet_unlock():
    d = request.json or {}
    try:
        addr = evm_wallet.unlock(d.get("address") or "", d.get("password") or "")
        return jsonify({"ok": True, "address": addr})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

@app.route("/api/evm/wallet/lock", methods=["POST"])
def api_evm_wallet_lock():
    evm_wallet.lock()
    return jsonify({"ok": True})

@app.route("/api/evm/wallet/remove", methods=["POST"])
def api_evm_wallet_vault_remove():
    try:
        evm_wallet.remove((request.json or {}).get("address") or "")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ---------------- EVM swap (Uniswap V3) ----------------
def _evm_chain_of(mint):
    """Which chain this EVM token trades on, per dexscreener's pool pick."""
    info = _evm_token_info(mint)
    if not info:
        raise LookupError("no DEX pool found for this token")
    return info["chain"]


@app.route("/api/evm/swap/config", methods=["GET", "POST"])
def api_evm_swap_config():
    cfg = evm_swap.load_cfg()
    if request.method == "POST":
        d = request.json or {}
        for k in ("live", "slippage_bps", "max_native_per_trade"):
            if k in d:
                cfg[k] = d[k]
        evm_swap.save_cfg(cfg)
    return jsonify({"ok": True, "cfg": cfg,
                    "chains": sorted(evm_swap.CHAINS)})


@app.route("/api/evm/quote")
def api_evm_quote():
    """Read-only pre-trade quote. Simulates against live chain state."""
    try:
        mint = (request.args.get("mint") or "").strip()
        side = (request.args.get("side") or "").strip()
        amount = float(request.args.get("amount") or 0)
        if not _is_evm(mint):
            raise ValueError("not an EVM token")
        if side not in ("buy", "sell"):
            raise ValueError("side must be buy or sell")
        if amount <= 0:
            raise ValueError("amount must be > 0")
        chain = _evm_chain_of(mint)
        if not evm_swap.is_tradeable_chain(chain):
            raise ValueError(f"{chain} has no verified router — view-only")
        q = evm_swap.quote(chain, mint, side, amount)
        cfg = evm_swap.load_cfg()
        return jsonify({"ok": True, "chain": chain, "live": bool(cfg["live"]),
                        "slippage_bps": cfg["slippage_bps"], **q})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/evm/holdings")
def api_evm_holdings():
    """Token balances of the UNLOCKED EVM wallet, flagged tradeable or not."""
    if not evm_wallet.ADDRESS:
        return jsonify({"ok": True, "address": None, "tokens": []})
    try:
        rows = _evm_wallet_scan(evm_wallet.ADDRESS)
        for r in rows:
            r["tradeable"] = bool(r.get("address")) and \
                evm_swap.is_tradeable_chain(r.get("chain"))
        return jsonify({"ok": True, "address": evm_wallet.ADDRESS,
                        "tokens": rows,
                        "total_usd": round(sum(r["usd"] or 0 for r in rows), 2)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/evm/swap", methods=["POST"])
def api_evm_swap():
    """Execute a REAL swap. Gated three ways: wallet must be unlocked, cfg.live
    must be armed, and the request must carry confirm=true (the UI's dialog).
    Nothing calls this automatically — it fires only on a user click."""
    d = request.json or {}
    try:
        mint = (d.get("mint") or "").strip()
        side = (d.get("side") or "").strip()
        amount = float(d.get("amount") or 0)
        if not _is_evm(mint):
            raise ValueError("not an EVM token")
        if side not in ("buy", "sell"):
            raise ValueError("side must be buy or sell")
        if amount <= 0:
            raise ValueError("amount must be > 0")
        chain = _evm_chain_of(mint)
        res = evm_swap.swap(chain, mint, side, amount,
                            confirm=bool(d.get("confirm")),
                            slippage_bps=d.get("slippageBps"))
        return jsonify({"ok": True, **res})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/evm/tx/<chain>/<tx_hash>")
def api_evm_tx(chain, tx_hash):
    try:
        return jsonify({"ok": True, **evm_swap.tx_status(chain, tx_hash)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/sniper")
def api_sniper():
    return jsonify({"ok": True, **sniper_status()})


try:
    import wallets_db as _wdb_mod   # unified cross-chain wallet registry
except Exception as _wdb_err:
    _wdb_mod = None
    print(f"[WDB] disabled — wallets_db import failed: {_wdb_err}")


@app.route("/api/wallets/db")
def api_wallets_db():
    """Curated registry + every subsystem's wallets, merged read-only.

    Deliberately does NO balance enrichment: /api/evm/wallets scans 7 chains
    per address and needs ~90s for one. Rows here render from JSON instantly;
    holdings stay on the on-demand endpoint.
    """
    if _wdb_mod is None:
        return jsonify({"ok": False, "error": "wallets_db unavailable"}), 200
    board = []
    try:
        if _kol_mod is not None and _KOL is not None:
            board = _KOL.board()
    except Exception:
        board = []
    try:
        rows = _wdb_mod.merged(sniper_cfg=sniper_cfg, kol_board=board)
        return jsonify({"ok": True, "wallets": rows,
                        "summary": _wdb_mod.summary(rows),
                        "kinds": list(_wdb_mod.KINDS),
                        "chains": list(_wdb_mod.CHAINS)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)[:200]}), 200


@app.route("/api/wallets/db/add", methods=["POST"])
def api_wallets_db_add():
    if _wdb_mod is None:
        return jsonify({"ok": False, "error": "wallets_db unavailable"}), 200
    d = request.json or {}
    try:
        row = _wdb_mod.add(d.get("address"), chain=d.get("chain"),
                           kind=(d.get("kind") or "watch"),
                           label=d.get("label") or "",
                           notes=d.get("notes") or "")
        return jsonify({"ok": True, "wallet": row})
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)[:200]}), 400


@app.route("/api/wallets/db/remove", methods=["POST"])
def api_wallets_db_remove():
    """Only ever removes CURATED rows. A derived wallet (sniper target, EVM
    copy target, scraped KOL) is owned by its own subsystem and is silently a
    no-op here rather than being half-deleted from a file we do not own."""
    if _wdb_mod is None:
        return jsonify({"ok": False, "error": "wallets_db unavailable"}), 200
    d = request.json or {}
    try:
        n = _wdb_mod.remove(d.get("address"))
        return jsonify({"ok": True, "removed": n})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)[:200]}), 400


_KOL = None
_KOL_LOCK = threading.Lock()


def _kol_source():
    """Lazy singleton. Started on first /api/kol hit rather than in
    sniper_start(): the leaderboard is useful whether or not the sniper is
    armed, and wiring a scraper into the sniper's startup path would let a
    kolscan outage delay detection."""
    global _KOL
    with _KOL_LOCK:
        if _KOL is None and _kol_mod is not None:
            _KOL = _kol_mod.KolSource(log=print)
            _KOL.start()
        return _KOL


@app.route("/api/kol")
def api_kol():
    """kolscan.io leaderboard + the persistence view kolscan does not serve.

    Two things this route deliberately does NOT do:
      * verify any PnL. Every number here is kolscan's claim, scraped. The
        `unverified` flag is not decoration — nothing downstream should treat
        a board row as evidence a wallet is worth copying.
      * touch sniper_config. Whether a KOL becomes the copy target is an
        explicit human action, not a side effect of a scrape.
    """
    if _kol_mod is None:
        return jsonify({"ok": False, "error": "kol_source unavailable"}), 200
    src = _kol_source()
    try:
        board = src.board()
        if not board:
            # First paint lands before the poller's first cycle finishes.
            try:
                board = src.poll_once()
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)[:200],
                                "status": src.status()}), 200
        pers = _kol_mod.persistence()
        tgt = (sniper_cfg.get("target") or "").strip()
        days_total = max([(p.get("days_total") or 0)
                          for p in pers.values()] or [1]) or 1
        rows = []
        for k in board:
            p = pers.get(k["wallet"]) or {}
            r = {**k,
                 "days_seen": p.get("days_seen"),
                 "median_rank": p.get("median_rank"),
                 "best_rank": p.get("best_rank"),
                 "trend": p.get("trend"),
                 "is_target": k["wallet"] == tgt}
            # composite rank — see kol_source.score(). kolscan orders by PnL
            # alone, which floats 1-trade flukes to the top; this is the column
            # to actually sort on.
            r["score"] = _kol_mod.score(r, days_total)
            rows.append(r)
        on = next((r for r in rows if r["is_target"]), None)
        return jsonify({
            "ok": True,
            "unverified": True,
            "board": rows,
            "status": src.status(),
            "days_total": max([(p.get("days_total") or 0)
                               for p in pers.values()] or [0]),
            # The finding that motivated this panel: the copy_target sleeve
            # went flat after 07-21 and the wallet it follows is not on the
            # board at all. Surfaced here so it cannot be missed.
            "target": {"wallet": tgt, "on_board": bool(on),
                       "rank": on["rank"] if on else None},
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)[:200],
                        "status": src.status() if src else None}), 200


@app.route("/api/smart_exit")
def api_smart_exit():
    """Live engine state + the smart-vs-fixed scoreboard the UI panel renders.

    Cached: exit_compare.jsonl only grows (one row per dual-resolved close), so
    re-scoring it on the panel's poll would be pure waste. Every failure mode
    degrades to the engine status alone — a bad byte in the journal must never
    500 this route, because the sniper panel renders off the same fetch."""
    try:
        smx = _smx_mod.ENGINE.status()
    except Exception:
        smx = None
    board, err = None, None
    try:
        now = time.time()
        c = _SMX_BOARD_CACHE
        mtime = (os.path.getmtime(EXIT_COMPARE_PATH)
                 if os.path.exists(EXIT_COMPARE_PATH) else 0)
        if c["board"] is not None and c["mtime"] == mtime and now - c["t"] < 15:
            board = c["board"]
        else:
            rows = []
            with open(EXIT_COMPARE_PATH, encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            rows.append(json.loads(line))
                        except Exception:
                            pass
            board = _smx_mod.scoreboard(rows)
            _SMX_BOARD_CACHE.update(t=now, mtime=mtime, board=board)
    except FileNotFoundError:
        board = {"n": 0}
    except Exception as e:
        err = str(e)[:120]
    return jsonify({"ok": True, "engine": smx, "board": board, "error": err,
                    "enforced": bool(sniper_cfg.get("smart_exit", True))})


@app.route("/api/sniper/config", methods=["POST"])
def api_sniper_config():
    d = request.json or {}
    try:
        allowed = {"target", "sol_per_snipe", "tp_pct", "sl_pct",
                   "timeout_min", "max_sol_per_day", "poll_secs",
                   "fast_exit", "fast_exit_start_s", "fast_exit_gap_s",
                   "fast_exit_chunks"}
        for k in allowed & set(d):
            if k == "target":
                sniper_cfg[k] = str(d[k]).strip()
            else:
                sniper_cfg[k] = float(d[k])
        # sane clamps
        sniper_cfg["sol_per_snipe"] = max(0.001, min(sniper_cfg["sol_per_snipe"], 1.0))
        sniper_cfg["max_sol_per_day"] = max(sniper_cfg["sol_per_snipe"],
                                            min(sniper_cfg["max_sol_per_day"], 5.0))
        sniper_cfg["poll_secs"] = max(2.0, min(sniper_cfg["poll_secs"], 30.0))
        _sniper_cfg_save(sniper_cfg)
        return jsonify({"ok": True, "config": sniper_cfg})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/sniper/start", methods=["POST"])
def api_sniper_start():
    sniper_cfg["enabled"] = True
    _sniper_cfg_save(sniper_cfg)
    sniper_start()
    return jsonify({"ok": True, **sniper_status()})


@app.route("/api/sniper/stop", methods=["POST"])
def api_sniper_stop():
    sniper_cfg["enabled"] = False
    _sniper_cfg_save(sniper_cfg)
    sniper_stop()
    return jsonify({"ok": True, **sniper_status()})


@app.route("/api/sniper/reconcile", methods=["POST"])
def api_sniper_reconcile():
    """Book the true on-chain PnL for any live close still missing it (backfill
    after a fill, or retry when RPC was slow). Fires the lookups off-thread.
    Pass {"force": true} to recompute even positions that already have a number
    (use after a correction to the PnL logic)."""
    force = bool((request.json or {}).get("force"))
    todo = [p for p in _sniper["positions"]
            if p.get("mode") == "live" and p.get("status") == "closed"
            and p.get("sig") and (force or p.get("real_pnl_sol") is None)]
    if force:
        for p in todo:
            p["real_pnl_sol"] = None             # clear so _finalize recomputes
    # process sequentially in ONE thread — parallel RPC bursts get rate-limited
    def _run():
        for p in todo:
            _finalize_real_pnl(p)
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "reconciling": len(todo)})


@app.route("/api/sniper/mode", methods=["POST"])
def api_sniper_mode():
    """Flip paper<->live. Live requires an unlocked wallet + explicit confirm."""
    d = request.json or {}
    try:
        go_live = bool(d.get("live"))
        if go_live:
            if KEYPAIR is None:
                raise ValueError("unlock a wallet before arming live")
            if not d.get("confirm"):
                raise ValueError("live mode needs explicit confirm")
        sniper_cfg["live"] = go_live
        _sniper_cfg_save(sniper_cfg)
        return jsonify({"ok": True, **sniper_status()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/dashboard")
def api_dashboard():
    try:
        require_wallet()
        h = get_holdings()
        st = trade_stats(PUBKEY)
        sol_price = h.get("sol_price") or 0
        held = {r["mint"]: r for r in h["holdings"]}
        unreal_total = 0.0
        rows = []
        for m in set(st["per"]) | set(held):
            agg = st["per"].get(m, {"buys": 0, "sells": 0, "realized": 0.0})
            openpos = st["open"].get(m)
            r = held.get(m)
            value_usd = r["usd"] if r else 0.0
            unreal = None
            if openpos and openpos["cost"] > 0 and r and sol_price:
                unreal = value_usd / sol_price - openpos["cost"]
                unreal_total += unreal
            sym = (r.get("symbol") if r else None) or _symbol_for(m)["symbol"]
            rows.append({"mint": m, "symbol": sym,
                         "buys": agg["buys"], "sells": agg["sells"],
                         "realized": round(agg["realized"], 4),
                         "unreal": None if unreal is None else round(unreal, 4),
                         "value_usd": round(value_usd, 2)})
        rows.sort(key=lambda r: -(r["value_usd"] + abs(r["realized"]) * sol_price))

        # equity snapshot (throttled) -> curve builds up as you use the app
        eq = [e for e in _read_jsonl(EQUITY_PATH) if e.get("wallet") == PUBKEY]
        now = time.time()
        total_now = round(h["total_usd"] + h["sol_usd"], 2)
        if not eq or now - eq[-1]["ts"] > 120:
            rec = {"ts": round(now, 1), "wallet": PUBKEY, "usd": total_now}
            _append_jsonl(EQUITY_PATH, rec)
            eq.append(rec)

        return jsonify({
            "ok": True,
            "realized": round(st["realized"], 4),
            "unrealized": round(unreal_total, 4),
            "win_rate": st["win_rate"], "sells": st["sells"],
            "trades": st["count"],
            "sol_balance": h["sol_balance"], "sol_price": sol_price,
            "equity_usd": total_now,
            "equity": [{"ts": e["ts"], "usd": e["usd"]} for e in eq],
            "pnl_events": st["events"],
            "tokens": rows,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


def _evm_token_payload(mint):
    """Threat-panel payload for a 0x token, shaped like the Solana response.
    View-only: no wallet balance, no Jupiter fields."""
    info = _evm_token_info(mint)
    if not info:
        raise LookupError("no DEX pair found for this address on any chain")
    p = info["pair"]
    bt = p.get("baseToken") or {}
    pinfo = p.get("info") or {}
    txns, vol, pchg = p.get("txns") or {}, p.get("volume") or {}, p.get("priceChange") or {}

    def win(w):
        t = txns.get(w) or {}
        b, s = t.get("buys") or 0, t.get("sells") or 0
        v, tot = vol.get(w) or 0, (t.get("buys") or 0) + (t.get("sells") or 0)
        # dexscreener gives total volume + buy/sell counts; split by count share
        return {"pc": pchg.get(w), "bv": round(v * b / tot, 2) if tot else 0,
                "sv": round(v * s / tot, 2) if tot else 0,
                "nb": (b - s) if tot else None, "traders": None}

    intel = {"mcap": p.get("marketCap") or p.get("fdv"),
             "liquidity": (p.get("liquidity") or {}).get("usd"),
             "holders": None, "createdAt": p.get("pairCreatedAt"),
             "launchpad": info["chain"], "verified": False, "organic": None,
             "top10pct": None, "dev_pct": None,
             "mint_off": None, "freeze_off": None,
             "stats": {"5m": win("m5"), "1h": win("h1"),
                       "6h": win("h6"), "24h": win("h24")}}
    return {"ok": True, "mint": mint, "evm": True, "chain": info["chain"],
            "gt_net": info["gt_net"], "pool": info["pool"],
            "symbol": bt.get("symbol") or mint[:6] + "…",
            "name": bt.get("name") or "", "decimals": None,
            "usdPrice": float(p.get("priceUsd") or 0) or None,
            "icon": pinfo.get("imageUrl"), "intel": intel,
            "dev": None, "tags": None,
            "social": {"imageUrl": pinfo.get("imageUrl"),
                       "header": pinfo.get("header"),
                       "websites": pinfo.get("websites") or [],
                       "socials": pinfo.get("socials") or []},
            "balance": 0, "balance_raw": 0, "balance_usd": None,
            "sol_balance": None, "wallet": PUBKEY}


@app.route("/api/token/<mint>")
def api_token(mint):
    if _is_evm(mint):
        try:
            return jsonify(_evm_token_payload(mint))
        except LookupError as e:
            return jsonify({"ok": False, "error": str(e)}), 404
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 400
    try:
        require_wallet()
        meta = token_meta(mint)
        soc = dex_social(mint)
        meta["social"] = soc
        if not meta.get("icon"):
            meta["icon"] = soc.get("imageUrl")
        raw, ui, dec = token_balance(mint)
        if dec is not None:
            meta["decimals"] = dec
        # Jupiter DROPS thin/dying tokens from tokens/v2 entirely — no symbol,
        # no price, no intel. FROGGY (ESoQUJ4i…) fell off it the same evening
        # its pool decayed to ~$1.1k. That is precisely the token you most want
        # a liquidity warning on, so fall back to dexscreener, which still
        # carries it, rather than showing an empty panel.
        #
        # Marked src="dexscreener" because the shapes are NOT equivalent:
        # dexscreener gives total volume per window but no buy/sell split, so
        # the FLOW metric goes unknown instead of being invented.
        if not meta.get("intel") or meta["intel"].get("liquidity") is None:
            snap = _ctx_mod.dex_snapshot(mint)
            if snap and snap.get("liq_usd") is not None:
                created = snap.get("pair_created_ms")
                meta["intel"] = {
                    "mcap": snap.get("mcap"), "liquidity": snap.get("liq_usd"),
                    "holders": None,
                    "createdAt": (datetime.utcfromtimestamp(created / 1000)
                                  .isoformat() + "Z") if created else None,
                    "launchpad": snap.get("dex"), "verified": False,
                    "organic": None, "top10pct": None, "dev_pct": None,
                    "mint_off": None, "freeze_off": None, "src": "dexscreener",
                    "stats": {
                        "5m":  {"vol": snap.get("vol_m5"),  "pc": snap.get("chg_m5")},
                        "1h":  {"vol": snap.get("vol_h1"),  "pc": snap.get("chg_h1")},
                        "6h":  {"vol": snap.get("vol_h6"),  "pc": snap.get("chg_h6")},
                        "24h": {"vol": snap.get("vol_h24"), "pc": snap.get("chg_h24")},
                    },
                }
                if meta.get("usdPrice") is None:
                    meta["usdPrice"] = snap.get("price_usd")
        # He pasted this CA into the trade box, which means he looked at it.
        # Most of these never become trades, and that is the point: a filter
        # learned only from taken trades has no negative class to learn against.
        try:
            if CTX is not None:
                CTX.record_considered(mint, symbol=meta.get("symbol"))
        except Exception:
            pass
        usd = (ui * meta["usdPrice"]) if meta["usdPrice"] else None
        return jsonify({"ok": True, "mint": mint, **meta,
                        "balance": ui, "balance_raw": raw,
                        "balance_usd": usd, "sol_balance": sol_balance(),
                        "wallet": PUBKEY})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/buy", methods=["POST"])
def api_buy():
    d = request.json or {}
    try:
        require_wallet()
        mint = d["mint"].strip()
        if _is_evm(mint):
            raise ValueError("EVM token — view-only; trading is Solana-only")
        sol_amt = float(d["sol"])
        if sol_amt <= 0:
            raise ValueError("amount must be > 0")
        lamports = int(sol_amt * 1e9)
        # wait=False: reply the moment the tx is SENT (~0.5-1s) instead of
        # blocking on on-chain confirmation (2-10s). The UI polls /api/tx/<sig>
        # for the real landed status; the trade journal is written off-thread
        # with the true status either way (same mechanism the sniper uses).
        res = execute_swap(SOL_MINT, mint, lamports,
                           int(d.get("slippageBps", SLIPPAGE_BPS)),
                           mev_protect=bool(d.get("mev", True)),
                           dynamic_slippage=bool(d.get("dynSlip", False)),
                           wait=bool(d.get("wait", False)),
                           fee_mode=d.get("feeMode"))
        return jsonify({"ok": res["status"] in ("confirmed", "submitted"), **res})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/sell", methods=["POST"])
def api_sell():
    d = request.json or {}
    try:
        require_wallet()
        mint = d["mint"].strip()
        if _is_evm(mint):
            raise ValueError("EVM token — view-only; trading is Solana-only")
        pct = float(d["pct"])
        if not 0 < pct <= 100:
            raise ValueError("pct must be in (0, 100]")
        # The balance read and the swap must be ATOMIC per mint, or a TP/SL fire
        # landing between them sizes this swap off tokens that are already gone.
        # See mint_lock().
        with mint_lock(mint):
            raw, ui, _ = token_balance(mint)
            amount = raw if pct >= 100 else int(raw * pct / 100)
            if amount <= 0:
                raise ValueError("no balance to sell")
            res = execute_swap(mint, SOL_MINT, amount,
                               int(d.get("slippageBps", SLIPPAGE_BPS)),
                               mev_protect=bool(d.get("mev", True)),
                               dynamic_slippage=bool(d.get("dynSlip", False)),
                               wait=bool(d.get("wait", False)),  # fast-ack, see api_buy
                               fee_mode=d.get("feeMode"))
        return jsonify({"ok": res["status"] in ("confirmed", "submitted"), **res})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ---------------- on-chain error decoding ----------------
# `{"InstructionError": [3, {"Custom": 6001}]}` is not an explanation, it is a
# lookup key — and the SAME number means different things in different programs,
# so it cannot be decoded without knowing which program threw. On 2026-08-09 a
# SELL showed exactly that and the honest answer required pulling the tx: the
# failing program was Jupiter v6 (not pump.fun, which is what the inner swap
# logs make it look like), and 6001 there is SlippageToleranceExceeded.
#
# Anchor programs number custom errors from 6000. Codes below are only the ones
# worth asserting; anything else renders generically WITH the program id and
# code, so it stays lookup-able instead of pretending to be understood.
JUP_V6 = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
PUMP_FUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_SWAP = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

ONCHAIN_ERRORS = {
    JUP_V6: {
        6000: ("EmptyRoute", "Jupiter had no route for this pair at execution time."),
        6001: ("SlippageToleranceExceeded",
               "SLIPPAGE — the price moved against you between the quote and the "
               "block, so the swap would have returned less than your minimum. "
               "The trade REVERTED: no tokens moved, you paid only the network fee."),
        6002: ("InvalidCalculation", "Jupiter's route math failed on-chain."),
        6003: ("MissingPlatformFeeAccount", "Route required a fee account that was not supplied."),
        6004: ("InvalidSlippage", "The slippage value sent was rejected as invalid."),
        6017: ("ExactOutAmountNotMatched", "An exact-out route could not deliver the exact amount."),
    },
    PUMP_FUN: {
        6002: ("TooMuchSolRequired",
               "SLIPPAGE ON A BUY — the curve moved up before the block landed and "
               "the buy would have cost more SOL than allowed. Nothing was bought."),
        6003: ("TooLittleSolReceived",
               "SLIPPAGE ON A SELL — the curve moved down before the block landed and "
               "the sell would have returned less SOL than allowed. Nothing was sold."),
        6005: ("BondingCurveComplete",
               "This token has GRADUATED off the pump.fun curve — trade it on the AMM pool instead."),
    },
}

# Native (non-Anchor) programs number errors from 0, so their codes collide
# with each other: System error 1 and Token error 1 are both "not enough
# funds", but of different things. Keyed by program id like the table above.
SYSTEM_PROGRAM = "11111111111111111111111111111111"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
ATA_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"

_TOKEN_ERRS = {
    0: ("NotRentExempt", "A token account was short of its rent deposit (~0.002 SOL). "
                         "Top up SOL. Nothing executed."),
    1: ("InsufficientTokenBalance",
        "NOT ENOUGH TOKENS: the wallet holds fewer tokens than the trade tried to move "
        "(already sold, partly sold, or wrong wallet). Nothing executed."),
    4: ("OwnerMismatch", "A token account belongs to a different wallet than the signer. "
                         "Check which wallet is selected. Nothing executed."),
    17: ("AccountFrozen", "This token account is FROZEN by the token's freeze authority. "
                          "The dev can block selling. Likely a honeypot."),
}

ONCHAIN_ERRORS.update({
    SYSTEM_PROGRAM: {
        0: ("AccountAlreadyInUse",
            "An account this trade tried to create already exists. Usually a stale "
            "route or a duplicate send. Retry. Nothing executed."),
        1: ("NotEnoughSOL",
            "NOT ENOUGH SOL: the wallet could not cover the buy amount plus fees, tip and "
            "token-account rent. Top up the wallet or buy smaller. Nothing executed."),
    },
    TOKEN_PROGRAM: _TOKEN_ERRS,
    TOKEN_2022: _TOKEN_ERRS,
})

PROGRAM_NAMES = {
    JUP_V6: "Jupiter", PUMP_FUN: "pump.fun", PUMP_SWAP: "PumpSwap",
    SYSTEM_PROGRAM: "System (SOL transfers)", TOKEN_PROGRAM: "Token program",
    TOKEN_2022: "Token-2022", ATA_PROGRAM: "Token-account program",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "Raydium AMM",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "Raydium CLMM",
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C": "Raydium CPMM",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo": "Meteora DLMM",
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB": "Meteora pools",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "Orca",
    "ComputeBudget111111111111111111111111111111": "Compute budget",
}

def program_label(pid):
    if not pid:
        return None
    return PROGRAM_NAMES.get(pid) or (pid[:8] + "…")

# Errors that are not InstructionError at all: the tx failed as a whole.
_TX_LEVEL_ERRORS = {
    "InsufficientFundsForFee": ("NotEnoughSOLForFee",
        "NOT ENOUGH SOL to pay the network fee. Top up the wallet."),
    "InsufficientFundsForRent": ("NotEnoughSOLForRent",
        "NOT ENOUGH SOL: after this trade the wallet would sit below Solana's minimum "
        "balance (rent). Keep ~0.002 SOL more in the wallet or buy smaller. Nothing executed."),
    "BlockhashNotFound": ("Expired",
        "The transaction EXPIRED before a validator took it. Nothing executed. Fire again."),
    "AlreadyProcessed": ("Duplicate", "This exact transaction already landed. Check solscan."),
    "AccountNotFound": ("WalletEmpty",
        "The paying wallet does not exist on-chain (0 SOL). Fund it first."),
}

# Built-in (non-custom) instruction errors, e.g. {"InstructionError":[2,"InsufficientFunds"]}
_BUILTIN_IX_ERRORS = {
    "InsufficientFunds": ("NotEnoughFunds",
        "NOT ENOUGH FUNDS for one of the transfers in this trade. Nothing executed."),
    "ComputationalBudgetExceeded": ("OutOfCompute",
        "Ran OUT OF COMPUTE: the route was too heavy for the compute limit. Retry. "
        "Nothing executed."),
    "ProgramFailedToComplete": ("OutOfCompute",
        "Ran OUT OF COMPUTE: the route was too heavy for the compute limit. Retry. "
        "Nothing executed."),
}

_LOG_FAIL_RE = re.compile(
    r"Program (\w+) failed: custom program error: 0x([0-9a-fA-F]+)")
_LOG_ANY_FAIL_RE = re.compile(r"Program (\w+) failed: (.+)")
_LAMPORTS_RE = re.compile(r"insufficient lamports (\d+), need (\d+)")

def _sol(lamports):
    return f"{lamports / 1e9:.4f} SOL"

def explain_tx_failure(sig, err=None):
    """Turn a landed failure into a sentence. One extra RPC, only on failure.

    Reads logMessages rather than trusting the instruction index: the index
    points at a TOP-LEVEL instruction, while the program that actually threw is
    usually a CPI several levels down. For the 2026-08-09 sell, index 3 was
    Jupiter and the logs showed pump.fun succeeding inside it — reading the
    index alone would have blamed the wrong program.

    2026-10-05: native programs (System, Token) are decoded too, and a System
    transfer shortfall reports the actual have/need amounts from the logs,
    so "custom error 1" reads as "wallet had X SOL, needed Y".
    """
    out = {"raw": json.dumps(err) if err else None, "reason": None,
           "program": None, "program_name": None, "code": None, "name": None}
    try:
        tx = rpc("getTransaction",
                 [sig, {"encoding": "jsonParsed",
                        "maxSupportedTransactionVersion": 0,
                        "commitment": "confirmed"}], trade=True)
        logs = ((tx or {}).get("meta") or {}).get("logMessages") or []
    except Exception:
        logs = []

    # Specific numbers beat generic wording: a short transfer logs exactly
    # how much the wallet had and how much was needed.
    shortfall = None
    for line in logs:
        m = _LAMPORTS_RE.search(line or "")
        if m:
            shortfall = (int(m.group(1)), int(m.group(2)))

    hit = None
    for line in reversed(logs):                 # innermost failure is last
        m = _LOG_FAIL_RE.search(line or "")
        if m:
            hit = (m.group(1), int(m.group(2), 16))
            break
    if hit:
        pid, code = hit
        out["program"], out["code"] = pid, code
        out["program_name"] = program_label(pid)
        known = ONCHAIN_ERRORS.get(pid, {}).get(code)
        if known:
            out["name"], out["reason"] = known
        else:
            out["reason"] = (f"{program_label(pid)} rejected the trade (its error #{code}, "
                             f"not one this app knows yet). Nothing executed except the "
                             f"network fee. Open solscan for that program's message.")
    else:
        # Not a custom error: check tx-level and built-in instruction errors.
        key = None
        if isinstance(err, str):
            key = err
        elif isinstance(err, dict):
            ie = err.get("InstructionError")
            if isinstance(ie, list) and len(ie) == 2 and isinstance(ie[1], str):
                key = ie[1]
            elif len(err) == 1:
                key = next(iter(err))
        known = _TX_LEVEL_ERRORS.get(key) or _BUILTIN_IX_ERRORS.get(key)
        if any("exceeded CUs meter" in (l or "") for l in logs):
            known = _BUILTIN_IX_ERRORS["ComputationalBudgetExceeded"]
        if known:
            out["name"], out["reason"] = known
        elif logs:
            for line in reversed(logs):
                m = _LOG_ANY_FAIL_RE.search(line or "")
                if m:
                    out["program"] = m.group(1)
                    out["program_name"] = program_label(m.group(1))
                    out["reason"] = (f"{program_label(m.group(1))} rejected the trade: "
                                     f"{m.group(2).strip()[:160]}. Nothing executed.")
                    break

    if shortfall:
        have, need = shortfall
        out["name"] = out["name"] or "NotEnoughSOL"
        out["reason"] = (f"NOT ENOUGH SOL: wallet had {_sol(have)}, this step needed "
                         f"{_sol(need)} (short {_sol(need - have)}). The buy amount plus "
                         f"fees, tip and token-account rent did not fit. Top up or buy "
                         f"smaller. Nothing executed.")

    if not out["reason"]:
        out["reason"] = ("The transaction landed on-chain but was rejected. "
                         "Nothing moved except the network fee.")
    return out


@app.route("/api/tx/<sig>")
def api_tx_status(sig):
    """Landed-status poll for fast-ack buys/sells (one pooled ~160ms RPC)."""
    try:
        st = rpc("getSignatureStatuses", [[sig]], trade=True)["value"][0]
    except Exception as e:
        return jsonify({"ok": False, "status": "unknown", "error": str(e)})
    if not st:
        return jsonify({"ok": True, "status": "pending"})
    if st.get("err"):
        info = explain_tx_failure(sig, st["err"])
        return jsonify({"ok": True, "status": "failed",
                        "error": json.dumps(st["err"]), **info})
    done = st.get("confirmationStatus") in ("confirmed", "finalized")
    return jsonify({"ok": True, "status": "confirmed" if done else "pending"})


# ---------------- UI ----------------
PAGE = open("JUP Terminator LIVE.html", encoding="utf-8").read()
# Read once at import, same as PAGE — with RELOAD=0 (mandatory while live, see
# .env) an edit to either file needs a manual trader restart to take effect.
POPOUT = open("popout.html", encoding="utf-8").read()


@app.route("/")
def index():
    return (PAGE.replace("{{SLIP}}", str(SLIPPAGE_BPS / 100))
                .replace("{{MINUSD}}", f"{MIN_HOLDING_USD:g}"))


@app.route("/popout")
def popout():
    """Small standalone sell window. Opened with window.open() from the ASSET
    INVENTORY header so the main tab is free to sit on gmgn/dexscreener."""
    return POPOUT.replace("{{SLIP}}", str(SLIPPAGE_BPS / 100))


if __name__ == "__main__":
    print(f"wallet: {PUBKEY or 'none — create/unlock in the UI'}")
    print(f"vault:  {VAULT_PATH} ({len(_vault_load()['wallets'])} stored)")
    print(f"http://127.0.0.1:{PORT}")
    # auto-restart on file edits (RELOAD=0 in .env to disable).
    # NOTE: a reload clears the in-memory unlocked wallet — re-unlock in the UI.
    reload_on_edit = os.environ.get("RELOAD", "1") == "1"
    # auto-resume the sniper watcher in the serving process only (not the
    # reloader's parent). Live mode never auto-fires without an unlocked wallet.
    is_serving = (not reload_on_edit) or os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    if is_serving and sniper_cfg.get("enabled"):
        sniper_start()
        print(f"sniper: watching {sniper_cfg['target'][:8]}… "
              f"({'LIVE' if sniper_cfg['live'] else 'PAPER'})")
    if is_serving:
        # Always RUNS, never armed (Executor.load forces armed=False on every
        # start). Watching costs one Jupiter batch every 3s and one trade-class
        # holdings read a minute; selling requires an explicit /api/tpsl/arm.
        tpsl_start()
        print("tpsl:    watching — DISARMED (arm it in the trade box)")
        # Costs nothing while the paper book is empty: its holdings refresh is
        # a local file read, and refresh_prices() returns immediately with no
        # positions, so no Jupiter budget is spent until you paper-trade.
        paper_start()
        # ASCII "SOL", not the U+25CE the UI uses. Windows stdout here is
        # cp1252, which cannot encode it -- this print runs during startup, so
        # a UnicodeEncodeError would take the whole trader down on boot.
        print(f"paper:   book open — {PAPER.sol_balance():.4f} SOL "
              f"({PAPER.stats()['open']} open) — DISARMED")
        if RENT_SWEEP_ENABLED:
            threading.Thread(target=_rent_sweep_loop, daemon=True,
                             name="rent-sweep").start()
            print(f"rent:    sweeping empty token accounts every "
                  f"{RENT_SWEEP_EVERY_S/60:.0f} min "
                  f"— dry run: GET /api/rent/sweep")
        # _chart_fetch, not the /api/chart stale-while-revalidate path: a
        # snapshot wants the freshest candles, not whatever the UI last painted.
        # The recorder's single worker serialises these, which is also what
        # keeps us inside GeckoTerminal's free-tier rate budget.
        CTX = _ctx_mod.Recorder(candles_fn=_chart_fetch)
        CTX.start()
        print(f"ctx:     recording decision context -> "
              f"{os.path.basename(_ctx_mod.CTX_PATH)}")
    if is_serving:
        evm_copy._load_cfg()
        if evm_copy.cfg.get("enabled") and evm_copy.cfg.get("targets"):
            evm_copy.start()
            print(f"evm-copy: watching {len(evm_copy.cfg['targets'])} "
                  f"target(s) — PAPER ONLY")
    app.run(host="127.0.0.1", port=PORT, threaded=True,
            use_reloader=reload_on_edit)
