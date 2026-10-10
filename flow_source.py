"""
flow_source.py — per-mint trade flow for smart_exit, sourced from Solana logs.

WHY THIS EXISTS (2026-07-27)
PumpPortal moved the per-mint trade feed behind a funded API key:

    {"message":"'subscribeTokenTrade' and 'subscribeAccountTrade' methods are
     only available when connecting with an API key funded with at least
     0.02 SOL."}

That reply carries no `txType`, so every branch of jup_trader's websocket loop
dropped it and the failure was completely silent. Cost: from the 07-24 ship to
07-27, all 36 smart-exit positions resolved with ZERO flow prints. `_decide_smart`
gates creator-sell / whale-dump / flow-flip / stall / gap-guard behind `if seen:`,
so only trail-lock and smart-timeout could ever fire — and the scoreboard duly
reported "smart loses to fixed by 0.46 SOL" while measuring a blind 15-minute
hold (median 900s) against a 35s ladder. The engine was never tested.

This module replaces that feed with `logsSubscribe` on the RPC already in .env
(Helius), so it costs no new credential. It decodes pump.fun's anchor TradeEvent
straight out of the `Program data:` log line.

VERIFIED 2026-07-27 against live mainnet:
  - mentions=[mint] delivered 138 trades in 30s vs 137 for the same mint on the
    program-wide stream — the narrow filter loses nothing.
  - decoded amounts land in SOL (e.g. 11456782 lamports -> 0.011457 SOL), which
    is the unit `whale_sol`/`flip_min_sol` are written in. Feeding lamports here
    would re-create the silent-dead-feed bug from the other direction.

SCOPE: pump.fun bonding-curve trades only. After graduation a mint's trades move
to the pump-amm program with a different event layout and will NOT be decoded —
irrelevant while grad_enabled is false, but it is a real gap if that comes back.
"""

from __future__ import annotations

import base64
import json
import struct
import threading
import time

from websocket import create_connection, WebSocketTimeoutException

PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
TRADE_EVENT_MIN_LEN = 105     # disc8 + mint32 + sol8 + tok8 + isBuy1 + user32 + ts8
RECV_TIMEOUT = 0.5            # short on purpose: subscribes must not wait behind
                              # a long recv — creator-sell/whale-dump fire in the
                              # first seconds and the fixed ghost is out by ~35s.
RECONNECT_MIN = 1.0
RECONNECT_MAX = 30.0

# Feed failover (2026-08-04). Helius free tier began 429-ing the logsSubscribe
# HANDSHAKE ("Handshake status 429 Too Many Requests"), which is not a timeout
# and not retryable at the same rate — the socket never opens at all. Cost
# before this existed: flow.blind_closes hit 27, i.e. 27 positions resolved
# with zero crowd evidence, which is precisely the dead-feed failure this
# module was written to prevent.
#
# Contributing cause: pyramid_runner.py opens a SECOND logsSubscribe socket on
# the same key. Two consumers on one free-tier credential is the contention.
PUBLIC_RPC = "https://api.mainnet-beta.solana.com"
FAILOVER_AFTER = 3          # consecutive connect failures before switching
RETURN_TO_PRIMARY_S = 900.0  # retry the paid endpoint after this long on public

_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58encode(raw: bytes) -> str:
    n = int.from_bytes(raw, "big")
    out = ""
    while n:
        n, rem = divmod(n, 58)
        out = _B58[rem] + out
    for byte in raw:
        if byte == 0:
            out = "1" + out
        else:
            break
    return out or "1"


def decode_trade_event(blob: bytes):
    """pump.fun TradeEvent -> dict shaped like the old PumpPortal payload, or
    None if this `Program data:` blob is some other event."""
    if len(blob) < TRADE_EVENT_MIN_LEN:
        return None
    try:
        mint = b58encode(blob[8:40])
        sol_lamports, _token_amount = struct.unpack_from("<QQ", blob, 40)
        is_buy = blob[56] == 1
        user = b58encode(blob[57:89])
    except Exception:
        return None
    sol = sol_lamports / 1e9
    if not (0 < sol < 100000):
        return None
    return {"mint": mint,
            "txType": "buy" if is_buy else "sell",
            "solAmount": sol,              # SOL, matching engine thresholds
            "traderPublicKey": user}


def ws_url_from_rpc(rpc_url: str) -> str:
    return rpc_url.replace("https://", "wss://").replace("http://", "ws://")


class FlowSource:
    """Owns one websocket to the RPC and mirrors the engine's subscribe queue
    onto it. Never opens or writes files; never signs anything."""

    def __init__(self, engine, rpc_url, ignore_trader=None, log=None,
                 fallback_url=PUBLIC_RPC):
        self.engine = engine
        self.ws_url = ws_url_from_rpc(rpc_url)
        # Failover target for the FEED ONLY. This socket never signs or sends
        # anything — it is logsSubscribe, a pure read — so running it on the
        # public endpoint is safe. `sendTransaction` still goes out over
        # RPC_URL in jup_trader and is deliberately NOT failed over: public
        # mainnet-beta is unstaked and drops sends silently under load, which
        # would lose real trades instead of just data.
        self.fallback_ws = ws_url_from_rpc(fallback_url) if fallback_url else None
        self.primary_ws = self.ws_url
        self.on_fallback = False
        self.failovers = 0
        self._fail_streak = 0
        self.ignore_trader = ignore_trader
        self._log = log or (lambda m: None)
        self._stop = False
        self._thread = None
        self._lock = threading.Lock()
        self._subs = {}        # mint -> subscription id
        self._pending = {}     # request id -> mint
        self._req = 0
        self.connected = False
        self.prints = 0
        self.last_event_ts = None
        self.last_error = None

    # ------------------------------------------------------------- lifecycle --

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop = False
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop = True

    def status(self):
        with self._lock:
            subs = len(self._subs)
        return {"connected": self.connected, "subscribed_mints": subs,
                "prints": self.prints, "last_event": self.last_event_ts,
                "last_error": self.last_error,
                # "which endpoint is this feed actually on" must be answerable
                # at a glance — see the FAILOVER note at the top of the module.
                "on_fallback": self.on_fallback,
                "endpoint": self.ws_url.split("//")[-1].split("/?")[0],
                "failovers": self.failovers}

    # ---------------------------------------------------------------- internals

    def _send_sub(self, ws, mint):
        with self._lock:
            if mint in self._subs or mint in self._pending.values():
                return
            self._req += 1
            rid = self._req
            self._pending[rid] = mint
        ws.send(json.dumps({
            "jsonrpc": "2.0", "id": rid, "method": "logsSubscribe",
            "params": [{"mentions": [mint]}, {"commitment": "processed"}],
        }))

    def _send_unsub(self, ws, mint):
        with self._lock:
            sub_id = self._subs.pop(mint, None)
            for rid, m in list(self._pending.items()):
                if m == mint:
                    self._pending.pop(rid, None)
        if sub_id is None:
            return
        self._req += 1
        ws.send(json.dumps({"jsonrpc": "2.0", "id": self._req,
                            "method": "logsUnsubscribe", "params": [sub_id]}))

    def _drain(self, ws):
        """Mirror the engine's pending (method, mint) ops onto this socket."""
        try:
            ops = self.engine.drain_ws()
        except Exception as e:
            self.last_error = f"drain: {str(e)[:80]}"
            return
        for method, mint in ops:
            try:
                if method == "subscribeTokenTrade":
                    self._send_sub(ws, mint)
                else:
                    self._send_unsub(ws, mint)
            except Exception as e:
                self.last_error = f"{method}: {str(e)[:80]}"
                raise      # socket is suspect; bounce the connection

    def _handle(self, msg):
        # subscription acks: {"result": <sub id>, "id": <our req id>}
        if "result" in msg and isinstance(msg.get("result"), int):
            rid = msg.get("id")
            with self._lock:
                mint = self._pending.pop(rid, None)
                if mint:
                    self._subs[mint] = msg["result"]
            return
        if msg.get("method") != "logsNotification":
            return
        value = (((msg.get("params") or {}).get("result") or {})
                 .get("value") or {})
        if value.get("err"):          # failed tx: never counts as crowd flow
            return
        for line in value.get("logs") or []:
            if not line.startswith("Program data: "):
                continue
            try:
                blob = base64.b64decode(line[len("Program data: "):])
            except Exception:
                continue
            ev = decode_trade_event(blob)
            if not ev:
                continue
            # the engine drops mints it is not tracking, so an unrelated event
            # riding along in the same tx is harmless.
            try:
                self.engine.record_trade(ev, ignore_trader=self.ignore_trader)
            except Exception as e:
                self.last_error = f"record: {str(e)[:80]}"
                continue
            self.prints += 1
            self.last_event_ts = time.time()

    def _maybe_failover(self):
        """Switch feed endpoints after FAILOVER_AFTER consecutive failures.

        Announced on BOTH transitions — a feed silently running somewhere other
        than where you think it is, is the whole class of bug this module
        exists for. `status()['on_fallback']` is the machine-readable form.
        """
        if not self.fallback_ws:
            return
        if self._fail_streak < FAILOVER_AFTER:
            return
        self._fail_streak = 0
        if not self.on_fallback:
            self.on_fallback = True
            self.failovers += 1
            self.ws_url = self.fallback_ws
            self._fallback_since = time.time()
            self._log("[FLOW] *** FAILED OVER TO PUBLIC RPC *** "
                      f"({self.fallback_ws.split('//')[-1]}) — feed only, "
                      "sends still use the private RPC")
        else:
            # already public and still failing: nothing better to switch to
            self._log("[FLOW] public RPC also failing — feed is down")

    def _run(self):
        backoff = RECONNECT_MIN
        self._fallback_since = None
        while not self._stop:
            ws = None
            try:
                # periodically try to come home: the paid endpoint is staked
                # and higher-limit, so public is a fallback, not a destination
                if (self.on_fallback and self._fallback_since
                        and time.time() - self._fallback_since > RETURN_TO_PRIMARY_S):
                    self.on_fallback = False
                    self.ws_url = self.primary_ws
                    self._fallback_since = None
                    self._fail_streak = 0
                    self._log("[FLOW] retrying primary RPC")
                ws = create_connection(self.ws_url, timeout=15)
                ws.settimeout(RECV_TIMEOUT)
                self.connected = True
                self.last_error = None
                self._fail_streak = 0
                backoff = RECONNECT_MIN
                # re-arm everything still held: a reconnect loses every sub, and
                # a position that silently stops receiving flow degrades to the
                # blind timeout this module exists to prevent.
                with self._lock:
                    self._subs.clear()
                    self._pending.clear()
                try:
                    for mint in self.engine.active_mints():
                        self._send_sub(ws, mint)
                except Exception:
                    pass
                while not self._stop:
                    self._drain(ws)
                    try:
                        raw = ws.recv()
                    except WebSocketTimeoutException:
                        continue
                    if not raw:
                        raise ConnectionError("socket closed by server")
                    try:
                        self._handle(json.loads(raw))
                    except Exception as e:
                        self.last_error = f"handle: {str(e)[:80]}"
            except Exception as e:
                self.last_error = str(e)[:120]
                # only a failure to OPEN counts toward failover; a mid-session
                # drop on a healthy endpoint is normal churn, not a bad endpoint
                if not self.connected:
                    self._fail_streak += 1
                self._log(f"[FLOW] disconnected: {str(e)[:100]}")
                self._maybe_failover()
            finally:
                self.connected = False
                try:
                    if ws:
                        ws.close()
                except Exception:
                    pass
            if self._stop:
                break
            time.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX)
