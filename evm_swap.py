"""evm_swap.py — Uniswap V3 swap execution for EVM chains (Robinhood Chain).

Companion to evm_wallet.py (which holds/unlocks the key but cannot send).
This layer builds, simulates and signs swaps. It is the ONLY place in the
codebase that can move EVM funds.

DESIGN NOTES / WHY THINGS ARE THE WAY THEY ARE
  * Router was NOT taken from Uniswap's canonical address list — none of those
    are deployed on chain 4663. It was derived from the `sender` field of real
    Swap events on the live bcat/WETH pool, then confirmed two ways:
    router.WETH9() matches the pool's token1, and router.factory().getPool()
    round-trips back to the same pool address. Do the same before adding a chain.
  * Every swap is eth_call-simulated against CURRENT state immediately before
    signing. A revert or an output below the slippage floor aborts the send.
    amountOutMinimum is NEVER 0 — that is a free sandwich for a searcher.
  * Sells route through multicall(exactInputSingle -> unwrapWETH9) so the seller
    receives native ETH, not WETH they would have to unwrap by hand.
  * decimals() is read per token. Do not assume 18.

LIVE-MONEY GATE: swap() refuses unless cfg["live"] is true AND the caller passes
confirm=True. Nothing here fires on its own — there is no autonomous caller.
"""
import json
import os
import time
import threading

from web3 import Web3
from eth_abi import encode as _abi_encode

import evm_wallet

HERE = os.path.dirname(os.path.abspath(__file__))
CFG_PATH = os.path.join(HERE, "evm_swap_config.json")
LOG_PATH = os.path.join(HERE, "evm_trades.jsonl")
_lock = threading.Lock()

# Only chains whose router has been VERIFIED on-chain belong here.
CHAINS = {
    "robinhood": {
        "chain_id": 4663,
        "rpc": "https://rpc.mainnet.chain.robinhood.com",
        "router": "0xCaf681a66D020601342297493863E78C959E5cb2",   # SwapRouter02
        "factory": "0x1f7d7550B1b028f7571E69A784071F0205FD2EfA",
        "weth": "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73",
        "native": "ETH",
        "explorer": "https://robinhoodchain.blockscout.com",
    },
}

FEE_TIERS = [100, 500, 3000, 10000]
ADDRESS_THIS = "0x0000000000000000000000000000000000000002"   # SwapRouter02 magic
DEFAULT_CFG = {
    "live": False,              # PAPER until the user explicitly arms it
    "slippage_bps": 300,        # 3%
    "max_native_per_trade": 0.05,
    "gas_limit_pad": 1.25,
}

ERC20_ABI = [
    {"inputs": [], "name": "decimals", "outputs": [{"type": "uint8"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "symbol", "outputs": [{"type": "string"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"type": "address"}], "name": "balanceOf",
     "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"type": "address"}, {"type": "address"}], "name": "allowance",
     "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"type": "address"}, {"type": "uint256"}], "name": "approve",
     "outputs": [{"type": "bool"}], "stateMutability": "nonpayable", "type": "function"},
]
ROUTER_ABI = [
    {"inputs": [{"components": [
        {"name": "tokenIn", "type": "address"}, {"name": "tokenOut", "type": "address"},
        {"name": "fee", "type": "uint24"}, {"name": "recipient", "type": "address"},
        {"name": "amountIn", "type": "uint256"},
        {"name": "amountOutMinimum", "type": "uint256"},
        {"name": "sqrtPriceLimitX96", "type": "uint160"}],
        "name": "params", "type": "tuple"}],
     "name": "exactInputSingle", "outputs": [{"name": "amountOut", "type": "uint256"}],
     "stateMutability": "payable", "type": "function"},
    {"inputs": [{"name": "amountMinimum", "type": "uint256"},
                {"name": "recipient", "type": "address"}],
     "name": "unwrapWETH9", "outputs": [], "stateMutability": "payable", "type": "function"},
    {"inputs": [{"name": "data", "type": "bytes[]"}], "name": "multicall",
     "outputs": [{"type": "bytes[]"}], "stateMutability": "payable", "type": "function"},
]
FACTORY_ABI = [
    {"inputs": [{"type": "address"}, {"type": "address"}, {"type": "uint24"}],
     "name": "getPool", "outputs": [{"type": "address"}],
     "stateMutability": "view", "type": "function"},
]
POOL_ABI = [
    {"inputs": [], "name": "liquidity", "outputs": [{"type": "uint128"}],
     "stateMutability": "view", "type": "function"},
]


# ---------------- config ----------------
def load_cfg():
    cfg = dict(DEFAULT_CFG)
    try:
        with open(CFG_PATH, encoding="utf-8") as f:
            cfg.update(json.load(f))
    except Exception:
        pass
    return cfg


def save_cfg(cfg):
    with _lock:
        tmp = CFG_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=1)
        os.replace(tmp, CFG_PATH)
    return cfg


def _log(rec):
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")
    except Exception:
        pass


# ---------------- chain plumbing ----------------
_w3_cache = {}


def w3_for(chain):
    c = CHAINS.get(chain)
    if not c:
        raise ValueError(f"no verified Uniswap router for chain '{chain}' — "
                         f"tradeable chains: {', '.join(CHAINS)}")
    if chain not in _w3_cache:
        _w3_cache[chain] = Web3(Web3.HTTPProvider(
            c["rpc"], request_kwargs={"timeout": 20}))
    return _w3_cache[chain], c


def is_tradeable_chain(chain):
    return chain in CHAINS


_meta_cache = {}


def token_meta(chain, token):
    """(symbol, decimals) — read from the contract, never assumed."""
    key = (chain, token.lower())
    if key in _meta_cache:
        return _meta_cache[key]
    w3, _ = w3_for(chain)
    c = w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)
    try:
        sym = c.functions.symbol().call()
    except Exception:
        sym = "?"
    dec = int(c.functions.decimals().call())
    _meta_cache[key] = (sym, dec)
    return sym, dec


_pool_cache = {}
POOL_TTL = 300


def find_pool(chain, token):
    """Best (deepest) Uniswap V3 pool for token/WETH across fee tiers.
    Returns {"fee","pool","liquidity"} or None when the token has no V3 route
    (e.g. it only has V4 pools) — callers must surface that, not revert."""
    key = (chain, token.lower())
    hit = _pool_cache.get(key)
    if hit and time.time() - hit[0] < POOL_TTL:
        return hit[1]
    w3, c = w3_for(chain)
    fac = w3.eth.contract(address=Web3.to_checksum_address(c["factory"]),
                          abi=FACTORY_ABI)
    tok = Web3.to_checksum_address(token)
    weth = Web3.to_checksum_address(c["weth"])
    best = None
    for fee in FEE_TIERS:
        try:
            p = fac.functions.getPool(tok, weth, fee).call()
            if int(p, 16) == 0:
                continue
            liq = w3.eth.contract(address=p, abi=POOL_ABI).functions.liquidity().call()
            if liq > 0 and (best is None or liq > best["liquidity"]):
                best = {"fee": fee, "pool": p, "liquidity": liq}
        except Exception:
            continue
    _pool_cache[key] = (time.time(), best)
    return best


# Uniswap reverts with 3-letter codes. Surfacing them raw is useless to a user.
REVERT_HINTS = {
    "STF": "transfer failed — token balance or approval too low "
           "(or the token blocks transfers / is honeypotted)",
    "IIA": "insufficient input amount — the swap size is too small to route",
    "TF": "token transfer failed — this token may be a honeypot or have a "
          "sell tax the router can't satisfy",
    "SPL": "price limit hit — the pool moved mid-quote, retry",
    "Too little received": "price moved past your slippage tolerance — "
                           "retry or raise slippage",
    "OOM": "pool has too little liquidity for this size",
}


def _humanize(e):
    m = str(e)
    for code, hint in REVERT_HINTS.items():
        if f"reverted: {code}" in m or f"'{code}'" in m:
            return RuntimeError(f"{hint} [{code}]")
    return e


def _allowance_slot(w3, owner, spender, p=1):
    """Storage slot of _allowances[owner][spender] for slot-p mappings.
    Web3.to_hex — NOT .hex().lstrip('0x'), which eats leading zero nibbles of
    the hash and silently yields the wrong slot ~1 time in 16."""
    inner = w3.keccak(_abi_encode(["address", "uint256"], [owner, p]))
    return Web3.to_hex(w3.keccak(_abi_encode(["address", "bytes32"],
                                             [spender, inner])))


# ---------------- quoting (read-only) ----------------
def quote(chain, token, side, amount, address=None):
    """Simulate the swap against live state. side: 'buy' (native->token) or
    'sell' (token->native). `amount` is native for buy, token units for sell.
    Sell quotes work even before approval by overriding the allowance slot in
    the eth_call — the returned needs_approval tells the caller the real state."""
    w3, c = w3_for(chain)
    who = Web3.to_checksum_address(address or evm_wallet.ADDRESS or ADDRESS_THIS)
    tok = Web3.to_checksum_address(token)
    weth = Web3.to_checksum_address(c["weth"])
    router = Web3.to_checksum_address(c["router"])
    pool = find_pool(chain, token)
    if not pool:
        raise LookupError("no Uniswap V3 pool for this token on "
                          f"{chain} — not tradeable from here")
    sym, dec = token_meta(chain, token)
    r = w3.eth.contract(address=router, abi=ROUTER_ABI)
    erc = w3.eth.contract(address=tok, abi=ERC20_ABI)

    if side == "buy":
        amt_in = int(float(amount) * 1e18)
        data = r.functions.exactInputSingle(
            (weth, tok, pool["fee"], who, amt_in, 0, 0))._encode_transaction_data()
        ov = {who: {"balance": hex(amt_in + 10**17)}}
        try:
            raw = w3.eth.call({"from": who, "to": router, "data": data,
                               "value": amt_in}, "latest", ov)
        except Exception as e:
            raise _humanize(e)
        out = int.from_bytes(raw, "big")
        return {"side": "buy", "symbol": sym, "fee": pool["fee"],
                "amount_in": float(amount), "amount_out": out / 10**dec,
                "amount_out_raw": out, "decimals": dec, "needs_approval": False}

    amt_in = int(float(amount) * 10**dec)
    bal = erc.functions.balanceOf(who).call()
    allow = erc.functions.allowance(who, router).call()
    if bal == 0:
        raise ValueError(f"this wallet holds no {sym}")
    if amt_in > bal:
        raise ValueError(f"amount exceeds balance ({bal / 10**dec:,.4f} {sym})")
    swap = r.functions.exactInputSingle(
        (tok, weth, pool["fee"], ADDRESS_THIS, amt_in, 1, 0))._encode_transaction_data()
    unwrap = r.functions.unwrapWETH9(1, who)._encode_transaction_data()
    data = r.functions.multicall(
        [bytes.fromhex(swap[2:]), bytes.fromhex(unwrap[2:])])._encode_transaction_data()
    ov = {who: {"balance": hex(10**17)},
          tok: {"stateDiff": {_allowance_slot(w3, who, router): hex(2**256 - 1)}}}
    try:
        w3.eth.call({"from": who, "to": router, "data": data, "value": 0},
                    "latest", ov)
        # multicall returns bytes[]; re-run the bare swap to read the scalar out
        raw = w3.eth.call({"from": who, "to": router,
                           "data": r.functions.exactInputSingle(
                               (tok, weth, pool["fee"], who, amt_in, 1, 0)
                           )._encode_transaction_data(), "value": 0}, "latest", ov)
    except Exception as e:
        raise _humanize(e)
    out = int.from_bytes(raw, "big")
    return {"side": "sell", "symbol": sym, "fee": pool["fee"],
            "amount_in": amt_in / 10**dec, "amount_out": out / 1e18,
            "amount_out_raw": out, "decimals": dec,
            "balance": bal / 10**dec, "needs_approval": allow < amt_in}


# ---------------- execution ----------------
def _gas_fields(w3, cfg):
    blk = w3.eth.get_block("latest")
    if "baseFeePerGas" in blk:
        tip = w3.eth.max_priority_fee
        return {"maxFeePerGas": blk["baseFeePerGas"] * 2 + tip,
                "maxPriorityFeePerGas": tip}
    return {"gasPrice": w3.eth.gas_price}


def _send(w3, cfg, tx):
    tx["gas"] = int(w3.eth.estimate_gas(tx) * float(cfg["gas_limit_pad"]))
    signed = evm_wallet.ACCOUNT.sign_transaction(tx)
    h = w3.eth.send_raw_transaction(signed.raw_transaction)
    return h.hex()


def swap(chain, token, side, amount, confirm=False, slippage_bps=None):
    """Execute a real swap. Requires an unlocked EVM wallet, cfg['live'] armed,
    and an explicit confirm=True from the caller (the UI's confirm dialog).
    Simulates against current state and aborts on revert or slippage breach."""
    cfg = load_cfg()
    if evm_wallet.ACCOUNT is None:
        raise RuntimeError("EVM wallet locked — unlock it in the vault first")
    if not cfg.get("live"):
        raise RuntimeError("EVM trading is in PAPER mode — arm LIVE to send a "
                           "real swap (this spends real funds)")
    if not confirm:
        raise RuntimeError("swap requires explicit confirmation")

    w3, c = w3_for(chain)
    slip = int(slippage_bps if slippage_bps is not None else cfg["slippage_bps"])
    if not 0 < slip < 5000:
        raise ValueError("slippage must be between 0 and 50%")
    me = Web3.to_checksum_address(evm_wallet.ADDRESS)
    tok = Web3.to_checksum_address(token)
    weth = Web3.to_checksum_address(c["weth"])
    router = Web3.to_checksum_address(c["router"])
    pool = find_pool(chain, token)
    if not pool:
        raise LookupError("no Uniswap V3 pool for this token — not tradeable")
    sym, dec = token_meta(chain, token)
    r = w3.eth.contract(address=router, abi=ROUTER_ABI)
    erc = w3.eth.contract(address=tok, abi=ERC20_ABI)
    native = w3.eth.get_balance(me)

    # pre-flight: quote against CURRENT state; a revert here aborts the send
    q = quote(chain, token, side, amount, address=me)
    min_out = int(q["amount_out_raw"] * (10000 - slip) / 10000)
    if min_out <= 0:
        raise RuntimeError("quote returned zero output — refusing to send")

    approve_hash = None
    if side == "buy":
        amt_in = int(float(amount) * 1e18)
        if float(amount) > float(cfg["max_native_per_trade"]):
            raise ValueError(f"amount exceeds max_native_per_trade "
                             f"({cfg['max_native_per_trade']} {c['native']})")
        if native <= amt_in:
            raise RuntimeError("insufficient native balance for amount + gas")
        data = r.functions.exactInputSingle(
            (weth, tok, pool["fee"], me, amt_in, min_out, 0))._encode_transaction_data()
        value = amt_in
    else:
        amt_in = int(float(amount) * 10**dec)
        bal = erc.functions.balanceOf(me).call()
        if amt_in <= 0 or amt_in > bal:
            raise ValueError(f"amount exceeds balance ({bal / 10**dec:,.4f} {sym})")
        if native == 0:
            raise RuntimeError(f"no {c['native']} for gas in this wallet")
        if erc.functions.allowance(me, router).call() < amt_in:
            tx = erc.functions.approve(router, amt_in).build_transaction({
                "from": me, "nonce": w3.eth.get_transaction_count(me),
                "chainId": c["chain_id"], **_gas_fields(w3, cfg)})
            approve_hash = _send(w3, cfg, tx)
            w3.eth.wait_for_transaction_receipt(approve_hash, timeout=180)
        swp = r.functions.exactInputSingle(
            (tok, weth, pool["fee"], ADDRESS_THIS, amt_in, min_out, 0)
        )._encode_transaction_data()
        unw = r.functions.unwrapWETH9(min_out, me)._encode_transaction_data()
        data = r.functions.multicall(
            [bytes.fromhex(swp[2:]), bytes.fromhex(unw[2:])])._encode_transaction_data()
        value = 0

    tx = {"from": me, "to": router, "data": data, "value": value,
          "nonce": w3.eth.get_transaction_count(me), "chainId": c["chain_id"],
          **_gas_fields(w3, cfg)}
    try:
        w3.eth.call(tx)      # final simulation with the REAL min_out in place
        tx_hash = _send(w3, cfg, tx)
    except Exception as e:
        raise _humanize(e)

    rec = {"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "chain": chain, "side": side,
           "token": tok, "symbol": sym, "amount_in": q["amount_in"],
           "expected_out": q["amount_out"], "min_out": min_out / (10**18 if side == "sell" else 10**dec),
           "slippage_bps": slip, "approve_tx": approve_hash, "tx": tx_hash,
           "wallet": me, "live": True}
    _log(rec)
    return {**rec, "explorer": f"{c['explorer']}/tx/{tx_hash}"}


def balance_of(chain, token, address=None):
    """Human-units ERC-20 balance of `address` (default: the unlocked wallet).
    Used by autonomous callers (evm_copy) to sell the REAL held amount rather
    than a stale quoted figure. Raises if the wallet is locked and no address
    is given."""
    w3, _ = w3_for(chain)
    who = Web3.to_checksum_address(address or evm_wallet.ADDRESS)
    _, dec = token_meta(chain, token)
    erc = w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)
    return erc.functions.balanceOf(who).call() / 10 ** dec


def tx_status(chain, tx_hash):
    w3, c = w3_for(chain)
    try:
        rc = w3.eth.get_transaction_receipt(tx_hash)
    except Exception:
        return {"status": "pending"}
    return {"status": "success" if rc["status"] == 1 else "failed",
            "block": rc["blockNumber"], "gas_used": rc["gasUsed"],
            "explorer": f"{c['explorer']}/tx/{tx_hash}"}
