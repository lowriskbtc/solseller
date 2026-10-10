"""evm_wallet.py — encrypted EVM key vault, mirroring the Solana wallet vault
in jup_trader.py (PBKDF2-SHA256 600k iters + Fernet, key entered in the UI and
never persisted in plaintext).

This module ONLY holds and unlocks a key and derives its address. It does not
send transactions — signing/swap execution is a separate layer (Phase 2 of
EVM_COPY_GAMEPLAN.md) and must stay paper-gated. Storing a key here does not
arm anything.

SECURITY: only ever load a FRESHLY GENERATED key that has never been pasted
into a chat, email, or any other channel. A key that has been transmitted in
plaintext anywhere is compromised and must not be used with real funds.
"""
import base64
import json
import os
import time
import threading

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from eth_account import Account

HERE = os.path.dirname(os.path.abspath(__file__))
VAULT_PATH = os.path.join(HERE, "evm_wallet.enc.json")
_KDF_ITERS = 600_000
_lock = threading.Lock()

# in-memory unlocked account (never written to disk); None until unlocked
ACCOUNT = None
ADDRESS = None


def _norm_priv(priv):
    """Accept a 0x-prefixed or bare 64-hex private key; return canonical 0x form.
    Raises ValueError on anything that isn't a 32-byte key."""
    p = priv.strip()
    if p.lower().startswith("0x"):
        p = p[2:]
    if len(p) != 64 or any(c not in "0123456789abcdefABCDEF" for c in p):
        raise ValueError("not a valid 32-byte (64 hex char) private key")
    return "0x" + p.lower()


def _fernet(password, salt):
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32,
                     salt=salt, iterations=_KDF_ITERS)
    return Fernet(base64.urlsafe_b64encode(kdf.derive(password.encode())))


def _load():
    if os.path.exists(VAULT_PATH):
        with open(VAULT_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"wallets": []}


def _save(v):
    with _lock:
        tmp = VAULT_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(v, f, indent=1)
        os.replace(tmp, VAULT_PATH)


def list_wallets():
    return [{"address": w["address"], "created": w.get("created")}
            for w in _load()["wallets"]]


def store(priv, password):
    """Encrypt a private key into the vault. Returns its checksummed address."""
    priv = _norm_priv(priv)
    acct = Account.from_key(priv)
    v = _load()
    if any(w["address"].lower() == acct.address.lower() for w in v["wallets"]):
        raise RuntimeError("wallet already in vault")
    salt = os.urandom(16)
    token = _fernet(password, salt).encrypt(priv.encode()).decode()
    v["wallets"].append({
        "address": acct.address,
        "created": time.strftime("%Y-%m-%d %H:%M:%S"),
        "salt": base64.b64encode(salt).decode(),
        "token": token,
    })
    _save(v)
    return acct.address


def create(password):
    """Generate a NEW key locally, store it encrypted, unlock it in-memory.
    Returns (address, private_key_0x). The private key is returned ONCE for the
    user to back up and is never persisted in plaintext."""
    acct = Account.create()
    priv = acct.key.hex()
    if not priv.startswith("0x"):
        priv = "0x" + priv
    store(priv, password)
    _set_unlocked(acct)
    return acct.address, priv


def unlock(address, password):
    for w in _load()["wallets"]:
        if w["address"].lower() == address.lower():
            try:
                priv = _fernet(password, base64.b64decode(w["salt"])) \
                    .decrypt(w["token"].encode()).decode()
            except InvalidToken:
                raise RuntimeError("wrong password")
            _set_unlocked(Account.from_key(priv))
            return ADDRESS
    raise RuntimeError("wallet not in vault")


def reveal(address, password):
    """Decrypt and return the plaintext private key for backup (password-gated,
    localhost only). Standard 'export private key' flow — the key is never
    written to disk in plaintext by this call."""
    for w in _load()["wallets"]:
        if w["address"].lower() == address.lower():
            try:
                return _fernet(password, base64.b64decode(w["salt"])) \
                    .decrypt(w["token"].encode()).decode()
            except InvalidToken:
                raise RuntimeError("wrong password")
    raise RuntimeError("wallet not in vault")


def remove(address):
    v = _load()
    keep = [w for w in v["wallets"] if w["address"].lower() != address.lower()]
    if len(keep) == len(v["wallets"]):
        raise RuntimeError("wallet not in vault")
    v["wallets"] = keep
    _save(v)
    lock()


def _set_unlocked(acct):
    global ACCOUNT, ADDRESS
    ACCOUNT = acct
    ADDRESS = acct.address


def lock():
    global ACCOUNT, ADDRESS
    ACCOUNT = None
    ADDRESS = None


def status():
    return {"unlocked": ACCOUNT is not None, "address": ADDRESS,
            "vault": list_wallets()}
