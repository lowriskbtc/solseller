"""Unit tests import only pure modules (never jup_trader.py, which loads keys and opens an RPC
session at import). The network is blocked and wallet/API credentials are removed."""
import os
import socket
import sys
from pathlib import Path

for k in list(os.environ):
    if any(s in k for s in ("PRIVATE_KEY", "KEYPAIR", "WALLET", "RPC_URL", "JUP_API_KEY", "HELIUS", "TG_")):
        os.environ.pop(k, None)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _blocked(*args, **kwargs):
    raise RuntimeError("Network access is blocked in tests")


socket.socket.connect = _blocked
socket.create_connection = _blocked
