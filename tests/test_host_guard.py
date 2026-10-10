"""The DNS-rebinding guard in jup_trader.py, exercised on a throwaway Flask app.

jup_trader.py itself is not imported (it loads wallet keys and opens RPC sessions at import),
so the guard block is extracted from the source and attached to a stub app."""
import os
from pathlib import Path

import pytest

flask = pytest.importorskip("flask")

SRC = (Path(__file__).resolve().parent.parent / "jup_trader.py").read_text(encoding="utf-8")


def make_app(extra_hosts=""):
    start = SRC.index("_ALLOWED_HOSTS = ")
    end = SRC.index("@app.before_request\ndef _tag_sellbox_request():")
    app = flask.Flask("stub")
    os.environ["JUP_ALLOWED_HOSTS"] = extra_hosts
    exec(SRC[start:end], {"app": app, "os": os, "jsonify": flask.jsonify})

    @app.route("/api/buy", methods=["POST"])
    def buy():
        return {"ok": True}

    return app.test_client()


@pytest.mark.parametrize("host", ["127.0.0.1:5010", "localhost:5010", "localhost", "[::1]:5010", "LOCALHOST:5010"])
def test_local_hosts_allowed(host):
    assert make_app().post("/api/buy", json={}, headers={"Host": host}).status_code == 200


@pytest.mark.parametrize("host", ["evil.example:5010", "127.0.0.1.evil.example", "attacker.test", ""])
def test_rebinding_hosts_rejected(host):
    assert make_app().post("/api/buy", json={}, headers={"Host": host}).status_code == 403


def test_extra_hosts_from_env():
    c = make_app("box.lan")
    assert c.post("/api/buy", json={}, headers={"Host": "box.lan:5010"}).status_code == 200
    assert c.post("/api/buy", json={}, headers={"Host": "other.lan"}).status_code == 403
