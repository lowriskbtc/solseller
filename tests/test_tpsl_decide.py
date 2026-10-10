"""tpsl.decide() is the whole take-profit / stop-loss trigger. These tests replay ticks through
it and pin every guard documented in tpsl.py, including the ones that once failed in production."""
import pytest

import tpsl
from tpsl import FIRE, HOLD, PosState, decide, limits_for, sell_pct_for

CFG = {"confirm_ticks": 2, "max_price_age_s": 20.0, "cooldown_s": 45.0, "max_fires_per_day": 40}
LIM = {"tp": 50.0, "sl": 30.0, "off": False}


def tick(st, pnl, *, now=1000.0, ts=None, age=1.0, basis="ok", in_flight=False, armed=True,
         cfg=CFG, limits=LIM, fires_today=0):
    return decide(st, pnl_pct=pnl, limits=limits, now=now, price_age_s=age, basis=basis,
                  in_flight=in_flight, armed=armed, cfg=cfg, fires_today=fires_today, price_ts=ts)


def test_in_band_holds():
    st = PosState("m")
    assert tick(st, 10.0)[0] == HOLD and st.state == tpsl.IDLE


def test_tp_fires_after_two_independent_prices():
    st = PosState("m")
    assert tick(st, 60.0, ts=1)[0] == HOLD                 # 1/2
    action, _, kind = tick(st, 61.0, ts=2)
    assert action == FIRE and kind == "tp"


def test_same_price_print_twice_does_not_confirm():
    """Regression: a 3s poll re-reading a ~6s cached price counted as two confirmations."""
    st = PosState("m")
    tick(st, 60.0, ts=1)
    action, reason, _ = tick(st, 60.0, ts=1)
    assert action == HOLD and "awaiting a new price print" in reason
    assert tick(st, 60.0, ts=2)[0] == FIRE


def test_stop_loss_fires_on_the_downside():
    st = PosState("m")
    tick(st, -31.0, ts=1)
    assert tick(st, -35.0, ts=2)[2] == "sl"


def test_fire_latches_until_back_in_band():
    """Regression: a 50% TP left the position above the threshold and the next tick sold the rest."""
    st = PosState("m")
    tick(st, 60.0, ts=1)
    assert tick(st, 60.0, ts=2)[0] == FIRE
    action, reason, _ = tick(st, 70.0, ts=3, now=2000.0)
    assert action == HOLD and "already fired" in reason
    tick(st, 10.0, ts=4, now=2000.0)                        # back in band re-arms
    tick(st, 60.0, ts=5, now=2000.0)
    assert tick(st, 60.0, ts=6, now=2000.0)[0] == FIRE


def test_decide_twice_on_same_state_never_double_fires():
    st = PosState("m")
    tick(st, 60.0, ts=1)
    assert tick(st, 60.0, ts=2)[0] == FIRE
    assert tick(st, 60.0, ts=3)[0] == HOLD


def test_stale_price_resets_a_half_built_streak():
    st = PosState("m")
    tick(st, 60.0, ts=1)
    action, reason, _ = tick(st, 60.0, ts=2, age=60.0)
    assert action == HOLD and "stale" in reason and st.streak == 0
    assert tick(st, 60.0, ts=3)[0] == HOLD                  # must re-confirm from scratch


@pytest.mark.parametrize("basis", ["estimate", "incomplete", "stale", "none"])
def test_unverified_basis_never_sells(basis):
    st = PosState("m")
    for ts in range(1, 5):
        action, _, _ = tick(st, 90.0, ts=ts, basis=basis)
        assert action == HOLD
    assert st.unprotected


@pytest.mark.parametrize("kw", [
    {"in_flight": True}, {"armed": False}, {"fires_today": 40}, {"limits": {**LIM, "off": True}},
])
def test_blocking_gates(kw):
    st = PosState("m")
    tick(st, 60.0, ts=1, **kw)
    assert tick(st, 60.0, ts=2, **kw)[0] == HOLD


def test_missing_or_nan_pnl_holds():
    st = PosState("m")
    assert tick(st, None, ts=1)[0] == HOLD
    assert tick(st, float("nan"), ts=2)[0] == HOLD and st.unprotected == "no live PnL"


def test_cooldown_after_a_fire():
    st = PosState("m")
    tick(st, 60.0, ts=1)
    tick(st, 60.0, ts=2, now=1000.0)                        # fires, latches TP
    tick(st, -40.0, ts=3, now=1010.0)
    action, reason, _ = tick(st, -40.0, ts=4, now=1010.0)   # SL confirmed but inside cooldown
    assert action == HOLD and "cooldown" in reason
    assert tick(st, -40.0, ts=5, now=1100.0)[0] == FIRE


def test_without_price_stamps_polls_are_counted():
    st = PosState("m")
    tick(st, 60.0)
    assert tick(st, 60.0)[0] == FIRE


# ---------------- limits and sizing ----------------

def test_limits_per_mint_override_and_zero_is_real():
    """Regression: a 0% stop ("get me out at break-even") used to fall back to the global."""
    cfg = {"tp_pct": 55, "sl_pct": 40, "per": {"A": {"sl": 0}, "B": {"tp": -20, "off": True}}}
    assert limits_for(cfg, "A") == {"tp": 55.0, "sl": 0.0, "off": False}
    assert limits_for(cfg, "B") == {"tp": 20.0, "sl": 40.0, "off": True}
    assert limits_for({}, "Z") == {"tp": tpsl.DEFAULTS["tp_pct"], "sl": tpsl.DEFAULTS["sl_pct"], "off": False}


@pytest.mark.parametrize("value,expected", [(50, 50.0), (150, 100.0), (-5, 0.0), ("junk", 100.0)])
def test_sell_pct_is_clamped(value, expected):
    assert sell_pct_for({"tp_sell_pct": value}, "tp") == expected


# ---------------- model output parsing (meme_agent) ----------------

@pytest.mark.parametrize("value,expected", [
    (True, True), ("true", True), ("yes", True), (1, True),
    ("false", False), ("no", False), ("0", False), (0, False), (None, False), ("maybe", False),
])
def test_meme_agent_reads_enter_strictly(value, expected):
    """Regression: bool("false") is True, so "enter": "false" was logged as an approval."""
    import meme_agent
    assert meme_agent._coerce({"enter": value, "confidence": 0.5})["enter"] is expected
