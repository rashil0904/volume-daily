#!/usr/bin/env python3
"""
test_exit_stage_timing.py -- standalone verifier for the two-pinned-wall-clock-
instant staging holds added to check_exit_925/force_exit_1159/square_off_239
(_hold_until, reusing run_entry_321's own _seconds_until mechanism -- see
run_trades.py's module note above _EXIT_925_PREP_AT).

Each of these three stages now has a prep-check hold (position load + Order
Book snapshot + UC-cache read -- none of it price-dependent) followed by a
fire hold (fresh LTP, then the actual sell/cover decision). This file proves
the ORDERING guarantee directly -- not by manipulating real wall-clock time
(which would mean either sleeping for real in a test, or depending on when in
the day the suite happens to run), but by recording every relevant call
(_hold_until itself, plus every prep-step and fire-step call) into one shared
ordered log per scenario and asserting the exact interleaving:

    prep-hold  ->  position load  ->  Order Book snapshot  ->  fire-hold  ->
    LTP fetch  ->  (only after that) any sell/buy/cover call

This is what "prep-step code runs only at/after :50, fire-step code runs only
at/after :00, no price-dependent call executes before :00" actually reduces
to structurally: _hold_until's own contract (block or warn-and-continue, but
always a strict sequence point) means nothing after a given _hold_until call
in the code can execute before that call returns -- so proving the call
ORDER proves the wall-clock ordering, without needing to fake datetime.now()
itself.

_hold_until is mocked LOCALLY in this file only (recording calls into the log,
never sleeping) -- every OTHER test file in this suite mocks it globally as a
plain no-op instead, since none of them are testing this staging behavior.

Mocks every broker-facing call (buy/sell/_poll_fill_safe/_broker_qty/
_broker_short_qty/_dhan_get_orders/get_ltp_batch/_available_balance/
_fetch_upper_circuit_batch) and the position-file read/write with an
in-memory store -- zero network calls, zero real file writes, zero real
sleeps.

Usage:
    python dhan/test_exit_stage_timing.py

Exit 0 on all-pass, exit 1 on any failure.
"""

import copy
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

for _m in ("data_loader",):
    sys.modules.setdefault(_m, types.ModuleType(_m))

import dhan.run_trades as rt   # noqa: E402

patch.object(rt, "tick_size", lambda sym: 0.05).start()
patch.object(rt, "_sync_pnl_workbook", lambda: None).start()

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
failures = 0


def check(label: str, condition: bool, detail: str = "") -> None:
    global failures
    if condition:
        print(f"  {PASS}  {label}")
    else:
        failures += 1
        print(f"  {FAIL}  {label}" + (f"  [{detail}]" if detail else ""))


class FakeStore:
    def __init__(self, positions):
        self.positions = copy.deepcopy(positions)

    def load(self):
        return copy.deepcopy(self.positions)

    def save(self, positions):
        self.positions = copy.deepcopy(positions)


def make_long(**overrides):
    row = {
        "broker": "dhan", "symbol": "TIMECO", "entry_date": "2026-08-17",
        "reference_price": 100.0, "shares_intended": 10,
        "actual_fill_price": 100.0, "actual_fill_quantity": 10,
        "entry_order_id": "E1", "status": "open",
        "entry_timestamp": "2026-08-17T15:21:00+05:30",
        # CNC -- _sell_margin_safe returns immediately after sell() for this
        # product (no ledger-lag status check/sleep), keeping this file's
        # mocking surface small and focused on the staging-hold ordering,
        # not on unrelated MTF retry mechanics already covered elsewhere.
        "product": "CNC",
    }
    row.update(overrides)
    return row


def make_short(**overrides):
    row = {
        "broker": "dhan", "symbol": "TIMECO", "direction": "short",
        "product": "INTRADAY", "source_exit_stage": "925",
        "entry_date": "2026-08-17", "entry_price": 100.0, "quantity": 10,
        "entry_order_id": "S1", "status": "short_open",
        "entry_timestamp": "2026-08-17T09:25:00+05:30",
        "cover_target_order_id": None, "stop_order_id": None,
    }
    row.update(overrides)
    return row


# ══════════════════════════════════════════════════════════════════════════
# [1] check_exit_925 -- prep (09:24:50) strictly before fire (09:25:00),
#     no price-dependent call before the fire-hold
# ══════════════════════════════════════════════════════════════════════════

def test_check_exit_925_ordering():
    print("\n[1] check_exit_925 -- prep-hold -> prep work -> fire-hold -> fire work")
    log: list = []

    def fake_hold_until(hh, mm, ss, label):
        log.append(("hold", hh, mm, ss, label))

    store = FakeStore([make_long()])

    def fake_load_long_pos():
        log.append(("load_positions",))
        return store.load()

    def fake_get_orders():
        log.append(("get_orders",))
        return []   # no target_order_id on this position -- snapshot content irrelevant here

    def fake_load_uc_cache():
        log.append(("load_uc_cache",))
        return {}

    def fake_get_ltp_batch(syms):
        log.append(("get_ltp_batch",))
        return {s: 110.0 for s in syms}   # 100 -> 110, positive P&L -> queues a sell

    def fake_sell(sym, exch, qty, **kw):
        log.append(("sell",))
        return "SELL-1"

    def fake_poll_fill_safe(oid, fallback_price, fallback_qty):
        log.append(("poll_fill",))
        return 110.0, fallback_qty

    with patch.object(rt, "_hold_until", fake_hold_until), \
         patch.object(rt, "_load_long_pos", fake_load_long_pos), \
         patch.object(rt, "_save_long_pos", store.save), \
         patch.object(rt, "_dhan_get_orders", fake_get_orders), \
         patch.object(rt, "_load_uc_cache", fake_load_uc_cache), \
         patch.object(rt, "get_ltp_batch", fake_get_ltp_batch), \
         patch.object(rt, "sell", fake_sell), \
         patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe), \
         patch.object(rt, "_broker_qty", lambda sym, product: (10, "NSE_EQ")), \
         patch.object(rt, "_open_short_place", lambda *a, **kw: None), \
         patch.object(rt, "_fetch_upper_circuit_batch", lambda syms: {}), \
         patch.object(rt, "_available_balance", lambda: 10_000_000.0), \
         patch.object(rt.notify, "send_exit_925", MagicMock()):
        rt.check_exit_925(dry_run=False)

    kinds = [entry[0] for entry in log]
    check("prep-hold (09:24:50) is the very first thing logged",
          log[0] == ("hold", 9, 24, 50, "check_exit_925 prep"), str(log[:3]))
    check("position load happens after the prep-hold",
          kinds.index("load_positions") > kinds.index("hold"), str(kinds))
    check("Order Book snapshot happens after the prep-hold, before any fire-hold",
          kinds.index("hold") < kinds.index("get_orders"), str(kinds))
    fire_hold_idx = next(i for i, e in enumerate(log) if e[0] == "hold"
                        and e[1:4] == (9, 25, 0))
    check("fire-hold (09:25:00) is logged, after the prep-hold",
          log[fire_hold_idx] == ("hold", 9, 25, 0, "check_exit_925 fire"))
    check("Order Book snapshot happens BEFORE the fire-hold, not after",
          kinds.index("get_orders") < fire_hold_idx, str(kinds))
    check("get_ltp_batch (price-dependent) happens AFTER the fire-hold, never before",
          kinds.index("get_ltp_batch") > fire_hold_idx, str(kinds))
    check("sell() (price-dependent decision) happens AFTER the fire-hold, never before",
          kinds.index("sell") > fire_hold_idx, str(kinds))
    check("sell() happens after get_ltp_batch (LTP resolved before the decision fires)",
          kinds.index("sell") > kinds.index("get_ltp_batch"), str(kinds))


# ══════════════════════════════════════════════════════════════════════════
# [2] force_exit_1159 -- prep (11:58:50) strictly before fire (11:59:00)
# ══════════════════════════════════════════════════════════════════════════

def test_force_exit_1159_ordering():
    print("\n[2] force_exit_1159 -- prep-hold -> prep work -> fire-hold -> fire work")
    log: list = []

    def fake_hold_until(hh, mm, ss, label):
        log.append(("hold", hh, mm, ss, label))

    store = FakeStore([make_long()])

    def fake_load_long_pos():
        log.append(("load_positions",))
        return store.load()

    def fake_get_orders():
        log.append(("get_orders",))
        return []

    def fake_load_uc_cache():
        log.append(("load_uc_cache",))
        return {}

    def fake_get_ltp_batch(syms):
        log.append(("get_ltp_batch",))
        return {s: 90.0 for s in syms}   # loss -- irrelevant at 11:59, unconditional sell anyway

    def fake_sell(sym, exch, qty, **kw):
        log.append(("sell",))
        return "SELL-1"

    def fake_poll_fill_safe(oid, fallback_price, fallback_qty):
        log.append(("poll_fill",))
        return 90.0, fallback_qty

    with patch.object(rt, "_hold_until", fake_hold_until), \
         patch.object(rt, "_load_long_pos", fake_load_long_pos), \
         patch.object(rt, "_save_long_pos", store.save), \
         patch.object(rt, "_dhan_get_orders", fake_get_orders), \
         patch.object(rt, "_load_uc_cache", fake_load_uc_cache), \
         patch.object(rt, "get_ltp_batch", fake_get_ltp_batch), \
         patch.object(rt, "sell", fake_sell), \
         patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe), \
         patch.object(rt, "_broker_qty", lambda sym, product: (10, "NSE_EQ")), \
         patch.object(rt, "_open_short_place", lambda *a, **kw: None), \
         patch.object(rt, "_fetch_upper_circuit_batch", lambda syms: {}), \
         patch.object(rt, "_available_balance", lambda: 10_000_000.0), \
         patch.object(rt.notify, "send_force_exit_1159", MagicMock()):
        rt.force_exit_1159(dry_run=False)

    kinds = [entry[0] for entry in log]
    check("prep-hold (11:58:50) is the very first thing logged",
          log[0] == ("hold", 11, 58, 50, "force_exit_1159 prep"), str(log[:3]))
    check("position load happens after the prep-hold",
          kinds.index("load_positions") > kinds.index("hold"), str(kinds))
    fire_hold_idx = next(i for i, e in enumerate(log) if e[0] == "hold"
                        and e[1:4] == (11, 59, 0))
    check("fire-hold (11:59:00) is logged, after the prep-hold",
          log[fire_hold_idx] == ("hold", 11, 59, 0, "force_exit_1159 fire"))
    check("Order Book snapshot happens BEFORE the fire-hold, not after",
          kinds.index("get_orders") < fire_hold_idx, str(kinds))
    check("get_ltp_batch (price-dependent) happens AFTER the fire-hold, never before",
          kinds.index("get_ltp_batch") > fire_hold_idx, str(kinds))
    check("sell() (unconditional force-sell) happens AFTER the fire-hold, never before",
          kinds.index("sell") > fire_hold_idx, str(kinds))


# ══════════════════════════════════════════════════════════════════════════
# [3] square_off_239 -- prep (14:38:50) strictly before fire (14:39:00)
# ══════════════════════════════════════════════════════════════════════════

def test_square_off_239_ordering():
    print("\n[3] square_off_239 -- prep-hold -> prep work -> fire-hold -> fire work")
    log: list = []

    def fake_hold_until(hh, mm, ss, label):
        log.append(("hold", hh, mm, ss, label))

    store = FakeStore([make_short()])   # no cover/stop order ids -> classifies neither_filled

    def fake_load_short_pos():
        log.append(("load_positions",))
        return store.load()

    def fake_get_orders():
        log.append(("get_orders",))
        return []

    def fake_get_ltp_batch(syms):
        log.append(("get_ltp_batch",))
        return {s: 105.0 for s in syms}

    def fake_buy(sym, exch, qty, **kw):
        log.append(("buy",))
        return "COVER-1"

    def fake_poll_fill_safe(oid, fallback_price, fallback_qty):
        log.append(("poll_fill",))
        return 105.0, fallback_qty

    with patch.object(rt, "_hold_until", fake_hold_until), \
         patch.object(rt, "_load_short_pos", fake_load_short_pos), \
         patch.object(rt, "_save_short_pos", store.save), \
         patch.object(rt, "_dhan_get_orders", fake_get_orders), \
         patch.object(rt, "get_ltp_batch", fake_get_ltp_batch), \
         patch.object(rt, "buy", fake_buy), \
         patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe), \
         patch.object(rt, "_broker_short_qty", lambda sym: 10), \
         patch.object(rt.notify, "send_square_off_239", MagicMock()):
        rt.square_off_239(dry_run=False)

    kinds = [entry[0] for entry in log]
    check("prep-hold (14:38:50) is the very first thing logged",
          log[0] == ("hold", 14, 38, 50, "square_off_239 prep"), str(log[:3]))
    check("position load happens after the prep-hold",
          kinds.index("load_positions") > kinds.index("hold"), str(kinds))
    fire_hold_idx = next(i for i, e in enumerate(log) if e[0] == "hold"
                        and e[1:4] == (14, 39, 0))
    check("fire-hold (14:39:00) is logged, after the prep-hold",
          log[fire_hold_idx] == ("hold", 14, 39, 0, "square_off_239 fire"))
    check("Order Book snapshot (+ classification) happens BEFORE the fire-hold",
          kinds.index("get_orders") < fire_hold_idx, str(kinds))
    check("get_ltp_batch (price-dependent) happens AFTER the fire-hold, never before",
          kinds.index("get_ltp_batch") > fire_hold_idx, str(kinds))
    check("buy() (force-cover, price-dependent) happens AFTER the fire-hold, never before",
          kinds.index("buy") > fire_hold_idx, str(kinds))


# ══════════════════════════════════════════════════════════════════════════
# [4] _hold_until itself -- late-target warning, not a silent skip
# ══════════════════════════════════════════════════════════════════════════

def test_hold_until_late_warns_instead_of_silent():
    print("\n[4] _hold_until -- an already-passed target logs an explicit warning")
    # A target far enough in the past that _seconds_until reports a large
    # negative value (well within the "not more than 60s early" branch's
    # complement) -- use "now" 30s after the target to keep this deterministic
    # regardless of when in the day the suite actually runs.
    import datetime as _dt
    fixed_now = _dt.datetime(2026, 9, 8, 9, 25, 30, tzinfo=rt._IST)

    printed = []
    with patch.object(rt, "_seconds_until",
                      lambda hh, mm, ss, now=None: -30.0), \
         patch("builtins.print", lambda *a, **kw: printed.append(" ".join(str(x) for x in a))):
        rt._hold_until(9, 25, 0, "test label")

    check("logs an explicit warning when the target has already passed",
          any("already passed" in line and "test label" in line for line in printed),
          str(printed))
    check("warning reports the actual lateness, not a vague message",
          any("30.000s late" in line for line in printed), str(printed))


def test_hold_until_far_future_returns_without_warning():
    print("\n[4b] _hold_until -- a target more than 60s away returns immediately, no warning")
    printed = []
    with patch.object(rt, "_seconds_until", lambda hh, mm, ss, now=None: 3600.0), \
         patch("builtins.print", lambda *a, **kw: printed.append(" ".join(str(x) for x in a))), \
         patch.object(rt.time, "sleep", lambda s: (_ for _ in ()).throw(
             AssertionError("must not sleep when target is >60s away"))):
        rt._hold_until(9, 25, 0, "test label")
    check("no output at all for a call far outside its normal window",
          printed == [], str(printed))


# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    test_check_exit_925_ordering()
    test_force_exit_1159_ordering()
    test_square_off_239_ordering()
    test_hold_until_late_warns_instead_of_silent()
    test_hold_until_far_future_returns_without_warning()

    print()
    if failures:
        print(f"{failures} check(s) FAILED.")
        sys.exit(1)
    print("All checks PASSED.")
    sys.exit(0)
