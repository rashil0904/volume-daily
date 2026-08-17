#!/usr/bin/env python3
"""
test_targets.py -- standalone verifier for the profit-target + UC-based
stop-loss mechanism added to zerodha/run_trades_mtf.py: place_targets_915_mtf(),
the target-status checks inserted into check_exit_925_mtf/force_exit_1159_mtf,
cover-target + stop-loss placement in _open_short(), and the OCO handling in
square_off_239_mtf(). Direct mirror of dhan/test_targets.py, adapted to Kite's
field names (status/filled_quantity/average_price, "COMPLETE" not "TRADED",
exchange "NSE" not "NSE_EQ", product "MIS" not "INTRADAY").

Mocks every broker-facing call this touches (buy/sell/place_order/order_status/
cancel_order/_broker_qty/_broker_short_qty/_available_margin/_mis_margin_check/
get_ltp/_poll_fill_safe/_fetch_upper_circuit) and the positions_zerodha.json
read/write (_load_pos/_save_pos) with an in-memory store -- zero network calls,
zero real file writes. Mirrors zerodha/test_state_machine.py's standalone
script style (no pytest in this repo).

Usage:
    python zerodha/test_targets.py

Exit 0 on all-pass, exit 1 on any failure.
"""

import copy
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

# Stub pipeline modules touched at import time (mirrors zerodha/test_state_machine.py)
for _m in ("data_loader",):
    sys.modules.setdefault(_m, types.ModuleType(_m))

import zerodha.run_trades_mtf as rt  # noqa: E402  (import after sys.path/stub setup)

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
    """Backs _load_pos/_save_pos with an in-memory list -- no real file I/O."""
    def __init__(self, positions):
        self.positions = copy.deepcopy(positions)
        self.save_count = 0

    def load(self):
        return copy.deepcopy(self.positions)

    def save(self, positions):
        self.positions = copy.deepcopy(positions)
        self.save_count += 1


def make_long(**overrides):
    row = {
        "broker": "zerodha", "symbol": "TESTCO", "entry_date": "2026-08-17",
        "reference_price": 100.0, "shares_intended": 10,
        "actual_fill_price": 100.0, "actual_fill_quantity": 10,
        "entry_order_id": "E1", "status": "open",
        "entry_timestamp": "2026-08-17T15:21:00+05:30", "product": "MTF",
    }
    row.update(overrides)
    return row


def make_short(**overrides):
    row = {
        "broker": "zerodha", "symbol": "TESTCO", "direction": "short",
        "product": "MIS", "source_exit_stage": "925",
        "entry_date": "2026-08-17", "entry_price": 100.0, "quantity": 10,
        "entry_order_id": "S1", "status": "short_open",
        "entry_timestamp": "2026-08-17T09:25:00+05:30",
    }
    row.update(overrides)
    return row


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (a) — place_targets_915_mtf\n")
# ─────────────────────────────────────────────────────────────────────────────

pos_no_target  = make_long(symbol="ALPHA", actual_fill_price=100.0, actual_fill_quantity=10)
pos_has_target = make_long(symbol="BETA", actual_fill_price=50.0, actual_fill_quantity=20,
                            target_order_id="EXISTING1", target_price=58.5)
store = FakeStore([pos_no_target, pos_has_target])

sell_calls = []
def fake_sell_a(symbol, exch, qty, **kw):
    sell_calls.append((symbol, exch, qty, kw.get("order_type"), kw.get("price"), kw.get("product")))
    return f"TGT-{symbol}"

with patch.object(rt, "_load_pos", store.load), \
     patch.object(rt, "_save_pos", store.save), \
     patch.object(rt, "sell", fake_sell_a), \
     patch.object(rt.notify, "send_target_placed", MagicMock()):
    rt.place_targets_915_mtf(dry_run=False)

check("(a) ALPHA (no target yet) gets a target order placed",
      any(c[0] == "ALPHA" for c in sell_calls))
alpha_call = next(c for c in sell_calls if c[0] == "ALPHA")
check("(a) ALPHA target order_type is LIMIT", alpha_call[3] == "LIMIT")
check("(a) ALPHA target price is 117.0 (100 * 1.17)", alpha_call[4] == 117.0, str(alpha_call))
check("(a) BETA (already has a target) does NOT get a duplicate order",
      not any(c[0] == "BETA" for c in sell_calls))
alpha_row = next(p for p in store.positions if p["symbol"] == "ALPHA")
check("(a) ALPHA row now has target_order_id saved", alpha_row.get("target_order_id") == "TGT-ALPHA")
check("(a) ALPHA row now has target_price saved", alpha_row.get("target_price") == 117.0)


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (b) — 9:25, target already COMPLETE\n")
# ─────────────────────────────────────────────────────────────────────────────

pos = make_long(symbol="GAMMA", actual_fill_price=100.0, actual_fill_quantity=10,
                 target_order_id="TGT-GAMMA", target_price=117.0, status="open")
store = FakeStore([pos])

order_status_calls = []
def fake_order_status_b(oid):
    order_status_calls.append(oid)
    return {"status": "COMPLETE", "filled_quantity": 10, "average_price": 117.0}

get_ltp_calls = []
def fake_get_ltp_b(sym):
    get_ltp_calls.append(sym)
    return 999.0

sell_calls_b = []
def fake_sell_b(*a, **kw):
    sell_calls_b.append((a, kw))
    return "SHOULD-NOT-HAPPEN"

open_short_calls = []
def fake_open_short_b(sym, qty, stage, dry_run=False):
    open_short_calls.append((sym, qty, stage))

with patch.object(rt, "_load_pos", store.load), \
     patch.object(rt, "_save_pos", store.save), \
     patch.object(rt, "_kite_order_status", fake_order_status_b), \
     patch.object(rt, "get_ltp", fake_get_ltp_b), \
     patch.object(rt, "sell", fake_sell_b), \
     patch.object(rt, "_open_short", fake_open_short_b), \
     patch.object(rt.notify, "send_target_hit", MagicMock()):
    rt.check_exit_925_mtf(dry_run=False)

check("(b) order_status was checked for the target", "TGT-GAMMA" in order_status_calls)
check("(b) get_ltp was NEVER called (existing no_data/pnl_live logic skipped)", get_ltp_calls == [])
check("(b) sell() was NEVER called (no market/half sell fired)", sell_calls_b == [])
row = store.positions[0]
check("(b) position marked exited_925", row["status"] == "exited_925")
check("(b) exit_price_925 == target's average_price", row["exit_price_925"] == 117.0)
check("(b) realized_pnl == (117-100)*10 == 170.0", row["realized_pnl"] == 170.0)
check("(b) mirrored short opened via to_short batching",
      open_short_calls == [("GAMMA", 10, "925")], str(open_short_calls))


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (c) — 9:25, target not traded, no-LTP-data fallback\n")
# ─────────────────────────────────────────────────────────────────────────────

pos = make_long(symbol="DELTA", actual_fill_price=100.0, actual_fill_quantity=10,
                 target_order_id="TGT-DELTA", target_price=117.0, status="open")
store = FakeStore([pos])

def fake_order_status_c(oid):
    return {"status": "OPEN", "filled_quantity": 0, "average_price": 0}

cancel_calls_c = []
def fake_cancel_c(oid):
    cancel_calls_c.append(oid)
    return oid

def fake_get_ltp_c(sym):
    raise ValueError("no LTP")

sell_calls_c = []
def fake_sell_c(symbol, exch, qty, **kw):
    sell_calls_c.append((symbol, exch, qty, kw.get("order_type"), kw.get("price")))
    if kw.get("order_type") == "LIMIT":
        return f"TGT2-{symbol}"
    return f"HALFSELL-{symbol}"

def fake_poll_fill_safe_c(oid, fallback_price, fallback_qty):
    return 99.0, fallback_qty

def fake_broker_qty_c(sym, product):
    return 10, "NSE"

open_short_calls_c = []
def fake_open_short_c(sym, qty, stage, dry_run=False):
    open_short_calls_c.append((sym, qty, stage))

with patch.object(rt, "_load_pos", store.load), \
     patch.object(rt, "_save_pos", store.save), \
     patch.object(rt, "_kite_order_status", fake_order_status_c), \
     patch.object(rt, "_kite_cancel_order", fake_cancel_c), \
     patch.object(rt, "get_ltp", fake_get_ltp_c), \
     patch.object(rt, "sell", fake_sell_c), \
     patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe_c), \
     patch.object(rt, "_broker_qty", fake_broker_qty_c), \
     patch.object(rt, "_open_short", fake_open_short_c), \
     patch.object(rt.notify, "send_exit_925_nodata", MagicMock()), \
     patch.object(rt.notify, "send_target_placed", MagicMock()):
    rt.check_exit_925_mtf(dry_run=False)

check("(c) cancel_order was called for the stale target", cancel_calls_c == ["TGT-DELTA"])
check("(c) a half-sell (MARKET) happened", any(c[3] == "MARKET" for c in sell_calls_c))
fresh_targets_c = [c for c in sell_calls_c if c[3] == "LIMIT"]
check("(c) a FRESH target LIMIT sell was placed for shares_remaining",
      len(fresh_targets_c) == 1, str(sell_calls_c))
check("(c) fresh target qty == shares_remaining (5)", fresh_targets_c[0][2] == 5)
check("(c) fresh target price == SAME target_price (117.0), not recomputed",
      fresh_targets_c[0][4] == 117.0)
row = store.positions[0]
check("(c) row status partial_exit_925_nodata", row["status"] == "partial_exit_925_nodata")
check("(c) row target_order_id updated to the fresh order", row["target_order_id"] == "TGT2-DELTA")
check("(c) row target_price UNCHANGED (still 117.0)", row["target_price"] == 117.0)


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (d) — 9:25, target not traded, pnl_live > 0\n")
# ─────────────────────────────────────────────────────────────────────────────

pos = make_long(symbol="EPSILON", actual_fill_price=100.0, actual_fill_quantity=10,
                 target_order_id="TGT-EPS", target_price=117.0, status="open")
store = FakeStore([pos])

call_order_d = []

def fake_order_status_d(oid):
    return {"status": "OPEN", "filled_quantity": 0, "average_price": 0}

def fake_cancel_d(oid):
    call_order_d.append(("cancel", oid))
    return oid

def fake_get_ltp_d(sym):
    return 105.0

def fake_sell_d(symbol, exch, qty, **kw):
    call_order_d.append(("sell", symbol, kw.get("order_type")))
    return f"MKT-{symbol}"

def fake_poll_fill_safe_d(oid, fallback_price, fallback_qty):
    return 105.0, fallback_qty

def fake_broker_qty_d(sym, product):
    return 10, "NSE"

open_short_calls_d = []
def fake_open_short_d(sym, qty, stage, dry_run=False):
    open_short_calls_d.append((sym, qty, stage))

with patch.object(rt, "_load_pos", store.load), \
     patch.object(rt, "_save_pos", store.save), \
     patch.object(rt, "_kite_order_status", fake_order_status_d), \
     patch.object(rt, "_kite_cancel_order", fake_cancel_d), \
     patch.object(rt, "get_ltp", fake_get_ltp_d), \
     patch.object(rt, "sell", fake_sell_d), \
     patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe_d), \
     patch.object(rt, "_broker_qty", fake_broker_qty_d), \
     patch.object(rt, "_open_short", fake_open_short_d), \
     patch.object(rt.notify, "send_exit_925", MagicMock()):
    rt.check_exit_925_mtf(dry_run=False)

check("(d) cancel_order called before the market-sell, in that order",
      call_order_d == [("cancel", "TGT-EPS"), ("sell", "EPSILON", "MARKET")], str(call_order_d))
row = store.positions[0]
check("(d) row exited_925", row["status"] == "exited_925")


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (e) — 9:25, target not traded, pnl_live <= 0\n")
# ─────────────────────────────────────────────────────────────────────────────

pos = make_long(symbol="ZETA", actual_fill_price=100.0, actual_fill_quantity=10,
                 target_order_id="TGT-ZETA", target_price=117.0, status="open")
store = FakeStore([pos])

def fake_order_status_e(oid):
    return {"status": "OPEN", "filled_quantity": 0, "average_price": 0}

cancel_calls_e = []
def fake_cancel_e(oid):
    cancel_calls_e.append(oid)
    return oid

def fake_get_ltp_e(sym):
    return 98.0

sell_calls_e = []
def fake_sell_e(*a, **kw):
    sell_calls_e.append((a, kw))
    return "SHOULD-NOT-HAPPEN"

with patch.object(rt, "_load_pos", store.load), \
     patch.object(rt, "_save_pos", store.save), \
     patch.object(rt, "_kite_order_status", fake_order_status_e), \
     patch.object(rt, "_kite_cancel_order", fake_cancel_e), \
     patch.object(rt, "get_ltp", fake_get_ltp_e), \
     patch.object(rt, "sell", fake_sell_e):
    rt.check_exit_925_mtf(dry_run=False)

check("(e) cancel_order NEVER called", cancel_calls_e == [])
check("(e) sell() NEVER called", sell_calls_e == [])
row = store.positions[0]
check("(e) target_order_id unchanged", row.get("target_order_id") == "TGT-ZETA")
check("(e) status still open (held for 11:59)", row["status"] == "open")


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (f) — 11:59 target-status handling\n")
# ─────────────────────────────────────────────────────────────────────────────

pos1 = make_long(symbol="ETA", actual_fill_price=100.0, actual_fill_quantity=10,
                  target_order_id="TGT-ETA", target_price=117.0, status="open")
store1 = FakeStore([pos1])

def fake_order_status_f1(oid):
    return {"status": "COMPLETE", "filled_quantity": 10, "average_price": 117.0}

sell_calls_f1 = []
def fake_sell_f1(*a, **kw):
    sell_calls_f1.append((a, kw))
    return "SHOULD-NOT-HAPPEN"

open_short_calls_f1 = []
def fake_open_short_f1(sym, qty, stage, dry_run=False):
    open_short_calls_f1.append((sym, qty, stage))

with patch.object(rt, "_load_pos", store1.load), \
     patch.object(rt, "_save_pos", store1.save), \
     patch.object(rt, "_kite_order_status", fake_order_status_f1), \
     patch.object(rt, "sell", fake_sell_f1), \
     patch.object(rt, "_broker_qty", lambda sym, product: (10, "NSE")), \
     patch.object(rt, "_open_short", fake_open_short_f1), \
     patch.object(rt.notify, "send_target_hit", MagicMock()), \
     patch.object(rt.notify, "send_nothing_open_at_1159", MagicMock()), \
     patch.object(rt.notify, "send_daily_summary", MagicMock()):
    rt.force_exit_1159_mtf(dry_run=False)

check("(f1) sell() never called when target already COMPLETE", sell_calls_f1 == [])
row1 = store1.positions[0]
check("(f1) status exited_1159", row1["status"] == "exited_1159")
check("(f1) mirrored short opened", open_short_calls_f1 == [("ETA", 10, "1159")])

pos2 = make_long(symbol="THETA", actual_fill_price=100.0, actual_fill_quantity=10,
                  target_order_id="TGT-THETA", target_price=117.0, status="open")
store2 = FakeStore([pos2])

call_order_f2 = []
def fake_order_status_f2(oid):
    return {"status": "OPEN", "filled_quantity": 0, "average_price": 0}
def fake_cancel_f2(oid):
    call_order_f2.append(("cancel", oid))
    return oid
def fake_sell_f2(symbol, exch, qty, **kw):
    call_order_f2.append(("sell", symbol))
    return f"MKT-{symbol}"
def fake_poll_fill_safe_f2(oid, fallback_price, fallback_qty):
    return 90.0, fallback_qty

open_short_calls_f2 = []
def fake_open_short_f2(sym, qty, stage, dry_run=False):
    open_short_calls_f2.append((sym, qty, stage))

with patch.object(rt, "_load_pos", store2.load), \
     patch.object(rt, "_save_pos", store2.save), \
     patch.object(rt, "_kite_order_status", fake_order_status_f2), \
     patch.object(rt, "_kite_cancel_order", fake_cancel_f2), \
     patch.object(rt, "sell", fake_sell_f2), \
     patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe_f2), \
     patch.object(rt, "_broker_qty", lambda sym, product: (10, "NSE")), \
     patch.object(rt, "_open_short", fake_open_short_f2), \
     patch.object(rt.notify, "send_force_exit_1159", MagicMock()), \
     patch.object(rt.notify, "send_daily_summary", MagicMock()):
    rt.force_exit_1159_mtf(dry_run=False)

check("(f2) cancel_order called before force-sell, in that order",
      call_order_f2 == [("cancel", "TGT-THETA"), ("sell", "THETA")], str(call_order_f2))
row2 = store2.positions[0]
check("(f2) status exited_1159 despite a loss (no P&L gate at 11:59)", row2["status"] == "exited_1159")


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (g) — _open_short places BOTH cover_target and stop-loss\n")
# ─────────────────────────────────────────────────────────────────────────────

def fake_fetch_upper_circuit_g(sym):
    return 220.0

sell_calls_g = []
buy_calls_g = []
place_order_calls_g = []
def fake_sell_g(symbol, exch, qty, **kw):
    sell_calls_g.append((symbol, exch, qty, kw.get("order_type")))
    return "SHORTOPEN-G"
def fake_buy_g(symbol, exch, qty, **kw):
    buy_calls_g.append((symbol, exch, qty, kw.get("order_type"), kw.get("price"), kw.get("product")))
    return "COVERTGT-G"
def fake_place_order_g(symbol, exch, txn, qty, **kw):
    place_order_calls_g.append((symbol, exch, txn, qty, kw.get("order_type"),
                                kw.get("trigger_price"), kw.get("product")))
    return "STOPLOSS-G"
def fake_poll_fill_safe_g(oid, fallback_price, fallback_qty):
    return 200.0, fallback_qty

store_g = FakeStore([])

with patch.object(rt, "_load_pos", store_g.load), \
     patch.object(rt, "_save_pos", store_g.save), \
     patch.object(rt, "get_ltp", lambda sym: 200.0), \
     patch.object(rt, "_mis_margin_check", lambda sym, qty: {"leverage": 5.0, "margin_required": 100.0}), \
     patch.object(rt, "_available_margin", lambda: 100000.0), \
     patch.object(rt, "_fetch_upper_circuit", fake_fetch_upper_circuit_g), \
     patch.object(rt, "sell", fake_sell_g), \
     patch.object(rt, "buy", fake_buy_g), \
     patch.object(rt, "place_order", fake_place_order_g), \
     patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe_g), \
     patch.object(rt.notify, "send_short_open", MagicMock()):
    rt._open_short("OMEGA", 10, "925", dry_run=False)

check("(g) short SELL was placed", sell_calls_g == [("OMEGA", "NSE", 10, "MARKET")])
check("(g) cover-target BUY was placed", len(buy_calls_g) == 1)
check("(g) cover-target order_type is LIMIT", buy_calls_g[0][3] == "LIMIT")
check("(g) cover-target price == 190.0 (200 * 0.95)", buy_calls_g[0][4] == 190.0)
check("(g) cover-target product is MIS", buy_calls_g[0][5] == "MIS")
check("(g) stop-loss order was placed", len(place_order_calls_g) == 1, str(place_order_calls_g))
check("(g) stop-loss order_type is SL-M", place_order_calls_g[0][4] == "SL-M")
check("(g) stop-loss trigger_price == 218.9 (220 * 0.995)", place_order_calls_g[0][5] == 218.9)
check("(g) stop-loss product is MIS", place_order_calls_g[0][6] == "MIS")
row_g = store_g.positions[0]
check("(g) row cover_target_order_id == COVERTGT-G", row_g.get("cover_target_order_id") == "COVERTGT-G")
check("(g) row stop_order_id == STOPLOSS-G", row_g.get("stop_order_id") == "STOPLOSS-G")
check("(g) row stop_trigger_price == 218.9", row_g.get("stop_trigger_price") == 218.9)


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (h) — circuit fetch fails -> stop-loss skipped, short+cover still placed\n")
# ─────────────────────────────────────────────────────────────────────────────

def fake_fetch_upper_circuit_h(sym):
    raise ConnectionError("circuit fetch blip")

place_order_calls_h = []
def fake_place_order_h(*a, **kw):
    place_order_calls_h.append((a, kw))
    return "SHOULD-NOT-HAPPEN"

buy_calls_h = []
def fake_buy_h(symbol, exch, qty, **kw):
    buy_calls_h.append((symbol, exch, qty, kw.get("order_type")))
    return "COVERTGT-H"

store_h = FakeStore([])

with patch.object(rt, "_load_pos", store_h.load), \
     patch.object(rt, "_save_pos", store_h.save), \
     patch.object(rt, "get_ltp", lambda sym: 200.0), \
     patch.object(rt, "_mis_margin_check", lambda sym, qty: {"leverage": 5.0, "margin_required": 100.0}), \
     patch.object(rt, "_available_margin", lambda: 100000.0), \
     patch.object(rt, "_fetch_upper_circuit", fake_fetch_upper_circuit_h), \
     patch.object(rt, "sell", lambda symbol, exch, qty, **kw: "SHORTOPEN-H"), \
     patch.object(rt, "buy", fake_buy_h), \
     patch.object(rt, "place_order", fake_place_order_h), \
     patch.object(rt, "_poll_fill_safe", lambda oid, fp, fq: (200.0, fq)), \
     patch.object(rt.notify, "send_short_open", MagicMock()), \
     patch.object(rt.notify, "send_circuit_fetch_failed", MagicMock()) as mock_cff:
    rt._open_short("PSI", 10, "925", dry_run=False)

check("(h) cover-target still placed despite circuit fetch failure", len(buy_calls_h) == 1)
check("(h) stop-loss place_order NEVER called", place_order_calls_h == [])
check("(h) circuit_fetch_failed notify was called", mock_cff.called)
row_h = store_h.positions[0]
check("(h) row status short_open (short itself succeeded)", row_h["status"] == "short_open")
check("(h) row stop_order_id is None (skipped)", row_h.get("stop_order_id") is None)


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (i) — square-off: cover_target filled -> stop-loss cancelled\n")
# ─────────────────────────────────────────────────────────────────────────────

short_i = make_short(symbol="RHO", entry_price=200.0, quantity=10,
                     cover_target_order_id="COVERTGT-RHO", cover_target_price=190.0,
                     stop_order_id="STOPLOSS-RHO", stop_trigger_price=218.9,
                     status="short_open")
store_i = FakeStore([short_i])

def fake_order_status_i(oid):
    if oid == "COVERTGT-RHO":
        return {"status": "COMPLETE", "filled_quantity": 10, "average_price": 190.0}
    if oid == "STOPLOSS-RHO":
        return {"status": "TRIGGER PENDING", "filled_quantity": 0, "average_price": 0}
    raise AssertionError(f"unexpected order id {oid}")

cancel_calls_i = []
def fake_cancel_i(oid):
    cancel_calls_i.append(oid)
    return oid

buy_calls_i = []
def fake_buy_i(*a, **kw):
    buy_calls_i.append((a, kw))
    return "SHOULD-NOT-HAPPEN"

with patch.object(rt, "_load_pos", store_i.load), \
     patch.object(rt, "_save_pos", store_i.save), \
     patch.object(rt, "_kite_order_status", fake_order_status_i), \
     patch.object(rt, "_kite_cancel_order", fake_cancel_i), \
     patch.object(rt, "buy", fake_buy_i), \
     patch.object(rt, "_broker_short_qty", lambda sym: 10), \
     patch.object(rt.notify, "send_cover_target_hit", MagicMock()):
    rt.square_off_239_mtf(dry_run=False)

check("(i) stop-loss order was cancelled", cancel_calls_i == ["STOPLOSS-RHO"])
check("(i) force-cover buy() never called", buy_calls_i == [])
row_i = store_i.positions[0]
check("(i) status short_closed", row_i["status"] == "short_closed")
check("(i) exit_order_id_239 == cover_target_order_id", row_i["exit_order_id_239"] == "COVERTGT-RHO")
check("(i) exit_price_239 == cover's fill (190.0)", row_i["exit_price_239"] == 190.0)


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (j) — square-off: stop-loss filled -> cover_target cancelled\n")
# ─────────────────────────────────────────────────────────────────────────────

short_j = make_short(symbol="SIGMA", entry_price=200.0, quantity=10,
                     cover_target_order_id="COVERTGT-SIG", cover_target_price=190.0,
                     stop_order_id="STOPLOSS-SIG", stop_trigger_price=218.9,
                     status="short_open")
store_j = FakeStore([short_j])

def fake_order_status_j(oid):
    if oid == "STOPLOSS-SIG":
        return {"status": "COMPLETE", "filled_quantity": 10, "average_price": 219.5}
    if oid == "COVERTGT-SIG":
        return {"status": "OPEN", "filled_quantity": 0, "average_price": 0}
    raise AssertionError(f"unexpected order id {oid}")

cancel_calls_j = []
def fake_cancel_j(oid):
    cancel_calls_j.append(oid)
    return oid

buy_calls_j = []
def fake_buy_j(*a, **kw):
    buy_calls_j.append((a, kw))
    return "SHOULD-NOT-HAPPEN"

with patch.object(rt, "_load_pos", store_j.load), \
     patch.object(rt, "_save_pos", store_j.save), \
     patch.object(rt, "_kite_order_status", fake_order_status_j), \
     patch.object(rt, "_kite_cancel_order", fake_cancel_j), \
     patch.object(rt, "buy", fake_buy_j), \
     patch.object(rt, "_broker_short_qty", lambda sym: 10), \
     patch.object(rt.notify, "send_short_stoploss_hit", MagicMock()):
    rt.square_off_239_mtf(dry_run=False)

check("(j) cover-target order was cancelled", cancel_calls_j == ["COVERTGT-SIG"])
check("(j) force-cover buy() never called", buy_calls_j == [])
row_j = store_j.positions[0]
check("(j) status short_closed", row_j["status"] == "short_closed")
check("(j) exit_order_id_239 == stop_order_id (NOT the target)",
      row_j["exit_order_id_239"] == "STOPLOSS-SIG")
check("(j) exit_price_239 == stop-loss's fill (219.5), NOT target's (190.0)",
      row_j["exit_price_239"] == 219.5)
check("(j) realized_pnl uses stop-loss fill: (200-219.5)*10 == -195.0",
      row_j["realized_pnl"] == -195.0)


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (k) — square-off: neither filled -> BOTH cancelled, force-cover proceeds\n")
# ─────────────────────────────────────────────────────────────────────────────

short_k = make_short(symbol="TAU", entry_price=200.0, quantity=10,
                     cover_target_order_id="COVERTGT-TAU", cover_target_price=190.0,
                     stop_order_id="STOPLOSS-TAU", stop_trigger_price=218.9,
                     status="short_open")
store_k = FakeStore([short_k])

def fake_order_status_k(oid):
    return {"status": "OPEN", "filled_quantity": 0, "average_price": 0}

cancel_calls_k = []
def fake_cancel_k(oid):
    cancel_calls_k.append(oid)
    return oid

def fake_buy_k(symbol, exch, qty, **kw):
    return f"FORCECOVER-{symbol}"

def fake_poll_fill_safe_k(oid, fallback_price, fallback_qty):
    return 205.0, fallback_qty

with patch.object(rt, "_load_pos", store_k.load), \
     patch.object(rt, "_save_pos", store_k.save), \
     patch.object(rt, "_kite_order_status", fake_order_status_k), \
     patch.object(rt, "_kite_cancel_order", fake_cancel_k), \
     patch.object(rt, "buy", fake_buy_k), \
     patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe_k), \
     patch.object(rt, "_broker_short_qty", lambda sym: 10), \
     patch.object(rt.notify, "send_square_off_239", MagicMock()):
    rt.square_off_239_mtf(dry_run=False)

check("(k) BOTH orders cancelled",
      set(cancel_calls_k) == {"COVERTGT-TAU", "STOPLOSS-TAU"}, str(cancel_calls_k))
row_k = store_k.positions[0]
check("(k) status short_closed via the existing force-cover path", row_k["status"] == "short_closed")
check("(k) exit_order_id_239 == the force-cover order (not target/stop)",
      row_k["exit_order_id_239"] == "FORCECOVER-TAU")


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (l) — square-off: BOTH show COMPLETE -> manual review, no silent pick\n")
# ─────────────────────────────────────────────────────────────────────────────

short_l = make_short(symbol="UPSILON", entry_price=200.0, quantity=10,
                     cover_target_order_id="COVERTGT-UPS", cover_target_price=190.0,
                     stop_order_id="STOPLOSS-UPS", stop_trigger_price=218.9,
                     status="short_open")
store_l = FakeStore([short_l])

def fake_order_status_l(oid):
    if oid == "COVERTGT-UPS":
        return {"status": "COMPLETE", "filled_quantity": 10, "average_price": 190.0}
    if oid == "STOPLOSS-UPS":
        return {"status": "COMPLETE", "filled_quantity": 10, "average_price": 219.0}
    raise AssertionError(f"unexpected order id {oid}")

cancel_calls_l = []
def fake_cancel_l(oid):
    cancel_calls_l.append(oid)
    return oid

buy_calls_l = []
def fake_buy_l(*a, **kw):
    buy_calls_l.append((a, kw))
    return "SHOULD-NOT-HAPPEN"

with patch.object(rt, "_load_pos", store_l.load), \
     patch.object(rt, "_save_pos", store_l.save), \
     patch.object(rt, "_kite_order_status", fake_order_status_l), \
     patch.object(rt, "_kite_cancel_order", fake_cancel_l), \
     patch.object(rt, "buy", fake_buy_l):
    rt.square_off_239_mtf(dry_run=False)

check("(l) NEITHER order cancelled (no automatic pick)", cancel_calls_l == [])
check("(l) force-cover buy() never called", buy_calls_l == [])
row_l = store_l.positions[0]
check("(l) position row completely unchanged (still short_open)", row_l["status"] == "short_open")


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (m) — order_status() raises -> skip, no side effects\n")
# ─────────────────────────────────────────────────────────────────────────────

pos = make_long(symbol="MU", actual_fill_price=100.0, actual_fill_quantity=10,
                 target_order_id="TGT-MU", target_price=117.0, status="open")
store = FakeStore([pos])

def fake_order_status_raises(oid):
    raise ConnectionError("network blip")

cancel_calls_m = []
def fake_cancel_m(oid):
    cancel_calls_m.append(oid)
    return oid

sell_calls_m = []
def fake_sell_m(*a, **kw):
    sell_calls_m.append((a, kw))
    return "SHOULD-NOT-HAPPEN"

get_ltp_calls_m = []
def fake_get_ltp_m(sym):
    get_ltp_calls_m.append(sym)
    return 999.0

with patch.object(rt, "_load_pos", store.load), \
     patch.object(rt, "_save_pos", store.save), \
     patch.object(rt, "_kite_order_status", fake_order_status_raises), \
     patch.object(rt, "_kite_cancel_order", fake_cancel_m), \
     patch.object(rt, "sell", fake_sell_m), \
     patch.object(rt, "get_ltp", fake_get_ltp_m):
    rt.check_exit_925_mtf(dry_run=False)

check("(m) cancel_order never called", cancel_calls_m == [])
check("(m) sell() never called", sell_calls_m == [])
check("(m) get_ltp never called (position skipped before existing logic ran)", get_ltp_calls_m == [])
row = store.positions[0]
check("(m) status still open", row["status"] == "open")

short_m = make_short(symbol="XI", cover_target_order_id="COVERTGT-XI", cover_target_price=190.0)
store_m = FakeStore([short_m])
buy_calls_m239 = []
with patch.object(rt, "_load_pos", store_m.load), \
     patch.object(rt, "_save_pos", store_m.save), \
     patch.object(rt, "_kite_order_status", fake_order_status_raises), \
     patch.object(rt, "_kite_cancel_order", lambda oid: (_ for _ in ()).throw(AssertionError("must not cancel"))), \
     patch.object(rt, "buy", lambda *a, **kw: buy_calls_m239.append(1) or "X"):
    rt.square_off_239_mtf(dry_run=False)
check("(m-239) order_status raise -> buy() never called, position skipped", buy_calls_m239 == [])


# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'─' * 55}")
if failures == 0:
    print("\033[32mAll scenarios PASSED\033[0m")
    sys.exit(0)
else:
    print(f"\033[31m{failures} assertion(s) FAILED\033[0m")
    sys.exit(1)
