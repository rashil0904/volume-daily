#!/usr/bin/env python3
"""
test_targets.py -- standalone verifier for the profit-target mechanism added to
dhan/run_trades.py: place_targets_915(), the target-status checks inserted into
check_exit_925/force_exit_1159/square_off_239, and cover-target placement in
_open_short().

Mocks every broker-facing call this touches (buy/sell/order_status/cancel_order/
_broker_qty/_broker_short_qty/_available_balance/_intraday_margin_check/get_ltp/
_poll_fill_safe) and the positions_dhan.json read/write (_load_pos/_save_pos)
with an in-memory store -- zero network calls, zero real file writes. Mirrors
zerodha/test_state_machine.py's standalone script style (no pytest in this repo).

Usage:
    python dhan/test_targets.py

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

import dhan.run_trades as rt  # noqa: E402  (import after sys.path/stub setup)

# _tick_round() now looks up each symbol's REAL tick size via dhan.instruments
# .tick_size() (see run_trades.py -- tick size varies per symbol, e.g. ₹0.10 for
# TVSSRICHAK vs ₹0.01 for CAMLINFINE, confirmed live 2026-08-18). None of this
# suite's fictional symbols (DELTA, EPSILON, IOTA, ...) exist in the real scrip
# master, so tick_size() would raise for every one of them -- pinned to a flat
# ₹0.05 here, matching every expected price already computed against that tick
# throughout this file.
patch.object(rt, "tick_size", lambda sym: 0.05).start()

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
        "broker": "dhan", "symbol": "TESTCO", "entry_date": "2026-08-17",
        "reference_price": 100.0, "shares_intended": 10,
        "actual_fill_price": 100.0, "actual_fill_quantity": 10,
        "entry_order_id": "E1", "status": "open",
        "entry_timestamp": "2026-08-17T15:21:00+05:30", "product": "MTF",
    }
    row.update(overrides)
    return row


def make_short(**overrides):
    row = {
        "broker": "dhan", "symbol": "TESTCO", "direction": "short",
        "product": "INTRADAY", "source_exit_stage": "925",
        "entry_date": "2026-08-17", "entry_price": 100.0, "quantity": 10,
        "entry_order_id": "S1", "status": "short_open",
        "entry_timestamp": "2026-08-17T09:25:00+05:30",
    }
    row.update(overrides)
    return row


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (a) — place_targets_915\n")
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
    rt.place_targets_915(dry_run=False)

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
beta_row = next(p for p in store.positions if p["symbol"] == "BETA")
check("(a) BETA row target_order_id unchanged", beta_row.get("target_order_id") == "EXISTING1")


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (b) — 9:25, target already TRADED\n")
# ─────────────────────────────────────────────────────────────────────────────

pos = make_long(symbol="GAMMA", actual_fill_price=100.0, actual_fill_quantity=10,
                 target_order_id="TGT-GAMMA", target_price=117.0, status="open")
store = FakeStore([pos])

order_status_calls = []
def fake_order_status_b(oid):
    order_status_calls.append(oid)
    return {"orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 117.0}

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
     patch.object(rt, "_dhan_order_status", fake_order_status_b), \
     patch.object(rt, "get_ltp", fake_get_ltp_b), \
     patch.object(rt, "sell", fake_sell_b), \
     patch.object(rt, "_open_short", fake_open_short_b), \
     patch.object(rt.notify, "send_target_hit", MagicMock()):
    rt.check_exit_925(dry_run=False)

check("(b) order_status was checked for the target", "TGT-GAMMA" in order_status_calls)
check("(b) get_ltp was NEVER called (existing no_data/pnl_live logic skipped)", get_ltp_calls == [])
check("(b) sell() was NEVER called (no market/half sell fired)", sell_calls_b == [])
row = store.positions[0]
check("(b) position marked exited_925", row["status"] == "exited_925")
check("(b) exit_price_925 == target's averageTradedPrice", row["exit_price_925"] == 117.0)
check("(b) exit_order_id_925 == target_order_id", row["exit_order_id_925"] == "TGT-GAMMA")
check("(b) realized_pnl == (117-100)*10 == 170.0", row["realized_pnl"] == 170.0)
check("(b) NO mirrored short on a target-hit exit (UC risk)",
      open_short_calls == [], str(open_short_calls))


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (c) — 9:25, target not traded, no-LTP-data fallback\n")
# ─────────────────────────────────────────────────────────────────────────────

pos = make_long(symbol="DELTA", actual_fill_price=100.0, actual_fill_quantity=10,
                 target_order_id="TGT-DELTA", target_price=117.0, status="open")
store = FakeStore([pos])

def fake_order_status_c(oid):
    return {"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0}

cancel_calls_c = []
def fake_cancel_c(oid):
    cancel_calls_c.append(oid)
    return oid

def fake_get_ltp_c(sym):
    raise ValueError("no LTP")  # forces the no_data branch

sell_calls_c = []
def fake_sell_c(symbol, exch, qty, **kw):
    sell_calls_c.append((symbol, exch, qty, kw.get("order_type"), kw.get("price")))
    # Both the half-sell and the fresh target are LIMIT now (see run_trades.py's
    # _tick_round-based conversion of every former MARKET order) -- distinguish
    # by price instead: the fresh target reuses the SAME target_price (117.0),
    # the half-sell computes its own tick-rounded price off fill_price.
    if kw.get("price") == 117.0:
        return f"TGT2-{symbol}"
    return f"HALFSELL-{symbol}"

def fake_poll_fill_safe_c(oid, fallback_price, fallback_qty):
    return 99.0, fallback_qty  # simulate the half-sell filling fully

def fake_broker_qty_c(sym, product):
    return 10, "NSE_EQ"

open_short_calls_c = []
def fake_open_short_c(sym, qty, stage, dry_run=False):
    open_short_calls_c.append((sym, qty, stage))

with patch.object(rt, "_load_pos", store.load), \
     patch.object(rt, "_save_pos", store.save), \
     patch.object(rt, "_dhan_order_status", fake_order_status_c), \
     patch.object(rt, "_dhan_cancel_order", fake_cancel_c), \
     patch.object(rt, "get_ltp", fake_get_ltp_c), \
     patch.object(rt, "sell", fake_sell_c), \
     patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe_c), \
     patch.object(rt, "_broker_qty", fake_broker_qty_c), \
     patch.object(rt, "_open_short", fake_open_short_c), \
     patch.object(rt.notify, "send_exit_925_nodata", MagicMock()), \
     patch.object(rt.notify, "send_target_placed", MagicMock()):
    rt.check_exit_925(dry_run=False)

check("(c) cancel_order was called for the stale target", cancel_calls_c == ["TGT-DELTA"])
half_sells_c = [c for c in sell_calls_c if c[4] != 117.0]
check("(c) a half-sell (LIMIT, tick-rounded 0.5% below fill price) happened",
      any(c[3] == "LIMIT" and c[4] == 99.5 for c in half_sells_c), str(sell_calls_c))
fresh_targets_c = [c for c in sell_calls_c if c[4] == 117.0]
check("(c) a FRESH target LIMIT sell was placed for shares_remaining",
      len(fresh_targets_c) == 1, str(sell_calls_c))
check("(c) fresh target qty == shares_remaining (5)", fresh_targets_c[0][2] == 5, str(fresh_targets_c))
check("(c) fresh target price == SAME target_price (117.0), not recomputed",
      fresh_targets_c[0][4] == 117.0)
row = store.positions[0]
check("(c) row status partial_exit_925_nodata", row["status"] == "partial_exit_925_nodata")
check("(c) row target_order_id updated to the fresh order", row["target_order_id"] == "TGT2-DELTA")
check("(c) row target_price UNCHANGED (still 117.0)", row["target_price"] == 117.0)
check("(c) mirrored short opened on the half sold (5 shares)",
      open_short_calls_c == [("DELTA", 5, "925")], str(open_short_calls_c))


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (d) — 9:25, target not traded, pnl_live > 0\n")
# ─────────────────────────────────────────────────────────────────────────────

pos = make_long(symbol="EPSILON", actual_fill_price=100.0, actual_fill_quantity=10,
                 target_order_id="TGT-EPS", target_price=117.0, status="open")
store = FakeStore([pos])

call_order_d = []

def fake_order_status_d(oid):
    return {"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0}

def fake_cancel_d(oid):
    call_order_d.append(("cancel", oid))
    return oid

def fake_get_ltp_d(sym):
    return 105.0  # pnl_live = (105-100)*10 = 50 > 0

def fake_sell_d(symbol, exch, qty, **kw):
    call_order_d.append(("sell", symbol, kw.get("order_type"), kw.get("price")))
    return f"MKT-{symbol}"

def fake_poll_fill_safe_d(oid, fallback_price, fallback_qty):
    return 105.0, fallback_qty

def fake_broker_qty_d(sym, product):
    return 10, "NSE_EQ"

open_short_calls_d = []
def fake_open_short_d(sym, qty, stage, dry_run=False):
    open_short_calls_d.append((sym, qty, stage))

with patch.object(rt, "_load_pos", store.load), \
     patch.object(rt, "_save_pos", store.save), \
     patch.object(rt, "_dhan_order_status", fake_order_status_d), \
     patch.object(rt, "_dhan_cancel_order", fake_cancel_d), \
     patch.object(rt, "get_ltp", fake_get_ltp_d), \
     patch.object(rt, "sell", fake_sell_d), \
     patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe_d), \
     patch.object(rt, "_broker_qty", fake_broker_qty_d), \
     patch.object(rt, "_open_short", fake_open_short_d), \
     patch.object(rt.notify, "send_exit_925", MagicMock()):
    rt.check_exit_925(dry_run=False)

check("(d) cancel_order called before the LIMIT sell, in that order",
      call_order_d == [("cancel", "TGT-EPS"), ("sell", "EPSILON", "LIMIT", 104.45)], str(call_order_d))
row = store.positions[0]
check("(d) row exited_925", row["status"] == "exited_925")
check("(d) mirrored short opened", open_short_calls_d == [("EPSILON", 10, "925")])


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (e) — 9:25, target not traded, pnl_live <= 0\n")
# ─────────────────────────────────────────────────────────────────────────────

pos = make_long(symbol="ZETA", actual_fill_price=100.0, actual_fill_quantity=10,
                 target_order_id="TGT-ZETA", target_price=117.0, status="open")
store = FakeStore([pos])

def fake_order_status_e(oid):
    return {"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0}

cancel_calls_e = []
def fake_cancel_e(oid):
    cancel_calls_e.append(oid)
    return oid

def fake_get_ltp_e(sym):
    return 98.0  # pnl_live = (98-100)*10 = -20 <= 0

sell_calls_e = []
def fake_sell_e(*a, **kw):
    sell_calls_e.append((a, kw))
    return "SHOULD-NOT-HAPPEN"

with patch.object(rt, "_load_pos", store.load), \
     patch.object(rt, "_save_pos", store.save), \
     patch.object(rt, "_dhan_order_status", fake_order_status_e), \
     patch.object(rt, "_dhan_cancel_order", fake_cancel_e), \
     patch.object(rt, "get_ltp", fake_get_ltp_e), \
     patch.object(rt, "sell", fake_sell_e):
    rt.check_exit_925(dry_run=False)

check("(e) cancel_order NEVER called", cancel_calls_e == [])
check("(e) sell() NEVER called", sell_calls_e == [])
row = store.positions[0]
check("(e) target_order_id unchanged", row.get("target_order_id") == "TGT-ZETA")
check("(e) target_price unchanged", row.get("target_price") == 117.0)
check("(e) status still open (held for 11:59)", row["status"] == "open")


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (f) — 11:59 target-status handling\n")
# ─────────────────────────────────────────────────────────────────────────────

# f1: target already TRADED
pos1 = make_long(symbol="ETA", actual_fill_price=100.0, actual_fill_quantity=10,
                  target_order_id="TGT-ETA", target_price=117.0, status="open")
store1 = FakeStore([pos1])

def fake_order_status_f1(oid):
    return {"orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 117.0}

sell_calls_f1 = []
def fake_sell_f1(*a, **kw):
    sell_calls_f1.append((a, kw))
    return "SHOULD-NOT-HAPPEN"

open_short_calls_f1 = []
def fake_open_short_f1(sym, qty, stage, dry_run=False):
    open_short_calls_f1.append((sym, qty, stage))

with patch.object(rt, "_load_pos", store1.load), \
     patch.object(rt, "_save_pos", store1.save), \
     patch.object(rt, "_dhan_order_status", fake_order_status_f1), \
     patch.object(rt, "sell", fake_sell_f1), \
     patch.object(rt, "_broker_qty", lambda sym, product: (10, "NSE_EQ")), \
     patch.object(rt, "_open_short", fake_open_short_f1), \
     patch.object(rt.notify, "send_target_hit", MagicMock()), \
     patch.object(rt.notify, "send_nothing_open_at_1159", MagicMock()), \
     patch.object(rt.notify, "send_daily_summary", MagicMock()):
    rt.force_exit_1159(dry_run=False)

check("(f1) sell() never called when target already TRADED", sell_calls_f1 == [])
row1 = store1.positions[0]
check("(f1) status exited_1159", row1["status"] == "exited_1159")
check("(f1) exit_order_id_1159 == target_order_id", row1["exit_order_id_1159"] == "TGT-ETA")
check("(f1) exit_price_1159 == 117.0", row1["exit_price_1159"] == 117.0)
check("(f1) NO mirrored short on a target-hit exit (UC risk)", open_short_calls_f1 == [])

# f2: target NOT traded -> cancel then force-sell, regardless of a mocked loss
pos2 = make_long(symbol="THETA", actual_fill_price=100.0, actual_fill_quantity=10,
                  target_order_id="TGT-THETA", target_price=117.0, status="open")
store2 = FakeStore([pos2])

call_order_f2 = []
def fake_order_status_f2(oid):
    return {"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0}
def fake_cancel_f2(oid):
    call_order_f2.append(("cancel", oid))
    return oid
def fake_sell_f2(symbol, exch, qty, **kw):
    call_order_f2.append(("sell", symbol))
    return f"MKT-{symbol}"
def fake_poll_fill_safe_f2(oid, fallback_price, fallback_qty):
    return 90.0, fallback_qty  # a LOSS -- 11:59 has no P&L gate, must still force-sell

open_short_calls_f2 = []
def fake_open_short_f2(sym, qty, stage, dry_run=False):
    open_short_calls_f2.append((sym, qty, stage))

with patch.object(rt, "_load_pos", store2.load), \
     patch.object(rt, "_save_pos", store2.save), \
     patch.object(rt, "_dhan_order_status", fake_order_status_f2), \
     patch.object(rt, "_dhan_cancel_order", fake_cancel_f2), \
     patch.object(rt, "sell", fake_sell_f2), \
     patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe_f2), \
     patch.object(rt, "_broker_qty", lambda sym, product: (10, "NSE_EQ")), \
     patch.object(rt, "_open_short", fake_open_short_f2), \
     patch.object(rt.notify, "send_force_exit_1159", MagicMock()), \
     patch.object(rt.notify, "send_daily_summary", MagicMock()):
    rt.force_exit_1159(dry_run=False)

check("(f2) cancel_order called before force-sell, in that order",
      call_order_f2 == [("cancel", "TGT-THETA"), ("sell", "THETA")], str(call_order_f2))
row2 = store2.positions[0]
check("(f2) status exited_1159 despite a loss (no P&L gate at 11:59)", row2["status"] == "exited_1159")
check("(f2) mirrored short still opened even on a losing force-exit",
      open_short_calls_f2 == [("THETA", 10, "1159")])


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (g) — _open_short places a cover target after fill\n")
# ─────────────────────────────────────────────────────────────────────────────

def fake_get_ltp_g(sym):
    return 200.0

def fake_intraday_margin_check_g(sym, qty, price):
    return {"leverage": 5.0, "margin_required": 100.0}

def fake_available_balance_g():
    return 100000.0

sell_calls_g = []
buy_calls_g = []
def fake_sell_g(symbol, exch, qty, **kw):
    sell_calls_g.append((symbol, exch, qty, kw.get("order_type"), kw.get("price"), kw.get("trigger_price")))
    return "SHORTOPEN-1"
def fake_buy_g(symbol, exch, qty, **kw):
    buy_calls_g.append((symbol, exch, qty, kw.get("order_type"), kw.get("price"),
                        kw.get("trigger_price"), kw.get("product")))
    return "STOPLOSS-1" if kw.get("order_type") == "STOP_LOSS" else "COVERTGT-1"
def fake_fetch_upper_circuit_g(sym):
    return 220.0

def fake_poll_fill_safe_g(oid, fallback_price, fallback_qty):
    return 200.0, fallback_qty  # short fills exactly at ltp

store_g = FakeStore([])

with patch.object(rt, "_load_pos", store_g.load), \
     patch.object(rt, "_save_pos", store_g.save), \
     patch.object(rt, "_shorting_skipped_today", lambda: False), \
     patch.object(rt, "get_ltp", fake_get_ltp_g), \
     patch.object(rt, "_intraday_margin_check", fake_intraday_margin_check_g), \
     patch.object(rt, "_available_balance", fake_available_balance_g), \
     patch.object(rt, "sell", fake_sell_g), \
     patch.object(rt, "buy", fake_buy_g), \
     patch.object(rt, "_fetch_upper_circuit", fake_fetch_upper_circuit_g), \
     patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe_g), \
     patch.object(rt.notify, "send_short_open", MagicMock()):
    rt._open_short("IOTA", 10, "925", dry_run=False)

check("(g) short-open SELL was placed as plain LIMIT, tick-rounded 0.5% below LTP",
      sell_calls_g == [("IOTA", "NSE_EQ", 10, "LIMIT", 199.0, None)], str(sell_calls_g))
check("(g) cover-target AND stop-loss BUYs were placed", len(buy_calls_g) == 2, str(buy_calls_g))
check("(g) cover-target order_type is LIMIT", buy_calls_g[0][3] == "LIMIT")
check("(g) cover-target price == 190.0 (200 * 0.95)", buy_calls_g[0][4] == 190.0)
check("(g) cover-target product is INTRADAY", buy_calls_g[0][6] == "INTRADAY")
check("(g) stop-loss order_type is STOP_LOSS", buy_calls_g[1][3] == "STOP_LOSS")
check("(g) stop-loss limit price == 220.0 (== UC)", buy_calls_g[1][4] == 220.0)
check("(g) stop-loss trigger == 218.9 (UC * 0.995, tick-rounded)", buy_calls_g[1][5] == 218.9)
check("(g) stop-loss product is INTRADAY", buy_calls_g[1][6] == "INTRADAY")
check("(g) exactly one position row saved", len(store_g.positions) == 1)
row_g = store_g.positions[0]
check("(g) row cover_target_order_id == COVERTGT-1", row_g.get("cover_target_order_id") == "COVERTGT-1")
check("(g) row cover_target_price == 190.0", row_g.get("cover_target_price") == 190.0)
check("(g) row stop_order_id == STOPLOSS-1", row_g.get("stop_order_id") == "STOPLOSS-1")
check("(g) row stop_trigger_price == 218.9", row_g.get("stop_trigger_price") == 218.9)
check("(g) row stop_limit_price == 220.0", row_g.get("stop_limit_price") == 220.0)


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (g3) — _open_short: circuit fetch fails -> stop-loss skipped, short+cover still placed\n")
# ─────────────────────────────────────────────────────────────────────────────

buy_calls_g3 = []
def fake_buy_g3(symbol, exch, qty, **kw):
    buy_calls_g3.append((symbol, exch, qty, kw.get("order_type")))
    return "COVERTGT-1"
def fake_fetch_upper_circuit_g3(sym):
    raise ValueError("no circuit data")

store_g3 = FakeStore([])

with patch.object(rt, "_load_pos", store_g3.load), \
     patch.object(rt, "_save_pos", store_g3.save), \
     patch.object(rt, "_shorting_skipped_today", lambda: False), \
     patch.object(rt, "get_ltp", fake_get_ltp_g), \
     patch.object(rt, "_intraday_margin_check", fake_intraday_margin_check_g), \
     patch.object(rt, "_available_balance", fake_available_balance_g), \
     patch.object(rt, "sell", fake_sell_g), \
     patch.object(rt, "buy", fake_buy_g3), \
     patch.object(rt, "_fetch_upper_circuit", fake_fetch_upper_circuit_g3), \
     patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe_g), \
     patch.object(rt.notify, "send_short_open", MagicMock()), \
     patch.object(rt.notify, "send_circuit_fetch_failed", MagicMock()):
    rt._open_short("KAPPA", 10, "925", dry_run=False)

check("(g3) only the cover-target BUY fired -- no stop-loss attempt",
      buy_calls_g3 == [("KAPPA", "NSE_EQ", 10, "LIMIT")], str(buy_calls_g3))
row_g3 = store_g3.positions[0]
check("(g3) row stop_order_id is None (circuit fetch failed)", row_g3.get("stop_order_id") is None)
check("(g3) row stop_trigger_price is None", row_g3.get("stop_trigger_price") is None)
check("(g3) row cover_target_order_id still recorded", row_g3.get("cover_target_order_id") == "COVERTGT-1")


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (g2) — _open_short skips entirely when _shorting_skipped_today() is True\n")
# ─────────────────────────────────────────────────────────────────────────────

store_g2 = FakeStore([])

def fail_if_called_g2(*a, **kw):
    raise AssertionError("should never be called -- shorting is skipped for today")

with patch.object(rt, "_load_pos", store_g2.load), \
     patch.object(rt, "_save_pos", store_g2.save), \
     patch.object(rt, "_shorting_skipped_today", lambda: True), \
     patch.object(rt, "get_ltp", fail_if_called_g2), \
     patch.object(rt, "_intraday_margin_check", fail_if_called_g2), \
     patch.object(rt, "sell", fail_if_called_g2), \
     patch.object(rt, "buy", fail_if_called_g2):
    rt._open_short("IOTA", 10, "925", dry_run=False)

check("(g2) no position row saved -- _open_short returned immediately",
      store_g2.positions == [], str(store_g2.positions))


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (h) — 2:39 cover-target status handling\n")
# ─────────────────────────────────────────────────────────────────────────────

# h1: cover target already TRADED
short1 = make_short(symbol="KAPPA", entry_price=200.0, quantity=10,
                     cover_target_order_id="COVERTGT-K", cover_target_price=190.0,
                     status="short_open")
store_h1 = FakeStore([short1])

def fake_order_status_h1(oid):
    return {"orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 190.0}

buy_calls_h1 = []
def fake_buy_h1(*a, **kw):
    buy_calls_h1.append((a, kw))
    return "SHOULD-NOT-HAPPEN"

with patch.object(rt, "_load_pos", store_h1.load), \
     patch.object(rt, "_save_pos", store_h1.save), \
     patch.object(rt, "_dhan_order_status", fake_order_status_h1), \
     patch.object(rt, "buy", fake_buy_h1), \
     patch.object(rt, "_broker_short_qty", lambda sym: 10), \
     patch.object(rt.notify, "send_cover_target_hit", MagicMock()):
    rt.square_off_239(dry_run=False)

check("(h1) buy() never called when cover target already TRADED", buy_calls_h1 == [])
row_h1 = store_h1.positions[0]
check("(h1) status short_closed", row_h1["status"] == "short_closed")
check("(h1) exit_order_id_239 == cover_target_order_id", row_h1["exit_order_id_239"] == "COVERTGT-K")
check("(h1) exit_price_239 == 190.0", row_h1["exit_price_239"] == 190.0)
check("(h1) realized_pnl == (200-190)*10 == 100.0", row_h1["realized_pnl"] == 100.0)

# h2: cover target NOT traded -> cancel then force-cover
short2 = make_short(symbol="LAMBDA", entry_price=200.0, quantity=10,
                     cover_target_order_id="COVERTGT-L", cover_target_price=190.0,
                     status="short_open")
store_h2 = FakeStore([short2])

call_order_h2 = []
def fake_order_status_h2(oid):
    return {"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0}
def fake_cancel_h2(oid):
    call_order_h2.append(("cancel", oid))
    return oid
def fake_buy_h2(symbol, exch, qty, **kw):
    call_order_h2.append(("buy", symbol))
    return f"COVER-{symbol}"
def fake_poll_fill_safe_h2(oid, fallback_price, fallback_qty):
    return 205.0, fallback_qty

with patch.object(rt, "_load_pos", store_h2.load), \
     patch.object(rt, "_save_pos", store_h2.save), \
     patch.object(rt, "_dhan_order_status", fake_order_status_h2), \
     patch.object(rt, "_dhan_cancel_order", fake_cancel_h2), \
     patch.object(rt, "buy", fake_buy_h2), \
     patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe_h2), \
     patch.object(rt, "_broker_short_qty", lambda sym: 10), \
     patch.object(rt.notify, "send_square_off_239", MagicMock()):
    rt.square_off_239(dry_run=False)

check("(h2) cancel_order called before the force-cover, in that order",
      call_order_h2 == [("cancel", "COVERTGT-L"), ("buy", "LAMBDA")], str(call_order_h2))
row_h2 = store_h2.positions[0]
check("(h2) status short_closed", row_h2["status"] == "short_closed")


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (i) — order_status() raises -> skip, no side effects\n")
# ─────────────────────────────────────────────────────────────────────────────

pos = make_long(symbol="MU", actual_fill_price=100.0, actual_fill_quantity=10,
                 target_order_id="TGT-MU", target_price=117.0, status="open")
store = FakeStore([pos])

def fake_order_status_raises(oid):
    raise ConnectionError("network blip")

cancel_calls_i = []
def fake_cancel_i(oid):
    cancel_calls_i.append(oid)
    return oid

sell_calls_i = []
def fake_sell_i(*a, **kw):
    sell_calls_i.append((a, kw))
    return "SHOULD-NOT-HAPPEN"

get_ltp_calls_i = []
def fake_get_ltp_i(sym):
    get_ltp_calls_i.append(sym)
    return 999.0

with patch.object(rt, "_load_pos", store.load), \
     patch.object(rt, "_save_pos", store.save), \
     patch.object(rt, "_dhan_order_status", fake_order_status_raises), \
     patch.object(rt, "_dhan_cancel_order", fake_cancel_i), \
     patch.object(rt, "sell", fake_sell_i), \
     patch.object(rt, "get_ltp", fake_get_ltp_i):
    rt.check_exit_925(dry_run=False)

check("(i) cancel_order never called", cancel_calls_i == [])
check("(i) sell() never called", sell_calls_i == [])
check("(i) get_ltp never called (position skipped before existing logic ran)", get_ltp_calls_i == [])
row = store.positions[0]
check("(i) position row completely unchanged", row == pos)
check("(i) status still open", row["status"] == "open")

# Bonus: same order_status-raises guard on the other two call sites.
long_1159 = make_long(symbol="NU", target_order_id="TGT-NU", target_price=117.0)
store_nu  = FakeStore([long_1159])
buy_calls_nu, sell_calls_nu = [], []
with patch.object(rt, "_load_pos", store_nu.load), \
     patch.object(rt, "_save_pos", store_nu.save), \
     patch.object(rt, "_dhan_order_status", fake_order_status_raises), \
     patch.object(rt, "_dhan_cancel_order", lambda oid: (_ for _ in ()).throw(AssertionError("must not cancel"))), \
     patch.object(rt, "sell", lambda *a, **kw: sell_calls_nu.append(1) or "X"), \
     patch.object(rt.notify, "send_daily_summary", MagicMock()):
    rt.force_exit_1159(dry_run=False)
check("(i-1159) order_status raise -> sell() never called, position skipped", sell_calls_nu == [])

short_239 = make_short(symbol="XI", cover_target_order_id="COVERTGT-XI", cover_target_price=190.0)
store_xi  = FakeStore([short_239])
with patch.object(rt, "_load_pos", store_xi.load), \
     patch.object(rt, "_save_pos", store_xi.save), \
     patch.object(rt, "_dhan_order_status", fake_order_status_raises), \
     patch.object(rt, "_dhan_cancel_order", lambda oid: (_ for _ in ()).throw(AssertionError("must not cancel"))), \
     patch.object(rt, "buy", lambda *a, **kw: buy_calls_nu.append(1) or "X"):
    rt.square_off_239(dry_run=False)
check("(i-239) order_status raise -> buy() never called, position skipped", buy_calls_nu == [])


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (stop-loss a) — square-off: cover_target filled -> stop-loss cancelled\n")
# ─────────────────────────────────────────────────────────────────────────────

short_c = make_short(symbol="RHO", entry_price=200.0, quantity=10,
                     cover_target_order_id="COVERTGT-RHO", cover_target_price=190.0,
                     stop_order_id="STOPLOSS-RHO", stop_trigger_price=218.9,
                     status="short_open")
store_slc = FakeStore([short_c])

def fake_order_status_slc(oid):
    if oid == "COVERTGT-RHO":
        return {"orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 190.0}
    if oid == "STOPLOSS-RHO":
        return {"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0}
    raise AssertionError(f"unexpected order id {oid}")

cancel_calls_slc = []
def fake_cancel_slc(oid):
    cancel_calls_slc.append(oid)
    return oid

buy_calls_slc = []
def fake_buy_slc(*a, **kw):
    buy_calls_slc.append((a, kw))
    return "SHOULD-NOT-HAPPEN"

with patch.object(rt, "_load_pos", store_slc.load), \
     patch.object(rt, "_save_pos", store_slc.save), \
     patch.object(rt, "_dhan_order_status", fake_order_status_slc), \
     patch.object(rt, "_dhan_cancel_order", fake_cancel_slc), \
     patch.object(rt, "buy", fake_buy_slc), \
     patch.object(rt, "_broker_short_qty", lambda sym: 10), \
     patch.object(rt.notify, "send_cover_target_hit", MagicMock()):
    rt.square_off_239(dry_run=False)

check("(sl-a) stop-loss order was cancelled", cancel_calls_slc == ["STOPLOSS-RHO"])
check("(sl-a) force-cover buy() never called", buy_calls_slc == [])
row_slc = store_slc.positions[0]
check("(sl-a) status short_closed", row_slc["status"] == "short_closed")
check("(sl-a) exit_order_id_239 == cover_target_order_id", row_slc["exit_order_id_239"] == "COVERTGT-RHO")
check("(sl-a) exit_price_239 == cover's fill (190.0)", row_slc["exit_price_239"] == 190.0)


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (stop-loss b) — square-off: stop-loss filled -> cover_target cancelled\n")
# ─────────────────────────────────────────────────────────────────────────────

short_d = make_short(symbol="SIGMA", entry_price=200.0, quantity=10,
                     cover_target_order_id="COVERTGT-SIG", cover_target_price=190.0,
                     stop_order_id="STOPLOSS-SIG", stop_trigger_price=218.9,
                     status="short_open")
store_sld = FakeStore([short_d])

def fake_order_status_sld(oid):
    if oid == "STOPLOSS-SIG":
        return {"orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 219.5}
    if oid == "COVERTGT-SIG":
        return {"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0}
    raise AssertionError(f"unexpected order id {oid}")

cancel_calls_sld = []
def fake_cancel_sld(oid):
    cancel_calls_sld.append(oid)
    return oid

buy_calls_sld = []
def fake_buy_sld(*a, **kw):
    buy_calls_sld.append((a, kw))
    return "SHOULD-NOT-HAPPEN"

with patch.object(rt, "_load_pos", store_sld.load), \
     patch.object(rt, "_save_pos", store_sld.save), \
     patch.object(rt, "_dhan_order_status", fake_order_status_sld), \
     patch.object(rt, "_dhan_cancel_order", fake_cancel_sld), \
     patch.object(rt, "buy", fake_buy_sld), \
     patch.object(rt, "_broker_short_qty", lambda sym: 10), \
     patch.object(rt.notify, "send_short_stoploss_hit", MagicMock()):
    rt.square_off_239(dry_run=False)

check("(sl-b) cover-target order was cancelled", cancel_calls_sld == ["COVERTGT-SIG"])
check("(sl-b) force-cover buy() never called", buy_calls_sld == [])
row_sld = store_sld.positions[0]
check("(sl-b) status short_closed", row_sld["status"] == "short_closed")
check("(sl-b) exit_order_id_239 == stop_order_id (NOT the target)",
      row_sld["exit_order_id_239"] == "STOPLOSS-SIG")
check("(sl-b) exit_price_239 == stop-loss's fill (219.5), NOT target's (190.0)",
      row_sld["exit_price_239"] == 219.5, str(row_sld["exit_price_239"]))
check("(sl-b) realized_pnl uses stop-loss fill: (200-219.5)*10 == -195.0",
      row_sld["realized_pnl"] == -195.0, str(row_sld["realized_pnl"]))


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (stop-loss c) — square-off: neither filled -> BOTH cancelled, force-cover proceeds\n")
# ─────────────────────────────────────────────────────────────────────────────

short_e = make_short(symbol="TAU", entry_price=200.0, quantity=10,
                     cover_target_order_id="COVERTGT-TAU", cover_target_price=190.0,
                     stop_order_id="STOPLOSS-TAU", stop_trigger_price=218.9,
                     status="short_open")
store_sle = FakeStore([short_e])

def fake_order_status_sle(oid):
    return {"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0}

cancel_calls_sle = []
def fake_cancel_sle(oid):
    cancel_calls_sle.append(oid)
    return oid

def fake_buy_sle(symbol, exch, qty, **kw):
    return f"FORCECOVER-{symbol}"

def fake_poll_fill_safe_sle(oid, fallback_price, fallback_qty):
    return 205.0, fallback_qty

with patch.object(rt, "_load_pos", store_sle.load), \
     patch.object(rt, "_save_pos", store_sle.save), \
     patch.object(rt, "_dhan_order_status", fake_order_status_sle), \
     patch.object(rt, "_dhan_cancel_order", fake_cancel_sle), \
     patch.object(rt, "buy", fake_buy_sle), \
     patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe_sle), \
     patch.object(rt, "_broker_short_qty", lambda sym: 10), \
     patch.object(rt.notify, "send_square_off_239", MagicMock()):
    rt.square_off_239(dry_run=False)

check("(sl-c) BOTH orders cancelled",
      set(cancel_calls_sle) == {"COVERTGT-TAU", "STOPLOSS-TAU"}, str(cancel_calls_sle))
row_sle = store_sle.positions[0]
check("(sl-c) status short_closed via the existing force-cover path", row_sle["status"] == "short_closed")
check("(sl-c) exit_order_id_239 == the force-cover order (not target/stop)",
      row_sle["exit_order_id_239"] == "FORCECOVER-TAU")


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (stop-loss d) — square-off: BOTH show TRADED -> manual review, no silent pick\n")
# ─────────────────────────────────────────────────────────────────────────────

short_f = make_short(symbol="UPSILON", entry_price=200.0, quantity=10,
                     cover_target_order_id="COVERTGT-UPS", cover_target_price=190.0,
                     stop_order_id="STOPLOSS-UPS", stop_trigger_price=218.9,
                     status="short_open")
store_slf = FakeStore([short_f])

def fake_order_status_slf(oid):
    if oid == "COVERTGT-UPS":
        return {"orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 190.0}
    if oid == "STOPLOSS-UPS":
        return {"orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 219.0}
    raise AssertionError(f"unexpected order id {oid}")

cancel_calls_slf = []
def fake_cancel_slf(oid):
    cancel_calls_slf.append(oid)
    return oid

buy_calls_slf = []
def fake_buy_slf(*a, **kw):
    buy_calls_slf.append((a, kw))
    return "SHOULD-NOT-HAPPEN"

with patch.object(rt, "_load_pos", store_slf.load), \
     patch.object(rt, "_save_pos", store_slf.save), \
     patch.object(rt, "_dhan_order_status", fake_order_status_slf), \
     patch.object(rt, "_dhan_cancel_order", fake_cancel_slf), \
     patch.object(rt, "buy", fake_buy_slf):
    rt.square_off_239(dry_run=False)

check("(sl-d) NEITHER order cancelled (no automatic pick)", cancel_calls_slf == [])
check("(sl-d) force-cover buy() never called", buy_calls_slf == [])
row_slf = store_slf.positions[0]
check("(sl-d) position row completely unchanged (still short_open)", row_slf["status"] == "short_open")
check("(sl-d) row unchanged entirely", row_slf == short_f)


# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'─' * 55}")
if failures == 0:
    print("\033[32mAll scenarios PASSED\033[0m")
    sys.exit(0)
else:
    print(f"\033[31m{failures} assertion(s) FAILED\033[0m")
    sys.exit(1)
