#!/usr/bin/env python3
"""
test_uc_staged_entry.py -- standalone verifier for dhan/uc_staged_entry.py (the
UC-based staged entry, Case A/B), the per_stock_capital snapshot in
dhan/live_monitor.py, and the Step 1/2/3 priority restructure in
dhan/run_trades.py's run_entry_321.

Mocks every broker-facing call (_margin_check/_available_balance/buy/
_poll_fill_strict/_tick_round's underlying tick_size, plus get_reference_price/
get_ltp_batch/security_id for the run_entry_321 scenarios) and the
position-file read/write (_load_long_pos/_save_long_pos) with an in-memory
store -- zero network calls, zero real file writes. Mirrors
dhan/test_targets.py's standalone script style (no pytest in this repo).

IMPORTANT: dhan/uc_staged_entry.py does `from dhan.run_trades import X` for
everything it reuses -- that binds its OWN local names, decoupled from
dhan.run_trades's names. Every mock below patches the name as it exists on
`uc` (the module under test), NOT on `rt`, except for `tick_size` (patched on
`rt`, since `_tick_round`'s function body resolves `tick_size` via
run_trades.py's own globals regardless of which module holds a reference to
the function) and the run_entry_321/live_monitor-specific scenarios, which
patch `rt`/`lm` directly.

Usage:
    python dhan/test_uc_staged_entry.py

Exit 0 on all-pass, exit 1 on any failure.
"""

import copy
import csv
import sys
import tempfile
import types
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

for _m in ("data_loader",):
    sys.modules.setdefault(_m, types.ModuleType(_m))

import dhan.run_trades as rt        # noqa: E402
import dhan.uc_staged_entry as uc   # noqa: E402

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
        self.save_count = 0

    def load(self):
        return copy.deepcopy(self.positions)

    def save(self, positions):
        self.positions = copy.deepcopy(positions)
        self.save_count += 1


def make_long(**overrides):
    row = {
        "broker": "dhan", "symbol": "TESTCO", "entry_date": date.today().isoformat(),
        "reference_price": 100.0, "shares_intended": 10,
        "actual_fill_price": 100.0, "actual_fill_quantity": 10,
        "entry_order_id": "E1", "status": "open",
        "entry_timestamp": "2026-08-25T15:21:00+05:30", "product": "MTF",
    }
    row.update(overrides)
    return row


def fake_margin_check_leveraged(sym, qty, price):
    return {"leverage": 3.0, "margin_required": qty * price / 3.0}


TODAY = date.today().isoformat()
PER_STOCK_CAPITAL = 300_000.0   # TOTAL_CAPITAL(1,500,000) / 5 qualified symbols


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (1) — Case A: leg1 fires, then leg2 retrace fires in-window\n")
# ─────────────────────────────────────────────────────────────────────────────

state1 = uc.UCState(symbol="ALPHA", prev_close=100.0, case_a_qualified=True,
                    hit_uc_before_1430=True, off_uc_in_window=True)
store1 = FakeStore([])
buy_calls_1 = []


def fake_buy_1(symbol, exch, qty, **kw):
    buy_calls_1.append((symbol, qty, kw.get("price"), kw.get("product")))
    return f"ORD-{len(buy_calls_1)}"


poll_results_1 = [(120.6, 1250, False, ""), (116.58, 1286, False, "")]


def fake_poll_1(order_id):
    return poll_results_1.pop(0)


with patch.object(uc, "_load_long_pos", store1.load), \
     patch.object(uc, "_save_long_pos", store1.save), \
     patch.object(uc, "_margin_check", fake_margin_check_leveraged), \
     patch.object(uc, "_available_balance", lambda: 10_000_000.0), \
     patch.object(uc, "buy", fake_buy_1), \
     patch.object(uc, "_poll_fill_strict", fake_poll_1), \
     patch.object(uc.notify, "send_entry", MagicMock()):

    now_1445 = datetime(2026, 8, 25, 14, 45, tzinfo=uc._IST)
    ev = uc.evaluate_tick(state1, 120.0, PER_STOCK_CAPITAL, now=now_1445)
    check("(1) 120 >= 100*1.19 fires case_a_leg1", ev == "case_a_leg1")
    check("(1) entry_status latches to order_placed synchronously",
          state1.entry_status == "order_placed")
    check("(1) capital_base captured on the state at trigger time",
          state1.capital_base == PER_STOCK_CAPITAL)

    uc.execute_case_a_leg1("ALPHA", state1, 120.0, upper_circuit=None, dry_run=False)
    check("(1) leg1 buys half of capital_base (150000) -> compute_shares=1250 @ 120",
          buy_calls_1 == [("ALPHA", 1250, 120.6, "MTF")], str(buy_calls_1))
    check("(1) state resolves to partially_filled, case_a_leg watching retrace",
          state1.entry_status == "partially_filled"
          and state1.case_a_leg == "leg1_filled_watching_retrace")
    row1 = store1.positions[0]
    check("(1) row case=A entry_status=partially_filled status=leg1_filled",
          row1["case"] == "A" and row1["entry_status"] == "partially_filled"
          and row1["status"] == "leg1_filled")
    check("(1) row filled_amount = 120.6*1250 = 150750.0",
          row1["filled_amount"] == 150750.0, str(row1["filled_amount"]))
    check("(1) row capital_base persisted for a later 3:21 completion to read back",
          row1["capital_base"] == PER_STOCK_CAPITAL)
    check("(1) row carries case_a_qualified=True", row1["case_a_qualified"] is True)

    # Window still 14:30-15:18 for leg2 -- even past leg1's own 15:00 cutoff.
    now_1505 = datetime(2026, 8, 25, 15, 5, tzinfo=uc._IST)
    ev2 = uc.evaluate_tick(state1, 116.0, PER_STOCK_CAPITAL, now=now_1505)
    check("(1) 116 <= 100*1.17 fires case_a_leg2 even AFTER leg1's own 15:00 window closed "
          "(leg2's window runs to 15:18)", ev2 == "case_a_leg2")
    check("(1) entry_status re-latches to order_placed", state1.entry_status == "order_placed")

    uc.execute_case_a_leg2("ALPHA", state1, 116.0, upper_circuit=None, dry_run=False)
    check("(1) leg2 buys compute_shares(remaining=149250, 116)=1286 at the tick-rounded limit",
          buy_calls_1[1] == ("ALPHA", 1286, uc._tick_round("ALPHA", 116.0 * 1.005), "MTF"),
          str(buy_calls_1))
    check("(1) state resolves to filled, case_a_leg=leg2_filled",
          state1.entry_status == "filled" and state1.case_a_leg == "leg2_filled")
    row1b = store1.positions[0]
    check("(1) row folded into ONE row, status flipped to open (picked up by _open_pos as-is)",
          len(store1.positions) == 1 and row1b["status"] == "open"
          and row1b["entry_status"] == "filled")
    check("(1) row actual_fill_quantity summed to 2536 (1250+1286)",
          row1b["actual_fill_quantity"] == 2536, str(row1b))
    check("(1) row filled_amount summed to 150750+149921.88=300671.88",
          row1b["filled_amount"] == 300671.88, str(row1b["filled_amount"]))


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (2) — Case A: leg1 fires, no retrace by 15:18 -> stays partially_filled\n")
# ─────────────────────────────────────────────────────────────────────────────

state2 = uc.UCState(symbol="BETA", prev_close=100.0, case_a_qualified=True,
                    hit_uc_before_1430=True, off_uc_in_window=True)
store2 = FakeStore([])

with patch.object(uc, "_load_long_pos", store2.load), \
     patch.object(uc, "_save_long_pos", store2.save), \
     patch.object(uc, "_margin_check", fake_margin_check_leveraged), \
     patch.object(uc, "_available_balance", lambda: 10_000_000.0), \
     patch.object(uc, "buy", lambda symbol, exch, qty, **kw: "ORD-2"), \
     patch.object(uc, "_poll_fill_strict", lambda oid: (120.6, 1250, False, "")), \
     patch.object(uc.notify, "send_entry", MagicMock()):

    now_1445 = datetime(2026, 8, 25, 14, 45, tzinfo=uc._IST)
    uc.evaluate_tick(state2, 120.0, PER_STOCK_CAPITAL, now=now_1445)
    uc.execute_case_a_leg1("BETA", state2, 120.0, upper_circuit=None, dry_run=False)
    check("(2) leg1 filled, state armed watching retrace", state2.entry_status == "partially_filled")

    # No retrace ever happens; window closes at 15:18.
    now_1519 = datetime(2026, 8, 25, 15, 19, tzinfo=uc._IST)
    ev = uc.evaluate_tick(state2, 90.0, PER_STOCK_CAPITAL, now=now_1519)
    check("(2) a retrace-shaped tick AFTER 15:18 fires nothing (leg2 window closed)", ev is None)
    check("(2) state stays partially_filled (not silently advanced/lost)",
          state2.entry_status == "partially_filled")
    row2 = store2.positions[0]
    check("(2) position row stays entry_status=partially_filled -- Step 1 at 3:21pm picks "
          "this up with priority, no separate finalize/timeout step needed",
          row2["entry_status"] == "partially_filled")


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (3) — Case B: direct 100% fill\n")
# ─────────────────────────────────────────────────────────────────────────────

state3 = uc.UCState(symbol="GAMMA", prev_close=100.0)
store3 = FakeStore([])
buy_calls_3 = []


def fake_buy_3(symbol, exch, qty, **kw):
    buy_calls_3.append((symbol, qty, kw.get("price"), kw.get("product")))
    return "ORD-3"


with patch.object(uc, "_load_long_pos", store3.load), \
     patch.object(uc, "_save_long_pos", store3.save), \
     patch.object(uc, "_margin_check", fake_margin_check_leveraged), \
     patch.object(uc, "_available_balance", lambda: 10_000_000.0), \
     patch.object(uc, "buy", fake_buy_3), \
     patch.object(uc, "_poll_fill_strict", lambda oid: (120.6, 2500, False, "")), \
     patch.object(uc.notify, "send_entry", MagicMock()):

    now_1510 = datetime(2026, 8, 25, 15, 10, tzinfo=uc._IST)
    ev = uc.evaluate_tick(state3, 120.0, PER_STOCK_CAPITAL, now=now_1510)
    check("(3) 120 >= 100*1.19 during Case B window fires case_b_fill", ev == "case_b_fill")
    check("(3) entry_status latches to order_placed", state3.entry_status == "order_placed")

    uc.execute_case_b("GAMMA", state3, 120.0, upper_circuit=None, dry_run=False)
    check("(3) Case B buys the FULL compute_shares(300000, 120)=2500 in one shot",
          buy_calls_3 == [("GAMMA", 2500, 120.6, "MTF")], str(buy_calls_3))
    check("(3) state resolves to filled directly (no case_a_leg involved)",
          state3.entry_status == "filled" and state3.case_a_leg is None)
    row3 = store3.positions[0]
    check("(3) row case=B, status=open directly (no legs)",
          row3["case"] == "B" and row3["status"] == "open" and row3["entry_status"] == "filled")
    check("(3) row carries case_a_qualified=False (Case B is only reachable when unqualified)",
          row3["case_a_qualified"] is False)


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (4) — Case A/B overlap tie-break during 15:00-15:18\n")
# ─────────────────────────────────────────────────────────────────────────────
# A symbol already armed watching Case A's retrace must NEVER also be
# evaluated as a Case B candidate during the 15:00-15:18 overlap. Also: a
# case_a_qualified symbol that hasn't even fired leg1 yet ("still being
# evaluated for leg 1") must also never fall through to Case B -- confirmed
# as a deliberate exclusion in evaluate_tick, not just an entry_status
# coincidence.

state4 = uc.UCState(symbol="DELTA", prev_close=100.0,
                    entry_status="partially_filled",
                    case_a_leg="leg1_filled_watching_retrace",
                    case="A", case_a_qualified=True,
                    capital_base=PER_STOCK_CAPITAL, filled_amount=150750.0)

now_1510b = datetime(2026, 8, 25, 15, 10, tzinfo=uc._IST)
# A tick well above the leg1 threshold (would trivially qualify for Case B's
# rising trigger if it were evaluated) but BELOW the leg2 retrace threshold --
# only case_a_leg2 should be a possible outcome here, never case_b_fill.
ev4 = uc.evaluate_tick(state4, 150.0, PER_STOCK_CAPITAL, now=now_1510b)
check("(4) a rising tick well above Case B's own threshold does NOT fire case_b_fill "
      "for a symbol already armed in Case A leg2-watch", ev4 != "case_b_fill")
check("(4) (and correctly fires nothing here, since 150 doesn't satisfy leg2's retrace "
      "condition either)", ev4 is None)

ev4b = uc.evaluate_tick(state4, 116.0, PER_STOCK_CAPITAL, now=now_1510b)
check("(4) the SAME symbol correctly fires case_a_leg2 (not case_b_fill) on a genuine retrace",
      ev4b == "case_a_leg2")

# And the reverse: a fresh not_attempted symbol during the overlap DOES still
# evaluate for Case B normally (the tie-break only excludes already-armed
# Case A symbols, not the whole window).
state4c = uc.UCState(symbol="EPSILON4", prev_close=100.0)
ev4c = uc.evaluate_tick(state4c, 120.0, PER_STOCK_CAPITAL, now=now_1510b)
check("(4) an untouched symbol during the SAME overlap window fires case_b_fill normally",
      ev4c == "case_b_fill")

# "Still being evaluated for leg 1": a symbol that passed the Case A
# qualification filter but hasn't crossed leg1's trigger yet (entry_status
# still not_attempted) must ALSO never fall into Case B during the overlap --
# confirmed this is checked even before leg 1 has fired.
state4d = uc.UCState(symbol="ZETA4", prev_close=100.0, case_a_qualified=True)
ev4d = uc.evaluate_tick(state4d, 120.0, PER_STOCK_CAPITAL, now=now_1510b)
check("(4) a case_a_qualified symbol that hasn't fired leg1 yet crosses the trigger during "
      "the overlap and fires case_a_leg1, NEVER case_b_fill", ev4d == "case_a_leg1")


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (4b) — Case A qualification filter: hit-UC-before-1430 + off-UC-in-window latches\n")
# ─────────────────────────────────────────────────────────────────────────────

t_1000 = datetime(2026, 8, 25, 10, 0, tzinfo=uc._IST)
t_1435 = datetime(2026, 8, 25, 14, 35, tzinfo=uc._IST)
t_1519 = datetime(2026, 8, 25, 15, 19, tzinfo=uc._IST)

# (a) Qualifies: hits UC before 14:30, then seen off UC during the window.
state4b_a = uc.UCState(symbol="OMEGA-A", prev_close=100.0)
uc.update_case_a_qualification(state4b_a, ltp=190.0, upper_circuit=190.0, now=t_1000)
check("(4b-a) LTP >= upper_circuit before 14:30 latches hit_uc_before_1430",
      state4b_a.hit_uc_before_1430 is True)
check("(4b-a) not yet qualified -- hasn't been seen off UC during the window yet",
      state4b_a.case_a_qualified is False)
uc.update_case_a_qualification(state4b_a, ltp=185.0, upper_circuit=190.0, now=t_1435)
check("(4b-a) LTP < upper_circuit during 14:30-15:18 latches off_uc_in_window",
      state4b_a.off_uc_in_window is True)
check("(4b-a) both latches true -> case_a_qualified becomes True",
      state4b_a.case_a_qualified is True)

# (b) FAILS: never hits UC before 14:30 at all.
state4b_b = uc.UCState(symbol="OMEGA-B", prev_close=100.0)
uc.update_case_a_qualification(state4b_b, ltp=150.0, upper_circuit=190.0, now=t_1000)
uc.update_case_a_qualification(state4b_b, ltp=185.0, upper_circuit=190.0, now=t_1435)
uc.update_case_a_qualification(state4b_b, ltp=185.0, upper_circuit=190.0, now=t_1519)
check("(4b-b) never hit UC before 14:30 -> hit_uc_before_1430 stays False",
      state4b_b.hit_uc_before_1430 is False)
check("(4b-b) never qualifies for Case A even after the window closes",
      state4b_b.case_a_qualified is False)

# (c) FAILS: hits UC before 14:30 but stays locked (never seen off UC) through 15:18.
state4b_c = uc.UCState(symbol="OMEGA-C", prev_close=100.0)
uc.update_case_a_qualification(state4b_c, ltp=190.0, upper_circuit=190.0, now=t_1000)
uc.update_case_a_qualification(state4b_c, ltp=190.0, upper_circuit=190.0, now=t_1435)
uc.update_case_a_qualification(state4b_c, ltp=190.0, upper_circuit=190.0, now=t_1519)
check("(4b-c) hit UC before 14:30", state4b_c.hit_uc_before_1430 is True)
check("(4b-c) stayed locked on UC the whole window -- off_uc_in_window never latches",
      state4b_c.off_uc_in_window is False)
check("(4b-c) never qualifies -- stayed locked through 15:18", state4b_c.case_a_qualified is False)

# A disqualified symbol's Case A leg mechanism never fires -- but it's still an
# ordinary Case B candidate like any other symbol; the qualification filter
# only gates the Case A leg path, not staged entry as a whole.
ev4b_casea = uc.evaluate_tick(state4b_c, 120.0, PER_STOCK_CAPITAL, now=t_1435)
check("(4b-c) a disqualified symbol crossing the trigger inside Case A's own window "
      "(14:35, before Case B opens at 15:00) fires nothing -- falls through untouched",
      ev4b_casea is None)

state4b_d = uc.UCState(symbol="OMEGA-D", prev_close=100.0)   # never qualified (default)
now_1510c = datetime(2026, 8, 25, 15, 10, tzinfo=uc._IST)
ev4b_d = uc.evaluate_tick(state4b_d, 120.0, PER_STOCK_CAPITAL, now=now_1510c)
check("(4b-d) a never-Case-A-qualified symbol still fires case_b_fill normally during "
      "15:00-15:18 -- qualification failure only excludes the Case A leg path",
      ev4b_d == "case_b_fill")

check("(4b) update_case_a_qualification degrades safely with upper_circuit=None (no crash)",
      uc.update_case_a_qualification(
          uc.UCState(symbol="X", prev_close=100.0), 120.0, None, now=t_1435) is None)
check("(4b) update_case_a_qualification degrades safely with upper_circuit=0 (falsy, no crash)",
      uc.update_case_a_qualification(
          uc.UCState(symbol="Y", prev_close=100.0), 120.0, 0, now=t_1435) is None)


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (5) — 3:21pm Step 1/2/3 priority ordering\n")
# ─────────────────────────────────────────────────────────────────────────────

partial_row = make_long(symbol="ZETA", case="A", case_a_leg="leg1_filled_watching_retrace",
                        entry_status="partially_filled", status="leg1_filled",
                        capital_base=300_000.0, filled_amount=150_750.0,
                        actual_fill_price=120.6, actual_fill_quantity=1250, product="MTF")
filled_row = make_long(symbol="ETA", case="B", entry_status="filled", status="open",
                       capital_base=300_000.0, filled_amount=299_800.0,
                       actual_fill_quantity=2500, product="CNC")
store5 = FakeStore([partial_row, filled_row])

tmp_dir  = tempfile.mkdtemp()
tmp_root = Path(tmp_dir)
tmp_trades_dir = tmp_root / "trades"
tmp_trades_dir.mkdir(parents=True, exist_ok=True)


def write_trade_list(rows: list[tuple[str, float]]) -> None:
    path = tmp_trades_dir / f"trade_list_{TODAY}.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "shares", "ref_price"])
        for sym, ref in rows:
            w.writerow([sym, 100, ref])


# ETA is deliberately in today's trade_list too (already filled via Case B --
# must still be skipped, not double-entered). ZETA deliberately NOT in the
# trade_list (Case A watches a broader universe) -- proves Step 1 still
# picks it up. THETA is a totally untouched fresh Step 3 symbol.
write_trade_list([("ETA", 50.0), ("THETA", 80.0)])

call_order = []


def fake_buy_5(symbol, exch, qty, **kw):
    call_order.append(symbol)
    return f"ORD-5-{symbol}"


margin_check_calls_5 = []


def fake_margin_check_5(sym, qty, price):
    margin_check_calls_5.append(sym)
    return {"leverage": 3.0, "margin_required": qty * price / 3.0}


with patch.object(rt, "_RESULTS_DIR", tmp_root), \
     patch.object(rt, "_LOG_DIR", tmp_trades_dir), \
     patch.object(rt, "_load_long_pos", store5.load), \
     patch.object(rt, "_save_long_pos", store5.save), \
     patch.object(rt, "get_reference_price",
                  lambda sym: {"ZETA": (116.0, 1520), "THETA": (80.0, 1520)}.get(sym, (100.0, 1520))), \
     patch.object(rt, "get_ltp_batch", lambda syms: {s: 100.0 for s in syms}), \
     patch.object(rt, "security_id", lambda sym: "999"), \
     patch.object(rt, "_margin_check", fake_margin_check_5), \
     patch.object(rt, "_available_balance", lambda: 10_000_000.0), \
     patch.object(rt, "buy", fake_buy_5), \
     patch.object(rt, "_poll_fill_strict", lambda oid: (100.5, 100, False, "")), \
     patch.object(rt.notify, "send_entry", MagicMock()):
    rt.run_entry_321(dry_run=False)

check("(5) ETA (already filled via Case B, also happens to be in today's trade_list) "
      "is skipped entirely", "ETA" not in call_order)
check("(5) ZETA (Step 1 priority, outside today's trade_list) is attempted BEFORE "
      "THETA (Step 3, fresh)", call_order.index("ZETA") < call_order.index("THETA"),
      str(call_order))
check("(5) ZETA's completion buys compute_shares(300000-150750, 116)=1286",
      "ZETA" in call_order)
zeta_row = next(p for p in store5.positions if p["symbol"] == "ZETA")
check("(5) ZETA row folded, status=open, entry_status=filled",
      zeta_row["status"] == "open" and zeta_row["entry_status"] == "filled")
check("(5) ZETA completion reused leg1's MTF product WITHOUT a fresh margin check",
      "ZETA" not in margin_check_calls_5)
check("(5) THETA (fresh Step 3) DID get a fresh margin check, same as any ordinary entry",
      "THETA" in margin_check_calls_5)
eta_row = next(p for p in store5.positions if p["symbol"] == "ETA")
check("(5) ETA row completely unchanged", eta_row == filled_row)


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (6) — per_stock_capital snapshot: once at 14:30, never recomputed\n")
# ─────────────────────────────────────────────────────────────────────────────

import dhan.live_monitor as lm   # noqa: E402

mon = lm.LiveMonitor("client1", "tok1", enable_uc_staged_entry=True, dry_run=True)
mon._executor = MagicMock()   # never actually place an order in this scenario

sid_a, sid_b = 111, 222
mon._states = {
    sid_a: lm._SymState(symbol="SIGMA", vol_threshold=1, prev_vwap=100.0,
                        upper_circuit=200.0, lower_circuit=50.0, qualified=True),
    sid_b: lm._SymState(symbol="TAU", vol_threshold=1, prev_vwap=100.0,
                        upper_circuit=200.0, lower_circuit=50.0, qualified=True),
}
mon._uc_states = {
    sid_a: uc.UCState(symbol="SIGMA", prev_close=100.0),
    sid_b: uc.UCState(symbol="TAU", prev_close=100.0),
}

check("(6) per_stock_capital starts unset (constructor default)",
      mon._per_stock_capital is None)

msg_1430 = {"type": "Quote Data", "security_id": str(sid_a), "LTP": "50.0", "volume": "0"}
with patch.object(lm, "datetime") as mock_dt:
    # A tick BEFORE 14:30 must NOT snapshot yet.
    mock_dt.now.return_value = datetime(2026, 8, 25, 14, 20, tzinfo=lm._IST)
    mon._on_message(None, msg_1430)
check("(6) a tick before 14:30 does not snapshot yet", mon._per_stock_capital is None)

with patch.object(lm, "datetime") as mock_dt:
    mock_dt.now.return_value = datetime(2026, 8, 25, 14, 35, tzinfo=lm._IST)
    mon._on_message(None, msg_1430)
check("(6) first tick at/after 14:30 snapshots per_stock_capital = TOTAL_CAPITAL/qualified(2)",
      mon._per_stock_capital == lm.TOTAL_CAPITAL / 2, str(mon._per_stock_capital))

# A third symbol "qualifies" afterward -- the snapshot must NOT change.
mon._states[333] = lm._SymState(symbol="UPSILON", vol_threshold=1, prev_vwap=100.0,
                                upper_circuit=200.0, lower_circuit=50.0, qualified=True)
with patch.object(lm, "datetime") as mock_dt2:
    mock_dt2.now.return_value = datetime(2026, 8, 25, 14, 40, tzinfo=lm._IST)
    mon._on_message(None, {"type": "Quote Data", "security_id": str(sid_b),
                           "LTP": "50.0", "volume": "0"})
check("(6) per_stock_capital UNCHANGED even though a 3rd symbol qualified afterward "
      "(snapshot is one-time, not recomputed)",
      mon._per_stock_capital == lm.TOTAL_CAPITAL / 2, str(mon._per_stock_capital))


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (7) — UC fetch/no-data mid-tick: degrade safely, no crash, no uncapped surprise\n")
# ─────────────────────────────────────────────────────────────────────────────

check("(7) _capped_limit_price with upper_circuit=None returns the plain uncapped limit",
      uc._capped_limit_price("KAPPA", 120.0, None) == uc._tick_round("KAPPA", 120.0 * 1.005))
check("(7) _capped_limit_price with upper_circuit=0 (falsy) also stays uncapped",
      uc._capped_limit_price("KAPPA", 120.0, 0) == uc._tick_round("KAPPA", 120.0 * 1.005))
check("(7) _capped_limit_price WITH a real UC below the plain limit caps it",
      uc._capped_limit_price("KAPPA", 120.0, 121.0) == uc._tick_round("KAPPA", 121.0 * 0.995))

state7 = uc.UCState(symbol="KAPPA", prev_close=100.0, case_a_qualified=True)
store7 = FakeStore([])
buy_calls_7 = []


def fake_buy_7(symbol, exch, qty, **kw):
    buy_calls_7.append((symbol, qty, kw.get("price")))
    return "ORD-7"


with patch.object(uc, "_load_long_pos", store7.load), \
     patch.object(uc, "_save_long_pos", store7.save), \
     patch.object(uc, "_margin_check", fake_margin_check_leveraged), \
     patch.object(uc, "_available_balance", lambda: 10_000_000.0), \
     patch.object(uc, "buy", fake_buy_7), \
     patch.object(uc, "_poll_fill_strict", lambda oid: (120.6, 1250, False, "")), \
     patch.object(uc.notify, "send_entry", MagicMock()):
    now_1445b = datetime(2026, 8, 25, 14, 45, tzinfo=uc._IST)
    uc.evaluate_tick(state7, 120.0, PER_STOCK_CAPITAL, now=now_1445b)
    uc.execute_case_a_leg1("KAPPA", state7, 120.0, upper_circuit=None, dry_run=False)

check("(7) leg1 still places a real order at the uncapped limit price when UC is unavailable "
      "(doesn't crash, doesn't silently skip)",
      buy_calls_7 == [("KAPPA", 1250, uc._tick_round("KAPPA", 120.0 * 1.005))], str(buy_calls_7))
check("(7) state resolves normally despite no UC data", state7.entry_status == "partially_filled")


try:
    import shutil
    shutil.rmtree(tmp_dir, ignore_errors=True)
except Exception:
    pass


print("\n" + "─" * 60)
if failures:
    print(f"\033[31m{failures} scenario(s) FAILED\033[0m")
    sys.exit(1)
else:
    print("\033[32mAll scenarios PASSED\033[0m")
    sys.exit(0)
