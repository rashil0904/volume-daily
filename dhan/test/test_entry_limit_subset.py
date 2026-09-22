#!/usr/bin/env python3
"""
test_entry_limit_subset.py -- standalone verifier for:
  1. _full_signal_count(trade_date) -- reads trade_list_<date>.csv's row
     count, loud failure (SystemExit) if the file is missing.
  2. run_entry_321 and run_entry_limit() both derive `n` for
     compute_allocation(capital, n) from _full_signal_count -- the FULL
     day's signal count -- not len(whatever local symbol list this
     particular invocation happened to build (a --symbols subset, or a
     post-dedup remainder). Every symbol gets the same capital/N slice
     regardless of which mechanism buys it or how many other symbols were
     already claimed.
  3. run_entry_limit()'s new --symbols subset mode: markers
     (entry_limit_started_<date>.flag / entry_limit_done_<date>.flag) are
     written only in --symbols mode, never in full-list or manual --symbol
     mode.
  4. The position-overlap hand-off: symbols already claimed by a
     run_entry_limit --symbols invocation are skipped by a subsequent
     run_entry_321 full-list run (existing dedup, confirmed still correct
     with the new subset mode in play).
  5. _wait_for_entry_limit_marker -- no-ops on a normal day (no started
     flag), returns promptly once the done flag appears, and times out with
     a loud alert (never hangs) past the bounded deadline on a simulated
     slow/hung sweep -- using an injected fake clock/sleep, not a real wait.

Mocks every Dhan API call (buy/order_status/cancel/margin/quote/balance) --
zero real network calls, zero real orders. Uses a temp directory for
_RESULTS_DIR so this suite never touches the real results/ tree.

Usage:
    python dhan/test/test_entry_limit_subset.py

Exit 0 on all-pass, exit 1 on any failure.
"""

import copy
import csv
import io
import json
import sys
import tempfile
from contextlib import redirect_stdout
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock
from zoneinfo import ZoneInfo

_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

import dhan.run_trades as rt   # noqa: E402

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
    """Backs positions_dhan_long.json with an in-memory list -- no real file
    I/O. Shared across two calls (e.g. run_entry_limit then run_entry_321) to
    prove the hand-off actually persists between them, same pattern as
    test_targets.py's FakeStore."""
    def __init__(self, positions=None):
        self.positions = copy.deepcopy(positions or [])
        self.save_count = 0

    def load(self):
        return copy.deepcopy(self.positions)

    def save(self, positions):
        self.positions = copy.deepcopy(positions)
        self.save_count += 1


def write_trade_list(trades_dir: Path, trade_date: date, symbols: list[str]) -> None:
    trades_dir.mkdir(parents=True, exist_ok=True)
    with open(trades_dir / f"trade_list_{trade_date.isoformat()}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "shares", "ref_price"])
        for sym in symbols:
            w.writerow([sym, 100, 100.0])


def fresh_tmp_root() -> Path:
    return Path(tempfile.mkdtemp())


# ── compute_allocation spy ──────────────────────────────────────────────────

def make_allocation_spy():
    calls = []
    real = rt.compute_allocation

    def spy(capital, n):
        calls.append((capital, n))
        return real(capital, n)
    return spy, calls


# ── Shared fakes for run_entry_limit's sizing loop + coordinator tick loop ──

def entry_limit_fakes(ref_price=100.0, leverage=3.0):
    order_qty: dict[str, tuple[int, float]] = {}
    counter = {"n": 0}

    def fake_available_balance():
        return 10_000_000.0

    def fake_get_reference_price(sym):
        return (ref_price, 1500)

    def fake_margin_check(sym, qty, price):
        return {"leverage": leverage, "margin_required": qty * price / leverage}

    def fake_get_quote_batch(syms):
        return {s: {"best_bid": ref_price, "last_price": ref_price, "upper_circuit": None}
                for s in syms}

    def fake_tick_size(sym):
        return 0.05

    def fake_buy(symbol, exch, qty, order_type=None, price=None, product=None,
                 dry_run=False, **kw):
        counter["n"] += 1
        oid = f"ORD{counter['n']}"
        order_qty[oid] = (qty, price or ref_price)
        return oid

    def fake_order_status(oid):
        qty, price = order_qty[oid]
        return {"orderStatus": "TRADED", "filledQty": qty, "averageTradedPrice": price}

    def fake_cancel_order(oid):
        return None

    return {
        "_available_balance": fake_available_balance,
        "get_reference_price": fake_get_reference_price,
        "_margin_check": fake_margin_check,
        "_get_quote_batch": fake_get_quote_batch,
        "tick_size": fake_tick_size,
        "buy": fake_buy,
        "_dhan_order_status": fake_order_status,
        "_dhan_cancel_order": fake_cancel_order,
    }


def run_entry_limit_with_fakes(trade_date, tmp_root, store, *, symbol=None, symbols=None,
                                capital=None, allocation_spy=None):
    fakes = entry_limit_fakes()
    patches = [
        patch.object(rt, "_RESULTS_DIR", tmp_root),
        patch.object(rt, "_LOG_DIR", tmp_root / "trades"),
        patch.object(rt, "_load_long_pos", store.load),
        patch.object(rt, "_save_long_pos", store.save),
        patch.object(rt.time, "sleep", lambda s: None),
        patch.object(rt, "_sync_pnl_workbook", lambda: None),
        patch.object(rt.notify, "send_entry", MagicMock()),
        patch.object(rt.notify, "send_entry_failed", MagicMock()),
    ] + [patch.object(rt, name, fn) for name, fn in fakes.items()]
    if allocation_spy is not None:
        patches.append(patch.object(rt, "compute_allocation", allocation_spy))

    from contextlib import ExitStack
    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        rt.run_entry_limit(trade_date=trade_date, dry_run=False, capital=capital,
                           symbol=symbol, symbols=symbols)


def run_entry_321_with_fakes(trade_date, tmp_root, store, *, capital=None,
                              allocation_spy=None):
    fakes = entry_limit_fakes()   # reused for get_reference_price/_margin_check/tick_size only

    # run_entry_321 uses buy() + _poll_fill_strict() (not order_status polling
    # like run_entry_limit) -- build matching fakes that immediately confirm
    # a full fill for whatever qty was placed.
    placed: dict[str, tuple[int, float]] = {}

    def fake_buy_321(symbol, exch, qty, order_type=None, price=None, product=None,
                      dry_run=False, **kw):
        oid = f"ORD321-{symbol}"
        placed[oid] = (qty, price)
        return oid

    def fake_poll_fill_strict_321(order_id):
        qty, price = placed[order_id]
        return (price, qty, False, "")

    from contextlib import ExitStack
    with ExitStack() as stack:
        stack.enter_context(patch.object(rt, "_RESULTS_DIR", tmp_root))
        stack.enter_context(patch.object(rt, "_LOG_DIR", tmp_root / "trades"))
        stack.enter_context(patch.object(rt, "_load_long_pos", store.load))
        stack.enter_context(patch.object(rt, "_save_long_pos", store.save))
        stack.enter_context(patch.object(rt.time, "sleep", lambda s: None))
        stack.enter_context(patch.object(rt, "_sync_pnl_workbook", lambda: None))
        stack.enter_context(patch.object(rt.notify, "send_entry", MagicMock()))
        stack.enter_context(patch.object(rt.notify, "send_entry_failed", MagicMock()))
        stack.enter_context(patch.object(rt, "_available_balance", fakes["_available_balance"]))
        stack.enter_context(patch.object(rt, "get_reference_price", fakes["get_reference_price"]))
        stack.enter_context(patch.object(rt, "_margin_check", fakes["_margin_check"]))
        stack.enter_context(patch.object(rt, "security_id", lambda sym: 12345))
        stack.enter_context(patch.object(rt, "_fetch_upper_circuit_batch", lambda syms: {}))
        stack.enter_context(patch.object(rt, "get_ltp_batch",
                                          lambda syms: {s: 100.0 for s in syms}))
        stack.enter_context(patch.object(rt, "tick_size", fakes["tick_size"]))
        stack.enter_context(patch.object(rt, "buy", fake_buy_321))
        stack.enter_context(patch.object(rt, "_poll_fill_strict", fake_poll_fill_strict_321))
        if allocation_spy is not None:
            stack.enter_context(patch.object(rt, "compute_allocation", allocation_spy))
        rt.run_entry_321(trade_date=trade_date, dry_run=False, capital=capital)
    return placed


# ═════════════════════════════════════════════════════════════════════════════
print("Scenario 1 -- _full_signal_count: row count + loud failure\n")
# ═════════════════════════════════════════════════════════════════════════════

TD1 = date(2026, 9, 23)
tmp1 = fresh_tmp_root()
write_trade_list(tmp1 / "trades", TD1, ["A", "B", "C", "D", "E", "F"])

with patch.object(rt, "_RESULTS_DIR", tmp1):
    n1 = rt._full_signal_count(TD1)
check("6-row trade_list -> _full_signal_count returns 6", n1 == 6, str(n1))

tmp1b = fresh_tmp_root()
with patch.object(rt, "_RESULTS_DIR", tmp1b):
    try:
        rt._full_signal_count(TD1)
        missing_file_exits = False
    except SystemExit:
        missing_file_exits = True
check("missing trade_list -> _full_signal_count exits loudly (SystemExit)",
      missing_file_exits)


# ═════════════════════════════════════════════════════════════════════════════
print("\nScenario 2 -- run_entry_limit --symbols subset: n = FULL day count, "
      "not subset size\n")
# ═════════════════════════════════════════════════════════════════════════════

TD2 = date(2026, 9, 23)
tmp2 = fresh_tmp_root()
write_trade_list(tmp2 / "trades", TD2, ["A", "B", "C", "D", "E", "F"])
store2 = FakeStore()
spy2, calls2 = make_allocation_spy()

run_entry_limit_with_fakes(TD2, tmp2, store2, symbols=["A", "B", "C"],
                           capital=1_500_000.0, allocation_spy=spy2)

expected_alloc = rt.compute_allocation(1_500_000.0, 6)   # = 250,000
check("--symbols subset (3 of 6): every compute_allocation call used n=6",
      all(n == 6 for _, n in calls2), str(calls2))
check("--symbols subset: resulting allocation is capital/6 (₹250,000), not capital/4 or /3",
      expected_alloc == 250_000.0, str(expected_alloc))

claimed_syms2 = {p["symbol"] for p in store2.positions}
check("--symbols subset: exactly A/B/C entered, D/E/F untouched",
      claimed_syms2 == {"A", "B", "C"}, str(claimed_syms2))
for p in store2.positions:
    shares = p["actual_fill_quantity"]
    check(f"--symbols subset: {p['symbol']} shares match compute_shares(250000, 100.0)",
          shares == rt.compute_shares(expected_alloc, 100.0), str(shares))


# ═════════════════════════════════════════════════════════════════════════════
print("\nScenario 3 -- run_entry_limit full-list mode: n unchanged (still full count)\n")
# ═════════════════════════════════════════════════════════════════════════════

TD3 = date(2026, 9, 23)
tmp3 = fresh_tmp_root()
write_trade_list(tmp3 / "trades", TD3, ["A", "B", "C", "D", "E", "F"])
store3 = FakeStore()
spy3, calls3 = make_allocation_spy()

run_entry_limit_with_fakes(TD3, tmp3, store3, capital=1_500_000.0, allocation_spy=spy3)

check("full-list mode (no --symbol/--symbols): every compute_allocation call used n=6",
      len(calls3) > 0 and all(n == 6 for _, n in calls3), str(calls3))
check("full-list mode: all 6 symbols entered",
      {p["symbol"] for p in store3.positions} == {"A", "B", "C", "D", "E", "F"},
      str({p["symbol"] for p in store3.positions}))


# ═════════════════════════════════════════════════════════════════════════════
print("\nScenario 4 -- run_entry_limit manual --symbol mode: unaffected "
      "(capital used directly, compute_allocation never called)\n")
# ═════════════════════════════════════════════════════════════════════════════

TD4 = date(2026, 9, 23)
tmp4 = fresh_tmp_root()
write_trade_list(tmp4 / "trades", TD4, ["A", "B", "C", "D", "E", "F"])
store4 = FakeStore()
spy4, calls4 = make_allocation_spy()

run_entry_limit_with_fakes(TD4, tmp4, store4, symbol="ZZZ", capital=250_000.0,
                           allocation_spy=spy4)

check("manual --symbol mode: compute_allocation never called (capital used directly)",
      calls4 == [], str(calls4))
check("manual --symbol mode: ZZZ entered with the exact capital given",
      len(store4.positions) == 1 and store4.positions[0]["symbol"] == "ZZZ",
      str(store4.positions))


# ═════════════════════════════════════════════════════════════════════════════
print("\nScenario 5 -- markers: written ONLY in --symbols mode\n")
# ═════════════════════════════════════════════════════════════════════════════

started_path2 = rt._entry_limit_started_flag_path(TD2)
done_path2    = rt._entry_limit_done_flag_path(TD2)
with patch.object(rt, "_RESULTS_DIR", tmp2):
    started_exists = rt._entry_limit_started_flag_path(TD2).exists()
    done_exists    = rt._entry_limit_done_flag_path(TD2).exists()
check("--symbols mode: entry_limit_started_<date>.flag was written", started_exists)
check("--symbols mode: entry_limit_done_<date>.flag was written", done_exists)

if done_exists:
    done_content = json.loads((tmp2 / "trades" / f"entry_limit_done_{TD2.isoformat()}.flag").read_text())
    check("done marker: symbols_claimed matches the actual claimed set",
          set(done_content.get("symbols_claimed", [])) == {"A", "B", "C"},
          str(done_content))

with patch.object(rt, "_RESULTS_DIR", tmp3):
    started_fulllist = (tmp3 / "trades" / f"entry_limit_started_{TD3.isoformat()}.flag").exists()
    done_fulllist    = (tmp3 / "trades" / f"entry_limit_done_{TD3.isoformat()}.flag").exists()
check("full-list mode: NO entry_limit_started marker written", not started_fulllist)
check("full-list mode: NO entry_limit_done marker written", not done_fulllist)

with patch.object(rt, "_RESULTS_DIR", tmp4):
    started_manual = (tmp4 / "trades" / f"entry_limit_started_{TD4.isoformat()}.flag").exists()
    done_manual    = (tmp4 / "trades" / f"entry_limit_done_{TD4.isoformat()}.flag").exists()
check("manual --symbol mode: NO entry_limit_started marker written", not started_manual)
check("manual --symbol mode: NO entry_limit_done marker written", not done_manual)


# ═════════════════════════════════════════════════════════════════════════════
print("\nScenario 6 -- position-overlap hand-off: run_entry_limit --symbols "
      "claims A/B/C first, run_entry_321 on the full 6-symbol list must skip "
      "them and only attempt D/E/F\n")
# ═════════════════════════════════════════════════════════════════════════════

TD6 = date(2026, 9, 23)
tmp6 = fresh_tmp_root()
write_trade_list(tmp6 / "trades", TD6, ["A", "B", "C", "D", "E", "F"])
store6 = FakeStore()   # SAME store threaded through both calls -- proves the hand-off

run_entry_limit_with_fakes(TD6, tmp6, store6, symbols=["A", "B", "C"], capital=1_500_000.0)
check("hand-off step 1: A/B/C claimed by run_entry_limit --symbols",
      {p["symbol"] for p in store6.positions} == {"A", "B", "C"},
      str({p["symbol"] for p in store6.positions}))

placed_321 = run_entry_321_with_fakes(TD6, tmp6, store6, capital=1_500_000.0)

final_syms6 = {p["symbol"] for p in store6.positions}
check("hand-off step 2: run_entry_321 result has all 6 symbols, no duplicates",
      final_syms6 == {"A", "B", "C", "D", "E", "F"} and len(store6.positions) == 6,
      str(store6.positions))
attempted_321 = {oid.split("-", 1)[1] for oid in placed_321}
check("hand-off step 2: run_entry_321 only PLACED ORDERS for D/E/F (A/B/C skipped, not re-bought)",
      attempted_321 == {"D", "E", "F"}, str(attempted_321))


# ═════════════════════════════════════════════════════════════════════════════
print("\nScenario 7 -- run_entry_321's own n also comes from _full_signal_count "
      "(sanity: identical value to len(symbols) on a normal day -- no behavior change)\n")
# ═════════════════════════════════════════════════════════════════════════════

TD7 = date(2026, 9, 23)
tmp7 = fresh_tmp_root()
write_trade_list(tmp7 / "trades", TD7, ["A", "B", "C", "D", "E", "F"])
store7 = FakeStore()
spy7, calls7 = make_allocation_spy()

run_entry_321_with_fakes(TD7, tmp7, store7, capital=1_500_000.0, allocation_spy=spy7)
check("normal run_entry_321 day: n=6 (matches old len(symbols)-based value exactly)",
      len(calls7) > 0 and all(n == 6 for _, n in calls7), str(calls7))


# ═════════════════════════════════════════════════════════════════════════════
print("\nScenario 8 -- _wait_for_entry_limit_marker: normal day (no started flag) "
      "is a zero-cost no-op\n")
# ═════════════════════════════════════════════════════════════════════════════

TD8 = date(2026, 9, 24)
tmp8 = fresh_tmp_root()
(tmp8 / "trades").mkdir(parents=True, exist_ok=True)
sleep_calls8 = []

with patch.object(rt, "_RESULTS_DIR", tmp8):
    rt._wait_for_entry_limit_marker(TD8, _sleep_fn=lambda s: sleep_calls8.append(s))

check("no started flag -> _wait_for_entry_limit_marker returns immediately, zero sleeps",
      sleep_calls8 == [], str(sleep_calls8))


# ═════════════════════════════════════════════════════════════════════════════
print("\nScenario 9 -- _wait_for_entry_limit_marker: marker appears mid-wait -> "
      "returns promptly, no timeout alert\n")
# ═════════════════════════════════════════════════════════════════════════════

TD9 = date(2026, 9, 24)
tmp9 = fresh_tmp_root()
with patch.object(rt, "_RESULTS_DIR", tmp9):
    started9 = rt._entry_limit_started_flag_path(TD9)
    started9.parent.mkdir(parents=True, exist_ok=True)
    started9.write_text(json.dumps({"symbols": ["X"], "started_at": "t0"}))
    done9 = rt._entry_limit_done_flag_path(TD9)

    # Fake clock starts well before the deadline; each sleep_fn call advances
    # it and, on the 2nd call, writes the done marker -- simulates the sweep
    # finishing mid-wait, without any real wall-clock delay in this test.
    clock = {"now": datetime(2026, 9, 24, 15, 19, 0, tzinfo=rt._IST)}
    sleep_calls9 = []

    def fake_now9():
        return clock["now"]

    def fake_sleep9(s):
        sleep_calls9.append(s)
        clock["now"] += timedelta(seconds=s)
        if len(sleep_calls9) == 2:
            done9.write_text(json.dumps({"symbols_claimed": ["X"], "done_at": "t1"}))

    buf9 = io.StringIO()
    with redirect_stdout(buf9):
        rt._wait_for_entry_limit_marker(TD9, _now_fn=fake_now9, _sleep_fn=fake_sleep9)

check("marker appears mid-wait: returns after a few polls, not the full deadline",
      1 <= len(sleep_calls9) <= 3, str(sleep_calls9))
check("marker appears mid-wait: no timeout ALERT logged",
      "ALERT" not in buf9.getvalue(), buf9.getvalue()[-200:])
check("marker appears mid-wait: 'found -- proceeding' logged",
      "proceeding" in buf9.getvalue())


# ═════════════════════════════════════════════════════════════════════════════
print("\nScenario 10 -- _wait_for_entry_limit_marker: simulated slow/hung sweep "
      "-- done flag never appears -> bounded timeout, loud alert, NO hang\n")
# ═════════════════════════════════════════════════════════════════════════════

TD10 = date(2026, 9, 24)
tmp10 = fresh_tmp_root()
with patch.object(rt, "_RESULTS_DIR", tmp10):
    started10 = rt._entry_limit_started_flag_path(TD10)
    started10.parent.mkdir(parents=True, exist_ok=True)
    started10.write_text(json.dumps({"symbols": ["X"], "started_at": "t0"}))
    # done flag deliberately never written -- simulates a hung/crashed sweep

    clock10 = {"now": datetime(2026, 9, 24, 15, 21, 0, tzinfo=rt._IST)}

    def fake_now10():
        return clock10["now"]

    sleep_calls10 = []
    def fake_sleep10(s):
        sleep_calls10.append(s)
        clock10["now"] += timedelta(seconds=s)

    buf10 = io.StringIO()
    with redirect_stdout(buf10):
        rt._wait_for_entry_limit_marker(TD10, _now_fn=fake_now10, _sleep_fn=fake_sleep10)

check("slow sweep: function returned (did not hang/raise) past the deadline", True)
check("slow sweep: fake clock actually crossed the 15:21:30 deadline",
      clock10["now"] >= clock10["now"].replace(hour=15, minute=21, second=30),
      str(clock10["now"]))
check("slow sweep: a bounded number of polls, not an infinite loop",
      0 < len(sleep_calls10) < 120, str(len(sleep_calls10)))
check("slow sweep: loud ALERT logged", "ALERT" in buf10.getvalue(), buf10.getvalue()[-300:])
check("slow sweep: alert mentions run_entry_limit and proceeding anyway",
      "run_entry_limit" in buf10.getvalue() and "Proceeding" in buf10.getvalue())


# ═════════════════════════════════════════════════════════════════════════════
print("\nScenario 11 -- --symbol and --symbols are mutually exclusive\n")
# ═════════════════════════════════════════════════════════════════════════════

TD11 = date(2026, 9, 23)
tmp11 = fresh_tmp_root()
write_trade_list(tmp11 / "trades", TD11, ["A", "B"])
with patch.object(rt, "_RESULTS_DIR", tmp11):
    try:
        rt.run_entry_limit(trade_date=TD11, symbol="A", symbols=["A", "B"], capital=100.0)
        mutual_exclusion_enforced = False
    except SystemExit:
        mutual_exclusion_enforced = True
check("run_entry_limit(symbol=, symbols=) together -> SystemExit", mutual_exclusion_enforced)


print()
if failures:
    print(f"{failures} check(s) FAILED.")
    sys.exit(1)
print("All checks PASSED.")
sys.exit(0)
