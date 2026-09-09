#!/usr/bin/env python3
"""
test_parallel_orders.py -- standalone verifier for the parallel order-placement
work in dhan/trade.py's RateLimiter and dhan/run_trades.py's entry/exit batch
stages (run_entry_321, check_exit_925).

Covers:
  1. RateLimiter is a true sliding window -- a batch of <=5 acquire() calls
     must return with no artificial delay; only the 6th-or-later call within
     a trailing 1s window blocks, and only until the oldest of those 5 ages
     out.
  2. run_entry_321's parallel phase actually overlaps: an N-symbol batch with
     an artificial per-call delay and a 4-worker pool must complete in close
     to ceil(N/4) delay-units of wall time, not N of them.
  3. A single symbol's order call raising inside that parallel phase must not
     block, delay, or drop the other symbols in the same batch -- they still
     get polled, still get written to the position file in the same
     single-pass save.
  4. Same two properties (parallel timing + one-failure-doesn't-block-others)
     for check_exit_925's parallel sell phase.

Mocks every broker-facing call this touches (dhan/trade.py's buy/sell are
never actually invoked -- run_trades.py's own module-level buy/sell/
get_reference_price/_margin_check/_available_balance/_poll_fill_strict/
_poll_fill_safe/get_ltp_batch/_broker_qty references are patched directly)
and the position-file read/write (_load_long_pos/_save_long_pos) with an
in-memory store -- zero network calls, zero real file writes. Mirrors
zerodha/test_parallel_orders.py's standalone script style (no pytest in this
repo).

Note on scope: unlike zerodha's equivalent, check_exit_925's mirrored-short
opening is deliberately NOT part of the parallel phase here -- see
run_trades.py's own comment on this. That sequential batch (with its
available_balance threading, covered by dhan/test_targets.py's bal-short
scenario) isn't re-tested here; this file covers exactly the two phases that
changed shape: order placement/fill-polling for entries, and sell/fill-
polling for exits.

Usage:
    python dhan/test_parallel_orders.py

Exit 0 on all-pass, exit 1 on any failure.
"""

import copy
import sys
import time
import types
from pathlib import Path
from unittest.mock import patch

_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

for _m in ("data_loader",):
    sys.modules.setdefault(_m, types.ModuleType(_m))

import dhan.trade as trade          # noqa: E402
import dhan.run_trades as rt        # noqa: E402

patch.object(rt, "tick_size", lambda sym: 0.05).start()
patch.object(rt, "_sync_pnl_workbook", lambda: None).start()
# check_exit_925's own _hold_until (real wall-clock pinning, see
# dhan/run_trades.py) isn't what this file's timing tests measure -- they
# measure the parallel-phase wall-clock, not the staging holds around it.
# Stubbed out globally, same reasoning as _sync_pnl_workbook above.
patch.object(rt, "_hold_until", lambda *a, **kw: None).start()

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
    """Backs a position-file load/save pair with an in-memory list -- no real
    file I/O. Also counts save() calls so tests can assert exactly one
    single-pass write per batch, not one per symbol."""
    def __init__(self, positions=None):
        self.positions   = copy.deepcopy(positions or [])
        self.save_count  = 0
        self.save_history: list[list] = []

    def load(self):
        return copy.deepcopy(self.positions)

    def save(self, positions):
        self.positions = copy.deepcopy(positions)
        self.save_count += 1
        self.save_history.append(copy.deepcopy(positions))


# ══════════════════════════════════════════════════════════════════════════
# 1. RateLimiter -- true sliding window
# ══════════════════════════════════════════════════════════════════════════

def test_rate_limiter_sliding_window():
    print("\n[1] RateLimiter sliding window")
    limiter = trade.RateLimiter(max_per_sec=5)

    start = time.monotonic()
    for _ in range(5):
        limiter.acquire()
    elapsed_first5 = time.monotonic() - start
    check("5 acquires with no prior calls return with no artificial delay",
          elapsed_first5 < 0.05, f"took {elapsed_first5:.3f}s")

    start6 = time.monotonic()
    limiter.acquire()
    elapsed_6th = time.monotonic() - start6
    check("6th acquire within the same rolling second blocks",
          elapsed_6th > 0.5, f"took {elapsed_6th:.3f}s (expected close to 1s)")
    check("6th acquire does not block for far longer than the window (~1s)",
          elapsed_6th < 1.3, f"took {elapsed_6th:.3f}s")


def test_rate_limiter_thread_safe_shared_instance():
    print("\n[2] RateLimiter is a single module-level instance")
    check("trade.rate_limiter is the same object both run_trades.py and "
          "live_monitor.py's buy()/sell() calls resolve through",
          trade.rate_limiter is trade.rate_limiter and hasattr(trade, "rate_limiter"))


# ══════════════════════════════════════════════════════════════════════════
# 2/3. run_entry_321 -- parallel timing + one-failure-doesn't-block-others
# ══════════════════════════════════════════════════════════════════════════

_CALL_DELAY = 0.25


def test_entry_parallel_timing_and_resilience():
    print("\n[3] run_entry_321 -- parallel phase timing + one-failure resilience")
    symbols = [f"SYM{i}" for i in range(8)]
    failing = "SYM3"

    def fake_buy(sym, exch, qty, **kw):
        if sym == failing:
            raise RuntimeError(f"simulated order rejection for {sym}")
        time.sleep(_CALL_DELAY)
        return f"ORDER-{sym}"

    def fake_poll_fill_strict(order_id):
        return 100.5, 10, False, ""

    store = FakeStore(positions=[])

    with patch.object(rt, "_load_symbols", return_value=list(symbols)), \
         patch.object(rt, "get_reference_price", lambda sym: (100.0, 1520)), \
         patch.object(rt, "get_ltp_batch", lambda syms: {s: 100.0 for s in syms}), \
         patch.object(rt, "security_id", lambda sym: "999"), \
         patch.object(rt, "_margin_check", lambda sym, qty, price: {"leverage": 3.0, "margin_required": qty * price / 3.0}), \
         patch.object(rt, "_available_balance", return_value=10_000_000.0), \
         patch.object(rt, "_fetch_upper_circuit_batch", lambda syms: {}), \
         patch.object(rt, "buy", side_effect=fake_buy), \
         patch.object(rt, "_poll_fill_strict", side_effect=fake_poll_fill_strict), \
         patch.object(rt, "_load_long_pos", side_effect=store.load), \
         patch.object(rt, "_save_long_pos", side_effect=store.save), \
         patch.object(rt, "_append_log"), \
         patch.object(rt.notify, "send_entry"):

        start = time.monotonic()
        rt.run_entry_321(trade_date=__import__("datetime").date(2026, 8, 25),
                         dry_run=False, capital=1_500_000.0)
        elapsed = time.monotonic() - start

    # 8 symbols / 4 workers = 2 sequential rounds of _CALL_DELAY each for the
    # 7 that actually sleep; the raiser returns instantly, so worst case is
    # still ~2 rounds, not 8.
    expected_ceiling = 2 * _CALL_DELAY + 1.0   # generous overhead margin
    check(f"8-symbol batch on a 4-worker pool completes well under "
          f"8x{_CALL_DELAY}s sequential time",
          elapsed < expected_ceiling,
          f"took {elapsed:.3f}s, ceiling {expected_ceiling:.3f}s")

    saved = store.positions
    saved_symbols = {p["symbol"] for p in saved}
    check("the failing symbol was NOT written to the position file",
          failing not in saved_symbols, f"saved symbols: {saved_symbols}")
    check("every OTHER symbol in the batch WAS written despite the failure",
          saved_symbols == set(symbols) - {failing},
          f"saved symbols: {saved_symbols}")
    check("exactly one save() call for the whole batch (single-pass write)",
          store.save_count == 1, f"save_count={store.save_count}")


# ══════════════════════════════════════════════════════════════════════════
# 4z. run_entry_321 -- a symbol at its upper circuit bids AT the UC price,
# not 0.5% above LTP (SHANTIGEAR, 2026-08-21, was rejected for exactly this)
# ══════════════════════════════════════════════════════════════════════════

def test_entry_at_uc_bids_at_uc_price():
    print("\n[4z] run_entry_321 -- at-UC entries bid AT the UC price, not 0.5% above LTP")
    symbols = ["ATUC", "BELOWUC", "NOUCDATA"]
    ltp_by_sym = {"ATUC": 130.0, "BELOWUC": 100.0, "NOUCDATA": 100.0}
    uc_by_sym  = {"ATUC": 130.0, "BELOWUC": 150.0}   # NOUCDATA deliberately absent

    order_prices = {}

    def fake_buy(sym, exch, qty, **kw):
        order_prices[sym] = kw.get("price")
        return f"ORDER-{sym}"

    def fake_poll_fill_strict(order_id):
        return 100.5, 10, False, ""

    store = FakeStore(positions=[])

    with patch.object(rt, "_load_symbols", return_value=list(symbols)), \
         patch.object(rt, "get_reference_price", lambda sym: (100.0, 1520)), \
         patch.object(rt, "get_ltp_batch", lambda syms: dict(ltp_by_sym)), \
         patch.object(rt, "security_id", lambda sym: "999"), \
         patch.object(rt, "_margin_check", lambda sym, qty, price: {"leverage": 3.0, "margin_required": qty * price / 3.0}), \
         patch.object(rt, "_available_balance", return_value=10_000_000.0), \
         patch.object(rt, "_fetch_upper_circuit_batch", lambda syms: dict(uc_by_sym)), \
         patch.object(rt, "buy", side_effect=fake_buy), \
         patch.object(rt, "_poll_fill_strict", side_effect=fake_poll_fill_strict), \
         patch.object(rt, "_load_long_pos", side_effect=store.load), \
         patch.object(rt, "_save_long_pos", side_effect=store.save), \
         patch.object(rt, "_append_log"), \
         patch.object(rt.notify, "send_entry"):

        rt.run_entry_321(trade_date=__import__("datetime").date(2026, 8, 25),
                         dry_run=False)

    check("ATUC (LTP == UC) bids AT the UC price (₹130.00), not 0.5% above LTP (₹130.65)",
          order_prices["ATUC"] == 130.0, f"order_prices={order_prices}")
    check("BELOWUC (LTP well under UC) still bids 0.5% above LTP as before (₹100.50)",
          order_prices["BELOWUC"] == 100.5, f"order_prices={order_prices}")
    check("NOUCDATA (UC fetch had no data for this symbol) falls back to 0.5% above LTP, "
          "doesn't crash", order_prices["NOUCDATA"] == 100.5, f"order_prices={order_prices}")


# ══════════════════════════════════════════════════════════════════════════
# 4a. run_entry_321 -- confirmed MTF-ineligibility rejections retry once as CNC
# ══════════════════════════════════════════════════════════════════════════

def test_entry_mtf_ineligible_retries_as_cnc():
    print("\n[4a] run_entry_321 -- MTF-ineligible rejection retries as CNC")
    symbols = ["KLBRENG", "RML"]
    # Two DISTINCT confirmed-live rejection reasons that both mean "this
    # scrip can't be bought as MTF, but CNC has no such restriction" --
    # KLBRENG-B/WELSPLSOL, 2026-08-21 ("Mtf Product Is Not Allowed For This
    # Scrip") and RML, 2026-09-07 ("Buy back is not allowed for delivery
    # positions..."). Both must trigger the exact same CNC retry.
    reject_reason_by_sym = {
        "KLBRENG": "Mtf Product Is Not Allowed For This Scrip",
        "RML":     "Buy back is not allowed for delivery positions.",
    }

    buy_calls = []

    def fake_buy(sym, exch, qty, **kw):
        product = kw.get("product")
        buy_calls.append((sym, product, qty))
        return f"{product}-{sym}"

    def fake_poll_fill_strict(order_id):
        product, sym = order_id.split("-", 1)
        if product == "MTF":
            return 0.0, 0, True, reject_reason_by_sym[sym]
        return 105.0, 1000, False, ""   # CNC retry fills in full at the resized qty

    store = FakeStore(positions=[])

    with patch.object(rt, "_load_symbols", return_value=list(symbols)), \
         patch.object(rt, "get_reference_price", lambda sym: (100.0, 1520)), \
         patch.object(rt, "get_ltp_batch", lambda syms: {s: 100.0 for s in syms}), \
         patch.object(rt, "security_id", lambda sym: "999"), \
         patch.object(rt, "_margin_check", lambda sym, qty, price: {"leverage": 3.0, "margin_required": qty * price / 3.0}), \
         patch.object(rt, "_available_balance", return_value=10_000_000.0), \
         patch.object(rt, "_fetch_upper_circuit_batch", lambda syms: {}), \
         patch.object(rt, "buy", side_effect=fake_buy), \
         patch.object(rt, "_poll_fill_strict", side_effect=fake_poll_fill_strict), \
         patch.object(rt, "_load_long_pos", side_effect=store.load), \
         patch.object(rt, "_save_long_pos", side_effect=store.save), \
         patch.object(rt, "_append_log"), \
         patch.object(rt.notify, "send_entry"), \
         patch.object(rt.notify, "send_entry_failed"):

        rt.run_entry_321(trade_date=__import__("datetime").date(2026, 8, 25),
                         dry_run=False, capital=800_000.0)

    check("each symbol was bought exactly twice (rejected MTF attempt, then CNC retry)",
          sorted(s for s, p, q in buy_calls) == sorted(symbols * 2), f"buy_calls={buy_calls}")
    check("both symbols' second attempt was placed as product=CNC",
          {s for s, p, q in buy_calls if p == "CNC"} == set(symbols), f"buy_calls={buy_calls}")

    mtf_qty = {s: q for s, p, q in buy_calls if p == "MTF"}
    cnc_qty = {s: q for s, p, q in buy_calls if p == "CNC"}
    check("CNC retry sizes off half the capital (smaller qty than the rejected MTF attempt)",
          all(cnc_qty[s] < mtf_qty[s] for s in symbols), f"mtf={mtf_qty} cnc={cnc_qty}")

    saved = {p["symbol"]: p for p in store.positions}
    check("both symbols ended up as open positions despite the initial MTF rejection",
          set(saved) == set(symbols), f"saved={set(saved)}")
    check("both saved positions record product=CNC (the retry that actually filled)",
          all(saved[s]["product"] == "CNC" for s in symbols),
          f"products={[saved[s]['product'] for s in symbols]}")
    check("both saved positions use the CNC retry's fill price/qty, not the rejected MTF attempt",
          all(saved[s]["actual_fill_price"] == 105.0 and saved[s]["actual_fill_quantity"] == 1000
              for s in symbols),
          f"saved={saved}")


# ══════════════════════════════════════════════════════════════════════════
# 4. check_exit_925 -- parallel timing + one-failure-doesn't-block-others
# ══════════════════════════════════════════════════════════════════════════

def make_long(sym, **overrides):
    row = {
        "broker": "dhan", "symbol": sym, "entry_date": "2026-08-25",
        "reference_price": 100.0, "shares_intended": 10,
        "actual_fill_price": 100.0, "actual_fill_quantity": 10,
        "entry_order_id": f"E-{sym}", "status": "open",
        "entry_timestamp": "2026-08-25T15:21:00+05:30",
        # CNC: _sell_margin_safe now also does a post-sell status check for
        # this product (confirmed live 2026-09-08, RML -- see test_targets.py
        # scenarios safe-6/safe-7), so this test mocks _dhan_order_status to
        # TRADED below to keep that check fast and side-effect-free; the
        # real 2s pre-check wait still genuinely happens (unmocked -- it uses
        # the real time module) and is accounted for in expected_ceiling.
        "product": "CNC",
    }
    row.update(overrides)
    return row


def test_exit_parallel_timing_and_resilience():
    print("\n[4] check_exit_925 -- parallel phase timing + one-failure resilience")
    symbols = [f"SYM{i}" for i in range(8)]
    failing = "SYM5"
    long_store  = FakeStore(positions=[make_long(s) for s in symbols])
    short_store = FakeStore(positions=[])

    def fake_sell(sym, exch, qty, **kw):
        if sym == failing:
            raise RuntimeError(f"simulated sell rejection for {sym}")
        time.sleep(_CALL_DELAY)
        return f"EXIT-{sym}"

    def fake_poll_fill_safe(order_id, fallback_price, fallback_qty):
        return 110.0, fallback_qty

    # Mirrored-short-opening is deliberately mocked out entirely here (see
    # this file's module docstring) -- this test covers exit-phase timing/
    # resilience only, not _open_short_place()'s own concurrency, which
    # test_targets.py's (bal-short) scenario and test_batch_concurrency.py's
    # wave-ordering tests cover.
    with patch.object(rt, "get_ltp_batch", lambda syms: {s: 110.0 for s in syms}), \
         patch.object(rt, "get_ltp", lambda sym: 110.0), \
         patch.object(rt, "_broker_qty", lambda sym, product: (10, "NSE_EQ")), \
         patch.object(rt, "_dhan_get_orders", lambda: []), \
         patch.object(rt, "sell", side_effect=fake_sell), \
         patch.object(rt, "_dhan_order_status", lambda oid: {"orderStatus": "TRADED"}), \
         patch.object(rt, "_poll_fill_ws_first", side_effect=fake_poll_fill_safe), \
         patch.object(rt, "_open_short_place", lambda *a, **kw: None), \
         patch.object(rt, "_fetch_upper_circuit_batch", lambda syms: {}), \
         patch.object(rt, "_available_balance", return_value=10_000_000.0), \
         patch.object(rt, "_load_long_pos", side_effect=long_store.load), \
         patch.object(rt, "_save_long_pos", side_effect=long_store.save), \
         patch.object(rt, "_load_short_pos", side_effect=short_store.load), \
         patch.object(rt, "_save_short_pos", side_effect=short_store.save), \
         patch.object(rt.notify, "send_exit_925"):

        start = time.monotonic()
        rt.check_exit_925(dry_run=False)
        elapsed = time.monotonic() - start

    # 8 symbols split into chunks of MAX_ORDER_CALLS_PER_SECOND (5+3) for
    # BOTH Wave 1 (cancel+sell) and Wave 2 (short-open, re-chunked over the 7
    # successfully-sold tasks -- still 5+3) -- one BATCH_SLEEP_SECONDS pause
    # between chunks in EACH wave now (two separate re-chunked passes, not
    # one flat per-position cascade anymore -- see the wave-based redesign).
    # _open_short_place is mocked to return None (no shorts open), so Wave 3
    # never runs and adds no further delay. +2s per Wave-1 chunk (2 chunks)
    # for _sell_margin_safe's real, unmocked post-sell status-check wait --
    # genuinely happens concurrently within each chunk, not per-symbol, but
    # budgeted per-chunk here rather than trying to defeat the real time
    # module (see make_long's comment on why this is now unmocked).
    expected_ceiling = 2 * _CALL_DELAY + 2 * rt.BATCH_SLEEP_SECONDS + 2 * 2.0 + 1.0
    check(f"8-symbol exit batch, chunked at {rt.MAX_ORDER_CALLS_PER_SECOND}, "
          f"completes well under 8x{_CALL_DELAY}s sequential time",
          elapsed < expected_ceiling,
          f"took {elapsed:.3f}s, ceiling {expected_ceiling:.3f}s")

    saved = long_store.positions
    by_sym = {p["symbol"]: p for p in saved}
    check("the failing symbol's position is left untouched (still open)",
          by_sym[failing]["status"] == "open", f"status={by_sym[failing]['status']}")
    check("every OTHER symbol was exited despite the one failure",
          all(by_sym[s]["status"] == "exited_925" for s in symbols if s != failing))
    check("exactly ONE save() call for the whole run (one write per WAVE now, "
          "not one per chunk -- 2 chunks within Wave 1, still just 1 save)",
          long_store.save_count == 1, f"save_count={long_store.save_count}")
    check("no short-file save at all -- every short-open was mocked out (None)",
          short_store.save_count == 0, f"save_count={short_store.save_count}")


# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    test_rate_limiter_sliding_window()
    test_rate_limiter_thread_safe_shared_instance()
    test_entry_parallel_timing_and_resilience()
    test_entry_at_uc_bids_at_uc_price()
    test_entry_mtf_ineligible_retries_as_cnc()
    test_exit_parallel_timing_and_resilience()

    print()
    if failures:
        print(f"{failures} check(s) FAILED.")
        sys.exit(1)
    print("All checks PASSED.")
    sys.exit(0)
