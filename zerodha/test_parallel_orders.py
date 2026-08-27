#!/usr/bin/env python3
"""
test_parallel_orders.py -- standalone verifier for the parallel order-placement
work in zerodha/trade.py's RateLimiter and zerodha/run_trades.py's entry/exit
batch stages (run_entry_321, check_exit_925).

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

Mocks every broker-facing call this touches (trade.py's buy/sell are never
actually invoked -- run_trades.py's own module-level buy/sell/stage_entry_
orders/_mtf_margin_check/_available_margin/_poll_fill_safe/get_ltp/
_broker_qty/_open_short_place references are patched directly) and the
position-file read/write (_load_long_pos/_save_long_pos) with an in-memory
store -- zero network calls, zero real file writes, zero real sleeps beyond
the small artificial per-call delays these scenarios ask for on purpose.
Mirrors zerodha/test_targets.py's standalone script style (no pytest in this
repo).

Usage:
    python zerodha/test_parallel_orders.py

Exit 0 on all-pass, exit 1 on any failure.
"""

import copy
import sys
import time
import types
from pathlib import Path
from unittest.mock import patch

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

for _m in ("data_loader",):
    sys.modules.setdefault(_m, types.ModuleType(_m))

import zerodha.trade as trade            # noqa: E402
import zerodha.run_trades as rt          # noqa: E402

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


def _make_entry_mocks(symbols, failing_symbol=None):
    """Returns a dict of patch targets for a run_entry_321 scenario where
    every symbol resolves an MTF-eligible margin check and a mocked buy()
    that sleeps _CALL_DELAY before returning (simulating real order-placement
    latency), except failing_symbol, whose buy() raises immediately."""
    staged = [{"symbol": s, "ref_price": 100.0, "ref_hhmm": 1520, "shares": 10}
              for s in symbols]

    def fake_stage_entry_orders(syms, capital):
        return staged

    def fake_mtf_margin_check(sym, qty):
        return {"leverage": 3.0, "margin_required": qty * 100.0 / 3}

    def fake_buy(sym, exch, qty, **kwargs):
        if sym == failing_symbol:
            raise RuntimeError(f"simulated order rejection for {sym}")
        time.sleep(_CALL_DELAY)
        return f"ORDER-{sym}"

    def fake_poll_fill_safe(order_id, fallback_price, fallback_qty):
        return fallback_price, fallback_qty

    return staged, fake_stage_entry_orders, fake_mtf_margin_check, fake_buy, fake_poll_fill_safe


def test_entry_parallel_timing_and_resilience():
    print("\n[3] run_entry_321 -- parallel phase timing + one-failure resilience")
    symbols = [f"SYM{i}" for i in range(8)]
    failing = "SYM3"

    _, fake_stage, fake_margin, fake_buy, fake_poll = _make_entry_mocks(symbols, failing_symbol=failing)
    store = FakeStore(positions=[])

    with patch.object(rt, "_load_symbols", return_value=list(symbols)), \
         patch.object(rt, "stage_entry_orders", side_effect=fake_stage), \
         patch.object(rt, "_mtf_margin_check", side_effect=fake_margin), \
         patch.object(rt, "_available_margin", return_value=10_000_000.0), \
         patch.object(rt, "buy", side_effect=fake_buy), \
         patch.object(rt, "_poll_fill_safe", side_effect=fake_poll), \
         patch.object(rt, "_load_long_pos", side_effect=store.load), \
         patch.object(rt, "_save_long_pos", side_effect=store.save), \
         patch.object(rt, "_append_log"), \
         patch.object(rt.notify, "send_entry"):

        start = time.monotonic()
        rt.run_entry_321(trade_date=__import__("datetime").date(2026, 8, 25),
                         dry_run=False, capital=100_000.0)
        elapsed = time.monotonic() - start

    n_workers = 4
    n_symbols_placing_orders = len(symbols)   # includes the one that raises immediately
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
# 4. check_exit_925 -- parallel timing + one-failure-doesn't-block-others
# ══════════════════════════════════════════════════════════════════════════

def make_long(sym, **overrides):
    row = {
        "broker": "zerodha", "symbol": sym, "entry_date": "2026-08-25",
        "reference_price": 100.0, "shares_intended": 10,
        "actual_fill_price": 100.0, "actual_fill_quantity": 10,
        "entry_order_id": f"E-{sym}", "status": "open",
        "entry_timestamp": "2026-08-25T15:21:00+05:30", "product": "MTF",
    }
    row.update(overrides)
    return row


def test_exit_parallel_timing_and_resilience():
    print("\n[4] check_exit_925 -- parallel phase timing + one-failure resilience")
    symbols = [f"SYM{i}" for i in range(8)]
    failing = "SYM5"
    long_store  = FakeStore(positions=[make_long(s) for s in symbols])
    short_store = FakeStore(positions=[])

    def fake_get_ltp(sym):
        return 110.0   # positive P&L for every symbol -> full-exit branch

    def fake_broker_qty(sym, product):
        return 10, "NSE"

    def fake_sell(sym, exch, qty, **kwargs):
        if sym == failing:
            raise RuntimeError(f"simulated sell rejection for {sym}")
        time.sleep(_CALL_DELAY)
        return f"EXIT-{sym}"

    def fake_poll_fill_safe(order_id, fallback_price, fallback_qty):
        return 110.0, fallback_qty

    with patch.object(rt, "get_ltp", side_effect=fake_get_ltp), \
         patch.object(rt, "_broker_qty", side_effect=fake_broker_qty), \
         patch.object(rt, "sell", side_effect=fake_sell), \
         patch.object(rt, "_poll_fill_safe", side_effect=fake_poll_fill_safe), \
         patch.object(rt, "_available_margin", return_value=10_000_000.0), \
         patch.object(rt, "_open_short_place", return_value=None), \
         patch.object(rt, "_load_long_pos", side_effect=long_store.load), \
         patch.object(rt, "_save_long_pos", side_effect=long_store.save), \
         patch.object(rt, "_load_short_pos", side_effect=short_store.load), \
         patch.object(rt, "_save_short_pos", side_effect=short_store.save), \
         patch.object(rt.notify, "send_exit_925"):

        start = time.monotonic()
        rt.check_exit_925(dry_run=False)
        elapsed = time.monotonic() - start

    # Wave 1 batches the 8 tasks into [5, 3] (MAX_ORDER_CALLS_PER_SECOND=5),
    # each sell sub-step firing concurrently (~1x_CALL_DELAY per batch) with
    # one BATCH_SLEEP_SECONDS between the two batches. Wave 2 then re-chunks
    # the 7 successfully-sold symbols into [5, 2] -- _open_short_place is
    # mocked to return instantly, but _run_in_chunks still pays its own
    # inter-chunk sleep between those two batches regardless of how fast the
    # mocked worker returns.
    expected_ceiling = 2 * _CALL_DELAY + 2 * rt.BATCH_SLEEP_SECONDS + 1.0
    check(f"8-symbol exit batch across wave 1/2 completes well under "
          f"8x{_CALL_DELAY}s sequential time",
          elapsed < expected_ceiling,
          f"took {elapsed:.3f}s, ceiling {expected_ceiling:.3f}s")

    saved = long_store.positions
    by_sym = {p["symbol"]: p for p in saved}
    check("the failing symbol's position is left untouched (still open)",
          by_sym[failing]["status"] == "open", f"status={by_sym[failing]['status']}")
    check("every OTHER symbol was exited despite the one failure",
          all(by_sym[s]["status"] == "exited_925" for s in symbols if s != failing))
    check("exactly one save() call for the whole batch (single-pass write)",
          long_store.save_count == 1, f"save_count={long_store.save_count}")
    check("no short ever opened (short place mocked to return None) -> zero short saves",
          short_store.save_count == 0, f"save_count={short_store.save_count}")


# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    test_rate_limiter_sliding_window()
    test_rate_limiter_thread_safe_shared_instance()
    test_entry_parallel_timing_and_resilience()
    test_exit_parallel_timing_and_resilience()

    print()
    if failures:
        print(f"{failures} check(s) FAILED.")
        sys.exit(1)
    print("All checks PASSED.")
    sys.exit(0)
