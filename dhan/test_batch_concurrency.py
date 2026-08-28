#!/usr/bin/env python3
"""
test_batch_concurrency.py -- standalone verifier for the WAVE-BASED
concurrency redesign (2026-08-27) of check_exit_925/force_exit_1159/
square_off_239: every position in a batch now performs the SAME action
before any position moves to the next action (cancel-all, then sell-all;
short-open-all; target/SL-all for 925/1159 -- cancel-all, then
force-cover-all for 239), so a concurrent Order-API burst is always
homogeneous by call type. Covers batching primitives (_run_batch,
_run_in_chunks, _run_exit_wave1), quote-call batching, the peak Order-API
concurrency invariant, thread-safe capital allocation (_BalanceTracker),
write-safety (one _save_long_pos/_save_short_pos per WAVE for 925/1159, one
per BATCH for 239 -- never from inside a worker thread), per-position
exception isolation, cross-wave ordering, and square_off_239's single
Order-Book pre-check (replacing per-order status calls).

Decision logic itself (target-hit fork, no-data fallback, OCO branches) is
NOT retested here -- that's dhan/test_targets.py's job, unchanged and still
passing. This file covers exactly the concurrency mechanics layered on top.

Mocks every broker-facing call (buy/sell/order_status/get_orders/
cancel_order/_broker_qty/_broker_short_qty/_available_balance/
_intraday_margin_check/get_ltp_batch/_fetch_upper_circuit_batch/
_poll_fill_safe) and the position-file read/write with in-memory stores --
zero network calls, zero real file writes.

Usage:
    python dhan/test_batch_concurrency.py

Exit 0 on all-pass, exit 1 on any failure.
"""

import copy
import sys
import threading
from contextlib import ExitStack
import time
import types
from pathlib import Path
from unittest.mock import patch

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

for _m in ("data_loader",):
    sys.modules.setdefault(_m, types.ModuleType(_m))

import dhan.run_trades as rt   # noqa: E402

patch.object(rt, "tick_size", lambda sym: 0.05).start()
patch.object(rt, "_sync_pnl_workbook", lambda: None).start()
# NOTE: rt.time IS the process-wide `time` module (import time returns the
# same singleton everywhere) -- patching rt.time.sleep globally would ALSO
# silence every time.sleep() call in THIS file, including the deliberate
# concurrency-measurement holds a couple of tests below rely on (e.g. test 3's
# "hold the in-flight window" pattern). So sleep is mocked LOCALLY, per test,
# only in tests that don't need a real wall-clock window of their own.

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
    def __init__(self, positions=None):
        self.positions    = copy.deepcopy(positions or [])
        self.save_count   = 0
        self.save_history: list[list] = []

    def load(self):
        return copy.deepcopy(self.positions)

    def save(self, positions):
        self.positions = copy.deepcopy(positions)
        self.save_count += 1
        self.save_history.append(copy.deepcopy(positions))


def make_long(sym, **overrides):
    row = {
        "broker": "dhan", "symbol": sym, "entry_date": "2026-08-26",
        "reference_price": 100.0, "shares_intended": 10,
        "actual_fill_price": 100.0, "actual_fill_quantity": 10,
        "entry_order_id": f"E-{sym}", "status": "open",
        "entry_timestamp": "2026-08-26T15:21:00+05:30",
        "product": "CNC",
    }
    row.update(overrides)
    return row


def make_short(sym, **overrides):
    row = {
        "broker": "dhan", "symbol": sym, "direction": "short",
        "product": "INTRADAY", "source_exit_stage": "925",
        "entry_date": "2026-08-26", "entry_price": 100.0, "quantity": 10,
        "entry_order_id": f"S-{sym}", "status": "short_open",
        "cover_target_order_id": f"COVER-{sym}", "cover_target_price": 95.0,
        "stop_order_id": f"STOP-{sym}", "stop_trigger_price": 130.0,
        "entry_timestamp": "2026-08-26T09:25:00+05:30",
    }
    row.update(overrides)
    return row


class TaggedEvents:
    """Thread-safe, monotonically-indexed event log -- used by the
    wave-ordering tests to prove Wave N's events all precede Wave N+1's,
    without relying on wall-clock timing (which is inherently flaky under
    real thread scheduling)."""
    def __init__(self):
        self._lock   = threading.Lock()
        self._events: list[tuple[int, str, str]] = []
        self._next   = 0

    def add(self, kind: str, sym: str) -> None:
        with self._lock:
            self._events.append((self._next, kind, sym))
            self._next += 1

    def indices(self, kind: str) -> list[int]:
        return [i for i, k, _ in self._events if k == kind]

    def __repr__(self):
        return repr(self._events)


# ══════════════════════════════════════════════════════════════════════════
# 1. _run_in_chunks -- chunk boundaries, worker coverage, inter-chunk sleep
# ══════════════════════════════════════════════════════════════════════════

def test_run_in_chunks_boundaries():
    print("\n[1] _run_in_chunks -- chunk boundaries for N=4,5,6,12 "
          f"(chunk_size={rt.MAX_ORDER_CALLS_PER_SECOND})")

    for n in (4, 5, 6, 12):
        items = list(range(n))
        seen: list[int] = []
        lock = threading.Lock()

        def worker(item):
            with lock:
                seen.append(item)
            return item * 10

        sleep_calls = []
        with patch.object(rt.time, "sleep", lambda s: sleep_calls.append(s)):
            chunks = list(rt._run_in_chunks(items, worker))

        sizes = [len(c) for c in chunks]
        expected_sizes = []
        remaining = n
        while remaining > 0:
            expected_sizes.append(min(remaining, rt.MAX_ORDER_CALLS_PER_SECOND))
            remaining -= expected_sizes[-1]

        check(f"N={n}: chunk sizes {sizes} match expected {expected_sizes}",
              sizes == expected_sizes, f"sizes={sizes}")
        check(f"N={n}: every item processed exactly once",
              sorted(seen) == sorted(items), f"seen={seen}")
        check(f"N={n}: results preserved (worker return values intact per chunk)",
              [v for c in chunks for v in c] == [i * 10 for i in items] or
              sorted(v for c in chunks for v in c) == sorted(i * 10 for i in items))
        expected_sleeps = len(expected_sizes) - 1
        check(f"N={n}: slept between chunks {expected_sleeps} time(s), "
              f"not after the last chunk", len(sleep_calls) == expected_sleeps,
              f"sleep_calls={sleep_calls}")
        if sleep_calls:
            check(f"N={n}: slept BATCH_SLEEP_SECONDS ({rt.BATCH_SLEEP_SECONDS}s) each time",
                  all(s == rt.BATCH_SLEEP_SECONDS for s in sleep_calls))


# ══════════════════════════════════════════════════════════════════════════
# 1b. _run_batch -- the single-burst primitive both _run_in_chunks and
#     _run_exit_wave1 are built from
# ══════════════════════════════════════════════════════════════════════════

def test_run_batch_primitive():
    print("\n[1b] _run_batch -- single concurrent burst, waits for all, "
          "empty input is a true no-op")

    check("empty list returns [] without spinning up a ThreadPoolExecutor",
          rt._run_batch([], lambda x: 1 / 0) == [])

    # A threading.Barrier forces genuine, deterministic simultaneous overlap
    # (all 5 workers must rendezvous before any proceeds) -- immune to any
    # time.sleep patching, unlike a sleep-based hold.
    n_items = 5
    barrier = threading.Barrier(n_items)

    def worker(item):
        barrier.wait(timeout=5)
        return item * 2

    results = rt._run_batch(list(range(1, n_items + 1)), worker)
    check(f"all {n_items} items genuinely ran concurrently (every worker reached "
          f"the barrier -- if _run_batch ran them one at a time, this would "
          f"time out and raise instead)",
          sorted(results) == [2, 4, 6, 8, 10], str(results))


# ══════════════════════════════════════════════════════════════════════════
# 1c. _run_exit_wave1 -- cancel-then-sell, two separate bursts per batch,
#     never overlapping with each other
# ══════════════════════════════════════════════════════════════════════════

def test_run_exit_wave1_cancel_before_sell():
    print("\n[1c] _run_exit_wave1 -- within a batch, EVERY cancel completes "
          "before ANY sell starts (2026-08-28: cancel MUST precede sell -- a "
          "still-resting target and a fresh sell for the same shares can't "
          "both be live at once, the broker rejects the new sell as "
          "\"trying to sell more than the quantity you currently hold\"); "
          "sleeps only between batches, not between a batch's own "
          "cancel/sell sub-steps")

    n = 4   # single batch (< MAX_ORDER_CALLS_PER_SECOND)
    events = TaggedEvents()

    def cancel_fn(item):
        events.add("cancel", str(item))
        return None

    def sell_fn(item):
        events.add("sell", str(item))
        return item

    # Ordering here is guaranteed structurally by _run_exit_wave1 (it waits
    # for the whole cancel burst via _run_batch before starting the sell
    # burst) -- no artificial delay needed to observe it correctly.
    sleep_calls = []
    with patch.object(rt.time, "sleep", lambda s: sleep_calls.append(s)):
        results = []
        for batch in rt._run_exit_wave1(list(range(n)), cancel_fn, sell_fn):
            results.extend(batch)

    check("every item's sell result came back", sorted(results) == list(range(n)), str(results))
    cancel_idx = events.indices("cancel")
    sell_idx   = events.indices("sell")
    check("ALL cancels (single batch, n=4) completed before ANY sell started",
          max(cancel_idx) < min(sell_idx), str(events))
    check("single batch -> zero inter-batch sleeps", sleep_calls == [], str(sleep_calls))

    # Multi-batch: n=7 -> batches of 5, 2. Cancel/sell never interleave WITHIN
    # a batch, and there's exactly one inter-batch sleep.
    events2 = TaggedEvents()
    sleep_calls2 = []
    with patch.object(rt.time, "sleep", lambda s: sleep_calls2.append(s)):
        for batch in rt._run_exit_wave1(list(range(7)),
                                        lambda it: events2.add("cancel", str(it)),
                                        lambda it: events2.add("sell", str(it)) or it):
            pass
    check("multi-batch (n=7, chunks of 5+2): exactly 1 inter-batch sleep",
          sleep_calls2 == [rt.BATCH_SLEEP_SECONDS], str(sleep_calls2))


# ══════════════════════════════════════════════════════════════════════════
# 2. Quote-batching -- exactly ONE call per stage-run, regardless of N
# ══════════════════════════════════════════════════════════════════════════

def test_quote_batching_call_counts():
    print("\n[2] Quote-batching -- get_ltp_batch/_fetch_upper_circuit_batch "
          "called exactly ONCE per stage-run for N=1,5,12 positions")

    for n in (1, 5, 12):
        symbols = [f"Q{n}_{i}" for i in range(n)]
        long_store = FakeStore(positions=[
            make_long(s, target_order_id=f"TGT-{s}", target_price=117.0) for s in symbols
        ])
        short_store = FakeStore(positions=[])

        ltp_calls     = []
        circuit_calls = []
        balance_calls = []

        def fake_get_ltp_batch(syms):
            ltp_calls.append(list(syms))
            return {s: 105.0 for s in syms}   # positive P&L -> all go to tasks

        def fake_circuit_batch(syms):
            circuit_calls.append(list(syms))
            return {s: 130.0 for s in syms}

        def fake_available_balance():
            balance_calls.append(1)
            return 10_000_000.0

        def fake_order_status(oid):
            return {"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0}

        def fake_sell(sym, exch, qty, **kw):
            return f"SELL-{sym}-{kw.get('product')}"

        def fake_buy(sym, exch, qty, **kw):
            return f"BUY-{sym}-{kw.get('order_type')}"

        def fake_poll_fill_safe(oid, fallback_price, fallback_qty):
            return 105.0, fallback_qty

        with patch.object(rt, "_load_long_pos", long_store.load), \
             patch.object(rt, "_save_long_pos", long_store.save), \
             patch.object(rt, "_load_short_pos", short_store.load), \
             patch.object(rt, "_save_short_pos", short_store.save), \
             patch.object(rt, "get_ltp_batch", fake_get_ltp_batch), \
             patch.object(rt, "get_ltp", lambda sym: 105.0), \
             patch.object(rt, "_fetch_upper_circuit_batch", fake_circuit_batch), \
             patch.object(rt, "_available_balance", fake_available_balance), \
             patch.object(rt, "_dhan_order_status", fake_order_status), \
             patch.object(rt, "_dhan_cancel_order", lambda oid: oid), \
             patch.object(rt, "_broker_qty", lambda sym, product: (10, "NSE_EQ")), \
             patch.object(rt, "_intraday_margin_check", lambda sym, qty, ltp: {"margin_required": 1.0}), \
             patch.object(rt, "sell", fake_sell), \
             patch.object(rt, "buy", fake_buy), \
             patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe), \
             patch.object(rt.time, "sleep", lambda secs: None), \
             patch.object(rt.notify, "send_exit_925", lambda **kw: None), \
             patch.object(rt.notify, "send_short_open", lambda **kw: None):
            rt.check_exit_925(dry_run=False)

        check(f"N={n}: get_ltp_batch called EXACTLY ONCE (not once per position)",
              len(ltp_calls) == 1, f"calls={len(ltp_calls)}")
        check(f"N={n}: that one call covered all {n} symbol(s)",
              ltp_calls and sorted(ltp_calls[0]) == sorted(symbols))
        check(f"N={n}: _fetch_upper_circuit_batch called EXACTLY ONCE "
              f"(not once per triggered short)", len(circuit_calls) == 1,
              f"calls={len(circuit_calls)}")
        check(f"N={n}: _available_balance() called EXACTLY ONCE for the whole "
              f"stage-run (not once per short)", balance_calls == [1],
              f"calls={balance_calls}")


# ══════════════════════════════════════════════════════════════════════════
# 3. Combined exit+short budget -- concurrent order-call burst never exceeds
#    MAX_ORDER_CALLS_PER_SECOND, at every wave (Wave 1 sell, Wave 2 short,
#    Wave 3 target/SL)
# ══════════════════════════════════════════════════════════════════════════

def test_combined_exit_and_short_budget():
    print("\n[3] Peak concurrent order-calls stays <= MAX_ORDER_CALLS_PER_SECOND "
          f"({rt.MAX_ORDER_CALLS_PER_SECOND}) across every wave -- Wave 1 sells, "
          "Wave 2 short-opens, and Wave 3 target/SL placements are now separate "
          "bursts (never mixed call types in the same burst), but each burst on "
          "its own must still respect the ceiling")

    n = 12   # 3 chunks of 5,5,2 per wave -- every exit ALSO opens a mirrored short
    symbols = [f"BUD{i}" for i in range(n)]
    long_store  = FakeStore(positions=[
        make_long(s, target_order_id=f"TGT-{s}", target_price=117.0) for s in symbols
    ])
    short_store = FakeStore(positions=[])

    active_count = 0
    peak_count   = 0
    lock = threading.Lock()

    def enter_call():
        nonlocal active_count, peak_count
        with lock:
            active_count += 1
            peak_count = max(peak_count, active_count)

    def exit_call():
        nonlocal active_count
        with lock:
            active_count -= 1

    def fake_sell(sym, exch, qty, **kw):
        enter_call()
        try:
            time.sleep(0.05)   # hold the "in-flight" window long enough to overlap
            return f"SELL-{sym}-{kw.get('product')}"
        finally:
            exit_call()

    def fake_buy(sym, exch, qty, **kw):
        enter_call()
        try:
            time.sleep(0.05)
            return f"BUY-{sym}-{kw.get('order_type')}"
        finally:
            exit_call()

    def fake_order_status(oid):
        return {"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0}

    def fake_poll_fill_safe(oid, fallback_price, fallback_qty):
        return 105.0, fallback_qty

    with patch.object(rt, "_load_long_pos", long_store.load), \
         patch.object(rt, "_save_long_pos", long_store.save), \
         patch.object(rt, "_load_short_pos", short_store.load), \
         patch.object(rt, "_save_short_pos", short_store.save), \
         patch.object(rt, "get_ltp_batch", lambda syms: {s: 105.0 for s in syms}), \
         patch.object(rt, "get_ltp", lambda sym: 105.0), \
         patch.object(rt, "_fetch_upper_circuit_batch", lambda syms: {s: 130.0 for s in syms}), \
         patch.object(rt, "_available_balance", lambda: 10_000_000.0), \
         patch.object(rt, "_dhan_order_status", fake_order_status), \
         patch.object(rt, "_dhan_cancel_order", lambda oid: oid), \
         patch.object(rt, "_broker_qty", lambda sym, product: (10, "NSE_EQ")), \
         patch.object(rt, "_intraday_margin_check", lambda sym, qty, ltp: {"margin_required": 1.0}), \
         patch.object(rt, "sell", fake_sell), \
         patch.object(rt, "buy", fake_buy), \
         patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe), \
         patch.object(rt.notify, "send_exit_925", lambda **kw: None), \
         patch.object(rt.notify, "send_short_open", lambda **kw: None):
        rt.check_exit_925(dry_run=False)

    check(f"peak concurrent order-calls ({peak_count}) never exceeded "
          f"MAX_ORDER_CALLS_PER_SECOND ({rt.MAX_ORDER_CALLS_PER_SECOND}) across "
          f"the whole run (Wave 1 sells, Wave 2 short-opens, Wave 3 target/SL "
          f"placements all measured by the same instrumented sell()/buy())",
          peak_count <= rt.MAX_ORDER_CALLS_PER_SECOND, f"peak={peak_count}")

    n_shorts = sum(1 for p in short_store.positions if p.get("status") == "short_open")
    check(f"all {n} exits actually triggered a mirrored short (real work happened, "
          f"the peak-count assertion above isn't vacuous)", n_shorts == n,
          f"n_shorts={n_shorts}")
    n_protected = sum(1 for p in short_store.positions if p.get("cover_target_order_id"))
    check(f"all {n} shorts also got a cover-target placed (Wave 3 actually ran)",
          n_protected == n, f"n_protected={n_protected}")


# ══════════════════════════════════════════════════════════════════════════
# 4. _BalanceTracker -- genuine concurrent stress, never double-allocates
# ══════════════════════════════════════════════════════════════════════════

def test_balance_tracker_concurrent_never_double_allocates():
    print("\n[4] _BalanceTracker -- real concurrent threads hammering "
          "try_reserve() never over-commit the pool")

    pool_size    = 1_000.0
    per_amount   = 100.0
    n_threads    = 50   # far more contenders than the pool can satisfy
    expected_wins = int(pool_size // per_amount)   # exactly 10 can succeed

    tracker = rt._BalanceTracker(pool_size)
    results: list[bool] = []
    lock = threading.Lock()
    barrier = threading.Barrier(n_threads)

    def worker():
        barrier.wait()   # maximize actual concurrent contention at the same instant
        won = tracker.try_reserve(per_amount)
        with lock:
            results.append(won)

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    n_won = sum(results)
    check(f"exactly {expected_wins} of {n_threads} concurrent reservations succeeded "
          f"(pool={pool_size}, amount={per_amount} each) -- never more, never fewer",
          n_won == expected_wins, f"n_won={n_won}")
    check("final tracker balance is exactly 0 (fully but not over-committed)",
          tracker.value == 0.0, f"value={tracker.value}")


# ══════════════════════════════════════════════════════════════════════════
# 5. One write PER WAVE (not per batch) -- near-simultaneous batch
#    completions within a wave, no lost writes, exactly ONE save for the
#    whole run
# ══════════════════════════════════════════════════════════════════════════

def test_one_write_per_wave_no_lost_writes():
    print("\n[5] Position-file writes -- exactly ONE _save_long_pos() for the "
          "WHOLE run (not once per batch -- Wave 1 now applies every batch's "
          "results in memory across all 3 chunks, and saves once at the end)")

    n = 12   # 3 chunks of 5, 5, 2 within Wave 1
    symbols = [f"WR{i}" for i in range(n)]
    long_store  = FakeStore(positions=[make_long(s) for s in symbols])
    short_store = FakeStore(positions=[])

    def fake_sell(sym, exch, qty, **kw):
        return f"SELL-{sym}"

    def fake_poll_fill_safe(oid, fallback_price, fallback_qty):
        return 110.0, fallback_qty

    with patch.object(rt, "_load_long_pos", long_store.load), \
         patch.object(rt, "_save_long_pos", long_store.save), \
         patch.object(rt, "_load_short_pos", short_store.load), \
         patch.object(rt, "_save_short_pos", short_store.save), \
         patch.object(rt, "get_ltp_batch", lambda syms: {s: 110.0 for s in syms}), \
         patch.object(rt, "get_ltp", lambda sym: 110.0), \
         patch.object(rt, "_fetch_upper_circuit_batch", lambda syms: {}), \
         patch.object(rt, "_available_balance", lambda: 10_000_000.0), \
         patch.object(rt, "_broker_qty", lambda sym, product: (10, "NSE_EQ")), \
         patch.object(rt, "_open_short_place", lambda *a, **kw: None), \
         patch.object(rt, "sell", fake_sell), \
         patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe), \
         patch.object(rt.time, "sleep", lambda secs: None), \
         patch.object(rt.notify, "send_exit_925", lambda **kw: None):
        rt.check_exit_925(dry_run=False)

    by_sym = {p["symbol"]: p for p in long_store.positions}
    check(f"all {n} positions present in the final saved state (none lost across "
          f"3 batches within Wave 1)", len(by_sym) == n, f"got {len(by_sym)}")
    check("every position correctly marked exited_925 -- no batch's in-memory "
          "update was lost before the final save",
          all(p["status"] == "exited_925" for p in by_sym.values()),
          str({s: p["status"] for s, p in by_sym.items()}))
    check("save() called EXACTLY ONCE for the whole run (one write per WAVE, "
          "not one per batch -- 3 batches, 1 save)",
          long_store.save_count == 1, f"save_count={long_store.save_count}")
    check("no short-file save at all -- every short-open was skipped this run",
          short_store.save_count == 0, f"save_count={short_store.save_count}")


# ══════════════════════════════════════════════════════════════════════════
# 6. Exception isolation -- one position's crash never affects batch-mates
# ══════════════════════════════════════════════════════════════════════════

def test_exception_isolation_within_batch():
    print("\n[6] One position's unexpected exception doesn't affect siblings "
          "in the same batch (check_exit_925, square_off_239)")

    # -- check_exit_925 (crash injected in Wave 1's sell sub-step, via
    # _broker_qty -- the same call site the exception lived at before the
    # wave split) --
    n = 5
    symbols = [f"EXC{i}" for i in range(n)]
    failing = "EXC2"
    long_store  = FakeStore(positions=[make_long(s) for s in symbols])
    short_store = FakeStore(positions=[])

    def fake_broker_qty(sym, product):
        if sym == failing:
            raise KeyError("simulated unexpected crash, not a normal RuntimeError")
        return 10, "NSE_EQ"

    def fake_poll_fill_safe(oid, fallback_price, fallback_qty):
        return 110.0, fallback_qty

    with patch.object(rt, "_load_long_pos", long_store.load), \
         patch.object(rt, "_save_long_pos", long_store.save), \
         patch.object(rt, "_load_short_pos", short_store.load), \
         patch.object(rt, "_save_short_pos", short_store.save), \
         patch.object(rt, "get_ltp_batch", lambda syms: {s: 110.0 for s in syms}), \
         patch.object(rt, "get_ltp", lambda sym: 110.0), \
         patch.object(rt, "_fetch_upper_circuit_batch", lambda syms: {}), \
         patch.object(rt, "_available_balance", lambda: 10_000_000.0), \
         patch.object(rt, "_broker_qty", fake_broker_qty), \
         patch.object(rt, "_open_short_place", lambda *a, **kw: None), \
         patch.object(rt, "sell", lambda sym, exch, qty, **kw: f"SELL-{sym}"), \
         patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe), \
         patch.object(rt.notify, "send_exit_925", lambda **kw: None):
        rt.check_exit_925(dry_run=False)

    by_sym = {p["symbol"]: p for p in long_store.positions}
    check("(925) the crashing position was left untouched (still open), "
          "not silently marked exited", by_sym[failing]["status"] == "open",
          f"status={by_sym[failing]['status']}")
    check("(925) every OTHER position in the SAME batch still exited despite "
          "one sibling's unexpected exception",
          all(by_sym[s]["status"] == "exited_925" for s in symbols if s != failing),
          str({s: p["status"] for s, p in by_sym.items()}))

    # -- square_off_239 (all 5 shorts are neither_filled -- no cover_target/
    # stop order_ids set -- so every one goes through the force-cover
    # sub-step's ThreadPoolExecutor burst; crash injected via
    # _broker_short_qty, the same call site the exception lived at before
    # the redesign) --
    short_symbols = [f"SQ{i}" for i in range(5)]
    failing_sq = "SQ3"
    sq_store = FakeStore(positions=[
        make_short(s, cover_target_order_id=None, stop_order_id=None) for s in short_symbols
    ])

    def fake_broker_short_qty_sq(sym):
        if sym == failing_sq:
            raise ValueError("simulated crash mid broker-qty check")
        return 10

    def fake_buy(sym, exch, qty, **kw):
        return f"COVER-{sym}"

    def fake_poll_fill_safe_sq(oid, fallback_price, fallback_qty):
        return 95.0, fallback_qty

    with patch.object(rt, "_load_short_pos", sq_store.load), \
         patch.object(rt, "_save_short_pos", sq_store.save), \
         patch.object(rt, "_dhan_get_orders", lambda: []), \
         patch.object(rt, "_dhan_cancel_order", lambda oid: oid), \
         patch.object(rt, "_broker_short_qty", fake_broker_short_qty_sq), \
         patch.object(rt, "get_ltp_batch", lambda syms: {s: 100.0 for s in syms}), \
         patch.object(rt, "buy", fake_buy), \
         patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe_sq), \
         patch.object(rt.notify, "send_square_off_239", lambda **kw: None):
        rt.square_off_239(dry_run=False)

    by_sym_sq = {p["symbol"]: p for p in sq_store.positions}
    check("(239) the crashing position was left untouched (still short_open)",
          by_sym_sq[failing_sq]["status"] == "short_open",
          f"status={by_sym_sq[failing_sq]['status']}")
    check("(239) every OTHER short in the SAME batch still squared off despite "
          "one sibling's unexpected exception",
          all(by_sym_sq[s]["status"] == "short_closed" for s in short_symbols if s != failing_sq),
          str({s: p["status"] for s, p in by_sym_sq.items()}))


# ══════════════════════════════════════════════════════════════════════════
# 7. Mirrored short anchor price -- the stage-start BATCHED ltp_cache value,
#    NOT a fresh per-thread quote (that design was tried and reverted --
#    confirmed live on 2026-08-27 to reliably 429: 4 of 5 concurrent
#    get_ltp() calls failed in every one of 4 live runs. A single isolated
#    batched call was separately confirmed reliable on its own -- the
#    earlier concern that even the batch call collides with live_monitor.py
#    was traced to the measurement script's own preceding burst, not to
#    live_monitor.py, which only fetches quotes once at startup)
# ══════════════════════════════════════════════════════════════════════════

def test_short_anchor_uses_batched_ltp_cache():
    print("\n[7] Mirrored short anchor price -- uses the stage-start batched "
          "ltp_cache value; get_ltp() (single-symbol) is never called anywhere "
          "in this flow")

    sym = "STALE1"   # fill_price defaults to 100.0 in make_long()
    long_store  = FakeStore(positions=[make_long(sym)])
    short_store = FakeStore(positions=[])

    BATCH_LTP = 105.0   # what the upfront batched get_ltp_batch returns

    get_ltp_batch_calls: list[list[str]] = []
    get_ltp_calls: list[str] = []
    order_prices: dict[str, float] = {}
    sell_counter = [0]

    def fake_get_ltp_batch(syms):
        get_ltp_batch_calls.append(list(syms))
        return {s: BATCH_LTP for s in syms}   # positive P&L (100 -> 105) -> queues a sell

    def fake_get_ltp(s):
        # Should NEVER be reached: check_exit_925/Wave 1/2/3 no longer call
        # get_ltp() at all, and ltp_cache covers this symbol so
        # _open_short_place's own "ltp is None" internal fallback (unrelated,
        # pre-existing, untouched by this change) doesn't trigger either.
        get_ltp_calls.append(s)
        return 999.0

    def fake_sell(s, exch, qty, **kw):
        sell_counter[0] += 1
        oid = f"SELL{sell_counter[0]}-{s}"
        order_prices[oid] = kw.get("price")   # every sell "fills" at its own requested price
        return oid

    def fake_buy(s, exch, qty, **kw):
        return f"BUY-{s}-{kw.get('order_type')}"

    def fake_poll_fill_safe(oid, fallback_price, fallback_qty):
        return order_prices.get(oid, fallback_price), fallback_qty

    with patch.object(rt, "_load_long_pos", long_store.load), \
         patch.object(rt, "_save_long_pos", long_store.save), \
         patch.object(rt, "_load_short_pos", short_store.load), \
         patch.object(rt, "_save_short_pos", short_store.save), \
         patch.object(rt, "get_ltp_batch", fake_get_ltp_batch), \
         patch.object(rt, "get_ltp", fake_get_ltp), \
         patch.object(rt, "_fetch_upper_circuit_batch", lambda syms: {s: 200.0 for s in syms}), \
         patch.object(rt, "_available_balance", lambda: 10_000_000.0), \
         patch.object(rt, "_broker_qty", lambda s, product: (10, "NSE_EQ")), \
         patch.object(rt, "_intraday_margin_check", lambda s, qty, ltp: {"margin_required": 1.0}), \
         patch.object(rt, "sell", fake_sell), \
         patch.object(rt, "buy", fake_buy), \
         patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe), \
         patch.object(rt.time, "sleep", lambda secs: None), \
         patch.object(rt.notify, "send_exit_925", lambda **kw: None), \
         patch.object(rt.notify, "send_short_open", lambda **kw: None):
        rt.check_exit_925(dry_run=False)

    check("get_ltp_batch (stage-start, upfront) called exactly once, covering this symbol",
          len(get_ltp_batch_calls) == 1 and sym in get_ltp_batch_calls[0],
          str(get_ltp_batch_calls))
    check("get_ltp (single-symbol) is NEVER called -- no per-thread fresh-quote "
          "fetch remains anywhere in this flow",
          get_ltp_calls == [], str(get_ltp_calls))

    check("mirrored short actually opened", len(short_store.positions) == 1,
          str(short_store.positions))
    if short_store.positions:
        row = short_store.positions[0]
        expected_price = rt._tick_round(sym, BATCH_LTP * 0.995)
        check(f"short's entry_price (₹{row['entry_price']}) reflects the stage-start "
              f"batched ltp_cache value (expected ₹{expected_price})",
              row["entry_price"] == expected_price,
              f"got {row['entry_price']}")


# ══════════════════════════════════════════════════════════════════════════
# 8. Cross-wave ordering -- Wave 1 (cancel+sell) fully completes, for EVERY
#    batch, before Wave 2 (short-open) starts at all; Wave 2 fully completes
#    before Wave 3 (target/SL) starts. Proven for both check_exit_925 and
#    force_exit_1159 (which share _run_exit_wave1).
# ══════════════════════════════════════════════════════════════════════════

def _run_wave_ordering_case(stage_fn, n, make_positions, extra_patches=None):
    symbols = [f"W{n}_{i}" for i in range(n)]
    long_store  = FakeStore(positions=make_positions(symbols))
    short_store = FakeStore(positions=[])
    events = TaggedEvents()

    def fake_order_status(oid):
        return {"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0}

    def fake_cancel(oid):
        events.add("cancel", oid)
        return oid

    def fake_sell(sym, exch, qty, **kw):
        # Wave 1's exit sell uses whatever product the position carries
        # (CNC here); Wave 2's short-open sell always uses INTRADAY -- the
        # two are distinguishable by that alone, same routing trick
        # test_targets.py's (bal-short) scenario already relies on.
        if kw.get("product") == "INTRADAY":
            events.add("short_sell", sym)
        else:
            events.add("exit_sell", sym)
        return f"SELL-{sym}"

    def fake_buy(sym, exch, qty, **kw):
        events.add("protect_buy", sym)
        return f"BUY-{sym}-{kw.get('order_type')}"

    def fake_poll_fill_safe(oid, fallback_price, fallback_qty):
        return 110.0, fallback_qty

    patches = [
        patch.object(rt, "_load_long_pos", long_store.load),
        patch.object(rt, "_save_long_pos", long_store.save),
        patch.object(rt, "_load_short_pos", short_store.load),
        patch.object(rt, "_save_short_pos", short_store.save),
        patch.object(rt, "get_ltp_batch", lambda syms: {s: 110.0 for s in syms}),
        patch.object(rt, "get_ltp", lambda sym: 110.0),
        # circuit_cache is deliberately empty ({}) below to keep this helper's
        # focus on ORDER-call sequencing -- that means _open_short_protect's
        # `circuit is None` branch fires for every short, so its single-symbol
        # fallback (_fetch_upper_circuit) must be mocked too, or it would hit
        # the real network exactly like the earlier live measurement script's
        # findings warned about.
        patch.object(rt, "_fetch_upper_circuit_batch", lambda syms: {}),
        patch.object(rt, "_fetch_upper_circuit", lambda sym: 200.0),
        patch.object(rt, "_available_balance", lambda: 10_000_000.0),
        patch.object(rt, "_dhan_order_status", fake_order_status),
        patch.object(rt, "_dhan_cancel_order", fake_cancel),
        patch.object(rt, "_broker_qty", lambda sym, product: (10, "NSE_EQ")),
        patch.object(rt, "_intraday_margin_check", lambda sym, qty, ltp: {"margin_required": 1.0}),
        patch.object(rt, "sell", fake_sell),
        patch.object(rt, "buy", fake_buy),
        patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe),
        patch.object(rt.time, "sleep", lambda secs: None),
        patch.object(rt.notify, "send_exit_925", lambda **kw: None),
        patch.object(rt.notify, "send_force_exit_1159", lambda **kw: None),
        patch.object(rt.notify, "send_short_open", lambda **kw: None),
        patch.object(rt.notify, "send_circuit_fetch_failed", lambda **kw: None),
        patch.object(rt.notify, "send_daily_summary", lambda **kw: None),
    ]
    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        stage_fn(dry_run=False)

    return events, long_store, short_store


def test_wave_ordering_check_exit_925():
    print("\n[8] check_exit_925 -- cross-wave ordering for N=12 (3 batches/wave): "
          "every Wave-1 cancel/exit-sell precedes every Wave-2 short-sell, which "
          "precedes every Wave-3 target/SL buy")

    n = 12
    events, long_store, short_store = _run_wave_ordering_case(
        rt.check_exit_925, n,
        lambda symbols: [make_long(s, target_order_id=f"TGT-{s}", target_price=117.0)
                         for s in symbols])

    cancel_idx      = events.indices("cancel")
    exit_sell_idx   = events.indices("exit_sell")
    short_sell_idx  = events.indices("short_sell")
    protect_buy_idx = events.indices("protect_buy")

    check(f"all {n} positions exited", len(exit_sell_idx) == n, str(events))
    check(f"all {n} mirrored shorts opened", len(short_sell_idx) == n, str(events))
    check(f"all {n} shorts protected (cover+stop)", len(protect_buy_idx) == 2 * n,
          f"got {len(protect_buy_idx)} buy events")

    check("Wave 1 (cancel+exit-sell) entirely precedes Wave 2 (short-sell) -- "
          "no short-open call fired before every batch's exit sell was done",
          max(cancel_idx + exit_sell_idx) < min(short_sell_idx), str(events))
    check("Wave 2 (short-sell) entirely precedes Wave 3 (target/SL buys) -- "
          "no protection order fired before every batch's short-open was done",
          max(short_sell_idx) < min(protect_buy_idx), str(events))


def test_wave_ordering_force_exit_1159():
    print("\n[9] force_exit_1159 -- cross-wave ordering for N=12, same "
          "invariant as check_exit_925 (shares _run_exit_wave1). Also confirms "
          "the target-cancel moved from sequential Phase 1 into Wave 1's "
          "concurrent cancel sub-step -- cancels are visible as batched events "
          "here, not fired one-at-a-time before any task list existed")

    n = 12
    events, long_store, short_store = _run_wave_ordering_case(
        rt.force_exit_1159, n,
        lambda symbols: [make_long(s, target_order_id=f"TGT-{s}", target_price=117.0)
                         for s in symbols])

    cancel_idx      = events.indices("cancel")
    exit_sell_idx   = events.indices("exit_sell")
    short_sell_idx  = events.indices("short_sell")
    protect_buy_idx = events.indices("protect_buy")

    check(f"all {n} target orders were cancelled (moved into Wave 1, still happened)",
          len(cancel_idx) == n, str(events))
    check(f"all {n} positions force-exited", len(exit_sell_idx) == n, str(events))
    check(f"all {n} mirrored shorts opened", len(short_sell_idx) == n, str(events))
    check("Wave 1 entirely precedes Wave 2",
          max(cancel_idx + exit_sell_idx) < min(short_sell_idx), str(events))
    check("Wave 2 entirely precedes Wave 3",
          max(short_sell_idx) < min(protect_buy_idx), str(events))


# ══════════════════════════════════════════════════════════════════════════
# 10. Wave 1 internal ordering, single batch -- a no-LTP-fallback task rides
#     in the SAME batch as normal "full" sell tasks; cancels for the WHOLE
#     mixed batch still all precede sells for the whole mixed batch
# ══════════════════════════════════════════════════════════════════════════

def test_wave1_mixed_fallback_and_full_same_batch():
    print("\n[10] check_exit_925 Wave 1 -- a no-LTP-fallback task and two "
          "normal profitable-exit tasks in the SAME batch (n=3, one batch): "
          "still cancel-all-then-sell-all, no separate path for the fallback")

    long_store = FakeStore(positions=[
        make_long("FULLA", target_order_id="TGT-FULLA", target_price=117.0),
        make_long("FULLB", target_order_id="TGT-FULLB", target_price=117.0),
        make_long("NODATA", target_order_id="TGT-NODATA", target_price=117.0),
    ])
    short_store = FakeStore(positions=[])
    events = TaggedEvents()

    def fake_order_status(oid):
        return {"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0}

    def fake_cancel(oid):
        events.add("cancel", oid)
        return oid

    def fake_sell(sym, exch, qty, **kw):
        events.add("sell", sym)
        return f"SELL-{sym}-{qty}"

    def fake_ltp_batch(syms):
        # NODATA is deliberately absent -> triggers the no-LTP fallback path;
        # FULLA/FULLB get a profitable LTP -> normal full-sell path.
        return {s: 110.0 for s in syms if s != "NODATA"}

    def fake_poll_fill_safe(oid, fallback_price, fallback_qty):
        return 110.0, fallback_qty

    with patch.object(rt, "_load_long_pos", long_store.load), \
         patch.object(rt, "_save_long_pos", long_store.save), \
         patch.object(rt, "_load_short_pos", short_store.load), \
         patch.object(rt, "_save_short_pos", short_store.save), \
         patch.object(rt, "get_ltp_batch", fake_ltp_batch), \
         patch.object(rt, "get_ltp", lambda sym: 110.0), \
         patch.object(rt, "_fetch_upper_circuit_batch", lambda syms: {}), \
         patch.object(rt, "_available_balance", lambda: 10_000_000.0), \
         patch.object(rt, "_dhan_order_status", fake_order_status), \
         patch.object(rt, "_dhan_cancel_order", fake_cancel), \
         patch.object(rt, "_broker_qty", lambda sym, product: (10, "NSE_EQ")), \
         patch.object(rt, "_open_short_place", lambda *a, **kw: None), \
         patch.object(rt, "sell", fake_sell), \
         patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe), \
         patch.object(rt.notify, "send_exit_925", lambda **kw: None), \
         patch.object(rt.notify, "send_exit_925_nodata", lambda **kw: None):
        rt.check_exit_925(dry_run=False)

    cancel_idx = events.indices("cancel")
    sell_idx   = events.indices("sell")
    check("all 3 tasks (2 full + 1 fallback) were cancelled -- same batch",
          len(cancel_idx) == 3, str(events))
    # NODATA's fallback also places a fresh target for the remainder in the
    # SAME sell sub-step -- so NODATA contributes 2 "sell" events (half-sell +
    # fresh target), FULLA/FULLB contribute 1 each -- 4 total.
    check("all sell-side activity (2 full sells + fallback's half-sell + its "
          "fresh-target-remainder) happened in the sell sub-step, none before "
          "every cancel in the batch completed",
          len(sell_idx) == 4 and max(cancel_idx) < min(sell_idx), str(events))

    by_sym = {p["symbol"]: p for p in long_store.positions}
    check("NODATA correctly took the no-data fallback path (partial exit)",
          by_sym["NODATA"]["status"] == "partial_exit_925_nodata",
          by_sym["NODATA"]["status"])
    check("FULLA/FULLB correctly took the normal full-exit path",
          by_sym["FULLA"]["status"] == "exited_925" and by_sym["FULLB"]["status"] == "exited_925")


# ══════════════════════════════════════════════════════════════════════════
# 11. square_off_239 -- single Order Book pre-check (not per-order status
#     calls), both_filled positions excluded without consuming a batch slot
# ══════════════════════════════════════════════════════════════════════════

def test_square_off_single_order_book_call_and_both_filled_exclusion():
    print("\n[11] square_off_239 -- ONE _dhan_get_orders() call regardless of N; "
          "_dhan_order_status is NEVER called; a both_filled position is "
          "excluded from batching entirely (manual review, no cancel/cover "
          "action), siblings unaffected")

    cover_hit  = make_short("COVERHIT", cover_target_order_id="C-1", stop_order_id="S-1")
    stop_hit   = make_short("STOPHIT", cover_target_order_id="C-2", stop_order_id="S-2")
    neither    = make_short("NEITHER", cover_target_order_id="C-3", stop_order_id="S-3")
    both       = make_short("BOTHHIT", cover_target_order_id="C-4", stop_order_id="S-4")
    sq_store   = FakeStore(positions=[cover_hit, stop_hit, neither, both])

    orders = [
        {"orderId": "C-1", "orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 95.0},
        {"orderId": "S-1", "orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0},
        {"orderId": "C-2", "orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0},
        {"orderId": "S-2", "orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 129.0},
        {"orderId": "C-3", "orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0},
        {"orderId": "S-3", "orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0},
        {"orderId": "C-4", "orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 95.0},
        {"orderId": "S-4", "orderStatus": "TRADED", "filledQty": 10, "averageTradedPrice": 129.0},
    ]
    get_orders_calls = [0]
    def fake_get_orders():
        get_orders_calls[0] += 1
        return orders

    def fail_if_order_status_called(oid):
        raise AssertionError(f"_dhan_order_status must never be called anymore, got oid={oid}")

    manual_review_calls = []
    cancel_calls = []
    def fake_cancel(oid):
        cancel_calls.append(oid)
        return oid

    def fake_buy(sym, exch, qty, **kw):
        return f"COVER-{sym}"

    def fake_poll_fill_safe(oid, fallback_price, fallback_qty):
        return 90.0, fallback_qty

    with patch.object(rt, "_load_short_pos", sq_store.load), \
         patch.object(rt, "_save_short_pos", sq_store.save), \
         patch.object(rt, "_dhan_get_orders", fake_get_orders), \
         patch.object(rt, "_dhan_order_status", fail_if_order_status_called), \
         patch.object(rt, "_dhan_cancel_order", fake_cancel), \
         patch.object(rt, "_broker_short_qty", lambda sym: 10), \
         patch.object(rt, "get_ltp_batch", lambda syms: {s: 100.0 for s in syms}), \
         patch.object(rt, "buy", fake_buy), \
         patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe), \
         patch.object(rt.notify, "send_cover_target_hit", lambda **kw: None), \
         patch.object(rt.notify, "send_short_stoploss_hit", lambda **kw: None), \
         patch.object(rt.notify, "send_square_off_239", lambda **kw: None), \
         patch.object(rt.notify, "send_square_off_manual_review",
                      lambda **kw: manual_review_calls.append(kw)):
        rt.square_off_239(dry_run=False)

    check("_dhan_get_orders() called EXACTLY ONCE for the whole run "
          "(not once per position)", get_orders_calls[0] == 1, str(get_orders_calls))

    by_sym = {p["symbol"]: p for p in sq_store.positions}
    check("COVERHIT resolved from the pre-fetched snapshot (cover target hit)",
          by_sym["COVERHIT"]["status"] == "short_closed"
          and by_sym["COVERHIT"]["exit_order_id_239"] == "C-1",
          str(by_sym["COVERHIT"]))
    check("STOPHIT resolved from the pre-fetched snapshot (stop-loss hit)",
          by_sym["STOPHIT"]["status"] == "short_closed"
          and by_sym["STOPHIT"]["exit_order_id_239"] == "S-2",
          str(by_sym["STOPHIT"]))
    check("NEITHER force-covered (both orders were pending)",
          by_sym["NEITHER"]["status"] == "short_closed"
          and by_sym["NEITHER"]["exit_order_id_239"] == "COVER-NEITHER",
          str(by_sym["NEITHER"]))
    check("BOTHHIT excluded entirely -- left untouched (still short_open), "
          "no cancel/cover action taken on it",
          by_sym["BOTHHIT"]["status"] == "short_open", str(by_sym["BOTHHIT"]))
    check("BOTHHIT's own order_ids were NEVER cancelled (excluded before batching)",
          "C-4" not in cancel_calls and "S-4" not in cancel_calls, str(cancel_calls))
    check("notify.send_square_off_manual_review fired once, for BOTHHIT only",
          len(manual_review_calls) == 1 and manual_review_calls[0]["symbol"] == "BOTHHIT",
          str(manual_review_calls))

    # Siblings (COVERHIT/STOPHIT/NEITHER) unaffected by BOTHHIT's exclusion --
    # each resolved independently and correctly despite sharing a batch.
    check("COVERHIT/STOPHIT/NEITHER's own cancels fired as expected "
          "(stop cancelled for COVERHIT, cover cancelled for STOPHIT, both "
          "cancelled for NEITHER)",
          set(cancel_calls) == {"S-1", "C-2", "C-3", "S-3"}, str(cancel_calls))


# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    test_run_in_chunks_boundaries()
    test_run_batch_primitive()
    test_run_exit_wave1_cancel_before_sell()
    test_quote_batching_call_counts()
    test_combined_exit_and_short_budget()
    test_balance_tracker_concurrent_never_double_allocates()
    test_one_write_per_wave_no_lost_writes()
    test_exception_isolation_within_batch()
    test_short_anchor_uses_batched_ltp_cache()
    test_wave_ordering_check_exit_925()
    test_wave_ordering_force_exit_1159()
    test_wave1_mixed_fallback_and_full_same_batch()
    test_square_off_single_order_book_call_and_both_filled_exclusion()

    print()
    if failures:
        print(f"{failures} check(s) FAILED.")
        sys.exit(1)
    print("All checks PASSED.")
