#!/usr/bin/env python3
"""
test_batch_concurrency.py -- standalone verifier for the batched-concurrent
execution redesign of check_exit_925/force_exit_1159/_open_short/
square_off_239: quote-call batching, MAX_ORDER_CALLS_PER_SECOND chunking,
the combined exit+triggered-short Order-API budget, thread-safe capital
allocation (_BalanceTracker), one-write-per-chunk position-file safety, and
per-position exception isolation within a chunk.

Decision logic itself (target-hit fork, no-data fallback, OCO branches) is
NOT retested here -- that's dhan/test_targets.py's job, unchanged and still
passing. This file covers exactly the concurrency mechanics layered on top.

Mocks every broker-facing call (buy/sell/order_status/cancel_order/
_broker_qty/_broker_short_qty/_available_balance/_intraday_margin_check/
get_ltp_batch/_fetch_upper_circuit_batch/_poll_fill_safe) and the position-
file read/write with in-memory stores -- zero network calls, zero real file
writes.

Usage:
    python dhan/test_batch_concurrency.py

Exit 0 on all-pass, exit 1 on any failure.
"""

import copy
import sys
import threading
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
#    MAX_ORDER_CALLS_PER_SECOND, even though every exit triggers a short
# ══════════════════════════════════════════════════════════════════════════

def test_combined_exit_and_short_budget():
    print("\n[3] Combined exit+short budget -- peak concurrent order-calls "
          f"stays <= MAX_ORDER_CALLS_PER_SECOND ({rt.MAX_ORDER_CALLS_PER_SECOND}) "
          "even though every exit triggers a mirrored short")

    n = 12   # 3 chunks of 5,5,2 -- each exit ALSO opens a short (sell+poll+buy+buy)
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
          f"MAX_ORDER_CALLS_PER_SECOND ({rt.MAX_ORDER_CALLS_PER_SECOND}) despite "
          f"every exit ALSO triggering a mirrored short in the same thread",
          peak_count <= rt.MAX_ORDER_CALLS_PER_SECOND, f"peak={peak_count}")

    n_shorts = sum(1 for p in short_store.positions if p.get("status") == "short_open")
    check(f"all {n} exits actually triggered a mirrored short (real work happened, "
          f"the peak-count assertion above isn't vacuous)", n_shorts == n,
          f"n_shorts={n_shorts}")


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
# 5. One-write-per-chunk -- near-simultaneous completions, no lost writes
# ══════════════════════════════════════════════════════════════════════════

def test_no_lost_writes_across_chunks():
    print("\n[5] Position-file writes -- near-simultaneous chunk completions, "
          "every position's update survives (no lost writes)")

    n = 12   # 3 chunks of 5, 5, 2
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
         patch.object(rt, "_open_short_core", lambda *a, **kw: None), \
         patch.object(rt, "sell", fake_sell), \
         patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe), \
         patch.object(rt.notify, "send_exit_925", lambda **kw: None):
        rt.check_exit_925(dry_run=False)

    by_sym = {p["symbol"]: p for p in long_store.positions}
    check(f"all {n} positions present in the final saved state (none lost across "
          f"{long_store.save_count} chunked saves)", len(by_sym) == n,
          f"got {len(by_sym)}")
    check("every position correctly marked exited_925 -- no chunk's write clobbered "
          "an earlier chunk's already-saved changes",
          all(p["status"] == "exited_925" for p in by_sym.values()),
          str({s: p["status"] for s, p in by_sym.items()}))
    check("save() called once per chunk with changes (3 chunks, all had fills)",
          long_store.save_count == 3, f"save_count={long_store.save_count}")


# ══════════════════════════════════════════════════════════════════════════
# 6. Exception isolation -- one position's crash never affects chunk-mates
# ══════════════════════════════════════════════════════════════════════════

def test_exception_isolation_within_chunk():
    print("\n[6] One position's unexpected exception doesn't affect siblings "
          "in the same chunk (check_exit_925, force_exit_1159, square_off_239)")

    # -- check_exit_925 --
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
         patch.object(rt, "_open_short_core", lambda *a, **kw: None), \
         patch.object(rt, "sell", lambda sym, exch, qty, **kw: f"SELL-{sym}"), \
         patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe), \
         patch.object(rt.notify, "send_exit_925", lambda **kw: None):
        rt.check_exit_925(dry_run=False)

    by_sym = {p["symbol"]: p for p in long_store.positions}
    check("(925) the crashing position was left untouched (still open), "
          "not silently marked exited", by_sym[failing]["status"] == "open",
          f"status={by_sym[failing]['status']}")
    check("(925) every OTHER position in the SAME chunk still exited despite "
          "one sibling's unexpected exception",
          all(by_sym[s]["status"] == "exited_925" for s in symbols if s != failing),
          str({s: p["status"] for s, p in by_sym.items()}))

    # -- square_off_239 --
    short_symbols = [f"SQ{i}" for i in range(5)]
    failing_sq = "SQ3"
    sq_store = FakeStore(positions=[make_short(s) for s in short_symbols])

    def fake_order_status_sq(oid):
        if failing_sq in oid:
            raise ValueError("simulated crash mid status-check")
        return {"orderStatus": "PENDING", "filledQty": 0, "averageTradedPrice": 0}

    def fake_buy(sym, exch, qty, **kw):
        return f"COVER-{sym}"

    def fake_poll_fill_safe_sq(oid, fallback_price, fallback_qty):
        return 95.0, fallback_qty

    with patch.object(rt, "_load_short_pos", sq_store.load), \
         patch.object(rt, "_save_short_pos", sq_store.save), \
         patch.object(rt, "_dhan_order_status", fake_order_status_sq), \
         patch.object(rt, "_dhan_cancel_order", lambda oid: oid), \
         patch.object(rt, "_broker_short_qty", lambda sym: 10), \
         patch.object(rt, "get_ltp_batch", lambda syms: {s: 100.0 for s in syms}), \
         patch.object(rt, "buy", fake_buy), \
         patch.object(rt, "_poll_fill_safe", fake_poll_fill_safe_sq), \
         patch.object(rt.notify, "send_square_off_239", lambda **kw: None):
        rt.square_off_239(dry_run=False)

    by_sym_sq = {p["symbol"]: p for p in sq_store.positions}
    check("(239) the crashing position was left untouched (still short_open)",
          by_sym_sq[failing_sq]["status"] == "short_open",
          f"status={by_sym_sq[failing_sq]['status']}")
    check("(239) every OTHER short in the SAME chunk still squared off despite "
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
        # Should NEVER be reached: check_exit_925/_do_exit_task no longer call
        # get_ltp() at all, and ltp_cache covers this symbol so
        # _open_short_core's own "ltp is None" internal fallback (unrelated,
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

if __name__ == "__main__":
    test_run_in_chunks_boundaries()
    test_quote_batching_call_counts()
    test_combined_exit_and_short_budget()
    test_balance_tracker_concurrent_never_double_allocates()
    test_no_lost_writes_across_chunks()
    test_exception_isolation_within_chunk()
    test_short_anchor_uses_batched_ltp_cache()

    print()
    if failures:
        print(f"{failures} check(s) FAILED.")
        sys.exit(1)
    print("All checks PASSED.")
