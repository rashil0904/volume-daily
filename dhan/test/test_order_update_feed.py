#!/usr/bin/env python3
"""
test_order_update_feed.py -- standalone verifier for the Phase 1 shadow-mode
Order Update WebSocket observation layer (dhan/order_update_feed.py).

Covers:
  1. _handle_message cache semantics -- first_traded_at_wallclock latches on
     the FIRST TRADED message and never moves on a later duplicate/update,
     while status/filled_qty/avg_price/last_updated_at_wallclock keep
     tracking the latest message.
  2. run()'s own reconnect loop is fully independent -- own connect count,
     own backoff (escalates on a fast reconnect, resets after a >=30s-held
     connection), no shared state with anything else. Mocks
     dhanhq.DhanContext/OrderUpdate directly (lazy-imported inside run()).
  3. enable_validation_logging()'s wrapper: WS-faster, polling-faster, and
     WEBSOCKET_MISSED cases all produce the correct [WS_VALIDATION] log line
     -- and the wrapped _poll_fill_strict/_poll_fill_safe return EXACTLY
     what the real function returned, untouched.
  4. FileBackedOrderCache reads back exactly what OrderUpdateFeed persisted.

Mocks dhanhq.DhanContext/OrderUpdate and dhan.run_trades._poll_fill_strict/
_poll_fill_safe -- zero real network calls, zero real WebSocket connections.
Uses a temp directory for the persisted cache file so this suite never
touches the real results/order_update_cache.json.

Usage:
    python dhan/test_order_update_feed.py

Exit 0 on all-pass, exit 1 on any failure.
"""

import json
import sys
import tempfile
import threading
import time
import types
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

for _m in ("data_loader",):
    sys.modules.setdefault(_m, types.ModuleType(_m))

import dhan.order_update_feed as ouf   # noqa: E402
import dhan.run_trades as rt           # noqa: E402

# _persist() writes to _CACHE_FILE the moment _PERSIST_EVERY_SECONDS has
# elapsed since the last write, which is immediately on a fresh OrderUpdateFeed
# instance (_last_persist_at starts at 0.0) -- left unpatched, any scenario
# below that calls _handle_message even once would silently write to the REAL
# results/order_update_cache.json. Every scenario gets a safe default tmp
# location here; tests [1], [2], and [4] that specifically want to inspect the
# persisted file's content locally override this with their own tmpdir.
_DEFAULT_TMPDIR = Path(tempfile.mkdtemp())
patch.object(ouf, "_CACHE_FILE", _DEFAULT_TMPDIR / "order_update_cache.json").start()
patch.object(ouf, "_RESULTS_DIR", _DEFAULT_TMPDIR).start()

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


def wait_until(predicate, timeout=2.0, interval=0.02) -> bool:
    """Polls predicate() until True or timeout -- used instead of a fixed
    sleep so these tests aren't flaky under load, and don't wait longer than
    necessary either."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


# ══════════════════════════════════════════════════════════════════════════
# [1] _handle_message cache semantics
# ══════════════════════════════════════════════════════════════════════════

def test_handle_message_cache_semantics():
    print("\n[1] _handle_message -- first_traded_at latches once, other fields keep updating")
    tmpdir = tempfile.mkdtemp()
    with patch.object(ouf, "_CACHE_FILE", Path(tmpdir) / "cache.json"), \
         patch.object(ouf, "_RESULTS_DIR", Path(tmpdir)):
        feed = ouf.OrderUpdateFeed("CID", "TOKEN")

        feed._handle_message({"Data": {"orderNo": "O1", "status": "PENDING"}})
        entry1 = feed.get_cached("O1")
        check("(1) PENDING message recorded, no first_traded_at yet",
              entry1["status"] == "PENDING" and entry1["first_traded_at_wallclock"] is None,
              str(entry1))

        feed._handle_message({"Data": {"orderNo": "O1", "status": "TRADED",
                                       "filledQty": 10, "averageTradedPrice": 100.5}})
        entry2 = feed.get_cached("O1")
        first_traded_at = entry2["first_traded_at_wallclock"]
        check("(1) TRADED message sets first_traded_at_wallclock",
              first_traded_at is not None, str(entry2))
        check("(1) filled_qty/avg_price reflect the TRADED message",
              entry2["filled_qty"] == 10 and entry2["avg_price"] == 100.5, str(entry2))

        feed._handle_message({"Data": {"orderNo": "O1", "status": "TRADED",
                                       "filledQty": 10, "averageTradedPrice": 999.0}})
        entry3 = feed.get_cached("O1")
        check("(1) a SECOND TRADED message does NOT move first_traded_at_wallclock",
              entry3["first_traded_at_wallclock"] == first_traded_at, str(entry3))
        check("(1) but avg_price/last_updated_at DO keep tracking the latest message",
              entry3["avg_price"] == 999.0, str(entry3))

        feed._handle_message({"no": "orderNo or orderId here"})
        check("(1) a message with no order id doesn't crash or pollute the cache",
              set(feed.snapshot().keys()) == {"O1"}, str(feed.snapshot()))


# ══════════════════════════════════════════════════════════════════════════
# [2] run()'s independent reconnect loop
# ══════════════════════════════════════════════════════════════════════════

def test_reconnect_loop_independence():
    print("\n[2] run() -- own reconnect loop, own backoff, unaffected by anything else")
    tmpdir = tempfile.mkdtemp()

    call_log = []   # records each connect attempt's on_update deliveries

    class FakeOrderUpdateClient:
        _attempt = [0]

        def __init__(self, dhan_context):
            self.dhan_context = dhan_context
            self.on_update = None

        def connect_to_dhan_websocket_sync(self):
            FakeOrderUpdateClient._attempt[0] += 1
            attempt = FakeOrderUpdateClient._attempt[0]
            call_log.append(attempt)
            if attempt == 1:
                self.on_update({"Data": {"orderNo": "A1", "status": "TRADED",
                                         "filledQty": 5, "averageTradedPrice": 50.0}})
                return   # simulate disconnect
            else:
                self.on_update({"Data": {"orderNo": "A2", "status": "TRADED",
                                         "filledQty": 7, "averageTradedPrice": 70.0}})
                feed.stop()   # end the test after the 2nd attempt
                return

    class FakeDhanContext:
        def __init__(self, client_id, access_token):
            pass

    with patch.object(ouf, "_CACHE_FILE", Path(tmpdir) / "cache.json"), \
         patch.object(ouf, "_RESULTS_DIR", Path(tmpdir)), \
         patch.object(ouf, "_PERSIST_EVERY_SECONDS", 0), \
         patch.object(ouf.time, "sleep", lambda s: None), \
         patch("dhanhq.DhanContext", FakeDhanContext), \
         patch("dhanhq.OrderUpdate", FakeOrderUpdateClient):
        feed = ouf.OrderUpdateFeed("CID", "TOKEN")
        feed.run()   # returns once feed.stop() is called from inside the fake client

    check("(2) both connect attempts actually ran", call_log == [1, 2], str(call_log))
    check("(2) connect_count reflects 2 attempts", feed._connect_count == 2,
          str(feed._connect_count))
    check("(2) backoff escalated after the first (fast) disconnect",
          feed._reconnect_backoff == ouf._RECONNECT_BACKOFF_INITIAL * 2,
          str(feed._reconnect_backoff))
    check("(2) both orders ended up in the cache despite the reconnect in between",
          set(feed.snapshot().keys()) == {"A1", "A2"}, str(feed.snapshot()))

    # Persisted-to-disk check, independent of the in-memory cache above.
    persisted = json.loads((Path(tmpdir) / "cache.json").read_text())
    check("(2) the persisted file reflects both orders too",
          set(persisted.keys()) == {"A1", "A2"}, str(persisted))


def test_backoff_resets_after_stable_connection():
    print("\n[2b] _on_connect_ok -- backoff resets after a connection held >=30s, "
          "not on every reconnect")
    feed = ouf.OrderUpdateFeed("CID", "TOKEN")
    feed._reconnect_backoff = 32
    feed._last_connect_at = time.monotonic() - 45   # held "for 45s" since last connect
    feed._on_connect_ok()
    check("(2b) a connection held >=30s resets backoff to the initial value",
          feed._reconnect_backoff == ouf._RECONNECT_BACKOFF_INITIAL,
          str(feed._reconnect_backoff))

    feed2 = ouf.OrderUpdateFeed("CID", "TOKEN")
    feed2._reconnect_backoff = 32
    feed2._last_connect_at = time.monotonic() - 3    # held only 3s -- a fast reconnect
    feed2._on_connect_ok()
    check("(2b) a connection held <30s does NOT reset backoff",
          feed2._reconnect_backoff == 32, str(feed2._reconnect_backoff))


# ══════════════════════════════════════════════════════════════════════════
# [3] enable_validation_logging -- WS faster / polling faster / missed
# ══════════════════════════════════════════════════════════════════════════

def test_validation_ws_faster():
    print("\n[3a] enable_validation_logging -- WS reported TRADED before polling confirmed")
    feed = ouf.OrderUpdateFeed("CID", "TOKEN")
    ws_time = datetime.now(ouf._IST) - timedelta(seconds=1)   # WS "saw" it 1s ago
    feed._cache["B1"] = {"status": "TRADED", "filled_qty": 10, "avg_price": 100.0,
                         "received_at_wallclock": ws_time.isoformat(),
                         "first_traded_at_wallclock": ws_time.isoformat(),
                         "last_updated_at_wallclock": ws_time.isoformat()}

    printed = []
    real_print = print
    def spy_print(*a, **kw):
        printed.append(" ".join(str(x) for x in a))
        real_print(*a, **kw)

    with patch.object(rt, "_poll_fill_strict", lambda oid: (100.0, 10, False, "")), \
         patch("builtins.print", spy_print):
        restore = ouf.enable_validation_logging(feed)
        try:
            result = rt._poll_fill_strict("B1")
            check("(3a) wrapped function returns EXACTLY the real function's result",
                  result == (100.0, 10, False, ""), str(result))
            ok = wait_until(lambda: any("[WS_VALIDATION]" in l and "B1" in l for l in printed))
            check("(3a) a [WS_VALIDATION] line was logged", ok, str(printed))
            line = next(l for l in printed if "[WS_VALIDATION]" in l and "B1" in l)
            check("(3a) log line reports WEBSOCKET as faster", "faster=WEBSOCKET" in line, line)
            check("(3a) delta_seconds is positive (WS earlier than polling)",
                  "delta_seconds=+" in line, line)
        finally:
            restore()


def test_validation_polling_faster():
    print("\n[3b] enable_validation_logging -- polling confirmed before WS reported it "
          "(but WS does report it within the grace window)")
    feed = ouf.OrderUpdateFeed("CID", "TOKEN")

    printed = []
    real_print = print
    def spy_print(*a, **kw):
        printed.append(" ".join(str(x) for x in a))
        real_print(*a, **kw)

    with patch.object(rt, "_poll_fill_strict", lambda oid: (100.0, 10, False, "")), \
         patch.object(ouf, "_VALIDATION_GRACE_SECONDS", 2.0), \
         patch.object(ouf, "_VALIDATION_POLL_INTERVAL", 0.02), \
         patch("builtins.print", spy_print):
        restore = ouf.enable_validation_logging(feed)
        try:
            rt._poll_fill_strict("B2")   # "polling" confirms right now, cache still empty
            time.sleep(0.15)
            # WS only reports it a moment AFTER polling already confirmed.
            feed._handle_message({"Data": {"orderNo": "B2", "status": "TRADED",
                                           "filledQty": 10, "averageTradedPrice": 100.0}})
            ok = wait_until(lambda: any("[WS_VALIDATION]" in l and "B2" in l for l in printed))
            check("(3b) a [WS_VALIDATION] line was logged", ok, str(printed))
            line = next(l for l in printed if "[WS_VALIDATION]" in l and "B2" in l)
            check("(3b) log line reports POLLING as faster", "faster=POLLING" in line, line)
            check("(3b) delta_seconds is negative (polling earlier than WS)",
                  "delta_seconds=-" in line, line)
        finally:
            restore()


def test_validation_websocket_missed():
    print("\n[3c] enable_validation_logging -- WS never reports it within the grace window")
    feed = ouf.OrderUpdateFeed("CID", "TOKEN")

    printed = []
    real_print = print
    def spy_print(*a, **kw):
        printed.append(" ".join(str(x) for x in a))
        real_print(*a, **kw)

    with patch.object(rt, "_poll_fill_safe", lambda oid, fp, fq: (100.0, fq)), \
         patch.object(ouf, "_VALIDATION_GRACE_SECONDS", 0.2), \
         patch.object(ouf, "_VALIDATION_POLL_INTERVAL", 0.02), \
         patch("builtins.print", spy_print):
        restore = ouf.enable_validation_logging(feed)
        try:
            result = rt._poll_fill_safe("B3", 100.0, 10)
            check("(3c) wrapped _poll_fill_safe still returns the real result unchanged",
                  result == (100.0, 10), str(result))
            ok = wait_until(lambda: any("[WS_VALIDATION]" in l and "B3" in l for l in printed),
                           timeout=2.0)
            check("(3c) a [WS_VALIDATION] line was eventually logged", ok, str(printed))
            line = next(l for l in printed if "[WS_VALIDATION]" in l and "B3" in l)
            check("(3c) log line reports WEBSOCKET_MISSED", "result=WEBSOCKET_MISSED" in line, line)
        finally:
            restore()


def test_validation_not_filled_logs_immediately():
    print("\n[3d] enable_validation_logging -- a rejected/unfilled poll result "
          "logs NOT_FILLED immediately, no grace-period wait")
    feed = ouf.OrderUpdateFeed("CID", "TOKEN")
    printed = []
    real_print = print
    def spy_print(*a, **kw):
        printed.append(" ".join(str(x) for x in a))
        real_print(*a, **kw)

    with patch.object(rt, "_poll_fill_strict", lambda oid: (0.0, 0, True, "REJECTED")), \
         patch("builtins.print", spy_print):
        restore = ouf.enable_validation_logging(feed)
        try:
            result = rt._poll_fill_strict("B4")
            check("(3d) wrapped function still returns the real (rejected) result unchanged",
                  result == (0.0, 0, True, "REJECTED"), str(result))
            line = next((l for l in printed if "[WS_VALIDATION]" in l and "B4" in l), None)
            check("(3d) NOT_FILLED logged synchronously, no thread/wait needed",
                  line is not None and "polling=NOT_FILLED" in line, str(printed))
        finally:
            restore()


def test_enable_validation_logging_restore():
    print("\n[3e] enable_validation_logging -- restore() puts the originals back")
    feed = ouf.OrderUpdateFeed("CID", "TOKEN")
    original = rt._poll_fill_strict
    restore = ouf.enable_validation_logging(feed)
    check("(3e) _poll_fill_strict is wrapped (no longer the original object)",
          rt._poll_fill_strict is not original)
    restore()
    check("(3e) restore() puts the exact original function back",
          rt._poll_fill_strict is original)


# ══════════════════════════════════════════════════════════════════════════
# [4] FileBackedOrderCache
# ══════════════════════════════════════════════════════════════════════════

def test_file_backed_cache_reads_persisted_data():
    print("\n[4] FileBackedOrderCache -- reads back exactly what OrderUpdateFeed persisted")
    tmpdir = tempfile.mkdtemp()
    with patch.object(ouf, "_CACHE_FILE", Path(tmpdir) / "cache.json"), \
         patch.object(ouf, "_RESULTS_DIR", Path(tmpdir)), \
         patch.object(ouf, "_PERSIST_EVERY_SECONDS", 0):
        feed = ouf.OrderUpdateFeed("CID", "TOKEN")
        feed._handle_message({"Data": {"orderNo": "C1", "status": "TRADED",
                                       "filledQty": 3, "averageTradedPrice": 30.0}})

        reader = ouf.FileBackedOrderCache()
        entry = reader.get_cached("C1")
        check("(4) FileBackedOrderCache sees the persisted order",
              entry is not None and entry["status"] == "TRADED", str(entry))
        check("(4) FileBackedOrderCache returns None for an unknown order_id",
              reader.get_cached("NOPE") is None)

    with patch.object(ouf, "_CACHE_FILE", Path(tmpdir) / "does-not-exist.json"):
        reader2 = ouf.FileBackedOrderCache()
        check("(4) missing cache file -> None, doesn't crash",
              reader2.get_cached("C1") is None)


def test_file_backed_cache_torn_write():
    print("\n[4b] FileBackedOrderCache -- a torn/partial write (live_monitor.py's feed "
          "rewrites this file from a SEPARATE process every few seconds) never raises")
    tmpdir = tempfile.mkdtemp()
    cache_path = Path(tmpdir) / "cache.json"

    with patch.object(ouf, "_CACHE_FILE", cache_path), \
         patch.object(ouf, "_logger", MagicMock()) as fake_logger:
        # Empty file -- e.g. caught between truncate and write.
        cache_path.write_text("")
        reader = ouf.FileBackedOrderCache()
        result = reader.get_cached("C1")
        check("(4b) empty file -> clean miss, not a raise", result is None)

        # Truncated/malformed JSON -- e.g. caught mid-write.
        cache_path.write_text('{"C1": {"status": "TR')
        result2 = reader.get_cached("C1")
        check("(4b) truncated JSON -> clean miss, not a raise", result2 is None)

        # Directory instead of a file (IsADirectoryError on .read_text()) --
        # not a realistic production case, but proves the except is broad
        # enough to catch more than just JSONDecodeError.
        cache_path.unlink()
        cache_path.mkdir()
        result3 = reader.get_cached("C1")
        check("(4b) unreadable path (e.g. IsADirectoryError) -> clean miss, not a raise",
              result3 is None)

        check("(4b) every failure was logged at DEBUG level",
              fake_logger.debug.call_count == 3, str(fake_logger.debug.call_args_list))
        check("(4b) never logged as a warning or error -- this is an expected race, not a fault",
              fake_logger.warning.call_count == 0 and fake_logger.error.call_count == 0)

    # A GOOD read still works normally once the write has actually landed --
    # confirms the fix doesn't over-swallow genuinely-available data.
    with patch.object(ouf, "_CACHE_FILE", cache_path.parent / "good.json"):
        (cache_path.parent / "good.json").write_text('{"C2": {"status": "TRADED"}}')
        reader2 = ouf.FileBackedOrderCache()
        check("(4b) a genuinely complete file still reads correctly",
              reader2.get_cached("C2") == {"status": "TRADED"})


# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    test_handle_message_cache_semantics()
    test_reconnect_loop_independence()
    test_backoff_resets_after_stable_connection()
    test_validation_ws_faster()
    test_validation_polling_faster()
    test_validation_websocket_missed()
    test_validation_not_filled_logs_immediately()
    test_enable_validation_logging_restore()
    test_file_backed_cache_reads_persisted_data()
    test_file_backed_cache_torn_write()

    print()
    if failures:
        print(f"{failures} check(s) FAILED.")
        sys.exit(1)
    print("All checks PASSED.")
    sys.exit(0)
