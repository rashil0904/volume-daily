#!/usr/bin/env python3
"""
test_reconcile_pending_shorts.py -- standalone verifier for
dhan/run_trades.py's _reconcile_pending_shorts() (called from every
square_off_239 bucket run + the 9:12am --reconcile slot): recovers a
short-open SELL order that filled AFTER its own confirmation poll gave up
and was discarded as "rejected" -- the bug first confirmed live on FISCHER
(2026-10-26, order 23126102637609): filled 6007@25.74 eighteen seconds
after placement, eight seconds past the poll's ~15s budget, left as a
completely untracked naked short for the rest of the day until Dhan's own
RMS auto-covered it at a ₹4,584 loss. See
_record_pending_short_verification's and _reconcile_pending_shorts' own
comments in run_trades.py for the full writeup.

Mocks _dhan_order_status, _load_pending_shorts/_save_pending_shorts,
_load_short_pos/_save_short_pos, _open_short_protect, and
notify.send_short_open -- zero network calls, zero real file writes.
Mirrors dhan/test/test_reconcile_stale_exits.py's standalone script style
(no pytest in this repo).

Usage:
    python dhan/test/test_reconcile_pending_shorts.py

Exit 0 on all-pass, exit 1 on any failure.
"""

import sys
import types
from pathlib import Path
from unittest.mock import patch

_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

for _m in ("data_loader",):
    sys.modules.setdefault(_m, types.ModuleType(_m))

import dhan.run_trades as rt  # noqa: E402

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
failures = 0


def check(label: str, condition: bool, detail: str = "") -> None:
    global failures
    if condition:
        print(f"  {PASS}  {label}")
    else:
        failures += 1
        print(f"  {FAIL}  {label}" + (f"  ({detail})" if detail else ""))


print("Scenario (1) -- empty pending list: no-op, zero API calls")
with patch.object(rt, "_load_pending_shorts", lambda: []), \
     patch.object(rt, "_dhan_order_status", lambda oid: (_ for _ in ()).throw(
         AssertionError("should not check any order -- pending list is empty"))), \
     patch.object(rt, "_load_short_pos", lambda: (_ for _ in ()).throw(
         AssertionError("should not touch short positions at all"))):
    rt._reconcile_pending_shorts(dry_run=False)
check("(1) no-op on empty pending list", True)


print("\nScenario (2) -- pending order confirmed TRADED (the FISCHER bug): recovered")
pending2 = [{"symbol": "FISCHER", "order_id": "OID-F1", "source_exit_stage": "1159",
             "return_bucket": "5-10", "entry_date": "2026-10-26"}]
saved_pending = []
saved_shorts = []
protect_calls = []
notify_calls = []


def fake_protect(row, dry_run, circuit=None):
    protect_calls.append(row["symbol"])
    row["cover_target_order_id"] = "COVER1"
    row["stop_order_id"] = "STOP1"
    return row


with patch.object(rt, "_load_pending_shorts", lambda: pending2), \
     patch.object(rt, "_save_pending_shorts", lambda e: saved_pending.append(list(e))), \
     patch.object(rt, "_dhan_order_status",
                  lambda oid: {"orderStatus": "TRADED", "averageTradedPrice": 25.74142,
                               "filledQty": 6007}), \
     patch.object(rt, "_load_short_pos", lambda: []), \
     patch.object(rt, "_save_short_pos", lambda p: saved_shorts.append(p)), \
     patch.object(rt, "_open_short_protect", fake_protect), \
     patch.object(rt.notify, "send_short_open",
                  lambda **kw: notify_calls.append(kw)):
    rt._reconcile_pending_shorts(dry_run=False)

check("(2) recovered row saved into short positions", len(saved_shorts) == 1 and
      len(saved_shorts[0]) == 1, str(saved_shorts))
if saved_shorts and saved_shorts[0]:
    row = saved_shorts[0][0]
    check("(2) symbol/status/entry price/qty match the REAL broker fill, not guessed",
          row["symbol"] == "FISCHER" and row["status"] == "short_open" and
          row["entry_price"] == 25.7414 and row["quantity"] == 6007, str(row))
    check("(2) return_bucket/source_exit_stage carried over from the pending entry",
          row["return_bucket"] == "5-10" and row["source_exit_stage"] == "1159")
check("(2) protective cover-target/stop-loss placed via _open_short_protect",
      protect_calls == ["FISCHER"])
check("(2) entry notification sent (never went out the first time)",
      len(notify_calls) == 1 and notify_calls[0]["entry_price"] == 25.74142)
check("(2) resolved entry removed from the pending file",
      saved_pending == [[]], str(saved_pending))


print("\nScenario (3) -- pending order confirmed REJECTED: discarded, nothing created")
pending3 = [{"symbol": "GHOST", "order_id": "OID-G1", "source_exit_stage": "916",
             "return_bucket": "10-15", "entry_date": "2026-10-26"}]
saved_pending3 = []
with patch.object(rt, "_load_pending_shorts", lambda: pending3), \
     patch.object(rt, "_save_pending_shorts", lambda e: saved_pending3.append(list(e))), \
     patch.object(rt, "_dhan_order_status", lambda oid: {"orderStatus": "REJECTED"}), \
     patch.object(rt, "_load_short_pos", lambda: (_ for _ in ()).throw(
         AssertionError("a confirmed rejection must never touch short positions"))), \
     patch.object(rt, "_open_short_protect", lambda row, dry_run, circuit=None: (_ for _ in ()).throw(
         AssertionError("a confirmed rejection must never place protective orders"))):
    rt._reconcile_pending_shorts(dry_run=False)
check("(3) rejected order discarded, pending file now empty", saved_pending3 == [[]],
      str(saved_pending3))


print("\nScenario (4) -- pending order still PENDING/unresolved: left for next check")
pending4 = [{"symbol": "SLOW", "order_id": "OID-S1", "source_exit_stage": "916",
             "return_bucket": "15-20", "entry_date": "2026-10-26"}]
saved_pending4 = []
with patch.object(rt, "_load_pending_shorts", lambda: pending4), \
     patch.object(rt, "_save_pending_shorts", lambda e: saved_pending4.append(list(e))), \
     patch.object(rt, "_dhan_order_status", lambda oid: {"orderStatus": "PENDING"}), \
     patch.object(rt, "_load_short_pos", lambda: (_ for _ in ()).throw(
         AssertionError("an unresolved order must never touch short positions yet"))):
    rt._reconcile_pending_shorts(dry_run=False)
check("(4) still-unresolved entry kept in the pending file",
      saved_pending4 == [pending4], str(saved_pending4))


print("\nScenario (5) -- dry_run=True with a TRADED order: detects, never writes")
pending5 = [{"symbol": "DRYRUN", "order_id": "OID-D1", "source_exit_stage": "1159",
             "return_bucket": "5-10", "entry_date": "2026-10-26"}]
with patch.object(rt, "_load_pending_shorts", lambda: pending5), \
     patch.object(rt, "_save_pending_shorts", lambda e: (_ for _ in ()).throw(
         AssertionError("dry_run must never save the pending file"))), \
     patch.object(rt, "_dhan_order_status",
                  lambda oid: {"orderStatus": "TRADED", "averageTradedPrice": 50.0,
                               "filledQty": 100}), \
     patch.object(rt, "_load_short_pos", lambda: (_ for _ in ()).throw(
         AssertionError("dry_run must never load/save short positions"))), \
     patch.object(rt, "_open_short_protect", lambda row, dry_run, circuit=None: (_ for _ in ()).throw(
         AssertionError("dry_run must never place protective orders"))):
    rt._reconcile_pending_shorts(dry_run=True)
check("(5) dry_run detects without writing anything", True)


print("\n" + "─" * 60)
if failures:
    print(f"\033[31m{failures} scenario(s) FAILED\033[0m")
    sys.exit(1)
else:
    print("\033[32mAll scenarios PASSED\033[0m")
    sys.exit(0)
