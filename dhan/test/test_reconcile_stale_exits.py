#!/usr/bin/env python3
"""
test_reconcile_stale_exits.py -- standalone verifier for
dhan/run_trades.py's _reconcile_stale_exits() (the --reconcile CLI mode,
9:12am cron): closes out a long whose broker holding is already lower than
our records -- the recurring poll-timeout-then-late-fill bug first seen on
YUKEN (2026-10-07) and repeated on KOTIC/MMWL/SUNCLAY/INDOKEM/SIMPLEXINF
through 2026-10-15.

Mocks _broker_qty, security_id, and dhan.charges.get_trades (this function's
only broker-facing calls) plus _save_long_pos -- zero network calls, zero
real file writes. Mirrors dhan/test/test_targets.py's standalone script
style (no pytest in this repo).

Usage:
    python dhan/test/test_reconcile_stale_exits.py

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


def make_trades_module(rows):
    mod = types.ModuleType("dhan.charges")
    mod.get_trades = lambda frm, to: rows
    return mod


print("Scenario (1) -- broker qty matches local: left alone, no reconciliation")
positions1 = [{"broker": "dhan", "symbol": "ALPHA", "status": "open",
               "entry_date": "2026-10-19", "product": "CNC",
               "actual_fill_price": 100.0, "actual_fill_quantity": 50}]
with patch.object(rt, "_broker_qty", lambda sym, product: (50, "NSE_EQ")), \
     patch.object(rt, "_save_long_pos", lambda p: (_ for _ in ()).throw(
         AssertionError("should not save -- nothing changed"))):
    rt._reconcile_stale_exits(positions1, dry_run=False)
check("(1) status untouched", positions1[0]["status"] == "open")
check("(1) no exit fields added", "realized_pnl" not in positions1[0])


print("\nScenario (2) -- broker fully sold, /trades sum matches exactly: reconciled")
positions2 = [{"broker": "dhan", "symbol": "BETA", "status": "open",
               "entry_date": "2026-10-17", "product": "CNC",
               "actual_fill_price": 200.0, "actual_fill_quantity": 30}]
trades2 = [{"securityId": "99", "transactionType": "SELL", "tradedQuantity": 30,
            "tradedPrice": 210.0, "orderId": "OID1", "exchangeTime": "2026-10-18T09:16:30"}]
saved = []
with patch.object(rt, "_broker_qty", lambda sym, product: (0, "NSE_EQ")), \
     patch.object(rt, "security_id", lambda sym: "99"), \
     patch.dict(sys.modules, {"dhan.charges": make_trades_module(trades2)}), \
     patch.object(rt, "_save_long_pos", lambda p: saved.append(True)):
    rt._reconcile_stale_exits(positions2, dry_run=False)
check("(2) status -> exited_reconciled", positions2[0]["status"] == "exited_reconciled",
      positions2[0]["status"])
check("(2) exit price is the real traded price, not guessed",
      positions2[0]["exit_price_reconciled"] == 210.0)
check("(2) P&L = (210-200)*30 = 300", positions2[0]["realized_pnl"] == 300.0,
      str(positions2[0]["realized_pnl"]))
check("(2) exit timestamp carries the exchange time + IST offset",
      positions2[0]["exit_timestamp_reconciled"] == "2026-10-18T09:16:30+05:30")
check("(2) position file saved exactly once", saved == [True])


print("\nScenario (3) -- broker lower, but /trades sum does NOT match the delta: "
      "left alone, flagged (no guessing)")
positions3 = [{"broker": "dhan", "symbol": "GAMMA", "status": "open",
               "entry_date": "2026-10-17", "product": "MTF",
               "actual_fill_price": 50.0, "actual_fill_quantity": 20}]
trades3 = [{"securityId": "7", "transactionType": "SELL", "tradedQuantity": 5,
            "tradedPrice": 48.0, "orderId": "OID2", "exchangeTime": "2026-10-18T09:16:30"}]
with patch.object(rt, "_broker_qty", lambda sym, product: (0, "NSE_EQ")), \
     patch.object(rt, "security_id", lambda sym: "7"), \
     patch.dict(sys.modules, {"dhan.charges": make_trades_module(trades3)}), \
     patch.object(rt, "_save_long_pos", lambda p: (_ for _ in ()).throw(
         AssertionError("should not save -- mismatch must not be guessed"))):
    rt._reconcile_stale_exits(positions3, dry_run=False)
check("(3) left open rather than guessed", positions3[0]["status"] == "open")


print("\nScenario (4) -- broker HIGHER than local (unexpected growth): left alone, flagged")
positions4 = [{"broker": "dhan", "symbol": "DELTA", "status": "open",
               "entry_date": "2026-10-17", "product": "CNC",
               "actual_fill_price": 10.0, "actual_fill_quantity": 10}]
with patch.object(rt, "_broker_qty", lambda sym, product: (15, "NSE_EQ")), \
     patch.object(rt, "_save_long_pos", lambda p: (_ for _ in ()).throw(
         AssertionError("should not save -- unexpected growth must not be guessed"))):
    rt._reconcile_stale_exits(positions4, dry_run=False)
check("(4) left open rather than guessed", positions4[0]["status"] == "open")


print("\nScenario (5) -- dry_run=True: detects and prints, but never writes")
positions5 = [{"broker": "dhan", "symbol": "EPSILON", "status": "open",
               "entry_date": "2026-10-17", "product": "CNC",
               "actual_fill_price": 100.0, "actual_fill_quantity": 10}]
trades5 = [{"securityId": "55", "transactionType": "SELL", "tradedQuantity": 10,
            "tradedPrice": 105.0, "orderId": "OID3", "exchangeTime": "2026-10-18T09:16:30"}]
with patch.object(rt, "_broker_qty", lambda sym, product: (0, "NSE_EQ")), \
     patch.object(rt, "security_id", lambda sym: "55"), \
     patch.dict(sys.modules, {"dhan.charges": make_trades_module(trades5)}), \
     patch.object(rt, "_save_long_pos", lambda p: (_ for _ in ()).throw(
         AssertionError("dry_run must never save"))):
    rt._reconcile_stale_exits(positions5, dry_run=True)
check("(5) dry_run leaves the record untouched", positions5[0]["status"] == "open")


print("\n" + "─" * 60)
if failures:
    print(f"\033[31m{failures} scenario(s) FAILED\033[0m")
    sys.exit(1)
else:
    print("\033[32mAll scenarios PASSED\033[0m")
    sys.exit(0)
