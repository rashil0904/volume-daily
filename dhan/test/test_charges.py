#!/usr/bin/env python3
"""
test_charges.py -- standalone verifier for dhan/charges.py's multi-order-id
attribution fix (2026-09-28).

Background: run_entry_limit's tranched entries (LIMIT tranches + a MARKET
cutoff sweep, each its own distinct Dhan orderId) used to save the literal
placeholder entry_order_id="limit_entry_multi" -- a string that can never
match a real orderId in Dhan's trade-book, permanently stranding that leg's
real brokerage/STT/exchange/SEBI/stamp/GST charges at 0/"pending" forever
(not just until the next day's sync, like a genuine same-day pending leg).
Confirmed live: 10/247 real positions carried this placeholder.

The fix: run_entry_limit now saves the REAL list of every order id used
across the entry (see _LimitSymState.entry_order_ids in run_trades.py), and
every charges.py function that reads entry_order_id was updated to accept
EITHER a single order-id string (run_entry_321, every exit leg -- unchanged
behavior) OR a list of order ids (a tranche entry), via _oid_list()/
_oid_key(). This file tests only the NEW list-handling paths -- charges.py
had no existing test coverage before this file.

Mirrors test_return_buckets.py's standalone script style (no pytest here).

Usage:
    python dhan/test/test_charges.py

Exit 0 on all-pass, exit 1 on any failure.
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ROOT))

import dhan.charges as ch  # noqa: E402  (import after sys.path setup)

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


def _trade(oid: str, **fields) -> dict:
    base = {"orderId": oid, "brokerageCharges": 0.0, "stt": 0.0,
            "exchangeTransactionCharges": 0.0, "sebiTax": 0.0,
            "stampDuty": 0.0, "serviceTax": 0.0}
    base.update(fields)
    return base


# ═════════════════════════════════════════════════════════════════════════
print("\n[1] _oid_list / _oid_key -- normalizing str | list | None")
# ═════════════════════════════════════════════════════════════════════════

check("(1a) single string -> one-element list", ch._oid_list("ORD1") == ["ORD1"])
check("(1b) list passes through unchanged", ch._oid_list(["ORD1", "ORD2"]) == ["ORD1", "ORD2"])
check("(1c) None -> []", ch._oid_list(None) == [])
check("(1d) empty string -> []", ch._oid_list("") == [])
check("(1e) empty list -> []", ch._oid_list([]) == [])

check("(1f) _oid_key single string passes through", ch._oid_key("ORD1") == "ORD1")
check("(1g) _oid_key list joins with '+'", ch._oid_key(["ORD1", "ORD2"]) == "ORD1+ORD2")
check("(1h) _oid_key None -> None", ch._oid_key(None) is None)


# ═════════════════════════════════════════════════════════════════════════
print("\n[2] _leg_charges -- multi-id summing, matches single-id semantics "
      "when fully/partially/never resolved")
# ═════════════════════════════════════════════════════════════════════════

trade_index_full = {
    "ORD1": _trade("ORD1", brokerageCharges=10.0, stt=5.0),
    "ORD2": _trade("ORD2", brokerageCharges=8.0, stt=3.0),
}

price_full, source_full = ch._leg_charges(["ORD1", "ORD2"], 100, 282.74, "MTF", "BUY", trade_index_full)
check("(2a) both ids resolved -> sums across both, source=api",
      price_full == 26.0 and source_full == "api", (price_full, source_full))

price_partial, source_partial = ch._leg_charges(
    ["ORD1", "ORD_MISSING"], 100, 282.74, "MTF", "BUY", trade_index_full)
check("(2b) one id resolved, one missing -> partial real sum (not 0), source=pending",
      price_partial == 15.0 and source_partial == "pending", (price_partial, source_partial))

price_none, source_none = ch._leg_charges(
    ["ORD_MISSING1", "ORD_MISSING2"], 100, 282.74, "MTF", "BUY", trade_index_full)
check("(2c) neither id resolved -> 0.0/pending (same as old single-id miss)",
      price_none == 0.0 and source_none == "pending", (price_none, source_none))

price_single, source_single = ch._leg_charges("ORD1", 100, 282.74, "MTF", "BUY", trade_index_full)
check("(2d) plain single-id string still works exactly as before",
      price_single == 15.0 and source_single == "api", (price_single, source_single))

price_zero_qty, source_zero_qty = ch._leg_charges(["ORD1", "ORD2"], 0, 282.74, "MTF", "BUY", trade_index_full)
check("(2e) zero qty short-circuits to 0.0/none regardless of id list",
      price_zero_qty == 0.0 and source_zero_qty == "none", (price_zero_qty, source_zero_qty))


# ═════════════════════════════════════════════════════════════════════════
print("\n[3] product_map / tracked_order_ids -- fan out a list entry_order_id "
      "into one entry per real order id")
# ═════════════════════════════════════════════════════════════════════════

fake_positions = [
    {"entry_order_id": ["ORD1", "ORD2"], "product": "MTF"},
    {"entry_order_id": "ORD3", "product": "CNC"},
]

import unittest.mock as mock  # noqa: E402
with mock.patch.object(ch, "_load_all_positions", lambda: fake_positions):
    pm = ch.product_map()
check("(3a) product_map fans a list entry_order_id out to one entry per id",
      pm.get("ORD1") == "MTF" and pm.get("ORD2") == "MTF", pm)
check("(3b) product_map still handles a plain single-id string",
      pm.get("ORD3") == "CNC", pm)

fake_positions_tracked = [
    {"entry_order_id": ["ORD1", "ORD2"], "exit_order_id_916": "ORD9"},
]
with mock.patch.object(ch, "_load_all_positions", lambda: fake_positions_tracked):
    ids = ch.tracked_order_ids()
check("(3c) tracked_order_ids fans a list value out (not added as an unhashable whole)",
      ids == {"ORD1", "ORD2", "ORD9"}, ids)


# ═════════════════════════════════════════════════════════════════════════
print("\n[4] position_charge_breakdown -- entry_oid as a list, exit_oid a "
      "plain string, categories summed across ALL of them")
# ═════════════════════════════════════════════════════════════════════════

trade_index_bd = {
    "ENTRY_A": _trade("ENTRY_A", brokerageCharges=4.0, stt=1.0),
    "ENTRY_B": _trade("ENTRY_B", brokerageCharges=2.0, stt=0.5),
    "EXIT_1":  _trade("EXIT_1",  brokerageCharges=3.0, stt=0.0),
}
pos_bd = {
    # product=CNC (not MTF) -- keeps this scenario focused on the category
    # split, without also exercising the MTF-interest formula fallback
    # (which would call the real funded_amount() API for a symbol that
    # doesn't exist).
    "direction": "long", "product": "CNC", "status": "exited_916",
    "entry_order_id": ["ENTRY_A", "ENTRY_B"],
    "exit_order_id_916": "EXIT_1",
    "actual_fill_quantity": 100, "actual_fill_price": 282.74,
    "exit_price_916": 300.0, "entry_date": "2026-09-28",
}
breakdown = ch.position_charge_breakdown(pos_bd, trade_index=trade_index_bd, interest_index={})
check("(4a) Brokerage summed across both entry-leg ids AND the exit leg",
      breakdown["Brokerage"] == 9.0, breakdown)
check("(4b) STT summed across both entry-leg ids AND the exit leg",
      breakdown["STT"] == 1.5, breakdown)


# ═════════════════════════════════════════════════════════════════════════
print("\n[5] mtf_interest_allocation_index + position_charge_summary -- write "
      "and read the SAME _oid_key() for a list entry_order_id (a plain dict "
      "lookup with a list key would raise TypeError: unhashable type)")
# ═════════════════════════════════════════════════════════════════════════

interest_index_multi = {ch._oid_key(["ENTRY_A", "ENTRY_B"]): 12.34}
pos_interest = {
    "direction": "long", "product": "MTF", "status": "open",
    "entry_order_id": ["ENTRY_A", "ENTRY_B"],
    "actual_fill_quantity": 100, "actual_fill_price": 282.74,
    "entry_date": "2026-09-28",
}
try:
    summary = ch.position_charge_summary(pos_interest, trade_index={}, interest_index=interest_index_multi)
    crashed = False
except TypeError as exc:
    crashed = True
    summary = None
check("(5a) a list entry_order_id never crashes the interest_index lookup",
      not crashed, "raised TypeError (list used as a raw dict key)")
check("(5b) the real ledger-based interest allocation IS found via the shared "
      "_oid_key() derivation (not silently missed and re-estimated)",
      summary is not None and summary["mtf_interest"] == 12.34 and summary["interest_source"] == "api",
      summary)


# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'─' * 55}")
if failures == 0:
    print("\033[32mAll scenarios PASSED\033[0m")
    sys.exit(0)
else:
    print(f"\033[31m{failures} assertion(s) FAILED\033[0m")
    sys.exit(1)
