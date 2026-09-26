#!/usr/bin/env python3
"""
test_return_buckets.py -- standalone verifier for the return-bucketed exit
schedule added to dhan/run_trades.py (2026-09-26): RETURN_BUCKETS,
_return_bucket_for_pct, _bucket_of, _load_return_pct_map, and the
return_bucket propagation through _open_short_place.

The single highest-stakes property tested here is the 4-way PARTITION
invariant: every open long position / open short must be claimed by
EXACTLY ONE of the 4 exit-stage invocations (legacy + "5-10" + "10-15" +
"15-20") -- never zero (stranded, never exited) and never more than one
(double-sold). check_exit_916/force_exit_1159/square_off_239 all filter via
`[p for p in _open_pos(positions) if _bucket_of(p) == bucket_label]` (or
_open_short_pos for shorts) -- this file replicates that exact expression
against a synthetic position set spanning all 4 buckets.

Mirrors test_targets.py's standalone script style (no pytest in this repo).

Usage:
    python dhan/test/test_return_buckets.py

Exit 0 on all-pass, exit 1 on any failure.
"""

import csv
import sys
import tempfile
import types
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

for _m in ("data_loader",):
    sys.modules.setdefault(_m, types.ModuleType(_m))

import dhan.run_trades as rt  # noqa: E402  (import after sys.path/stub setup)

# _tick_round() looks up each symbol's real tick size via dhan.trade
# .tick_size() -- none of this suite's fictional symbols exist in the real
# scrip master, so pin a flat tick size (matches test_targets.py's own setup).
patch.object(rt, "tick_size", lambda sym: 0.05).start()

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


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (a) — _return_bucket_for_pct boundary checks\n")
# ─────────────────────────────────────────────────────────────────────────────
# Ranges are 5.00-10.00 / 10.01-15.00 / 15.01-20.00+ -- each bucket's lower
# bound is EXCLUSIVE of the previous bucket's top (2026-09-26 correction).

check("(a1) 4.99% -> defensive '5-10'", rt._return_bucket_for_pct(4.99) == "5-10")
check("(a2) exactly 5.0% -> '5-10'",    rt._return_bucket_for_pct(5.0)  == "5-10")
check("(a3) 9.99% -> '5-10'",           rt._return_bucket_for_pct(9.99) == "5-10")
check("(a4) exactly 10.0% -> '5-10' (NOT '10-15')", rt._return_bucket_for_pct(10.0) == "5-10")
check("(a5) 10.01% -> '10-15'",         rt._return_bucket_for_pct(10.01) == "10-15")
check("(a6) 14.99% -> '10-15'",         rt._return_bucket_for_pct(14.99) == "10-15")
check("(a7) exactly 15.0% -> '10-15' (NOT '15-20')", rt._return_bucket_for_pct(15.0) == "10-15")
check("(a8) 15.01% -> '15-20'",         rt._return_bucket_for_pct(15.01) == "15-20")
check("(a9) 20.0% -> '15-20'",          rt._return_bucket_for_pct(20.0) == "15-20")
check("(a10) 37.0% (>20%) -> '15-20'",  rt._return_bucket_for_pct(37.0) == "15-20")
check("(a11) never returns 'legacy'",
      all(rt._return_bucket_for_pct(x) != "legacy" for x in (0.0, 5.0, 10.0, 15.0, 20.0, 99.0)))


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (b) — RETURN_BUCKETS structural checks\n")
# ─────────────────────────────────────────────────────────────────────────────

check("(b1) all 4 buckets present", set(rt.RETURN_BUCKETS) == {"legacy", "5-10", "10-15", "15-20"})
check("(b2) legacy bucket reuses the untouched module constants exactly",
      rt.RETURN_BUCKETS["legacy"]["exit_916_prep"]  == rt._EXIT_916_PREP_AT
      and rt.RETURN_BUCKETS["legacy"]["exit_916_fire"]  == rt._EXIT_916_FIRE_AT
      and rt.RETURN_BUCKETS["legacy"]["exit_1159_prep"] == rt._EXIT_1159_PREP_AT
      and rt.RETURN_BUCKETS["legacy"]["exit_1159_fire"] == rt._EXIT_1159_FIRE_AT
      and rt.RETURN_BUCKETS["legacy"]["squareoff_prep"] == rt._SQUAREOFF_PREP_AT
      and rt.RETURN_BUCKETS["legacy"]["squareoff_fire"] == rt._SQUAREOFF_FIRE_AT)
check("(b3) every bucket has all 6 schedule keys",
      all(set(v) == {"exit_916_prep", "exit_916_fire", "exit_1159_prep",
                     "exit_1159_fire", "squareoff_prep", "squareoff_fire"}
          for v in rt.RETURN_BUCKETS.values()))
check("(b4) 5-10 bucket's winner-exit fires at 9:16 (matches legacy's time)",
      rt.RETURN_BUCKETS["5-10"]["exit_916_fire"] == (9, 16, 0))
check("(b5) 10-15 bucket's winner-exit fires at 9:18",
      rt.RETURN_BUCKETS["10-15"]["exit_916_fire"] == (9, 18, 0))
check("(b6) 15-20 bucket's winner-exit fires at 9:44",
      rt.RETURN_BUCKETS["15-20"]["exit_916_fire"] == (9, 44, 0))
check("(b7) 15-20 bucket's short square-off fires at 14:01 (before its own force-exit's "
      "clock time of day -- i.e. 2:01pm, not 11:48am -- sanity-checking no am/pm mixup)",
      rt.RETURN_BUCKETS["15-20"]["squareoff_fire"] == (14, 1, 0))


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (c) — _bucket_of\n")
# ─────────────────────────────────────────────────────────────────────────────

check("(c1) no return_bucket key at all -> legacy", rt._bucket_of({}) == "legacy")
check("(c2) return_bucket explicitly None -> legacy", rt._bucket_of({"return_bucket": None}) == "legacy")
check("(c3) return_bucket explicitly '' -> legacy", rt._bucket_of({"return_bucket": ""}) == "legacy")
check("(c4) recorded bucket passes through unchanged",
      rt._bucket_of({"return_bucket": "10-15"}) == "10-15")


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (d) — partition/completeness invariant (long positions)\n")
# ─────────────────────────────────────────────────────────────────────────────
# One synthetic long per bucket (incl. legacy), run through the EXACT filter
# expression check_exit_916/force_exit_1159 use. Every position must be
# claimed by exactly one of the 4 invocations -- no orphans, no double-claims.

synthetic_longs = [
    {"broker": "dhan", "status": "open", "symbol": "LEGACY1"},                       # no key at all
    {"broker": "dhan", "status": "open", "symbol": "LEGACY2", "return_bucket": None}, # explicit None
    {"broker": "dhan", "status": "open", "symbol": "A", "return_bucket": "5-10"},
    {"broker": "dhan", "status": "open", "symbol": "B", "return_bucket": "10-15"},
    {"broker": "dhan", "status": "open", "symbol": "C", "return_bucket": "15-20"},
    {"broker": "dhan", "status": "partial_exit_916_nodata", "symbol": "D", "return_bucket": "5-10"},
]

open_ps = rt._open_pos(synthetic_longs)
check("(d1) _open_pos claims all 6 synthetic positions (both eligible statuses)",
      len(open_ps) == 6, str(open_ps))

invocation_labels = ["legacy", "5-10", "10-15", "15-20"]
claimed_longs = {
    label: [p for p in open_ps if rt._bucket_of(p) == label]
    for label in invocation_labels
}

check("(d2) every position claimed exactly once across the 4 invocations",
      sum(len(v) for v in claimed_longs.values()) == len(open_ps),
      {k: [p["symbol"] for p in v] for k, v in claimed_longs.items()})

seen_syms: list[str] = []
for v in claimed_longs.values():
    seen_syms.extend(p["symbol"] for p in v)
check("(d3) no position claimed by more than one bucket invocation",
      len(seen_syms) == len(set(seen_syms)), seen_syms)
check("(d4) union of all 4 invocations covers every open position",
      set(seen_syms) == {p["symbol"] for p in open_ps})
check("(d5) legacy invocation claims exactly LEGACY1 + LEGACY2",
      {p["symbol"] for p in claimed_longs["legacy"]} == {"LEGACY1", "LEGACY2"})
check("(d6) '5-10' invocation claims exactly A + D",
      {p["symbol"] for p in claimed_longs["5-10"]} == {"A", "D"})


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (e) — partition/completeness invariant (short positions)\n")
# ─────────────────────────────────────────────────────────────────────────────

synthetic_shorts = [
    {"broker": "dhan", "status": "short_open", "symbol": "SLEGACY"},
    {"broker": "dhan", "status": "short_open", "symbol": "SA", "return_bucket": "5-10"},
    {"broker": "dhan", "status": "short_open", "symbol": "SB", "return_bucket": "10-15"},
    {"broker": "dhan", "status": "short_open", "symbol": "SC", "return_bucket": "15-20"},
]

open_shorts = rt._open_short_pos(synthetic_shorts)
claimed_shorts = {
    label: [p for p in open_shorts if rt._bucket_of(p) == label]
    for label in invocation_labels
}
seen_short_syms: list[str] = []
for v in claimed_shorts.values():
    seen_short_syms.extend(p["symbol"] for p in v)

check("(e1) every short claimed exactly once", len(seen_short_syms) == len(set(seen_short_syms)))
check("(e2) union covers every open short", set(seen_short_syms) == {p["symbol"] for p in open_shorts})
check("(e3) '15-20' invocation claims exactly SC",
      {p["symbol"] for p in claimed_shorts["15-20"]} == {"SC"})


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (f) — _open_short_place threads return_bucket onto the short row\n")
# ─────────────────────────────────────────────────────────────────────────────

sell_calls_f: list[tuple] = []
def fake_sell_f(symbol, exch, qty, **kw):
    sell_calls_f.append((symbol, exch, qty, kw.get("order_type"), kw.get("price"), kw.get("product")))
    return "SHORTOPEN-F"

with patch.object(rt, "sell", fake_sell_f), \
     patch.object(rt, "_shorting_skipped_today", lambda: False), \
     patch.object(rt.notify, "send_short_open", MagicMock()):
    row_f = rt._open_short_place(
        "XYZ", 10, "916", dry_run=True, ltp=100.0,
        balance=rt._BalanceTracker(1_000_000.0),
        precomputed_margins={"XYZ": {"qty": 10, "margin_info": {"leverage": 3.0, "margin_required": 500.0}}},
        return_bucket="10-15",
    )

check("(f1) short row built (not None)", row_f is not None, row_f)
check("(f2) return_bucket threaded onto the short row", row_f is not None and row_f["return_bucket"] == "10-15")
check("(f3) _bucket_of resolves the threaded row to '10-15'",
      row_f is not None and rt._bucket_of(row_f) == "10-15")

# No return_bucket passed (the standalone manual _open_short()/_open_short_core()
# call shape) -- must default to None, which _bucket_of reads as "legacy".
with patch.object(rt, "sell", fake_sell_f), \
     patch.object(rt, "_shorting_skipped_today", lambda: False), \
     patch.object(rt.notify, "send_short_open", MagicMock()):
    row_f_default = rt._open_short_place(
        "XYZ2", 10, "916", dry_run=True, ltp=100.0,
        balance=rt._BalanceTracker(1_000_000.0),
        precomputed_margins={"XYZ2": {"qty": 10, "margin_info": {"leverage": 3.0, "margin_required": 500.0}}},
    )
check("(f4) return_bucket omitted -> row's return_bucket is None",
      row_f_default is not None and row_f_default["return_bucket"] is None)
check("(f5) _bucket_of resolves the un-tagged row to 'legacy' (safe default)",
      row_f_default is not None and rt._bucket_of(row_f_default) == "legacy")


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (g) — _load_return_pct_map parsing\n")
# ─────────────────────────────────────────────────────────────────────────────

_tmp_dir = tempfile.mkdtemp()
_tmp_trade_date = date(2099, 1, 1)   # far-future date, guaranteed not to collide with a real file
_tmp_trades_dir = Path(_tmp_dir) / "results" / "trades"
_tmp_trades_dir.mkdir(parents=True)
_tmp_path = _tmp_trades_dir / f"trade_list_{_tmp_trade_date.isoformat()}.csv"
with open(_tmp_path, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["symbol", "shares", "ref_price", "return_pct"])
    w.writeheader()
    w.writerow({"symbol": "GOODCO",  "shares": 10, "ref_price": 100.0, "return_pct": 12.34})
    w.writerow({"symbol": "EMPTYCO", "shares": 10, "ref_price": 100.0, "return_pct": ""})
    w.writerow({"symbol": "GARBAGECO", "shares": 10, "ref_price": 100.0, "return_pct": "not-a-number"})

with patch.object(rt, "_RESULTS_DIR", Path(_tmp_dir) / "results"):
    pct_map = rt._load_return_pct_map(_tmp_trade_date)

check("(g1) valid row parsed into the map", pct_map.get("GOODCO") == 12.34, pct_map)
check("(g2) empty return_pct OMITTED (not defaulted to 0.0)", "EMPTYCO" not in pct_map, pct_map)
check("(g3) unparsable return_pct OMITTED (not defaulted to 0.0)", "GARBAGECO" not in pct_map, pct_map)
check("(g4) map has exactly one entry", len(pct_map) == 1, pct_map)

try:
    import shutil
    shutil.rmtree(_tmp_dir, ignore_errors=True)
except Exception:
    pass


# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'─' * 55}")
if failures == 0:
    print("\033[32mAll scenarios PASSED\033[0m")
    sys.exit(0)
else:
    print(f"\033[31m{failures} assertion(s) FAILED\033[0m")
    sys.exit(1)
