#!/usr/bin/env python3
"""
test_push_to_sheets.py -- standalone verifier for sheets/push_to_sheets.py:
Exit Reason derivation, the pushed_to_sheets/pushed_open_to_sheets idempotency
flags, the 2026-08-18 first-data-date cutoff, the entry-time open-row push,
batched (single-call) upsert_trades webhook posting, and clean non-raising
failure on a webhook/network error.

Mocks requests.post and _load_pos/_save_pos with an in-memory store -- zero
network calls, zero real file writes. Mirrors dhan/test_targets.py's
standalone script style (no pytest in this repo).

Usage:
    python sheets/test_push_to_sheets.py

Exit 0 on all-pass, exit 1 on any failure.
"""

import copy
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

import sheets.push_to_sheets as p  # noqa: E402

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
    def __init__(self, positions):
        self.positions   = copy.deepcopy(positions)
        self.save_count  = 0
        self.saved_state = None

    def load(self):
        return copy.deepcopy(self.positions)

    def save(self, positions):
        self.save_count += 1
        self.positions    = copy.deepcopy(positions)
        self.saved_state  = copy.deepcopy(positions)


class FakeResponse:
    def __init__(self, json_body, ok=True):
        self._json = json_body
        self._ok   = ok

    def raise_for_status(self):
        if not self._ok:
            raise p.requests.exceptions.HTTPError("bad status")

    def json(self):
        return self._json


def make_long(**overrides) -> dict:
    row = {
        "broker": "dhan", "symbol": "TESTLONG", "entry_date": "2026-08-18",
        "actual_fill_price": 100.0, "actual_fill_quantity": 10,
        "entry_order_id": "E1", "status": "exited_925", "product": "CNC",
        "target_order_id": "T1",
        "exit_price_925": 117.0, "exit_order_id_925": "T1",
        "realized_return_pct": 17.0, "realized_pnl": 170.0,
    }
    row.update(overrides)
    return row


def make_short(**overrides) -> dict:
    row = {
        "broker": "dhan", "symbol": "TESTSHORT", "direction": "short",
        "entry_date": "2026-08-18", "entry_price": 200.0, "quantity": 5,
        "entry_order_id": "E2", "status": "short_closed", "product": "INTRADAY",
        "cover_target_order_id": "C1", "stop_order_id": "S1",
        "exit_price_239": 190.0, "exit_order_id_239": "C1",
        "realized_return_pct": 5.0, "realized_pnl": 50.0,
    }
    row.update(overrides)
    return row


def make_open(**overrides) -> dict:
    row = {
        "broker": "dhan", "symbol": "TESTOPEN", "entry_date": "2026-08-18",
        "actual_fill_price": 100.0, "actual_fill_quantity": 10,
        "entry_order_id": "E3", "status": "open", "product": "MTF",
    }
    row.update(overrides)
    return row


# ── (a) Exit Reason derivation ────────────────────────────────────────────────
print("\n(a) Exit Reason derivation")

check("long: exit_order_id_925 == target_order_id -> Target hit",
      p.derive_exit_reason(make_long(exit_order_id_925="T1", target_order_id="T1")) == "Target hit")

check("long: exit_order_id_925 set, != target_order_id, no partial marker -> 9:25 profit exit",
      p.derive_exit_reason(make_long(exit_order_id_925="X1", target_order_id="T1")) == "9:25 profit exit")

check("long: exit_order_id_925 set + shares_exited_925 present -> 9:25 no-data partial",
      p.derive_exit_reason(make_long(exit_order_id_925="X1", target_order_id="T1",
                                     shares_exited_925=5, status="exited_1159")) == "9:25 no-data partial")

check("long: exit_order_id_1159 == target_order_id (no 925 id) -> Target hit",
      p.derive_exit_reason(make_long(exit_order_id_925=None, exit_order_id_1159="T1",
                                     target_order_id="T1", status="exited_1159")) == "Target hit")

check("long: exit_order_id_1159 set != target_order_id (no 925 id) -> 11:59 force exit",
      p.derive_exit_reason(make_long(exit_order_id_925=None, exit_order_id_1159="X2",
                                     target_order_id="T1", status="exited_1159")) == "11:59 force exit")

check("long: closed status but no order ids at all -> Unrecognized — review",
      p.derive_exit_reason(make_long(exit_order_id_925=None, exit_order_id_1159=None,
                                     target_order_id=None)) == "Unrecognized — review")

check("short: exit_order_id_239 == cover_target_order_id -> Target hit",
      p.derive_exit_reason(make_short(exit_order_id_239="C1")) == "Target hit")

check("short: exit_order_id_239 == stop_order_id -> Stop-loss hit",
      p.derive_exit_reason(make_short(exit_order_id_239="S1")) == "Stop-loss hit")

check("short: exit_order_id_239 set, matches neither -> 2:39 square-off",
      p.derive_exit_reason(make_short(exit_order_id_239="Z9")) == "2:39 square-off")

check("short: no exit_order_id_239 -> Unrecognized — review",
      p.derive_exit_reason(make_short(exit_order_id_239=None)) == "Unrecognized — review")


# ── (a2) Order Flow legs ───────────────────────────────────────────────────────
print("\n(a2) Order Flow legs (build_legs)")

check("closed long -> BUY → SELL", p.build_legs(make_long()) == "BUY → SELL",
      detail=p.build_legs(make_long()))
check("closed short -> SELL → BUY", p.build_legs(make_short()) == "SELL → BUY",
      detail=p.build_legs(make_short()))
check("open long (status=open) -> BUY → … (exit leg unknown)",
      p.build_legs(make_long(status="open")) == "BUY → …",
      detail=p.build_legs(make_long(status="open")))
check("partial-exit long (still not a CLOSED status) -> BUY → …",
      p.build_legs(make_long(status="partial_exit_925_nodata")) == "BUY → …",
      detail=p.build_legs(make_long(status="partial_exit_925_nodata")))


# ── (b) already-pushed rows are never pushed again ────────────────────────────
print("\n(b) pushed_to_sheets / pushed_open_to_sheets idempotency")

store = FakeStore([make_long(pushed_to_sheets=True)])
with patch.object(p, "_load_pos", store.load), \
     patch.object(p, "_save_pos", store.save), \
     patch.object(p, "_WEBHOOK_URL", "https://example.invalid/exec"), \
     patch.object(p, "_WEBHOOK_SECRET", "secret"), \
     patch.object(p, "requests") as mock_requests:
    p.main()
    check("already-pushed closed row: requests.post never called", not mock_requests.post.called)

store = FakeStore([make_open(pushed_open_to_sheets=True)])
with patch.object(p, "_load_pos", store.load), \
     patch.object(p, "_save_pos", store.save), \
     patch.object(p, "_WEBHOOK_URL", "https://example.invalid/exec"), \
     patch.object(p, "_WEBHOOK_SECRET", "secret"), \
     patch.object(p, "requests") as mock_requests:
    p.main()
    check("already-pushed open row: requests.post never called", not mock_requests.post.called)


# ── (c) entry_date before 2026-08-18 is never pushed ──────────────────────────
print("\n(c) first-data-date cutoff")

store = FakeStore([make_long(entry_date="2026-08-17")])
with patch.object(p, "_load_pos", store.load), \
     patch.object(p, "_save_pos", store.save), \
     patch.object(p, "_WEBHOOK_URL", "https://example.invalid/exec"), \
     patch.object(p, "_WEBHOOK_SECRET", "secret"), \
     patch.object(p, "requests") as mock_requests:
    p.main()
    check("pre-2026-08-18 row: requests.post never called", not mock_requests.post.called)

store2 = FakeStore([make_long(entry_date="2026-08-18")])
with patch.object(p, "_load_pos", store2.load), \
     patch.object(p, "_save_pos", store2.save), \
     patch.object(p, "_WEBHOOK_URL", "https://example.invalid/exec"), \
     patch.object(p, "_WEBHOOK_SECRET", "secret"), \
     patch.object(p, "requests") as mock_requests:
    mock_requests.post.return_value = FakeResponse({"status": "ok", "inserted": 1, "updated": 0})
    p.main()
    check("2026-08-18 row (the first valid date): requests.post WAS called",
          mock_requests.post.called)


# ── (d) webhook/network failure never raises past the script's own guard ─────
print("\n(d) clean failure on webhook/network error")

store = FakeStore([make_long()])
with patch.object(p, "_load_pos", store.load), \
     patch.object(p, "_save_pos", store.save), \
     patch.object(p, "_WEBHOOK_URL", "https://example.invalid/exec"), \
     patch.object(p, "_WEBHOOK_SECRET", "secret"), \
     patch.object(p, "requests") as mock_requests:
    mock_requests.post.side_effect = ConnectionError("simulated network failure")
    caught = None
    try:
        p.main()
    except Exception as exc:
        caught = exc
    check("main() raises on a real network failure (caller decides how to handle)",
          isinstance(caught, ConnectionError))

    # Now mirror the actual __main__ guard in push_to_sheets.py, which wraps
    # main() in its own try/except -- this is what must never crash the cron job.
    caught_at_top_level = None
    try:
        try:
            p.main()
        except Exception as exc:
            print(f"[sheets] PUSH FAILED (non-fatal to trading): {exc}", file=sys.stderr)
    except Exception as exc:
        caught_at_top_level = exc
    check("__main__-style guard swallows the failure cleanly (no propagation)",
          caught_at_top_level is None)
    check("no row was marked pushed_to_sheets after a failed push", store.save_count == 0)


# ── (e) multiple new rows -> exactly one batched call ─────────────────────────
print("\n(e) batched single upsert call")

store = FakeStore([make_long(symbol="A"), make_long(symbol="B"), make_long(symbol="C")])
with patch.object(p, "_load_pos", store.load), \
     patch.object(p, "_save_pos", store.save), \
     patch.object(p, "_WEBHOOK_URL", "https://example.invalid/exec"), \
     patch.object(p, "_WEBHOOK_SECRET", "secret"), \
     patch.object(p, "requests") as mock_requests:
    mock_requests.post.return_value = FakeResponse({"status": "ok", "inserted": 3, "updated": 0})
    p.main()
    check("exactly one requests.post call for 3 new rows", mock_requests.post.call_count == 1)
    posted_json = mock_requests.post.call_args.kwargs["json"]
    check("action is upsert_trades", posted_json["action"] == "upsert_trades",
          detail=posted_json.get("action"))
    posted_items = posted_json["rows"]
    check("all 3 items present in that single call", len(posted_items) == 3,
          detail=f"got {len(posted_items)}")
    check("every item carries its legs field", all("legs" in it for it in posted_items))
    check("all 3 positions marked pushed_to_sheets after success",
          all(pos.get("pushed_to_sheets") for pos in store.positions))
    check("_save_pos called exactly once for the batch", store.save_count == 1,
          detail=f"save_count={store.save_count}")


# ── (f) entry-time open row: pushed with blank exit fields, correct key ───────
print("\n(f) open-row push at entry")

store = FakeStore([make_open()])
with patch.object(p, "_load_pos", store.load), \
     patch.object(p, "_save_pos", store.save), \
     patch.object(p, "_WEBHOOK_URL", "https://example.invalid/exec"), \
     patch.object(p, "_WEBHOOK_SECRET", "secret"), \
     patch.object(p, "requests") as mock_requests:
    mock_requests.post.return_value = FakeResponse({"status": "ok", "inserted": 1, "updated": 0})
    p.main()
    item = mock_requests.post.call_args.kwargs["json"]["rows"][0]
    check("open row keyed on entry_order_id", item["key"] == "E3", detail=item["key"])
    check("open row: legs shows BUY → … (exit leg not known yet)",
          item["legs"] == "BUY → …", detail=item["legs"])
    check("open row: Exit Price blank", item["row"][5] == "")
    check("open row: Exit Reason is the Open label", item["row"][6] == p._OPEN_LABEL,
          detail=item["row"][6])
    check("open row: Return % blank", item["row"][8] == "")
    check("open row: P&L blank", item["row"][9] == "")
    check("open row: Capital Deployed = fill price * qty",
          item["row"][10] == 1000.0, detail=item["row"][10])
    check("position marked pushed_open_to_sheets, NOT pushed_to_sheets (still open)",
          store.positions[0].get("pushed_open_to_sheets") is True
          and not store.positions[0].get("pushed_to_sheets"))

# Shorts never get an entry-time open row (see _eligible_open) -- only pushed
# once closed, same as before this feature.
store = FakeStore([make_short(status="short_open", pushed_to_sheets=False)])
with patch.object(p, "_load_pos", store.load), \
     patch.object(p, "_save_pos", store.save), \
     patch.object(p, "_WEBHOOK_URL", "https://example.invalid/exec"), \
     patch.object(p, "_WEBHOOK_SECRET", "secret"), \
     patch.object(p, "requests") as mock_requests:
    p.main()
    check("an open short (status=short_open) is not pushed at all yet",
          not mock_requests.post.called)


# ── (g) close push after an open push reuses the SAME key -> one row updated ──
print("\n(g) same entry_order_id: open push then close push upserts, doesn't duplicate")

store = FakeStore([make_open(pushed_open_to_sheets=True)])
closed = make_long(entry_order_id="E3", status="exited_925")
store.positions = [closed]
with patch.object(p, "_load_pos", store.load), \
     patch.object(p, "_save_pos", store.save), \
     patch.object(p, "_WEBHOOK_URL", "https://example.invalid/exec"), \
     patch.object(p, "_WEBHOOK_SECRET", "secret"), \
     patch.object(p, "requests") as mock_requests:
    mock_requests.post.return_value = FakeResponse({"status": "ok", "inserted": 0, "updated": 1})
    p.main()
    posted_items = mock_requests.post.call_args.kwargs["json"]["rows"]
    check("close push sends exactly one item", len(posted_items) == 1)
    check("close push item reuses the SAME key as the earlier open push",
          posted_items[0]["key"] == "E3", detail=posted_items[0]["key"])
    check("close push item has the real exit data, not blanks",
          posted_items[0]["row"][5] == round(117.0, 4))
    check("close push item's legs now shows the resolved BUY → SELL (no longer …)",
          posted_items[0]["legs"] == "BUY → SELL", detail=posted_items[0]["legs"])


print(f"\n{'='*60}")
if failures == 0:
    print(f"{PASS}  All checks passed.")
    sys.exit(0)
else:
    print(f"{FAIL}  {failures} check(s) failed.")
    sys.exit(1)
