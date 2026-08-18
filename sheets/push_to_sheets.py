"""
sheets/push_to_sheets.py — pushes Dhan trades to the Google Sheets dashboard,
via the Apps Script Web App webhook (see sheets/apps_script/Code.gs).

Runs on cron after every checkpoint that touches a position (--entry as well
as --exit-925, --exit-1159, --square-off-239 in dhan/run_trades.py), so
positions_dhan.json has already been written by the time this reads it.

Two kinds of row, one webhook action ("upsert_trades"):
  - OPEN  -- pushed right after entry (3:21pm), before any exit is known.
             Exit Price/Return %/P&L are blank; Exit Reason reads "🟢 Open".
  - CLOSED -- pushed after whichever checkpoint actually closes the position
              (925 target/profit exit, 1159 force exit, 239 short square-off).

Every row carries the broker's own entry_order_id as an idempotency key.
Code.gs's _upsertTrades matches on that key: the OPEN push creates the row,
the later CLOSED push overwrites that SAME row in place (not a second row) --
that's what lets the dashboard show a position live from entry instead of
only appearing once it's already closed.

Every item also carries `legs` (see build_legs) -- the visible entry-leg/
exit-leg pair ("BUY → SELL" for a long, "SELL → BUY" for a short, exit leg
shown as "…" while still open) written to the sheet's Order Flow column,
separate from the row's own Entry Price/Exit Price numbers.

This is a pure side-effect: a Sheets/webhook failure must never raise past
main() or affect the trading scripts, which run independently of this.

Idempotency: each stage is only pushed once. "pushed_open_to_sheets" and
"pushed_to_sheets" are separate flags in positions_dhan.json (a position
passes through both, entry then close) -- flags, not timing or content
matching, are what future runs check.

First day of data is 2026-08-18: positions with an earlier entry_date are
never pushed, even if otherwise eligible (older rows predate the dashboard).
"""

import json
import os
import sys
from datetime import date

import requests

from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

from dhan.run_trades import _load_pos, _save_pos  # reuse, don't reimplement

_FIRST_DATE       = date(2026, 8, 18)
_CLOSED_STATUSES  = {"exited_925", "exited_1159", "short_closed"}

_TRADE_LOG_HEADERS = [
    "Date", "Symbol", "Direction", "Entry Price", "Qty",
    "Exit Price", "Exit Reason", "Product", "Return %", "P&L", "Capital Deployed",
]

_EXIT_REASON_EMOJI = {
    "Target hit":            "🎯",
    "Stop-loss hit":         "🛑",
    "11:59 force exit":      "⏰",
    "9:25 profit exit":      "💰",
    "2:39 square-off":       "🔁",
    "9:25 no-data partial":  "⚠️",
    "Unrecognized — review": "❓",
}

_OPEN_LABEL = "🟢 Open"

_env = _ROOT / "pipeline" / ".env"
if _env.exists():
    for _ln in _env.read_text().splitlines():
        _ln = _ln.strip()
        if _ln and not _ln.startswith("#") and "=" in _ln:
            _k, _, _v = _ln.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

_WEBHOOK_URL    = os.environ.get("SHEETS_WEBHOOK_URL")
_WEBHOOK_SECRET = os.environ.get("SHEETS_WEBHOOK_SECRET")


def derive_exit_reason(pos: dict) -> str:
    """Compares this row's own recorded order IDs against its target/cover/stop
    order IDs -- never infers from timing. See module docstring in
    dhan/run_trades.py's square_off_239/check_exit_925/force_exit_1159 for the
    order-id fields this reads.

    KNOWN PRECEDENCE EDGE CASE: a long that went through the 9:25 no-data
    partial fallback (shares_exited_925 present) AND then hit its (fresh,
    carried-forward) target on the remainder at 11:59 is tagged "9:25 no-data
    partial" here, not "Target hit" -- exit_order_id_925-based rules are
    checked before exit_order_id_1159-based ones, per the exact rule order
    specified for this dashboard. Flagging this rather than silently picking
    one interpretation.
    """
    if pos.get("direction") == "short":
        exit_oid = pos.get("exit_order_id_239")
        if not exit_oid:
            return "Unrecognized — review"
        if exit_oid == pos.get("cover_target_order_id"):
            return "Target hit"
        if exit_oid == pos.get("stop_order_id"):
            return "Stop-loss hit"
        return "2:39 square-off"

    exit_925 = pos.get("exit_order_id_925")
    if exit_925:
        if exit_925 == pos.get("target_order_id"):
            return "Target hit"
        if "shares_exited_925" in pos:
            return "9:25 no-data partial"
        return "9:25 profit exit"

    exit_1159 = pos.get("exit_order_id_1159")
    if exit_1159:
        if exit_1159 == pos.get("target_order_id"):
            return "Target hit"
        return "11:59 force exit"

    return "Unrecognized — review"


def _exit_price(pos: dict) -> float:
    status = pos.get("status")
    if status == "exited_925":
        return float(pos.get("exit_price_925") or 0)
    if status == "exited_1159":
        return float(pos.get("exit_price_1159") or 0)
    if status == "short_closed":
        return float(pos.get("exit_price_239") or 0)
    return 0.0


def build_row(pos: dict) -> list:
    is_short    = pos.get("direction") == "short"
    entry_price = float(pos.get("entry_price" if is_short else "actual_fill_price") or 0)
    qty         = int(pos.get("quantity" if is_short else "actual_fill_quantity") or 0)
    exit_price  = _exit_price(pos)
    reason      = derive_exit_reason(pos)
    reason_cell = f"{_EXIT_REASON_EMOJI.get(reason, '')} {reason}".strip()

    return [
        pos.get("entry_date", ""),
        pos.get("symbol", ""),
        "Short" if is_short else "Long",
        round(entry_price, 4),
        qty,
        round(exit_price, 4),
        reason_cell,
        pos.get("product", ""),
        round(float(pos.get("realized_return_pct") or 0), 4),
        round(float(pos.get("realized_pnl") or 0), 2),
        round(entry_price * qty, 2),
    ]


def build_legs(pos: dict) -> str:
    """Visible entry-leg -> exit-leg pair for the Order Flow column -- e.g.
    "BUY → SELL" for a long, "SELL → BUY" for a short (opening a short means
    selling first, closing it means buying to cover). The exit leg reads "…"
    while the position is still open (pushed at entry, before any exit is
    known) rather than guessing which leg will eventually close it."""
    is_short  = pos.get("direction") == "short"
    entry_leg = "SELL" if is_short else "BUY"
    exit_leg  = "BUY" if is_short else "SELL"
    if pos.get("status") not in _CLOSED_STATUSES:
        exit_leg = "…"
    return f"{entry_leg} → {exit_leg}"


def build_open_row(pos: dict) -> list:
    """Entry-time row -- whatever fields exist right after the 3:21pm fill.
    Exit Price/Return %/P&L are genuinely unknown yet, left blank rather than
    0 (0 would misleadingly read as a completed trade with no P&L)."""
    entry_price = float(pos.get("actual_fill_price") or 0)
    qty         = int(pos.get("actual_fill_quantity") or 0)
    return [
        pos.get("entry_date", ""),
        pos.get("symbol", ""),
        "Long",
        round(entry_price, 4),
        qty,
        "",
        _OPEN_LABEL,
        pos.get("product", ""),
        "",
        "",
        round(entry_price * qty, 2),
    ]


def _eligible(pos: dict) -> bool:
    if pos.get("pushed_to_sheets"):
        return False
    if pos.get("status") not in _CLOSED_STATUSES:
        return False
    try:
        entry_date = date.fromisoformat(pos.get("entry_date", ""))
    except ValueError:
        return False
    return entry_date >= _FIRST_DATE


def _eligible_open(pos: dict) -> bool:
    """Long entries only -- shorts are opened intraday from a 925/1159 exit
    and squared off same day at 239, so they're never open long enough to be
    worth a separate entry-time row (they still get pushed once, closed, via
    _eligible/build_row above, same as before this feature)."""
    if pos.get("direction") == "short":
        return False
    if pos.get("pushed_open_to_sheets"):
        return False
    if pos.get("status") != "open":
        return False
    try:
        entry_date = date.fromisoformat(pos.get("entry_date", ""))
    except ValueError:
        return False
    return entry_date >= _FIRST_DATE


def main() -> None:
    if not _WEBHOOK_URL or not _WEBHOOK_SECRET:
        print("[sheets] SHEETS_WEBHOOK_URL / SHEETS_WEBHOOK_SECRET not configured "
              "in pipeline/.env -- skipping push.", file=sys.stderr)
        return

    positions    = _load_pos()
    open_pending = [p for p in positions if _eligible_open(p)]
    closed_pending = [p for p in positions if _eligible(p)]

    if not open_pending and not closed_pending:
        print("[sheets] Nothing new to push.")
        return

    items = (
        [{"key": p.get("entry_order_id", ""), "legs": build_legs(p), "row": build_open_row(p)}
         for p in open_pending]
        + [{"key": p.get("entry_order_id", ""), "legs": build_legs(p), "row": build_row(p)}
           for p in closed_pending]
    )
    print(f"[sheets] Pushing {len(open_pending)} open + {len(closed_pending)} closed "
          f"row(s) to the dashboard...")

    resp = requests.post(
        _WEBHOOK_URL,
        json={"secret": _WEBHOOK_SECRET, "action": "upsert_trades", "rows": items},
        timeout=30,
    )
    resp.raise_for_status()
    result = resp.json()
    if result.get("status") != "ok":
        raise RuntimeError(f"webhook returned an error: {result}")

    for p in open_pending:
        p["pushed_open_to_sheets"] = True
    for p in closed_pending:
        p["pushed_to_sheets"] = True
    _save_pos(positions)

    print(f"[sheets] Pushed {len(open_pending)} open, {len(closed_pending)} closed row(s). "
          f"Webhook: {result.get('inserted', 0)} inserted, {result.get('updated', 0)} updated.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[sheets] PUSH FAILED (non-fatal to trading): {exc}", file=sys.stderr)
