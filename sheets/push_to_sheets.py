"""
sheets/push_to_sheets.py — pushes newly-closed Dhan trades to the Google Sheets
dashboard, via the Apps Script Web App webhook (see sheets/apps_script/Code.gs).

Runs on cron a few minutes after each checkpoint that can close a position
(--exit-925, --exit-1159, --square-off-239 in dhan/run_trades.py), so
positions_dhan.json has already been written by the time this reads it.

This is a pure side-effect: a Sheets/webhook failure must never raise past
main() or affect the trading scripts, which run independently of this.

Idempotency: a row is only pushed once. After a successful push, each pushed
position is marked "pushed_to_sheets": true in positions_dhan.json, and that
flag -- not timing or content matching -- is what future runs check.

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


def main() -> None:
    if not _WEBHOOK_URL or not _WEBHOOK_SECRET:
        print("[sheets] SHEETS_WEBHOOK_URL / SHEETS_WEBHOOK_SECRET not configured "
              "in pipeline/.env -- skipping push.", file=sys.stderr)
        return

    positions = _load_pos()
    pending   = [p for p in positions if _eligible(p)]

    if not pending:
        print("[sheets] No newly-closed trades to push.")
        return

    rows = [build_row(p) for p in pending]
    print(f"[sheets] Pushing {len(rows)} newly-closed trade(s) to the dashboard...")

    resp = requests.post(
        _WEBHOOK_URL,
        json={"secret": _WEBHOOK_SECRET, "action": "append_trades", "rows": rows},
        timeout=30,
    )
    resp.raise_for_status()
    result = resp.json()
    if result.get("status") != "ok":
        raise RuntimeError(f"webhook returned an error: {result}")

    for p in pending:
        p["pushed_to_sheets"] = True
    _save_pos(positions)

    print(f"[sheets] Pushed and marked {len(pending)} row(s). "
          f"Webhook confirmed {result.get('appended')} appended.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[sheets] PUSH FAILED (non-fatal to trading): {exc}", file=sys.stderr)
