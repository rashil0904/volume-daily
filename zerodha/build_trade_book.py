"""
zerodha/build_trade_book.py — Flatten positions_zerodha.json into a P&L-ready trade book
==========================================================================================
Reads   : results/positions_zerodha.json  (live trade state, written by run_trades.py)
Writes  : results/trade_book.csv          (one row per position)

Columns: Stock Name, Position entry date, Position Exit date, No of shares,
         Entry Price, Exit Price, Realised PnL, Realised PnL Pct

Open positions (not yet exited, at any stage -- 945, 1200, or a 945-nodata partial)
get entry-side fields only; exit date/price/P&L stay blank until they actually close.

Usage:
    python zerodha/build_trade_book.py
"""

import csv
import json
import sys
from pathlib import Path

_ROOT     = Path(__file__).resolve().parent.parent
_POS_FILE = _ROOT / "results" / "positions_zerodha.json"
_OUT_FILE = _ROOT / "results" / "trade_book.csv"

_FIELDNAMES = [
    "Stock Name", "Position entry date", "Position Exit date", "No of shares",
    "Entry Price", "Exit Price", "Realised PnL", "Realised PnL Pct",
]

_EXIT_STAGE_KEYS = {
    "exited_945":              ("exit_price_945",  "exit_timestamp_945"),
    "exited_1200":             ("exit_price_1200", "exit_timestamp_1200"),
    "partial_exit_945_nodata": ("exit_price_945",  "exit_timestamp_945"),
}


def _build_row(p: dict) -> dict:
    entry_price = p.get("actual_fill_price")
    qty         = p.get("actual_fill_quantity")

    status = p.get("status")
    exit_price = exit_ts = None

    stage_info = _EXIT_STAGE_KEYS.get(status)
    if stage_info:
        price_key, ts_key = stage_info
        exit_price = p.get(price_key)
        exit_ts    = p.get(ts_key)

    exit_date  = exit_ts.split("T")[0] if exit_ts else None
    realized   = p.get("realized_pnl")
    return_pct = p.get("realized_return_pct")

    return {
        "Stock Name":           p.get("symbol"),
        "Position entry date":  p.get("entry_date"),
        "Position Exit date":   exit_date,
        "No of shares":         qty,
        "Entry Price":          entry_price,
        "Exit Price":           exit_price,
        "Realised PnL":         realized,
        "Realised PnL Pct":     return_pct,
    }


def main() -> None:
    if not _POS_FILE.exists():
        sys.exit(f"[trade_book] No positions file: {_POS_FILE}")

    positions = json.loads(_POS_FILE.read_text())
    if not positions:
        print("[trade_book] No positions on record — nothing to write.")
        return

    rows = [_build_row(p) for p in positions]

    with open(_OUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    closed   = [r for r in rows if r["Realised PnL"] is not None]
    open_ps  = [r for r in rows if r["Position Exit date"] is None]
    realized = sum(r["Realised PnL"] for r in closed)

    print(f"[trade_book] Wrote {len(rows)} rows -> {_OUT_FILE}")
    print(f"[trade_book] Closed: {len(closed)}  Realized P&L: {round(realized, 2):+,.2f}")
    print(f"[trade_book] Open: {len(open_ps)}")


if __name__ == "__main__":
    main()
