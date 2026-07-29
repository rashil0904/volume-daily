"""
zerodha/build_trade_book.py — Flatten positions_zerodha.json into a P&L-ready trade book
==========================================================================================
Reads   : results/positions_zerodha.json  (live trade state, written by run_trades.py)
Writes  : results/trade_book.csv          (one row per position)

Columns: Stock Name, Position entry date, Position Exit date, No of shares,
         Entry Price, Exit Price, Realised PnL, Realised PnL Pct,
         Total Charges, Net PnL, Net PnL Pct

Open positions (not yet exited, at any stage -- 945, 1200, or a 945-nodata partial)
get entry-side fields only; exit date/price/P&L/charges stay blank until they close.

Charges are pulled directly from Kite's /charges/orders API (actual, not estimated) for
the entry (BUY) and exit (SELL) order of each closed position, plus a flat ₹15.34 DP
charge on the exit leg (DP charges aren't exposed by any Kite API — confirmed against
Zerodha Console manually).

Usage:
    python zerodha/build_trade_book.py
"""

import csv
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from zerodha.auth import get_session, BASE_URL

_POS_FILE = _ROOT / "results" / "positions_zerodha.json"
_OUT_FILE = _ROOT / "results" / "trade_book.csv"

_FIELDNAMES = [
    "Stock Name", "Position entry date", "Position Exit date", "No of shares",
    "Entry Price", "Exit Price", "Realised PnL", "Realised PnL Pct",
    "Total Charges", "Net PnL", "Net PnL Pct",
]

_EXIT_STAGE_KEYS = {
    "exited_945":              ("exit_price_945",  "exit_timestamp_945",  "exit_order_id_945"),
    "exited_1200":             ("exit_price_1200", "exit_timestamp_1200", "exit_order_id_1200"),
    "partial_exit_945_nodata": ("exit_price_945",  "exit_timestamp_945",  "exit_order_id_945"),
}

DP_CHARGE = 15.34  # flat, per scrip, sell-side only — not available via any Kite API


def _charge_leg(order_id, symbol, transaction_type, qty, price):
    return {
        "order_id": order_id, "exchange": "NSE", "tradingsymbol": symbol,
        "transaction_type": transaction_type, "variety": "regular", "product": "CNC",
        "order_type": "MARKET", "quantity": qty, "average_price": price,
    }


def _fetch_charge(leg: dict) -> float:
    """Fetch actual charges for a single order leg.

    IMPORTANT: must be called with exactly one leg per request. Kite's
    /charges/orders silently nets BUY+SELL legs of the *same symbol* within
    a single request (as if it were a same-day intraday square-off), even
    when the two orders are from entirely different calendar days/trades.
    Batching legs together silently produces wrong (understated) charges.
    """
    s, _ = get_session()
    r = s.post(BASE_URL + "/charges/orders", json=[leg])
    r.raise_for_status()
    return r.json()["data"][0]["charges"]["total"]


def _build_rows(positions: list) -> list:
    charges = {}  # position_index -> {"entry": total, "exit": total}
    for i, p in enumerate(positions):
        entry_leg = _charge_leg(p["entry_order_id"], p["symbol"], "BUY",
                                 p["actual_fill_quantity"], p["actual_fill_price"])
        try:
            charges.setdefault(i, {})["entry"] = _fetch_charge(entry_leg)
        except Exception as exc:
            print(f"[trade_book] WARNING: entry charge fetch failed for {p['symbol']} ({exc}).")
            charges.setdefault(i, {})["entry"] = None

        stage_info = _EXIT_STAGE_KEYS.get(p.get("status"))
        if stage_info:
            price_key, _, order_id_key = stage_info
            exit_leg = _charge_leg(p[order_id_key], p["symbol"], "SELL",
                                    p["actual_fill_quantity"], p[price_key])
            try:
                charges.setdefault(i, {})["exit"] = _fetch_charge(exit_leg)
            except Exception as exc:
                print(f"[trade_book] WARNING: exit charge fetch failed for {p['symbol']} ({exc}).")
                charges.setdefault(i, {})["exit"] = None

    rows = []
    for i, p in enumerate(positions):
        entry_price = p.get("actual_fill_price")
        qty         = p.get("actual_fill_quantity")

        status = p.get("status")
        exit_price = exit_ts = None
        stage_info = _EXIT_STAGE_KEYS.get(status)
        if stage_info:
            price_key, ts_key, _ = stage_info
            exit_price = p.get(price_key)
            exit_ts    = p.get(ts_key)

        exit_date  = exit_ts.split("T")[0] if exit_ts else None
        realized   = p.get("realized_pnl")
        return_pct = p.get("realized_return_pct")

        total_charges = net_pnl = net_pnl_pct = None
        leg_totals = charges.get(i, {})
        if exit_date and leg_totals.get("entry") is not None and leg_totals.get("exit") is not None:
            total_charges = round(leg_totals["entry"] + leg_totals["exit"] + DP_CHARGE, 2)
            net_pnl       = round(realized - total_charges, 2)
            invested      = entry_price * qty
            net_pnl_pct   = round(net_pnl / invested * 100, 4) if invested else 0

        rows.append({
            "Stock Name":           p.get("symbol"),
            "Position entry date":  p.get("entry_date"),
            "Position Exit date":   exit_date,
            "No of shares":         qty,
            "Entry Price":          entry_price,
            "Exit Price":           exit_price,
            "Realised PnL":         realized,
            "Realised PnL Pct":     return_pct,
            "Total Charges":        total_charges,
            "Net PnL":              net_pnl,
            "Net PnL Pct":          net_pnl_pct,
        })
    return rows


def main() -> None:
    if not _POS_FILE.exists():
        sys.exit(f"[trade_book] No positions file: {_POS_FILE}")

    positions = json.loads(_POS_FILE.read_text())
    if not positions:
        print("[trade_book] No positions on record — nothing to write.")
        return

    rows = _build_rows(positions)

    with open(_OUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    closed     = [r for r in rows if r["Realised PnL"] is not None]
    open_ps    = [r for r in rows if r["Position Exit date"] is None]
    realized   = sum(r["Realised PnL"] for r in closed)
    net_closed = [r for r in closed if r["Net PnL"] is not None]
    net        = sum(r["Net PnL"] for r in net_closed)

    print(f"[trade_book] Wrote {len(rows)} rows -> {_OUT_FILE}")
    print(f"[trade_book] Closed: {len(closed)}  Realized P&L: {round(realized, 2):+,.2f}")
    print(f"[trade_book] Open: {len(open_ps)}")
    if net_closed:
        print(f"[trade_book] Net P&L (after charges, {len(net_closed)} rows): {round(net, 2):+,.2f}")


if __name__ == "__main__":
    main()
