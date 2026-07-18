"""
Execute today's trade list via Upstox API.

Reads results/trade_list_<date>.csv (output of prepare_data.py) and places
a MARKET BUY delivery order for each stock.

Usage:
    python upstox/execute_trades.py                          # live mode, today's trades
    python upstox/execute_trades.py --dry-run                # preview without placing orders
    python upstox/execute_trades.py --mode sandbox           # sandbox mode
    python upstox/execute_trades.py --date 2026-07-17        # specific date
    python upstox/execute_trades.py --date 2026-07-17 --dry-run
    python upstox/execute_trades.py --mode sandbox --dry-run
"""

import csv
import math
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from upstox.trade import buy

_ROOT        = Path(__file__).resolve().parent.parent
_RESULTS_DIR = _ROOT / "results"

TOTAL_CAPITAL = 500_000  # used only for old-format CSVs without a shares column


def _load_trades(trade_date: date) -> list[dict]:
    """
    Load trade list CSV for the given date.
    Supports both formats:
      New: symbol, shares, ref_price
      Old: symbol, market_cap_cr, entry_price_315pm, ...  (no shares column)
    """
    path = _RESULTS_DIR / f"trade_list_{trade_date.isoformat()}.csv"
    if not path.exists():
        sys.exit(f"No trade list found for {trade_date}: {path}")

    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        raw = list(reader)

    if not raw:
        print(f"Trade list is empty for {trade_date}.")
        return []

    has_shares = "shares" in raw[0]
    n = len(raw)

    for row in raw:
        symbol = row["symbol"].strip().upper()
        if has_shares:
            shares    = int(row["shares"])
            ref_price = float(row["ref_price"])
        else:
            # Old format — recalculate shares from capital allocation
            price     = float(row.get("entry_price_315pm") or row.get("ref_price") or 0)
            allocation = 125_000 if n <= 4 else TOTAL_CAPITAL // n
            shares    = math.floor(allocation / price) if price > 0 else 0
            ref_price = price

        if shares <= 0:
            print(f"  SKIP {symbol} — shares={shares} (price too high or zero)")
            continue
        rows.append({"symbol": symbol, "shares": shares, "ref_price": ref_price})

    return rows


def execute(trade_date: date | None = None, dry_run: bool = False, mode: str = "live"):
    if trade_date is None:
        trade_date = date.today()

    trades = _load_trades(trade_date)
    if not trades:
        print("Nothing to execute.")
        return

    print(f"\nUpstox execute_trades — {trade_date}  ({len(trades)} stock(s))  "
          + f"mode={mode}  " + ("[DRY RUN]" if dry_run else "[LIVE]"))
    print(f"{'Symbol':<15}  {'Shares':>6}  {'Ref Price':>10}  {'Status'}")
    print("-" * 55)

    results = {"ok": [], "failed": []}
    for t in trades:
        sym, qty, price = t["symbol"], t["shares"], t["ref_price"]
        try:
            order_id = buy(sym, "NSE", qty, order_type="MARKET",
                           product="D", dry_run=dry_run, mode=mode)
            status = f"order_id={order_id}" if not dry_run else "DRY_RUN"
            print(f"  {sym:<13}  {qty:>6}  {price:>10.2f}  ✓ {status}")
            results["ok"].append(sym)
        except Exception as exc:
            print(f"  {sym:<13}  {qty:>6}  {price:>10.2f}  ✗ {exc}")
            results["failed"].append(sym)

    print("-" * 55)
    print(f"Done — {len(results['ok'])} placed, {len(results['failed'])} failed.")
    if results["failed"]:
        print(f"  Failed: {', '.join(results['failed'])}")


if __name__ == "__main__":
    args = sys.argv[1:]

    dry_run = "--dry-run" in args
    args    = [a for a in args if a != "--dry-run"]

    mode = "live"
    if "--mode" in args:
        idx  = args.index("--mode")
        mode = args[idx + 1].lower()
        args = args[:idx] + args[idx + 2:]

    trade_date = None
    if "--date" in args:
        idx        = args.index("--date")
        trade_date = date.fromisoformat(args[idx + 1])

    execute(trade_date=trade_date, dry_run=dry_run, mode=mode)
