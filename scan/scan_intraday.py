#!/usr/bin/env python3
"""
scan/scan_intraday.py — anytime intraday preview scanner
=========================================================
Run at any time during the trading day to see which symbols are building
toward a signal, using the same fixed 6x volume bar as the official run —
not a lowered/prorated one, so a pass here means the real threshold, not
an early-session approximation of it.

Differences from main.py's official 3:01 PM run (signal_engine STRICT mode):
  - Reference candle : latest available candle (not hardcoded 15:00)
  - Volume threshold : same fixed 6x 36-day avg as strict mode (not scaled
                        down by elapsed time) -- early-session checks will
                        rarely pass since less volume has accumulated yet
  - Output path      : results/scans/scan_<date>_<HHMM>.csv (not trade_list_)
  - Sends a Telegram message labeled "PREVIEW" — distinct from the official
    send_success() trade-list message, and not read by any execution script

Usage:
    python scan/scan_intraday.py
"""

import csv
import math
import os
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_ROOT        = Path(__file__).resolve().parent.parent
_PIPELINE_DIR = _ROOT / "pipeline"
sys.path.insert(0, str(_PIPELINE_DIR))

# Load .env before importing data_loader (which also loads it, but be explicit)
_env_file = _PIPELINE_DIR / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

import data_loader
import signal_engine
import notify

# ── Paths ──────────────────────────────────────────────────────────────────────
MCAP_DAILY_DIR = _ROOT / "data" / "market_cap_daily"
INSTRUMENTS_DIR = _ROOT / "data" / "instruments"
SCANS_DIR      = _ROOT / "results" / "scans"
SCANS_DIR.mkdir(parents=True, exist_ok=True)

_IST     = ZoneInfo("Asia/Kolkata")
TODAY    = date.today()
NOW_HHMM = int(datetime.now(_IST).strftime("%H%M"))

TOTAL_CAPITAL = 500_000

FIELDNAMES = ["symbol", "shares", "ref_price", "as_of_hhmm",
              "volume_ratio_vs_prorated", "return_pct"]


def _load_today_mcap() -> dict:
    mcap_file = MCAP_DAILY_DIR / f"market_cap_{TODAY.isoformat()}.csv"
    if not mcap_file.exists():
        candidates = sorted(MCAP_DAILY_DIR.glob("market_cap_*.csv"))
        if not candidates:
            sys.exit(f"ERROR: No market_cap_*.csv found — run main.py first.")
        mcap_file = candidates[-1]
        print(f"  [scan] market_cap for {TODAY} not found — using {mcap_file.name}")
    universe: dict = {}
    with open(mcap_file, newline="") as f:
        for row in csv.DictReader(f):
            sym      = row["symbol"].strip().upper()
            mcap_str = (row.get("mcap_cr") or "").strip()
            universe[sym] = float(mcap_str) if mcap_str else 0.0
    return universe


def _load_instrument_lookup() -> dict:
    inst_file = INSTRUMENTS_DIR / "upstox_instruments.csv"
    if not inst_file.exists():
        return {}
    with open(inst_file, newline="") as f:
        return {r["symbol"].strip().upper(): r for r in csv.DictReader(f)}


def main():
    as_of_hhmm = NOW_HHMM
    universe   = _load_today_mcap()

    print(f"scan_intraday.py — {TODAY.isoformat()} as of {as_of_hhmm:04d} IST  "
          f"({len(universe):,} symbols in mcap file)")
    print("  Preview only — not the official 3:01 PM trade list.")
    print("  Volume threshold is the same fixed 6x 36-day avg as the official run (not prorated).\n")

    # Refresh 15-min candles for today's mcap universe before checking signals
    inst_lookup = _load_instrument_lookup()
    matched     = [inst_lookup[sym] for sym in sorted(universe) if sym in inst_lookup]
    if matched:
        print(f"  Refreshing 15min candles for {len(matched):,} symbols …")
        data_loader.load_candles(matched, interval="15minute", mode="intraday")
        print()

    raw_signals = signal_engine.get_signals(universe, mode="prorated", as_of_hhmm=as_of_hhmm)

    if not raw_signals:
        print(f"\n  No signals as of {as_of_hhmm:04d} IST — preview file not written.")
        try:
            notify.send_scan_preview(TODAY.isoformat(), as_of_hhmm, [])
        except Exception as exc:
            print(f"  WARNING: Telegram notification failed: {exc}", file=sys.stderr)
        return

    n          = len(raw_signals)
    allocation = 125_000 if n <= 4 else TOTAL_CAPITAL // n
    print(f"\n  Capital: ₹{TOTAL_CAPITAL:,.0f}  |  {n} signal(s)  →  ₹{allocation:,.0f}/stock (preview sizing)")

    rows = []
    for s in raw_signals:
        shares = math.floor(allocation / s["ref_price"]) if s["ref_price"] > 0 else 0
        rows.append({
            "symbol":                   s["symbol"],
            "shares":                   shares,
            "ref_price":                round(s["ref_price"], 2),
            "as_of_hhmm":               s["ref_hhmm"],
            "volume_ratio_vs_prorated": s["volume_ratio"],
            "return_pct":               s["return_pct"],
        })

    out_path = SCANS_DIR / f"scan_{TODAY.isoformat()}_{as_of_hhmm:04d}.csv"
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(rows)

    print(f"\n  {'Symbol':<15}  {'Shares':>7}  {'Ref Price':>10}  "
          f"{'As Of':>6}  {'Vol Ratio':>9}  {'Return %':>9}")
    print(f"  {'-'*15}  {'-'*7}  {'-'*10}  {'-'*6}  {'-'*9}  {'-'*9}")
    for r in rows:
        print(f"  {r['symbol']:<15}  {r['shares']:>7}  ₹{r['ref_price']:>9,.2f}  "
              f"{r['as_of_hhmm']:>6}  {r['volume_ratio_vs_prorated']:>9.2f}  "
              f"{r['return_pct']:>9.2f}")

    print(f"\n  Preview scan → {out_path}")
    print(f"  This is NOT the official trade list — not read by run_trades.py.")

    try:
        notify.send_scan_preview(TODAY.isoformat(), as_of_hhmm, rows)
    except Exception as exc:
        print(f"  WARNING: Telegram notification failed: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
