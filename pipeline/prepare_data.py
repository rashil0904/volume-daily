#!/usr/bin/env python3
"""
prepare_data.py
===============
Produces today's trade list using today's live market cap fetch + candle history.

Signal conditions (ALL must pass):
  1. Market cap : symbol present in market_cap_daily/market_cap_<today>.csv
  2. Volume     : cum volume 09:15–14:45 >= 6× 36-day rolling avg full-day volume
                  (prior trading days only; zero-volume days excluded; min_periods=36)
  3. Return     : OPEN of 15:00 candle >= 5% above prev trading day's VWAP close
                  (VWAP = volume-weighted (H+L+C)/3 from prev day's 15:00 + 15:15 candles)

Capital allocation (TOTAL_CAPITAL = 5,00,000):
  signals <= 4  →  1,25,000 per stock
  signals >= 5  →  1,00,000 per stock
  shares = floor(allocation / open_of_3pm_candle)

Output: results/trade_list_<today>.csv  (not written if no signals fire)
"""

import csv
import math
import sys
from datetime import date
from pathlib import Path

import pandas as pd

_ROOT            = Path(__file__).resolve().parent.parent
MCAP_DAILY_DIR   = _ROOT / "data" / "market_cap_daily"
CANDLES_DIR      = _ROOT / "data" / "candles"
RESULTS_DIR      = _ROOT / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

TODAY            = date.today()
VOLUME_MULT      = 6
MIN_PERIODS      = 36
RETURN_THRESHOLD = 5.0   # percent

TOTAL_CAPITAL    = 500_000   # ₹5,00,000

FIELDNAMES = ["symbol", "shares", "ref_price"]


def load_today_mcap():
    mcap_file = MCAP_DAILY_DIR / f"market_cap_{TODAY.isoformat()}.csv"
    if not mcap_file.exists():
        candidates = sorted(MCAP_DAILY_DIR.glob("market_cap_*.csv"))
        if not candidates:
            sys.exit(f"ERROR: {mcap_file} not found — run fetch_market_cap.py first.")
        mcap_file = candidates[-1]
        print(f"  [prepare_data] market_cap for {TODAY} not found — using {mcap_file.name}")
    universe = {}
    with open(mcap_file, newline="") as f:
        for row in csv.DictReader(f):
            sym = row["symbol"].strip().upper()
            mcap_str = (row.get("mcap_cr") or "").strip()
            universe[sym] = float(mcap_str) if mcap_str else 0.0
    return universe


def check_symbol(symbol):
    csv_path = CANDLES_DIR / f"{symbol}.csv"
    if not csv_path.exists():
        return None

    df = pd.read_csv(csv_path)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.dropna(subset=["open", "high", "low", "close", "volume"], inplace=True)
    if df.empty:
        return None

    df["ts"]   = pd.to_datetime(df["timestamp"])
    if df["ts"].dt.tz is not None:
        df["ts"] = df["ts"].dt.tz_convert("Asia/Kolkata")
    df["date"] = df["ts"].dt.date
    df["hhmm"] = df["ts"].dt.hour * 100 + df["ts"].dt.minute

    today_df = df[df["date"] == TODAY]
    if today_df.empty:
        return None

    # 36-day rolling avg of full-day volume (prior trading days, non-zero only)
    full_day_vol  = df.groupby("date")["volume"].sum()
    dates         = sorted(full_day_vol.index.tolist())
    prior_nonzero = [full_day_vol[d] for d in dates if d < TODAY and full_day_vol[d] > 0]
    if len(prior_nonzero) < MIN_PERIODS:
        return None

    avg_36  = sum(prior_nonzero[-MIN_PERIODS:]) / MIN_PERIODS
    cum_vol = today_df[(today_df["hhmm"] >= 915) & (today_df["hhmm"] <= 1445)]["volume"].sum()

    # Open of today's 15:00 candle — used for signal check and share sizing
    c300 = today_df[today_df["hhmm"] == 1500]
    if c300.empty:
        return None
    open_300 = float(c300["open"].iloc[0])

    # Previous trading day's VWAP close (15:00 + 15:15 candles)
    prev_dates = [d for d in dates if d < TODAY]
    if not prev_dates:
        return None
    prev_d    = prev_dates[-1]
    prev_df   = df[df["date"] == prev_d]
    vwap_rows = prev_df[prev_df["hhmm"].isin([1500, 1515])]
    if vwap_rows.empty or vwap_rows["volume"].sum() == 0:
        return None
    tp        = (vwap_rows["high"] + vwap_rows["low"] + vwap_rows["close"]) / 3
    prev_vwap = float((tp * vwap_rows["volume"]).sum() / vwap_rows["volume"].sum())

    return_pct    = (open_300 / prev_vwap - 1) * 100
    passes_volume = bool(cum_vol >= VOLUME_MULT * avg_36)
    passes_return = bool(return_pct >= RETURN_THRESHOLD)

    if not (passes_volume and passes_return):
        return None

    return {"symbol": symbol, "open_300": open_300}


def main():
    universe = load_today_mcap()
    print(f"prepare_data.py — {TODAY.isoformat()}  ({len(universe):,} symbols in mcap file)")

    raw_signals = []
    no_candles  = 0
    no_signal   = 0

    for sym in sorted(universe):
        result = check_symbol(sym)
        if result is None:
            if not (CANDLES_DIR / f"{sym}.csv").exists():
                no_candles += 1
            else:
                no_signal += 1
        else:
            raw_signals.append(result)

    print(f"  Symbols with no candle file : {no_candles}")
    print(f"  No signal (<36d history or conditions not met): {no_signal}")
    print(f"  Signals today               : {len(raw_signals)}")

    if not raw_signals:
        print(f"\n  No signals for {TODAY.isoformat()} — trade list not written.")
        return

    # Capital allocation
    n          = len(raw_signals)
    allocation = 125_000 if n <= 4 else TOTAL_CAPITAL // n
    print(f"\n  Capital: ₹{TOTAL_CAPITAL:,.0f}  |  {n} signal(s)  →  ₹{allocation:,.0f} per stock")

    signals = []
    for s in raw_signals:
        shares = math.floor(allocation / s["open_300"]) if s["open_300"] > 0 else 0
        signals.append({
            "symbol":    s["symbol"],
            "shares":    shares,
            "ref_price": round(s["open_300"], 2),
        })

    trade_path = RESULTS_DIR / f"trade_list_{TODAY.isoformat()}.csv"
    with open(trade_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(signals)

    print(f"\n  {'Symbol':<15}  {'Shares':>7}  {'Ref Price (3PM Open)':>20}")
    print(f"  {'-'*15}  {'-'*7}  {'-'*20}")
    for r in signals:
        print(f"  {r['symbol']:<15}  {r['shares']:>7}  ₹{r['ref_price']:>19,.2f}")

    print(f"\n  Trade list → {trade_path}")


if __name__ == "__main__":
    main()
