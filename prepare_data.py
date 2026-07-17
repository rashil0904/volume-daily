#!/usr/bin/env python3
"""
prepare_data.py
===============
Produces today's trade list using today's live market cap fetch + candle history.

Signal conditions (ALL 3 must pass):
  1. Market cap : symbol present in market_cap_daily/market_cap_<today>.csv
  2. Volume     : cum volume 09:15–14:45 >= 6× 36-day rolling avg full-day volume
                  (prior trading days only; zero-volume days excluded; min_periods=36)
  3. Return     : OPEN of 15:00 candle >= 5% above prev trading day's VWAP close
                  (VWAP = volume-weighted (H+L+C)/3 from prev day's 15:00 + 15:15 candles)

Entry price = OPEN of the 15:15 candle.
Output: results/trade_list_<today>.csv  (not written if no signals fire)
"""

import csv
import sys
from datetime import date
from pathlib import Path

import pandas as pd

MCAP_DAILY_DIR   = Path("market_cap_daily")
CANDLES_DIR      = Path("candles")
RESULTS_DIR      = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)

TODAY            = date.today()
VOLUME_MULT      = 6
MIN_PERIODS      = 36
RETURN_THRESHOLD = 5.0   # percent

FIELDNAMES = [
    "symbol", "market_cap_cr", "entry_price_315pm",
    "prev_day_vwap_close", "return_pct_vs_prev_close",
    "cum_volume_to_3pm", "avg_36day_volume", "volume_ratio",
    "passes_volume", "passes_return",
]


def load_today_mcap():
    mcap_file = MCAP_DAILY_DIR / f"market_cap_{TODAY.isoformat()}.csv"
    if not mcap_file.exists():
        sys.exit(f"ERROR: {mcap_file} not found — run fetch_market_cap.py first.")
    universe = {}
    with open(mcap_file, newline="") as f:
        for row in csv.DictReader(f):
            sym = row["symbol"].strip().upper()
            mcap_str = (row.get("mcap_cr") or "").strip()
            universe[sym] = float(mcap_str) if mcap_str else 0.0
    return universe


def check_symbol(symbol, mcap):
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
    full_day_vol = df.groupby("date")["volume"].sum()
    dates        = sorted(full_day_vol.index.tolist())
    prior_nonzero = [full_day_vol[d] for d in dates if d < TODAY and full_day_vol[d] > 0]
    if len(prior_nonzero) < MIN_PERIODS:
        return None   # insufficient history

    avg_36 = sum(prior_nonzero[-MIN_PERIODS:]) / MIN_PERIODS

    # Cumulative volume 09:15–14:45
    cum_vol = today_df[(today_df["hhmm"] >= 915) & (today_df["hhmm"] <= 1445)]["volume"].sum()

    # Open of today's 15:00 candle
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
    volume_ratio  = cum_vol / avg_36 if avg_36 > 0 else 0.0
    passes_volume = bool(cum_vol >= VOLUME_MULT * avg_36)
    passes_return = bool(return_pct >= RETURN_THRESHOLD)

    if not (passes_volume and passes_return):
        return None

    # Entry price: open of 15:15 candle
    c315 = today_df[today_df["hhmm"] == 1515]
    entry_price = float(c315["open"].iloc[0]) if not c315.empty else None

    return {
        "symbol":                   symbol,
        "market_cap_cr":            round(mcap, 2),
        "entry_price_315pm":        round(entry_price, 2) if entry_price is not None else "",
        "prev_day_vwap_close":      round(prev_vwap, 4),
        "return_pct_vs_prev_close": round(return_pct, 4),
        "cum_volume_to_3pm":        int(cum_vol),
        "avg_36day_volume":         round(avg_36, 2),
        "volume_ratio":             round(volume_ratio, 4),
        "passes_volume":            passes_volume,
        "passes_return":            passes_return,
    }


def main():
    universe = load_today_mcap()
    print(f"prepare_data.py — {TODAY.isoformat()}  ({len(universe):,} symbols in mcap file)")

    signals    = []
    no_candles = 0
    no_signal  = 0

    for sym, mcap in sorted(universe.items()):
        result = check_symbol(sym, mcap)
        if result is None:
            csv_path = CANDLES_DIR / f"{sym}.csv"
            if not csv_path.exists():
                no_candles += 1
            else:
                no_signal += 1
        else:
            signals.append(result)

    print(f"  Symbols with no candle file: {no_candles}")
    print(f"  No signal (conditions not met or <36 days history): {no_signal}")
    print(f"  Signals today              : {len(signals)}")

    if not signals:
        print(f"\n  No signals for {TODAY.isoformat()} — trade list not written.")
        return

    trade_path = RESULTS_DIR / f"trade_list_{TODAY.isoformat()}.csv"
    with open(trade_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(signals)

    print(f"\n  Trade list → {trade_path}")
    print(f"\n  {'Symbol':<15}  {'Entry':>8}  {'Ret%':>7}  {'VolRatio':>9}  MCap Cr")
    print(f"  {'-'*15}  {'-'*8}  {'-'*7}  {'-'*9}  {'-'*9}")
    for r in signals:
        print(f"  {r['symbol']:<15}  {str(r['entry_price_315pm']):>8}  "
              f"{r['return_pct_vs_prev_close']:>7.2f}  {r['volume_ratio']:>9.2f}x  "
              f"{r['market_cap_cr']:>9,.0f}")


if __name__ == "__main__":
    main()
