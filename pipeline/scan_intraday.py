#!/usr/bin/env python3
"""
scan_intraday.py
=================
General anytime intraday scanner — same volume + return signal logic as
prepare_data.py, but not anchored to the 3 PM candle. Run it at any point
during the day to preview which symbols are currently building toward a
signal, using whatever candle data has already been fetched.

This is a manual preview tool, separate from the 3:01 PM production run.
It does NOT touch run_daily.py or prepare_data.py, and it does NOT fetch
new data itself — it only reads data/candles/<SYMBOL>.csv and today's
market-cap file, whatever state they're already in. Run fetch_candles.py
first (or just rely on whatever the daily cron has already fetched) to
get the freshest possible read.

Differences from prepare_data.py (the official 3:01 PM scanner):
  - Reference candle : latest available candle for today, not hardcoded 15:00.
  - Volume condition  : cumulative 09:15-to-now volume compared against a
                        threshold PRORATED to elapsed time within the
                        09:15-14:45 reference window (instead of a fixed
                        full-window cutoff) — otherwise every check before
                        14:45 would show ~0 signals purely from having less
                        time elapsed, not from weaker activity.
  - Return condition  : unchanged — latest candle's open vs previous day's
                        VWAP (15:00 + 15:15 candles), still >= 5%.
  - Output            : results/scan_<date>_<HHMM>.csv — kept separate from
                        trade_list_<date>.csv. Not read by execute_trades.py
                        or run_trades.py.

Caveat: prorating means very early in the session (few minutes after 09:15)
the threshold is tiny and a single active candle can trip it — treat early
readings as noisy, not confirmed signals.

Usage:
    python pipeline/scan_intraday.py
"""

import csv
import math
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

_ROOT          = Path(__file__).resolve().parent.parent
_PIPELINE_DIR  = Path(__file__).resolve().parent
sys.path.insert(0, str(_PIPELINE_DIR))
import notify

MCAP_DAILY_DIR = _ROOT / "data" / "market_cap_daily"
CANDLES_DIR    = _ROOT / "data" / "candles"
RESULTS_DIR    = _ROOT / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

_IST      = ZoneInfo("Asia/Kolkata")
TODAY     = date.today()
NOW_HHMM  = int(datetime.now(_IST).strftime("%H%M"))

VOLUME_MULT       = 6
MIN_PERIODS       = 36
RETURN_THRESHOLD  = 5.0     # percent
WINDOW_START_HHMM = 915
WINDOW_END_HHMM   = 1445    # same reference window end prepare_data.py uses

TOTAL_CAPITAL = 500_000     # ₹5,00,000

FIELDNAMES = ["symbol", "shares", "ref_price", "as_of_hhmm", "volume_ratio_vs_prorated", "return_pct"]


def _hhmm_to_minutes(hhmm: int) -> int:
    h, m = divmod(hhmm, 100)
    return h * 60 + m


def load_today_mcap():
    mcap_file = MCAP_DAILY_DIR / f"market_cap_{TODAY.isoformat()}.csv"
    if not mcap_file.exists():
        candidates = sorted(MCAP_DAILY_DIR.glob("market_cap_*.csv"))
        if not candidates:
            sys.exit(f"ERROR: {mcap_file} not found — run fetch_market_cap.py first.")
        mcap_file = candidates[-1]
        print(f"  [scan_intraday] market_cap for {TODAY} not found — using {mcap_file.name}")
    universe = {}
    with open(mcap_file, newline="") as f:
        for row in csv.DictReader(f):
            sym = row["symbol"].strip().upper()
            mcap_str = (row.get("mcap_cr") or "").strip()
            universe[sym] = float(mcap_str) if mcap_str else 0.0
    return universe


def check_symbol(symbol, as_of_hhmm):
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

    # Only consider candles up to as_of_hhmm — simulates "what this would show right now"
    today_df = today_df[today_df["hhmm"] <= as_of_hhmm]
    if today_df.empty:
        return None

    # 36-day rolling avg of full-day volume (prior trading days, non-zero only) — same as prepare_data.py
    full_day_vol  = df.groupby("date")["volume"].sum()
    dates         = sorted(full_day_vol.index.tolist())
    prior_nonzero = [full_day_vol[d] for d in dates if d < TODAY and full_day_vol[d] > 0]
    if len(prior_nonzero) < MIN_PERIODS:
        return None
    avg_36 = sum(prior_nonzero[-MIN_PERIODS:]) / MIN_PERIODS

    # Cumulative volume so far today, capped at the reference window end
    window_end = min(as_of_hhmm, WINDOW_END_HHMM)
    cum_vol = today_df[(today_df["hhmm"] >= WINDOW_START_HHMM) & (today_df["hhmm"] <= window_end)]["volume"].sum()

    # Prorate the volume threshold to elapsed time within the 09:15-14:45 window
    elapsed_min      = max(_hhmm_to_minutes(window_end) - _hhmm_to_minutes(WINDOW_START_HHMM), 1)
    full_window_min  = _hhmm_to_minutes(WINDOW_END_HHMM) - _hhmm_to_minutes(WINDOW_START_HHMM)
    prorated_threshold = VOLUME_MULT * avg_36 * (elapsed_min / full_window_min)

    # Reference candle = latest available candle up to as_of_hhmm (generalizes the hardcoded 15:00 open)
    latest_row = today_df.sort_values("hhmm").iloc[-1]
    ref_open   = float(latest_row["open"])
    ref_hhmm   = int(latest_row["hhmm"])

    # Previous trading day's VWAP close (15:00 + 15:15 candles) — unchanged from prepare_data.py
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

    return_pct    = (ref_open / prev_vwap - 1) * 100
    volume_ratio  = (cum_vol / prorated_threshold) if prorated_threshold > 0 else 0.0
    passes_volume = bool(cum_vol >= prorated_threshold)
    passes_return = bool(return_pct >= RETURN_THRESHOLD)

    if not (passes_volume and passes_return):
        return None

    return {
        "symbol":       symbol,
        "ref_open":     ref_open,
        "ref_hhmm":     ref_hhmm,
        "return_pct":   round(return_pct, 2),
        "volume_ratio": round(volume_ratio, 2),
    }


def main():
    as_of_hhmm = NOW_HHMM
    universe = load_today_mcap()
    print(f"scan_intraday.py — {TODAY.isoformat()} as of {as_of_hhmm:04d} IST  ({len(universe):,} symbols in mcap file)")
    print("  Preview only — not the official 3:01 PM trade list. Volume threshold is prorated to")
    print("  elapsed time in the 09:15-14:45 window; reference price is the latest available candle,")
    print("  not the 15:00 open. Results will differ from prepare_data.py's official run.\n")

    raw_signals = []
    no_candles  = 0
    no_signal   = 0

    for sym in sorted(universe):
        result = check_symbol(sym, as_of_hhmm)
        if result is None:
            if not (CANDLES_DIR / f"{sym}.csv").exists():
                no_candles += 1
            else:
                no_signal += 1
        else:
            raw_signals.append(result)

    print(f"  Symbols with no candle file : {no_candles}")
    print(f"  No signal yet                : {no_signal}")
    print(f"  Signals as of {as_of_hhmm:04d}          : {len(raw_signals)}")

    if not raw_signals:
        print(f"\n  No signals as of {as_of_hhmm:04d} IST — preview file not written.")
        try:
            notify.send_scan_preview(TODAY.isoformat(), as_of_hhmm, [])
        except Exception as exc:
            print(f"  WARNING: Telegram notification failed: {exc}", file=sys.stderr)
        return

    n          = len(raw_signals)
    allocation = 125_000 if n <= 4 else TOTAL_CAPITAL // n
    print(f"\n  Capital: ₹{TOTAL_CAPITAL:,.0f}  |  {n} signal(s)  →  ₹{allocation:,.0f} per stock (preview sizing)")

    rows = []
    for s in raw_signals:
        shares = math.floor(allocation / s["ref_open"]) if s["ref_open"] > 0 else 0
        rows.append({
            "symbol":                   s["symbol"],
            "shares":                   shares,
            "ref_price":                round(s["ref_open"], 2),
            "as_of_hhmm":               s["ref_hhmm"],
            "volume_ratio_vs_prorated": s["volume_ratio"],
            "return_pct":               s["return_pct"],
        })

    out_path = RESULTS_DIR / f"scan_{TODAY.isoformat()}_{as_of_hhmm:04d}.csv"
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(rows)

    print(f"\n  {'Symbol':<15}  {'Shares':>7}  {'Ref Price':>10}  {'As Of':>6}  {'Vol Ratio':>9}  {'Return %':>9}")
    print(f"  {'-'*15}  {'-'*7}  {'-'*10}  {'-'*6}  {'-'*9}  {'-'*9}")
    for r in rows:
        print(f"  {r['symbol']:<15}  {r['shares']:>7}  ₹{r['ref_price']:>9,.2f}  {r['as_of_hhmm']:>6}  "
              f"{r['volume_ratio_vs_prorated']:>9.2f}  {r['return_pct']:>9.2f}")

    print(f"\n  Preview scan → {out_path}")
    print(f"  This is NOT the official trade list — not read by execute_trades.py or run_trades.py.")

    try:
        notify.send_scan_preview(TODAY.isoformat(), as_of_hhmm, rows)
    except Exception as exc:
        print(f"  WARNING: Telegram notification failed: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
