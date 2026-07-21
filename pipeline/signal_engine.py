#!/usr/bin/env python3
"""
signal_engine.py — all signal calculation for the NSE pipeline
===============================================================
Operates on 15-min candle CSV files already written by data_loader.py.
No fetching, no Telegram, no 1-min data touches this module.

Signal conditions (ALL must pass):
  1. Market cap : symbol present in the universe dict passed by the caller
                  (populated from today's market_cap_<date>.csv by main.py / scan_intraday.py)
  2. Volume     : cumulative volume from window start to reference time
                  >= 6× 36-day rolling avg of prior full-day volume
                  (zero-volume days excluded, shifted, strict min_periods=36)
  3. Return     : OPEN of reference candle >= 5% above prev trading day's VWAP close
                  (VWAP = volume-weighted (H+L+C)/3 from prev day's 15:00 + 15:15 candles)

Mode differences:
  STRICT   (main.py 3:01pm run):
    - Reference candle fixed at 15:00
    - Volume window: 09:15–14:45 (full session window)
    - Volume threshold: 6× 36-day avg (no proration)

  PRORATED (scan_intraday.py anytime preview):
    - Reference candle: latest available candle up to as_of_hhmm
    - Volume window: 09:15 to min(as_of_hhmm, 14:45)
    - Volume threshold prorated to elapsed time within 09:15–14:45 window

Public API
----------
  get_signals(universe, mode, as_of_hhmm)  -> list of signal dicts
"""

import csv
from datetime import date
from pathlib import Path

import pandas as pd

_ROOT       = Path(__file__).resolve().parent.parent
CANDLES_DIR = _ROOT / "data" / "candles"

VOLUME_MULT       = 6
MIN_PERIODS       = 36
RETURN_THRESHOLD  = 5.0     # percent
WINDOW_START_HHMM = 915
WINDOW_END_HHMM   = 1445    # last candle of the volume-counting window


def _hhmm_to_minutes(hhmm: int) -> int:
    h, m = divmod(hhmm, 100)
    return h * 60 + m


def _check_symbol(symbol: str, today: date, mode: str,
                  as_of_hhmm: int | None) -> dict | None:
    """
    Core per-symbol signal check. Returns a signal dict on pass, None on fail.
    Signal dict keys: symbol, ref_price, ref_hhmm, return_pct, volume_ratio
    volume_ratio is None in strict mode (threshold not prorated there).
    """
    csv_path = CANDLES_DIR / f"{symbol}.csv"
    if not csv_path.exists():
        return None

    df = pd.read_csv(csv_path)
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.dropna(subset=["open", "high", "low", "close", "volume"], inplace=True)
    if df.empty:
        return None

    df["ts"] = pd.to_datetime(df["timestamp"])
    if df["ts"].dt.tz is not None:
        df["ts"] = df["ts"].dt.tz_convert("Asia/Kolkata")
    df["date"] = df["ts"].dt.date
    df["hhmm"] = df["ts"].dt.hour * 100 + df["ts"].dt.minute

    today_df = df[df["date"] == today]
    if today_df.empty:
        return None

    # 36-day rolling avg of prior full-day volume (non-zero prior days only)
    full_day_vol  = df.groupby("date")["volume"].sum()
    dates         = sorted(full_day_vol.index.tolist())
    prior_nonzero = [full_day_vol[d] for d in dates if d < today and full_day_vol[d] > 0]
    if len(prior_nonzero) < MIN_PERIODS:
        return None
    avg_36 = sum(prior_nonzero[-MIN_PERIODS:]) / MIN_PERIODS

    # Previous trading day's VWAP close from its 15:00 + 15:15 candles
    prev_dates = [d for d in dates if d < today]
    if not prev_dates:
        return None
    prev_d    = prev_dates[-1]
    prev_df   = df[df["date"] == prev_d]
    vwap_rows = prev_df[prev_df["hhmm"].isin([1500, 1515])]
    if vwap_rows.empty or vwap_rows["volume"].sum() == 0:
        return None
    tp        = (vwap_rows["high"] + vwap_rows["low"] + vwap_rows["close"]) / 3
    prev_vwap = float((tp * vwap_rows["volume"]).sum() / vwap_rows["volume"].sum())

    # ── Mode-specific reference candle and volume threshold ───────────────────
    volume_ratio: float | None = None

    if mode == "strict":
        c300 = today_df[today_df["hhmm"] == 1500]
        if c300.empty:
            return None
        ref_price = float(c300["open"].iloc[0])
        ref_hhmm  = 1500
        cum_vol   = today_df[
            (today_df["hhmm"] >= WINDOW_START_HHMM) &
            (today_df["hhmm"] <= WINDOW_END_HHMM)
        ]["volume"].sum()
        passes_volume = bool(cum_vol >= VOLUME_MULT * avg_36)

    else:  # prorated
        today_df = today_df[today_df["hhmm"] <= as_of_hhmm]
        if today_df.empty:
            return None
        latest   = today_df.sort_values("hhmm").iloc[-1]
        ref_price = float(latest["open"])
        ref_hhmm  = int(latest["hhmm"])

        window_end = min(as_of_hhmm, WINDOW_END_HHMM)
        cum_vol    = today_df[
            (today_df["hhmm"] >= WINDOW_START_HHMM) &
            (today_df["hhmm"] <= window_end)
        ]["volume"].sum()
        elapsed_min     = max(
            _hhmm_to_minutes(window_end) - _hhmm_to_minutes(WINDOW_START_HHMM), 1
        )
        full_window_min = (
            _hhmm_to_minutes(WINDOW_END_HHMM) - _hhmm_to_minutes(WINDOW_START_HHMM)
        )
        prorated_threshold = VOLUME_MULT * avg_36 * (elapsed_min / full_window_min)
        passes_volume      = bool(cum_vol >= prorated_threshold)
        volume_ratio       = round(cum_vol / prorated_threshold, 2) if prorated_threshold > 0 else 0.0

    # Return condition (same for both modes)
    return_pct    = (ref_price / prev_vwap - 1) * 100
    passes_return = bool(return_pct >= RETURN_THRESHOLD)

    if not (passes_volume and passes_return):
        return None

    return {
        "symbol":       symbol,
        "ref_price":    ref_price,
        "ref_hhmm":     ref_hhmm,
        "return_pct":   round(return_pct, 2),
        "volume_ratio": volume_ratio,
    }


def get_signals(universe: dict, mode: str = "strict",
                as_of_hhmm: int | None = None) -> list:
    """
    Run the signal check across all symbols in the universe dict.

    universe    : {symbol: mcap_cr} — all symbols already pass the market-cap condition.
    mode        : "strict" (main.py 3pm run) | "prorated" (scan_intraday.py anytime preview)
    as_of_hhmm  : HHMM integer (e.g. 1400 for 14:00) — required for mode="prorated"

    Returns list of signal dicts (keys: symbol, ref_price, ref_hhmm, return_pct, volume_ratio).
    """
    if mode == "prorated" and as_of_hhmm is None:
        raise ValueError("as_of_hhmm is required for mode='prorated'")

    today    = date.today()
    signals  = []
    no_file  = 0
    no_signal = 0

    for sym in sorted(universe):
        result = _check_symbol(sym, today, mode, as_of_hhmm)
        if result is None:
            if not (CANDLES_DIR / f"{sym}.csv").exists():
                no_file += 1
            else:
                no_signal += 1
        else:
            signals.append(result)

    print(f"  Symbols with no candle file          : {no_file}")
    print(f"  No signal (<36d history or conditions not met): {no_signal}")
    print(f"  Signals                              : {len(signals)}")
    return signals
