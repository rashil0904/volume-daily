"""
common/calc_utils.py — shared calculation logic
=================================================
Pure functions consolidating calculation logic that was previously
duplicated (and, in one case, drifted) across zerodha/, pipeline/, and
live/:

  - pick_reference_price      : zerodha/run_trades.py, run_trades_mtf.py
  - compute_allocation        : run_trades.py, run_trades_mtf.py,
                                 pipeline/main.py, scan/scan_intraday.py,
                                 zerodha/execute_trades.py
  - compute_shares            : all five files above
  - load_clean_candles        : pipeline/signal_engine.py, live/live_monitor.py
  - compute_36day_avg_volume  : same two
  - compute_prev_day_vwap     : same two

No I/O beyond load_clean_candles's CSV read (candle *fetching* itself
stays in each caller, which already differ in how they obtain candles).
"""

import math
from datetime import date, datetime

import pandas as pd


# ── Reference price ────────────────────────────────────────────────────────────

def _close_at(candles: list, hhmm: int) -> float | None:
    for c in candles:
        try:
            dt = datetime.fromisoformat(str(c[0]))
            if dt.hour * 100 + dt.minute == hhmm:
                return float(c[4])
        except (IndexError, ValueError, TypeError):
            continue
    return None


def _latest_close_before(candles: list, max_hhmm: int) -> tuple[float, int] | None:
    best = None
    for c in candles:
        try:
            dt   = datetime.fromisoformat(str(c[0]))
            hhmm = dt.hour * 100 + dt.minute
            if hhmm <= max_hhmm and (best is None or hhmm > best[1]):
                best = (float(c[4]), hhmm)
        except (IndexError, ValueError, TypeError):
            continue
    return best


def pick_reference_price(candles: list, primary_hhmm: int = 1514,
                         fallback_hhmm: int = 1513) -> tuple[float, int]:
    """15:14 close -> 15:13 close -> latest candle at/before primary_hhmm.
    Takes an already-fetched list of candles (does NOT fetch itself).
    Raises ValueError if candles is empty or nothing qualifies."""
    for hhmm in (primary_hhmm, fallback_hhmm):
        price = _close_at(candles, hhmm)
        if price is not None:
            return price, hhmm
    latest = _latest_close_before(candles, primary_hhmm)
    if latest is not None:
        return latest
    raise ValueError(
        f"No candle data at all up to {primary_hhmm // 100:02d}:{primary_hhmm % 100:02d} "
        f"({len(candles)} candles)."
    )


# ── Allocation / sizing ────────────────────────────────────────────────────────

def compute_allocation(capital: float, n: int) -> float:
    """capital/4 if n<=4 else capital/n — float division, the single
    canonical rule. Replaces both the dynamic version already used in
    run_trades.py/run_trades_mtf.py and the hardcoded
    125_000-if-n<=4-else-capital//n version that had drifted in
    main.py/scan_intraday.py/execute_trades.py -- always float division,
    never a hardcoded literal."""
    return capital / 4 if n <= 4 else capital / n


def compute_shares(allocation: float, ref_price: float) -> int:
    """floor(allocation/ref_price), or 0 if ref_price<=0."""
    return math.floor(allocation / ref_price) if ref_price > 0 else 0


# ── Candle cleaning / volume / VWAP ────────────────────────────────────────────

def load_clean_candles(csv_path) -> pd.DataFrame:
    """Read a candles CSV, coerce OHLCV numeric, dropna, derive date/hhmm
    columns. Matches the boilerplate previously inline in signal_engine.py
    and live_monitor.py exactly. Caller is responsible for checking the
    path exists first (matches prior behavior at both call sites)."""
    df = pd.read_csv(csv_path)
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.dropna(subset=["open", "high", "low", "close", "volume"], inplace=True)
    if df.empty:
        return df

    df["ts"] = pd.to_datetime(df["timestamp"])
    if df["ts"].dt.tz is not None:
        df["ts"] = df["ts"].dt.tz_convert("Asia/Kolkata")
    df["date"] = df["ts"].dt.date
    df["hhmm"] = df["ts"].dt.hour * 100 + df["ts"].dt.minute
    return df


def compute_36day_avg_volume(df: pd.DataFrame, today: date,
                             min_periods: int = 36) -> float | None:
    """36-day rolling avg of prior non-zero full-day volume. Returns None
    if insufficient history (fewer than min_periods prior non-zero days)."""
    full_day_vol  = df.groupby("date")["volume"].sum()
    dates         = sorted(full_day_vol.index.tolist())
    prior_nonzero = [full_day_vol[d] for d in dates if d < today and full_day_vol[d] > 0]
    if len(prior_nonzero) < min_periods:
        return None
    return sum(prior_nonzero[-min_periods:]) / min_periods


def compute_prev_day_vwap(df: pd.DataFrame, today: date) -> float | None:
    """Volume-weighted (H+L+C)/3 from prev trading day's 15:00+15:15
    candles. Returns None if unavailable."""
    full_day_vol = df.groupby("date")["volume"].sum()
    dates        = sorted(full_day_vol.index.tolist())
    prev_dates   = [d for d in dates if d < today]
    if not prev_dates:
        return None
    prev_d    = prev_dates[-1]
    prev_df   = df[df["date"] == prev_d]
    vwap_rows = prev_df[prev_df["hhmm"].isin([1500, 1515])]
    if vwap_rows.empty or vwap_rows["volume"].sum() == 0:
        return None
    tp = (vwap_rows["high"] + vwap_rows["low"] + vwap_rows["close"]) / 3
    return float((tp * vwap_rows["volume"]).sum() / vwap_rows["volume"].sum())
