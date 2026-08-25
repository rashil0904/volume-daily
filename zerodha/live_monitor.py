#!/usr/bin/env python3
"""
live_monitor.py — KiteTicker live signal monitor for NSE mid-cap momentum
=========================================================================
Monitoring and notification only by default. No orders. No positions. No
execution -- UNLESS --enable-uc-staged-entry is passed (default off), which
turns on a second, independent tick-driven state machine (Case A/B, see the
"UC-based staged entry" section below) that places real orders. Telegram is
the only output otherwise -- no CSV event log, no local logs/ directory.

Run at or after 9:15 AM IST (market open):
    python live_monitor.py
    python live_monitor.py --enable-uc-staged-entry [--dry-run]

Prerequisites:
    python -m zerodha.auth   # once per day, before market open

UC-based staged entry (Case A/B) -- off by default
---------------------------------------------------
Runs ALONGSIDE the normal 15:21 entry (zerodha/run_trades.py's
run_entry_321), never replacing it. Watches live ticks for a symbol popping
19% above its previous close before the normal 15:21 entry would otherwise
buy it:

    Case A qualification filter (checked continuously from market open,
    independent of the capital snapshot / windows below):
        A symbol only becomes Case-A-eligible if it (1) hit upper circuit at
        some point BEFORE 14:30, AND (2) is subsequently seen off UC (LTP <
        upper_circuit) at some point during the 14:30-15:18 window. Both are
        one-way latches -- once case_a_qualified is True it never unlatches.
    Case A leg 1 (14:30-15:18 IST): LTP crosses up through prev_close * 1.19
        -> buy 50% of per_stock_capital.
    Case A leg 2 (14:30-15:18 IST): LTP retraces back down to
        prev_close * 1.17 -> buy the other 50%. If leg 2's window closes
        without firing, the remaining balance is completed with PRIORITY by
        the 15:21 entry (Step 1 there), not treated as a fresh entry.
    Case B (15:00-15:18 IST): for a symbol that is NOT case_a_qualified,
        LTP crosses up through prev_close * 1.19 -> buy 100% of
        per_stock_capital in one shot. No legs, no retrace, no UC-proximity
        gate (deliberately simple, not an oversight).

Order shape: every staged-entry buy fires as MARKET with the same 0.5%
market_protection collar as every other "fires right now" order in this
pipeline (see zerodha/trade.py) -- not a capped LIMIT. This is a deliberate
choice for this codebase (kept at MARKET+0.5% even though a Case A/B trigger
fires exactly when a stock has just moved sharply toward its circuit, which
is the higher-risk case for a protection-collar false rejection) -- revisit
this order type first if false "outside circuit limits" rejections show up
on staged-entry buys specifically.

per_stock_capital is a snapshot taken ONCE, at the first tick observed
at/after 14:30 IST: TOTAL_CAPITAL / n_qualified, where n_qualified is the
count of symbols that have already fired the MAIN momentum signal
(evaluate_tick's "qualified" event) by that moment -- not a UC-specific
count. Never recomputed later even as more symbols qualify. Persisted onto
the position row as capital_base so run_entry_321 (a separate cron
invocation, no shared memory with this long-running process) can read the
original allocation back via the position file.

Concurrency: KiteTicker's on_ticks delivers a LIST of ticks per callback
(unlike Dhan's MarketFeed, which calls back once per instrument) -- but
those ticks are still processed one at a time, synchronously, in a single
Python thread before on_ticks returns. evaluate_tick()/_evaluate_uc_tick()
latch entry_status to "order_placed" the instant they decide to fire,
synchronously, before the actual order-placement call is ever handed to
self._executor (a small ThreadPoolExecutor) -- that synchronous latch alone
is enough to stop a second tick (whether later in the same batch or a
future callback) from re-firing the same trigger while the first order is
still in flight. The final mutation (order_placed -> a terminal or reverted
state) happens inside the executor thread, deliberately not under
self._lock -- a single attribute write is atomic under the GIL, and the
worst a stale read causes is one wasted tick, never a duplicate fire.
"""

# ── IPv4 monkeypatch — must precede any import that touches networking ────────
import socket as _socket
_orig_getaddrinfo = _socket.getaddrinfo
def _ipv4_only_getaddrinfo(*args, **kwargs):
    return [r for r in _orig_getaddrinfo(*args, **kwargs) if r[0] == _socket.AF_INET]
_socket.getaddrinfo = _ipv4_only_getaddrinfo
# ─────────────────────────────────────────────────────────────────────────────

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
_ROOT         = Path(__file__).resolve().parent.parent   # project root (one above live/)
_PIPELINE_DIR = _ROOT / "pipeline"
_CANDLES_DIR  = _ROOT / "data" / "candles"
_TOKEN_FILE   = _ROOT / "zerodha" / ".token.json"
_IST          = ZoneInfo("Asia/Kolkata")

# Add pipeline/ to path so pipeline modules resolve
sys.path.insert(0, str(_PIPELINE_DIR))
sys.path.insert(0, str(_ROOT))

# Load .env before importing pipeline modules that consume it at module level
_env_file = _PIPELINE_DIR / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

# notify is imported before kiteconnect (and wraps it below) so a broken/missing
# dependency still reaches Telegram instead of dying silently at import time.
import notify

try:
    from kiteconnect import KiteConnect, KiteTicker
except Exception as exc:
    try:
        notify.send_monitor_start_failure(f"Import failed: {exc}")
    except Exception:
        pass
    raise

import data_loader
from common.calc_utils import (load_clean_candles, compute_36day_avg_volume,
                               compute_prev_day_vwap, compute_shares)

# UC-staged-entry order placement reuses run_trades.py's own leverage decision,
# fill polling, and position-file I/O rather than duplicating any of it -- see
# the "UC-based staged entry" docstring section above. Deferred import isn't
# needed: run_trades.py has no module-level dependency on live_monitor.py (no
# import cycle), and its own module-level work is all inert (function/constant
# definitions only, no network calls at import time).
from zerodha.trade import buy as _trade_buy
from zerodha.run_trades import (
    TOTAL_CAPITAL, _mtf_margin_check, _available_margin, _poll_fill_safe,
    _load_long_pos, _save_long_pos, _ts,
)

# ── Strategy constants (mirror signal_engine.py) ──────────────────────────────
_MIN_PERIODS       = 36
_VOLUME_MULT       = 6
_RETURN_MULT       = 1.05    # LTP >= prev_vwap * 1.05  (5%)
_CIRCUIT_WARN_PCT  = 1.0     # alert when within 1% of upper circuit
_WINDOW_START      = 915
_WINDOW_END        = 1445

# ── Operational constants ─────────────────────────────────────────────────────
_QUOTE_CHUNK     = 500       # max symbols per kite.quote() call
_HEARTBEAT_SECS  = 60 * 60  # 60 minutes


# ── Auth ──────────────────────────────────────────────────────────────────────

def _get_kite_credentials() -> tuple[str, str]:
    """
    Validate/refresh token via zerodha.auth.get_session(), then read
    access_token from zerodha/.token.json. Returns (api_key, access_token).
    """
    from zerodha.auth import get_session
    get_session()  # validates existing token; triggers browser login if stale
    token_data   = json.loads(_TOKEN_FILE.read_text())
    api_key      = os.environ.get("ZERODHA_API_KEY", "").strip()
    access_token = token_data.get("access_token", "")
    if not api_key:
        raise EnvironmentError("ZERODHA_API_KEY not set in pipeline/.env")
    if not access_token:
        raise RuntimeError(f"No access_token in {_TOKEN_FILE}")
    return api_key, access_token


# ── Volume baseline + VWAP precomputation ────────────────────────────────────

@dataclass
class _Baseline:
    symbol:        str
    vol_threshold: float   # 6 × avg_36
    prev_vwap:     float   # VWAP from prev day's 15:00 + 15:15 candles


def _compute_baselines(universe: dict) -> dict[str, _Baseline]:
    """
    Precompute per-symbol vol_threshold and prev_vwap from candle CSVs.
    Mirrors the inline logic in signal_engine._check_symbol().
    Symbols lacking ≥36-day history or yesterday's VWAP candles are dropped.
    """
    today     = date.today()
    baselines = {}

    for sym in universe:
        csv_path = _CANDLES_DIR / f"{sym}.csv"
        if not csv_path.exists():
            continue
        try:
            df = load_clean_candles(csv_path)
            if df.empty:
                continue

            # 36-day rolling avg of prior full-day volume (non-zero days only)
            avg_36 = compute_36day_avg_volume(df, today, _MIN_PERIODS)
            if avg_36 is None:
                continue
            threshold = _VOLUME_MULT * avg_36

            # Previous trading day's VWAP from 15:00 + 15:15 candles
            prev_vwap = compute_prev_day_vwap(df, today)
            if prev_vwap is None:
                continue

            baselines[sym] = _Baseline(sym, threshold, prev_vwap)

        except Exception as exc:
            print(f"  [baseline] {sym} — skipped: {exc}", file=sys.stderr)

    return baselines


# ── Circuit limits ────────────────────────────────────────────────────────────

def _fetch_circuit_limits(kite: KiteConnect,
                          symbols: list[str]) -> dict[str, tuple[float, float]]:
    """
    Batch-fetch upper/lower circuit limits via kite.quote() in chunks of 500.
    Returns {symbol: (upper_circuit, lower_circuit)}.
    """
    result  = {}
    kite_keys = [f"NSE:{s}" for s in symbols]

    for i in range(0, len(kite_keys), _QUOTE_CHUNK):
        chunk = kite_keys[i : i + _QUOTE_CHUNK]
        try:
            quotes = kite.quote(chunk)
        except Exception as exc:
            print(f"  [circuit] chunk {i}–{i+len(chunk)-1} failed: {exc}", file=sys.stderr)
            time.sleep(2)
            continue

        for key, data in quotes.items():
            sym   = key.split(":")[-1]
            upper = float(data.get("upper_circuit_limit") or 0.0)
            lower = float(data.get("lower_circuit_limit") or 0.0)
            result[sym] = (upper, lower)

        if i + _QUOTE_CHUNK < len(kite_keys):
            time.sleep(0.4)

    return result


# ── Instrument token mapping ──────────────────────────────────────────────────

def _build_token_maps(kite: KiteConnect,
                      symbols: set[str]) -> tuple[dict[int, str], dict[str, int]]:
    """
    Fetch kite.instruments("NSE"), match to our universe.
    Returns (token_to_sym, sym_to_token).
    """
    print("Fetching Kite NSE instrument master…")
    instruments = kite.instruments("NSE")
    token_to_sym: dict[int, str] = {}
    sym_to_token: dict[str, int] = {}

    for inst in instruments:
        ts = (inst.get("tradingsymbol") or "").upper().strip()
        if ts in symbols and inst.get("instrument_type") in ("EQ", "BE"):
            tok = inst["instrument_token"]
            if ts not in sym_to_token:  # take first EQ match
                token_to_sym[tok]  = ts
                sym_to_token[ts]   = tok

    unmatched = symbols - set(sym_to_token)
    print(f"  Matched {len(sym_to_token)}/{len(symbols)} symbols to tokens.")
    if unmatched:
        sample = sorted(unmatched)[:20]
        print(f"  Unmatched ({len(unmatched)}): {sample}{'…' if len(unmatched) > 20 else ''}")

    return token_to_sym, sym_to_token


# ── Per-symbol live state ─────────────────────────────────────────────────────

@dataclass
class _SymState:
    symbol:        str
    vol_threshold: float
    prev_vwap:     float
    upper_circuit: float
    lower_circuit: float

    ltp:          float = 0.0
    cum_vol:      float = 0.0
    qualified:    bool  = False
    near_circuit: bool  = False


_EVAL_CUTOFF_HHMM = 1500   # signal_engine takes over from 3:01 PM onwards


def evaluate_tick(state: "_SymState", ltp: float, cum_vol: float) -> set[str]:
    """
    Pure state-machine evaluation for one tick on one symbol.

    Updates state.ltp and state.cum_vol unconditionally (so heartbeat can
    report live price/volume at any time), then checks the qualified and
    near_circuit conditions only before 15:00 IST — after that, signal_engine
    runs the authoritative 3:01 PM batch check, so duplicate live alerts
    would be noise. The WebSocket connection and heartbeat keep running past
    15:00; only new event transitions are suppressed.

    Fires each condition at most once (one-time flag transition). Returns a
    set of event strings that newly fired this tick: subset of
    {"qualified", "near_circuit"}.

    Must be called under the caller's lock when used from concurrent code.
    Does NOT call Telegram, does NOT log.
    """
    state.ltp     = ltp
    state.cum_vol = cum_vol

    now  = datetime.now(_IST)
    hhmm = now.hour * 100 + now.minute
    if hhmm >= _EVAL_CUTOFF_HHMM:
        return set()

    fired: set[str] = set()

    passes_vol    = cum_vol >= state.vol_threshold
    passes_return = (
        ltp > 0 and state.prev_vwap > 0
        and ltp >= state.prev_vwap * _RETURN_MULT
    )

    if passes_vol and passes_return and not state.qualified:
        state.qualified = True
        fired.add("qualified")

    if state.qualified and not state.near_circuit:
        if state.upper_circuit > 0 and ltp > 0:
            pct = (state.upper_circuit - ltp) / ltp * 100
            if pct <= _CIRCUIT_WARN_PCT:
                state.near_circuit = True
                fired.add("near_circuit")

    return fired


# ── UC-based staged entry (Case A/B) ────────────────────────────────────────────
# See the module docstring's "UC-based staged entry" section for the full
# behavioral spec. Everything below is pure logic (no I/O) except the
# execute_* functions, which place real orders -- gated entirely behind
# LiveMonitor's enable_uc_staged_entry flag (default off).

_CASE_A_START = dtime(14, 30)   # leg 1 and leg 2 share this window
_CASE_A_END   = dtime(15, 18)
_CASE_B_START = dtime(15, 0)
_CASE_B_END   = dtime(15, 18)

_LEG_UP_MULT   = 1.19   # prev_close * 1.19 -- leg 1 (Case A) / the fill trigger (Case B)
_LEG_DOWN_MULT = 1.17   # prev_close * 1.17 -- leg 2 retrace (Case A only)


def load_prev_close(symbol: str) -> float | None:
    """Reads data/candles/<symbol>.csv (this pipeline's own 15-min history) and
    returns the close of the LAST candle strictly before today's date. Returns
    None if the file's missing or has no prior-day rows -- caller should just
    skip this symbol for Case A/B (it still gets the normal, untouched 15:21
    entry). NOT sourced from a live/running quote field -- a running "last
    price" field can silently track TODAY's current price rather than
    yesterday's actual close (confirmed on the Dhan side of this pipeline,
    2026-08-24: THOMASCOOK's ohlc.close exactly equalled last_price mid-
    afternoon), which would quietly break every threshold below. Worth
    re-confirming this doesn't also hold for whatever Kite quote field might
    look tempting to use instead."""
    path = _CANDLES_DIR / f"{symbol}.csv"
    if not path.exists():
        return None
    today_str = date.today().isoformat()
    last_close: float | None = None
    with open(path, newline="") as f:
        next(f, None)   # header
        for line in f:
            parts = line.rstrip("\n").split(",")
            if len(parts) < 5:
                continue
            ts_str = parts[0]
            if ts_str.startswith(today_str):
                break   # today's rows are appended last -- stop before them
            try:
                last_close = float(parts[4])
            except ValueError:
                continue
    return last_close


@dataclass
class UCState:
    symbol:     str
    prev_close: float | None
    case:         str | None = None   # None | "A" | "B"
    entry_status: str = "not_attempted"
    # not_attempted | order_placed | partially_filled | filled
    case_a_leg:   str | None = None   # None | leg1_filled_watching_retrace | leg2_filled
    filled_amount: float = 0.0        # cumulative rupees filled today
    capital_base:  float = 0.0        # per_stock_capital snapshot this symbol used
    # Case A qualification filter -- both latches are one-way, never unlatch.
    hit_uc_before_1430: bool = False
    off_uc_in_window:   bool = False
    case_a_qualified:   bool = False


def _in_case_a_window(t: dtime) -> bool:
    return _CASE_A_START <= t < _CASE_A_END


def _in_case_b_window(t: dtime) -> bool:
    return _CASE_B_START <= t < _CASE_B_END


def update_case_a_qualification(state: UCState, ltp: float, upper_circuit: float | None,
                                now: datetime | None = None) -> None:
    """Latches the Case A qualification filter from live ticks. Must be called
    on EVERY tick from market open onward (not gated on the per_stock_capital
    snapshot or the 14:30 window) -- "hit UC before 14:30" has to be observed
    well before either exists. hit_uc_before_1430 latches True on the first
    tick before 14:30 where LTP >= upper_circuit; off_uc_in_window latches
    True on the first tick during 14:30-15:18 where LTP < upper_circuit, but
    only once hit_uc_before_1430 is already set. case_a_qualified is the AND
    of both -- once True it stays True for the rest of the day."""
    if state.case_a_qualified or not upper_circuit or upper_circuit <= 0 or ltp <= 0:
        return
    now_t = (now or datetime.now(_IST)).time()
    if now_t < _CASE_A_START:
        if ltp >= upper_circuit:
            state.hit_uc_before_1430 = True
    elif _in_case_a_window(now_t):
        if state.hit_uc_before_1430 and ltp < upper_circuit:
            state.off_uc_in_window = True
    if state.hit_uc_before_1430 and state.off_uc_in_window:
        state.case_a_qualified = True


def _evaluate_uc_tick(state: UCState, ltp: float, per_stock_capital: float,
                      now: datetime | None = None) -> str | None:
    """Pure, side-effecting-on-state-only (no I/O). Returns a fired event name
    or None. Latches entry_status to "order_placed" the instant it fires --
    see module docstring's Concurrency section for why that alone is
    race-safe. Assumes update_case_a_qualification() has already been called
    for this tick (this file's _on_ticks calls it unconditionally, first, on
    every tick, before this)."""
    if state.prev_close is None or ltp <= 0 or state.prev_close <= 0:
        return None
    now_t = (now or datetime.now(_IST)).time()

    if state.entry_status == "not_attempted":
        if (state.case_a_qualified and _in_case_a_window(now_t)
                and ltp >= state.prev_close * _LEG_UP_MULT):
            state.entry_status = "order_placed"
            state.capital_base = per_stock_capital
            return "case_a_leg1"
        # Tie-break: a case_a_qualified symbol never fires Case B, even
        # before leg 1 itself has fired.
        if (not state.case_a_qualified and _in_case_b_window(now_t)
                and ltp >= state.prev_close * _LEG_UP_MULT):
            state.entry_status = "order_placed"
            state.capital_base = per_stock_capital
            return "case_b_fill"
        return None

    if (state.entry_status == "partially_filled"
            and state.case_a_leg == "leg1_filled_watching_retrace"
            and _in_case_a_window(now_t)
            and ltp <= state.prev_close * _LEG_DOWN_MULT):
        state.entry_status = "order_placed"
        return "case_a_leg2"

    return None


# ── UC-based staged entry: order placement ──────────────────────────────────────

def _place_staged_buy(sym: str, qty: int, ltp: float, dry_run: bool) -> dict | None:
    """One staged-entry BUY leg -- same MTF-then-CNC leverage decision as
    run_entry_321, MARKET with the standard 0.5% protection collar (see
    module docstring's order-shape note). Returns None (caller must not write
    a position row, and must revert its state latch) if nothing filled."""
    margin_info  = _mtf_margin_check(sym, qty)
    has_leverage = margin_info is not None and margin_info["leverage"] >= 2
    product      = "MTF" if has_leverage else "CNC"
    margin_required = margin_info["margin_required"] if has_leverage else qty * ltp

    available = _available_margin()
    if available is None or available < margin_required:
        print(f"[uc_staged] {sym}: SKIP — insufficient/unverifiable margin.")
        return None

    try:
        order_id = _trade_buy(sym, "NSE", qty, order_type="MARKET", product=product, dry_run=dry_run)
    except Exception as exc:
        print(f"[uc_staged] {sym}: ORDER FAILED — {exc}")
        return None

    if dry_run:
        return {"order_id": order_id, "product": product, "fill_price": ltp, "fill_qty": qty}

    fill_price, fill_qty = _poll_fill_safe(order_id, ltp, qty)
    if fill_qty == 0:
        print(f"[uc_staged] {sym}: NOT FILLED.")
        return None

    return {"order_id": order_id, "product": product, "fill_price": fill_price, "fill_qty": fill_qty}


def execute_case_a_leg1(sym: str, state: UCState, ltp: float, dry_run: bool = False) -> None:
    leg_capital = state.capital_base / 2
    leg_qty     = compute_shares(leg_capital, ltp)
    if leg_qty == 0:
        print(f"[uc_staged] {sym}: Case A leg1 SKIP — 0 shares at LTP {ltp:.2f}.")
        state.entry_status = "not_attempted"
        return

    result = _place_staged_buy(sym, leg_qty, ltp, dry_run)
    if result is None:
        state.entry_status = "not_attempted"   # retry-eligible on a later tick this window
        return

    fill_amount = result["fill_price"] * result["fill_qty"]
    positions = _load_long_pos()
    positions.append({
        "broker": "zerodha", "symbol": sym, "entry_date": date.today().isoformat(),
        "case": "A", "case_a_qualified": state.case_a_qualified,
        "case_a_leg": "leg1_filled_watching_retrace",
        "entry_status": "partially_filled", "status": "leg1_filled",
        "capital_base": state.capital_base, "filled_amount": round(fill_amount, 2),
        "reference_price": ltp, "shares_intended": leg_qty,
        "actual_fill_price": result["fill_price"], "actual_fill_quantity": result["fill_qty"],
        "entry_order_id": result["order_id"], "entry_timestamp": _ts(),
        "product": result["product"],
    })
    if not dry_run:
        _save_long_pos(positions)
    state.case         = "A"
    state.entry_status = "partially_filled"
    state.case_a_leg    = "leg1_filled_watching_retrace"
    state.filled_amount = fill_amount
    try:
        notify.send_entry(broker="zerodha", symbol=f"{sym} [Case A leg1]", ref_price=ltp,
                          shares=result["fill_qty"], order_id=result["order_id"], dry_run=dry_run)
    except Exception as exc:
        print(f"  [notify] Case A leg1 failed: {exc}", file=sys.stderr)


def execute_case_a_leg2(sym: str, state: UCState, ltp: float, dry_run: bool = False) -> None:
    positions = _load_long_pos()
    today = date.today().isoformat()
    pos = next((p for p in positions if p.get("symbol") == sym
               and p.get("entry_date") == today and p.get("case") == "A"
               and p.get("entry_status") == "partially_filled"), None)
    if pos is None:
        print(f"[uc_staged] {sym}: Case A leg2 fired but no leg1 row found — skipping.")
        state.entry_status = "partially_filled"
        return

    remaining = pos["capital_base"] - pos["filled_amount"]
    leg_qty   = compute_shares(remaining, ltp)
    if leg_qty <= 0:
        state.entry_status = "filled"
        state.case_a_leg    = "leg2_filled"
        return

    result = _place_staged_buy(sym, leg_qty, ltp, dry_run)
    if result is None:
        state.entry_status = "partially_filled"   # stay armed -- retry-eligible, or
        state.case_a_leg    = "leg1_filled_watching_retrace"  # run_entry_321's Step 1 completes it regardless
        return

    fill_amount = result["fill_price"] * result["fill_qty"]
    total_qty   = pos["actual_fill_quantity"] + result["fill_qty"]
    avg_price   = ((pos["actual_fill_price"] * pos["actual_fill_quantity"]
                   + result["fill_price"] * result["fill_qty"]) / total_qty)
    pos.update({
        "status": "open", "entry_status": "filled", "case_a_leg": "leg2_filled",
        "filled_amount": round(pos["filled_amount"] + fill_amount, 2),
        "actual_fill_price": round(avg_price, 4), "actual_fill_quantity": total_qty,
        "leg2_order_id": result["order_id"], "leg2_fill_price": result["fill_price"],
        "leg2_fill_quantity": result["fill_qty"], "leg2_timestamp": _ts(),
    })
    if not dry_run:
        _save_long_pos(positions)
    state.entry_status  = "filled"
    state.case_a_leg     = "leg2_filled"
    state.filled_amount += fill_amount
    try:
        notify.send_entry(broker="zerodha", symbol=f"{sym} [Case A leg2]", ref_price=ltp,
                          shares=result["fill_qty"], order_id=result["order_id"], dry_run=dry_run)
    except Exception as exc:
        print(f"  [notify] Case A leg2 failed: {exc}", file=sys.stderr)


def execute_case_b(sym: str, state: UCState, ltp: float, dry_run: bool = False) -> None:
    qty = compute_shares(state.capital_base, ltp)
    if qty == 0:
        print(f"[uc_staged] {sym}: Case B SKIP — 0 shares at LTP {ltp:.2f}.")
        state.entry_status = "not_attempted"
        return

    result = _place_staged_buy(sym, qty, ltp, dry_run)
    if result is None:
        state.entry_status = "not_attempted"   # retry-eligible on a later tick this window
        return

    fill_amount = result["fill_price"] * result["fill_qty"]
    positions = _load_long_pos()
    positions.append({
        "broker": "zerodha", "symbol": sym, "entry_date": date.today().isoformat(),
        "case": "B", "case_a_qualified": state.case_a_qualified,
        "entry_status": "filled", "status": "open",
        "capital_base": state.capital_base, "filled_amount": round(fill_amount, 2),
        "reference_price": ltp, "shares_intended": qty,
        "actual_fill_price": result["fill_price"], "actual_fill_quantity": result["fill_qty"],
        "entry_order_id": result["order_id"], "entry_timestamp": _ts(),
        "product": result["product"],
    })
    if not dry_run:
        _save_long_pos(positions)
    state.case          = "B"
    state.entry_status  = "filled"
    state.filled_amount = fill_amount
    try:
        notify.send_entry(broker="zerodha", symbol=f"{sym} [Case B]", ref_price=ltp,
                          shares=result["fill_qty"], order_id=result["order_id"], dry_run=dry_run)
    except Exception as exc:
        print(f"  [notify] Case B failed: {exc}", file=sys.stderr)


# ── Monitor ────────────────────────────────────────────────────────────────────

class LiveMonitor:
    def __init__(self, api_key: str, access_token: str,
                enable_uc_staged_entry: bool = False, dry_run: bool = False):
        self._api_key      = api_key
        self._access_token = access_token
        self._states: dict[int, _SymState] = {}       # instrument_token -> state
        self._all_tokens: list[int] = []
        self._lock    = threading.Lock()
        self._started_notified = False   # fire send_monitor_started() once, on first connect only

        # UC-based staged entry (Case A/B) -- off by default. When on, ticks
        # also drive a second, independent state machine per symbol, and
        # firing an order goes through self._executor so a slow broker call
        # never blocks _on_ticks.
        self._enable_uc_staged_entry = enable_uc_staged_entry
        self._dry_run                = dry_run
        self._uc_states: dict[int, UCState] = {}   # instrument_token -> UCState
        self._executor = ThreadPoolExecutor(max_workers=4) if enable_uc_staged_entry else None
        # per_stock_capital = TOTAL_CAPITAL / qualified_count, snapshotted
        # EXACTLY ONCE at the first tick observed at/after 14:30 -- not
        # recomputed as more symbols qualify later in the day. None until
        # that snapshot happens. Independent of this: the Case A
        # qualification latch runs on every tick from market open, well
        # before this snapshot exists.
        self._per_stock_capital: float | None = None

    # ── Startup ───────────────────────────────────────────────────────────────

    def setup(self) -> None:
        kite = KiteConnect(api_key=self._api_key)
        kite.set_access_token(self._access_token)

        print("Loading market cap…")
        universe, mcap_status = data_loader.load_market_cap()
        print(f"  {len(universe)} eligible symbols (mcap_status={mcap_status})")

        print(f"Precomputing volume baselines + VWAP thresholds for {len(universe)} symbols…")
        baselines = _compute_baselines(universe)
        print(f"  {len(baselines)} symbols have ≥{_MIN_PERIODS}-day candle history.")

        print("Building Kite instrument token map…")
        token_to_sym, sym_to_token = _build_token_maps(kite, set(baselines))

        trackable = {s: b for s, b in baselines.items() if s in sym_to_token}
        print(f"  Trackable (baseline + token): {len(trackable)}")

        n_chunks = (len(trackable) + _QUOTE_CHUNK - 1) // _QUOTE_CHUNK
        print(f"Fetching circuit limits via kite.quote() [{n_chunks} chunk(s)]…")
        circuits = _fetch_circuit_limits(kite, list(trackable))
        print(f"  Circuit limits received for {len(circuits)}/{len(trackable)} symbols.")

        for sym, baseline in trackable.items():
            token        = sym_to_token[sym]
            upper, lower = circuits.get(sym, (0.0, 0.0))
            self._states[token] = _SymState(
                symbol        = sym,
                vol_threshold = baseline.vol_threshold,
                prev_vwap     = baseline.prev_vwap,
                upper_circuit = upper,
                lower_circuit = lower,
            )

        self._all_tokens = list(self._states)
        print(f"\nReady — monitoring {len(self._all_tokens)} symbols via KiteTicker MODE_QUOTE.")

        if self._enable_uc_staged_entry:
            print("UC-based staged entry ENABLED — loading previous-day closes…")
            n_have_prev_close = 0
            for token, state in self._states.items():
                prev_close = load_prev_close(state.symbol)
                if prev_close is not None:
                    n_have_prev_close += 1
                self._uc_states[token] = UCState(symbol=state.symbol, prev_close=prev_close)
            print(f"  {n_have_prev_close}/{len(self._uc_states)} symbols have a usable "
                  f"prev_close (rest fall through to the normal 15:21 entry untouched).")

    # ── WebSocket callbacks ───────────────────────────────────────────────────

    def _on_ticks(self, ws, ticks: list) -> None:
        # KiteTicker hands us a LIST of ticks per callback -- each one is
        # still processed one at a time, synchronously, in this same thread
        # before this method returns (see module docstring's Concurrency
        # note for why that's sufficient for the UC-staged-entry latch below).
        for tick in ticks:
            token = tick.get("instrument_token")
            state = self._states.get(token)
            if state is None:
                continue

            ltp     = float(tick.get("last_price")   or 0.0)
            cum_vol = float(tick.get("volume_traded") or 0.0)

            with self._lock:
                fired = evaluate_tick(state, ltp, cum_vol)
                if "qualified"    in fired: self._fire_qualified(state)
                if "near_circuit" in fired: self._fire_near_circuit(state)

                if self._enable_uc_staged_entry:
                    now      = datetime.now(_IST)
                    uc_state = self._uc_states.get(token)

                    # Case A qualification latch runs on EVERY tick from
                    # market open onward -- "hit UC before 14:30" must be
                    # observed long before the 14:30 window or the capital
                    # snapshot exist.
                    if uc_state is not None:
                        update_case_a_qualification(uc_state, ltp, state.upper_circuit, now=now)

                    # One-time capital snapshot: TOTAL_CAPITAL / qualified_count
                    # (the MAIN momentum signal's qualified count, not a
                    # UC-specific one), taken at the first tick observed
                    # at/after 14:30 -- never recomputed later.
                    if self._per_stock_capital is None and now.time() >= _CASE_A_START:
                        n_qualified = sum(1 for s in self._states.values() if s.qualified)
                        self._per_stock_capital = TOTAL_CAPITAL / max(n_qualified, 1)
                        print(f"[uc_staged] per_stock_capital snapshot: "
                              f"₹{TOTAL_CAPITAL:,.0f} / {n_qualified} qualified "
                              f"= ₹{self._per_stock_capital:,.2f}")

                    if uc_state is not None and self._per_stock_capital is not None:
                        uc_event = _evaluate_uc_tick(uc_state, ltp, self._per_stock_capital, now=now)
                        if uc_event is not None:
                            self._fire_uc_staged(uc_event, uc_state, ltp)

    # ── UC staged entry dispatch (called under self._lock; only submits to
    #    self._executor, never calls the slow order-placement function
    #    inline) ────────────────────────────────────────────────────────────

    def _fire_uc_staged(self, event: str, uc_state: "UCState", ltp: float) -> None:
        fn = {
            "case_a_leg1": execute_case_a_leg1,
            "case_a_leg2": execute_case_a_leg2,
            "case_b_fill": execute_case_b,
        }.get(event)
        if fn is None:
            return
        self._executor.submit(fn, uc_state.symbol, uc_state, ltp, self._dry_run)

    def _on_connect(self, ws, response) -> None:
        n = len(self._all_tokens)
        print(f"[WebSocket] Connected — subscribing {n} tokens in MODE_QUOTE…")
        ws.subscribe(self._all_tokens)
        ws.set_mode(ws.MODE_QUOTE, self._all_tokens)
        if not self._started_notified:
            self._started_notified = True
            try:
                notify.send_monitor_started(n)
            except Exception:
                pass

    def _on_close(self, ws, code, reason) -> None:
        print(f"[WebSocket] Closed — code={code}  reason={reason}")
        try:
            notify.send_monitor_disconnect(code=code, reason=str(reason or ""))
        except Exception:
            pass

    def _on_reconnect(self, ws, n_reconnect: int) -> None:
        print(f"[WebSocket] Reconnecting (attempt {n_reconnect})…")
        try:
            notify.send_monitor_reconnect(attempt=n_reconnect)
        except Exception:
            pass
        # Resubscribe after reconnect; on_connect also handles this on success
        if self._all_tokens:
            try:
                ws.subscribe(self._all_tokens)
                ws.set_mode(ws.MODE_QUOTE, self._all_tokens)
            except Exception:
                pass

    def _on_error(self, ws, code, reason) -> None:
        print(f"[WebSocket] Error — code={code}  reason={reason}", file=sys.stderr)

    def _on_order_update(self, ws, data) -> None:
        pass

    # ── Signal events (called under self._lock) ───────────────────────────────

    def _fire_qualified(self, state: _SymState) -> None:
        ts          = datetime.now(_IST).strftime("%H:%M:%S")
        vol_ratio   = state.cum_vol / state.vol_threshold if state.vol_threshold else 0.0
        vwap_target = state.prev_vwap * _RETURN_MULT
        print(
            f"[{ts}] QUALIFIED {state.symbol:12s}  "
            f"vol={state.cum_vol:>12,.0f} / {state.vol_threshold:>12,.0f} ({vol_ratio:.2f}x)  "
            f"ltp=₹{state.ltp:>9,.2f}  vwap_target=₹{vwap_target:,.2f}"
        )
        try:
            notify.send_monitor_qualified(
                symbol      = state.symbol,
                ts_str      = ts,
                cum_vol     = int(state.cum_vol),
                threshold   = int(state.vol_threshold),
                vol_ratio   = vol_ratio,
                ltp         = state.ltp,
                prev_vwap   = state.prev_vwap,
                vwap_target = vwap_target,
            )
        except Exception as exc:
            print(f"  [notify] qualified failed: {exc}", file=sys.stderr)

    def _fire_near_circuit(self, state: _SymState) -> None:
        ts           = datetime.now(_IST).strftime("%H:%M:%S")
        pct_to_upper = (state.upper_circuit - state.ltp) / state.ltp * 100
        print(
            f"[{ts}] NEAR CIRCUIT {state.symbol:12s}  "
            f"ltp=₹{state.ltp:>9,.2f}  upper=₹{state.upper_circuit:,.2f}  "
            f"gap={pct_to_upper:.2f}%"
        )
        try:
            notify.send_monitor_near_circuit(
                symbol        = state.symbol,
                ts_str        = ts,
                ltp           = state.ltp,
                pct_to_upper  = pct_to_upper,
                upper_circuit = state.upper_circuit,
            )
        except Exception as exc:
            print(f"  [notify] near_circuit failed: {exc}", file=sys.stderr)

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    def _heartbeat_loop(self) -> None:
        while True:
            time.sleep(_HEARTBEAT_SECS)
            with self._lock:
                n_qualified    = sum(1 for s in self._states.values() if s.qualified)
                n_near_circuit = sum(1 for s in self._states.values() if s.near_circuit)
                n_tracking     = len(self._states)
            ts = datetime.now(_IST).strftime("%H:%M:%S")
            print(
                f"[{ts}] Heartbeat — tracking={n_tracking}  "
                f"qualified={n_qualified}  near_circuit={n_near_circuit}"
            )
            try:
                notify.send_monitor_heartbeat(
                    ts_str      = ts,
                    tracking    = n_tracking,
                    qualified   = n_qualified,
                    near_circuit = n_near_circuit,
                )
            except Exception as exc:
                print(f"  [notify] heartbeat failed: {exc}", file=sys.stderr)

    # ── Main run ──────────────────────────────────────────────────────────────

    def run(self) -> None:
        self.setup()

        ticker = KiteTicker(
            self._api_key,
            self._access_token,
            reconnect=True,
            reconnect_max_tries=50,
            reconnect_max_delay=60,
        )

        ticker.on_ticks        = self._on_ticks
        ticker.on_connect      = self._on_connect
        ticker.on_close        = self._on_close
        ticker.on_reconnect    = self._on_reconnect
        ticker.on_error        = self._on_error
        ticker.on_order_update = self._on_order_update

        hb = threading.Thread(target=self._heartbeat_loop, daemon=True)
        hb.start()

        print("Starting KiteTicker…")
        ticker.connect(threaded=False)  # blocks until permanent close


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _parser = argparse.ArgumentParser(description="Zerodha KiteTicker live signal monitor")
    _parser.add_argument("--enable-uc-staged-entry", action="store_true",
                         help="Enable the Case A/B UC-based staged entry ladder "
                              "(places real orders). Default off -- monitoring/alerting only.")
    _parser.add_argument("--dry-run", action="store_true",
                         help="With --enable-uc-staged-entry: simulate staged-entry orders "
                              "without placing them.")
    _args = _parser.parse_args()

    try:
        api_key, access_token = _get_kite_credentials()
        LiveMonitor(api_key, access_token,
                   enable_uc_staged_entry=_args.enable_uc_staged_entry,
                   dry_run=_args.dry_run).run()
    except Exception as exc:
        print(f"live_monitor.py failed to start: {exc}", file=sys.stderr)
        try:
            notify.send_monitor_start_failure(str(exc))
        except Exception:
            pass
        sys.exit(1)
