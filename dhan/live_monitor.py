#!/usr/bin/env python3
"""
dhan/live_monitor.py — Dhan MarketFeed live signal monitor for NSE mid-cap momentum
=====================================================================================
Monitoring and notification only by default. No orders. No positions. No
execution -- UNLESS --enable-uc-staged-entry is passed (default off), which
additionally watches for the UC-based staged entry (Case A/B, see the
"UC-based staged entry" section below -- folded in here rather than kept as a
separate uc_staged_entry.py file, matching the dhan/ 4-file redesign and
zerodha/live_monitor.py's own Case A/B, which has always lived inline) and
places real orders through it. Telegram is the only output otherwise -- no
CSV event log, no local logs/ directory.
Independent of zerodha/live_monitor.py (Zerodha/KiteTicker) -- same strategy math
(evaluate_tick, thresholds), different data source, kept as a separate file per
this pipeline's pattern of not cross-wiring broker-specific execution paths.

Uses the official `dhanhq` package's MarketFeed WebSocket client (pip install
dhanhq) for ticks, and this project's own dhan/auth.py + dhan/trade.py
for REST calls (circuit limits) and symbol->securityId resolution -- consistent
with how dhan/trade.py and dhan/run_trades.py already talk to Dhan directly via
`requests` rather than the dhanhq package's own REST wrappers.

Run at or after 9:15 AM IST (market open):
    python -m dhan.live_monitor

Prerequisites:
    python -m dhan.auth <ACCESS_TOKEN>   # once per day, before market open

CAVEAT (flagging since this hasn't been run against a live connection yet):
dhanhq's MarketFeed.run() only auto-retries reconnects that fail *after* the
first successful connect (the retry loop lives inside its internal message
loop); a failure on the very first connect attempt propagates and would exit
this script. run() below wraps that in its own outer retry loop as a
safety net. Also: MarketFeed's on_close/on_error callbacks pass less detail
than KiteTicker's (no distinct code/reason), so send_monitor_disconnect here
carries less information than the Zerodha version's. Both worth re-checking
once this has run against real market data.

NOTE on naming: this file's own tick evaluator for the qualified/near_circuit
signal (evaluate_tick, below, operating on _SymState) and the UC-based staged
entry's own tick evaluator (uc_evaluate_tick, further down, operating on
UCState) are two independent state machines over two independent per-symbol
dataclasses -- kept as distinctly-named functions specifically so folding the
latter in here doesn't shadow the former.
"""

# ── IPv4 monkeypatch — must precede any import that touches networking ────────
import socket as _socket
_orig_getaddrinfo = _socket.getaddrinfo
def _ipv4_only_getaddrinfo(*args, **kwargs):
    return [r for r in _orig_getaddrinfo(*args, **kwargs) if r[0] == _socket.AF_INET]
_socket.getaddrinfo = _ipv4_only_getaddrinfo
# ─────────────────────────────────────────────────────────────────────────────

import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime
from datetime import time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

# ── Paths ─────────────────────────────────────────────────────────────────────
_ROOT         = Path(__file__).resolve().parent.parent   # project root (one above dhan/)
_PIPELINE_DIR = _ROOT / "pipeline"
_CANDLES_DIR  = _ROOT / "data" / "candles"
_TOKEN_FILE   = _ROOT / "dhan" / ".token.json"
_IST          = ZoneInfo("Asia/Kolkata")

sys.path.insert(0, str(_PIPELINE_DIR))
sys.path.insert(0, str(_ROOT))

_env_file = _PIPELINE_DIR / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

import notify

try:
    from dhanhq import DhanContext, MarketFeed
except Exception as exc:
    try:
        notify.send_monitor_start_failure(f"[dhan] Import failed: {exc}")
    except Exception:
        pass
    raise

import data_loader
from common.calc_utils import (load_clean_candles, compute_36day_avg_volume,
                               compute_prev_day_vwap, compute_shares)
from dhan.auth import BASE_URL as _DHAN_BASE, get_session as _dhan_session
from dhan.trade import security_id, buy
from dhan.run_trades import (TOTAL_CAPITAL, _ts, _margin_check, _available_balance,
                             _tick_round, _load_long_pos, _save_long_pos, _poll_fill_strict)

# ── Strategy constants (mirror signal_engine.py / zerodha/live_monitor.py exactly) ─
_MIN_PERIODS       = 36
_VOLUME_MULT       = 6
_RETURN_MULT       = 1.05    # LTP >= prev_vwap * 1.05  (5%)
_CIRCUIT_WARN_PCT  = 1.0     # alert when within 1% of upper circuit
_WINDOW_START      = 915
_WINDOW_END        = 1445

# ── Operational constants ─────────────────────────────────────────────────────
_QUOTE_CHUNK     = 900       # Dhan docs: up to 1000 securityIds per /marketfeed/quote call
_HEARTBEAT_SECS  = 60 * 60   # 60 minutes


# ── Auth ──────────────────────────────────────────────────────────────────────

def _get_dhan_credentials() -> tuple[str, str]:
    """
    Validate/refresh via dhan.auth.get_session() (raises DhanAuthError if no
    valid token), then read client_id/access_token directly -- same pattern
    zerodha/live_monitor.py uses for Zerodha.
    """
    _dhan_session()  # validates existing token
    token_data   = json.loads(_TOKEN_FILE.read_text())
    client_id    = os.environ.get("DHAN_CLIENT_ID", "").strip()
    access_token = token_data.get("access_token", "")
    if not client_id:
        raise EnvironmentError("[dhan] DHAN_CLIENT_ID not set in pipeline/.env")
    if not access_token:
        raise RuntimeError(f"[dhan] No access_token in {_TOKEN_FILE}")
    return client_id, access_token


# ── Volume baseline + VWAP precomputation (identical to zerodha/live_monitor.py) ──

@dataclass
class _Baseline:
    symbol:        str
    vol_threshold: float   # 6 × avg_36
    prev_vwap:     float   # VWAP from prev day's 15:00 + 15:15 candles


def _compute_baselines(universe: dict) -> dict[str, _Baseline]:
    """Precompute per-symbol vol_threshold and prev_vwap from candle CSVs.
    Mirrors the inline logic in signal_engine._check_symbol()."""
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

            avg_36 = compute_36day_avg_volume(df, today, _MIN_PERIODS)
            if avg_36 is None:
                continue
            threshold = _VOLUME_MULT * avg_36

            prev_vwap = compute_prev_day_vwap(df, today)
            if prev_vwap is None:
                continue

            baselines[sym] = _Baseline(sym, threshold, prev_vwap)

        except Exception as exc:
            print(f"  [baseline] {sym} — skipped: {exc}", file=sys.stderr)

    return baselines


# ── Circuit limits ────────────────────────────────────────────────────────────

def _fetch_circuit_limits(sym_to_sid: dict[str, int]) -> dict[str, tuple[float, float]]:
    """Batch-fetch upper/lower circuit limits via POST /marketfeed/quote, chunked
    at 900 securityIds per call (Dhan's documented cap is 1000). Returns
    {symbol: (upper_circuit, lower_circuit)}."""
    result = {}
    session, _ = _dhan_session()
    sids       = list(sym_to_sid.values())
    sid_to_sym = {sid: sym for sym, sid in sym_to_sid.items()}

    for i in range(0, len(sids), _QUOTE_CHUNK):
        chunk = sids[i : i + _QUOTE_CHUNK]
        try:
            resp = session.post(f"{_DHAN_BASE}/marketfeed/quote",
                                json={"NSE_EQ": chunk}, timeout=30)
            resp.raise_for_status()
            data = resp.json().get("data", {}).get("NSE_EQ", {})
        except Exception as exc:
            print(f"  [circuit] chunk {i}–{i+len(chunk)-1} failed: {exc}", file=sys.stderr)
            time.sleep(2)
            continue

        for sid_str, row in data.items():
            sym = sid_to_sym.get(int(sid_str))
            if sym is None:
                continue
            upper = float(row.get("upper_circuit_limit") or 0.0)
            lower = float(row.get("lower_circuit_limit") or 0.0)
            result[sym] = (upper, lower)

        if i + _QUOTE_CHUNK < len(sids):
            time.sleep(0.4)

    return result


# ── securityId mapping ─────────────────────────────────────────────────────────

def _build_security_id_maps(symbols: set[str]) -> tuple[dict[int, str], dict[str, int]]:
    """Resolve every symbol to its Dhan securityId via the cached scrip master
    (dhan/trade.py). Returns (sid_to_sym, sym_to_sid)."""
    sid_to_sym: dict[int, str] = {}
    sym_to_sid: dict[str, int] = {}
    unmatched: list[str] = []

    for sym in symbols:
        try:
            sid = int(security_id(sym))
        except (ValueError, TypeError):
            unmatched.append(sym)
            continue
        sid_to_sym[sid] = sym
        sym_to_sid[sym] = sid

    print(f"  Matched {len(sym_to_sid)}/{len(symbols)} symbols to Dhan securityIds.")
    if unmatched:
        sample = sorted(unmatched)[:20]
        print(f"  Unmatched ({len(unmatched)}): {sample}{'…' if len(unmatched) > 20 else ''}")

    return sid_to_sym, sym_to_sid


# ── Per-symbol live state (identical shape to zerodha/live_monitor.py) ───────────

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
    """Pure state-machine evaluation for one tick on one symbol -- byte-for-byte
    the same rule as zerodha/live_monitor.py's evaluate_tick(), duplicated rather
    than imported to keep this file self-contained (see module docstring)."""
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


# ══════════════════════════════════════════════════════════════════════════════
# UC-based staged entry, Case A / Case B (Dhan only)
# ══════════════════════════════════════════════════════════════════════════════
# Runs ALONGSIDE the existing 3:21pm entry (dhan/run_trades.py's
# run_entry_321), never replacing it. Watches live ticks (fed in by this
# file's MarketFeed websocket, gated behind --enable-uc-staged-entry, default
# off) for an early, opportunistic entry whenever a symbol pops 19% above its
# previous close before the normal 3:21pm entry would otherwise buy it:
#
#     Case A qualification filter (checked continuously from market open,
#     independent of the capital snapshot / windows below -- see
#     update_case_a_qualification()):
#         A symbol only becomes Case-A-eligible if it (1) hit upper circuit at
#         some point BEFORE 14:30, AND (2) is subsequently seen off UC (LTP <
#         upper_circuit) at some point during the 14:30-15:18 window. Both are
#         one-way latches -- once case_a_qualified is True it never unlatches.
#         A symbol that never hit UC before 14:30, or hit UC but stays locked
#         through 15:18 without opening up, is never a Case A candidate --
#         falls through untouched to the normal 3:21pm entry.
#     Case A leg 1 (14:30-15:18 IST -- same window as leg 2, widened from an
#                   earlier 14:30-15:00):
#         For a case_a_qualified symbol: LTP crosses up through
#         prev_close * 1.19 -> buy 50% of per_stock_capital.
#     Case A leg 2 (14:30-15:18 IST):
#         LTP retraces back down to prev_close * 1.17 -> buy the other 50%.
#         If leg 2's window closes without firing, the remaining balance is
#         completed with PRIORITY by the 3:21pm entry (Step 1 there), not
#         treated as a fresh entry -- see run_entry_321.
#     Case B (15:00-15:18 IST):
#         For a symbol that is NOT case_a_qualified: LTP crosses up through
#         prev_close * 1.19 -> buy 100% of per_stock_capital in one shot. No
#         legs, no retrace, no UC-proximity gate (explicitly dropped, not an
#         oversight -- confirmed three times now).
#
# Case A/B tie-break during their overlapping 15:00-15:18 window: a symbol is
# excluded from Case B as soon as case_a_qualified is True, even if leg 1
# hasn't fired yet (confirmed -- "still being evaluated for leg 1" counts).
# This is a deliberate check in uc_evaluate_tick, not an accidental
# consequence of entry_status alone (entry_status-only would let a
# qualified-but-not-yet-triggered symbol slip into Case B, which is exactly
# what's excluded here).
#
# per_stock_capital is NOT computed in this section -- it's a snapshot taken
# once by LiveMonitor._on_message (the only place with access to the
# "qualified" count driving it) and passed in as a plain parameter to every
# execute_* call, then persisted onto the position row (capital_base) so the
# 3:21pm entry -- a separate cron invocation, no shared memory with this
# long-running process -- can read back the exact original allocation for a
# Step 1 completion.
#
# State design
# ------------
# Each watched symbol gets one UCState, held on LiveMonitor's own instance
# (one dict, guarded by the same threading.Lock already used for its existing
# per-symbol _SymState states). uc_evaluate_tick() is a pure function (no I/O)
# -- it decides whether to fire and, critically, LATCHES entry_status to
# "order_placed" the instant it decides to fire, synchronously, before the
# caller ever hands the slow order-placement call to a thread pool. Since
# dhanhq's MarketFeed delivers one message at a time to _on_message (never
# concurrently), this synchronous latch is enough on its own to prevent a
# second tick from re-firing the same trigger while the first order is still
# in flight -- no lock needed around the state mutation itself, only around
# dict access shared with the heartbeat thread.
#
# On confirmed success the executor resolves "order_placed" to "filled" or
# "partially_filled" and writes the position row. On failure it reverts to
# the prior *armable* state (not a terminal one) so a later tick can retry
# within the same window -- leg 1 and Case B revert to "not_attempted" (a
# permanently-missed trigger is fine, it just falls through to the 3:21pm
# entry's Step 3 for the full amount, identical to "never triggered", since
# no position row was ever written); leg 2 reverts to "partially_filled"
# (stays armed rather than stranding the position -- either a later tick
# retries it, or the 3:21pm entry's Step 1 completes it regardless of
# whether that retry ever happened).
#
# That final mutation (order_placed -> terminal/reverted) happens inside the
# executor thread, NOT under LiveMonitor's lock -- deliberately not locked,
# not an oversight. It's safe because (a) CPython's GIL makes a single
# attribute assignment atomic, so a concurrent read never sees a torn value,
# and (b) the only thing that could go wrong from a stale read --
# uc_evaluate_tick observing "order_placed" a moment longer than strictly
# necessary -- just means it correctly does nothing that cycle and picks up
# the real state on the next one. Nothing ever re-fires an order from a stale
# read, since "order_placed" isn't a recognized armable state in
# uc_evaluate_tick.
#
# Position-row fields this section adds are ADDITIVE, not a replacement for
# the existing `status` field every other position row already uses --
# `status` still flips to the ordinary "open" once a symbol is fully filled
# (Case A leg 2, or Case B), so place_targets_915/check_exit_925/
# force_exit_1159 need ZERO changes; they already only match status in
# ("open", "partial_exit_925_nodata"). The new fields (`case`,
# `case_a_qualified`, `entry_status`, `case_a_leg`, `filled_amount`,
# `capital_base`) exist purely for this feature's own tracking and for
# run_entry_321's Step 1/2/3 priority ordering.

_CASE_A_START = dtime(14, 30)   # leg 1 and leg 2 share this window (widened -- both used to differ)
_CASE_A_END   = dtime(15, 18)
_CASE_B_START = dtime(15, 0)
_CASE_B_END   = dtime(15, 18)

_LEG_UP_MULT   = 1.19   # prev_close * 1.19 -- leg 1 (Case A) / the fill trigger (Case B)
_LEG_DOWN_MULT = 1.17   # prev_close * 1.17 -- leg 2 retrace (Case A only)


def load_prev_close(symbol: str) -> float | None:
    """Reads data/candles/<symbol>.csv (this pipeline's own 15-min history,
    kept fresh by the existing EOD/intraday jobs) and returns the close of the
    LAST candle strictly before today's date. Returns None if the file's
    missing or has no prior-day rows -- caller should just skip this symbol
    for Case A/B (it still gets the normal, untouched 3:21pm entry).

    NOT sourced from Dhan's own ohlc.close (REST /marketfeed/quote or the
    websocket Quote Data packet's "close" field) -- confirmed live 2026-08-24
    that field tracks TODAY's running/last price, not the previous trading
    day's close (THOMASCOOK: ohlc.close exactly equalled last_price mid-
    afternoon)."""
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
    # Case A qualification filter (see update_case_a_qualification below) --
    # both latches are one-way, never unlatch once True.
    hit_uc_before_1430: bool = False
    off_uc_in_window:   bool = False
    case_a_qualified:   bool = False


def _in_case_a_window(t: dtime) -> bool:
    return _CASE_A_START <= t < _CASE_A_END


def _in_case_b_window(t: dtime) -> bool:
    return _CASE_B_START <= t < _CASE_B_END


def update_case_a_qualification(state: UCState, ltp: float, upper_circuit: float | None,
                                now: datetime | None = None) -> None:
    """Latches the Case A qualification filter from live ticks. Must be
    called on EVERY tick from market open onward (not gated on the
    per_stock_capital snapshot or the 14:30 window -- "hit UC before 14:30"
    has to be observed well before either exists). hit_uc_before_1430
    latches True on the first tick before 14:30 where LTP >= upper_circuit;
    off_uc_in_window latches True on the first tick during the 14:30-15:18
    window where LTP < upper_circuit, but only once hit_uc_before_1430 is
    already set. case_a_qualified is the AND of both -- once True it stays
    True for the rest of the day (checked first below as a cheap no-op)."""
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


def uc_evaluate_tick(state: UCState, ltp: float, per_stock_capital: float,
                     now: datetime | None = None) -> str | None:
    """Pure, side-effecting-on-state-only (no I/O) -- the UC-staged-entry
    counterpart to this file's own evaluate_tick() above (different
    dataclass, different rules -- see the NOTE on naming in the module
    docstring). Returns a fired event name or None. Latches entry_status to
    "order_placed" the instant it fires -- see the "UC-based staged entry"
    section docstring above for why that alone is race-safe against a second
    tick re-firing the same trigger while the first order is still in
    flight. Assumes update_case_a_qualification() has already been called
    for this tick (_on_message calls it unconditionally, first, on every
    tick)."""
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
        # before leg 1 itself has fired ("still being evaluated for leg 1").
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


def _capped_limit_price(sym: str, trigger_price: float, upper_circuit: float | None) -> float:
    """0.5% above the trigger, capped 0.5% below the day's upper circuit --
    same fix already shipped for the sell-side 17% target (place_targets_915,
    GOPAL 2026-08-20). Buying right after a 19% pop is exactly the scenario
    where an uncapped limit can legally exceed a stock's circuit (e.g. a
    20%-band stock) and get rejected outright."""
    limit_price = _tick_round(sym, trigger_price * 1.005)
    if upper_circuit:
        limit_price = min(limit_price, _tick_round(sym, upper_circuit * 0.995))
    return limit_price


def _place_staged_buy(sym: str, qty: int, limit_price: float, dry_run: bool) -> dict | None:
    """One staged-entry BUY leg -- same MTF-then-CNC decision, and the same
    CNC-retry-on-genuine-MTF-ineligibility-rejection logic, as
    run_entry_321 (a Case A/B trigger can hit an MTF-ineligible scrip exactly
    like KLBRENG-B/WELSPLSOL did, 2026-08-21). Unlike run_entry_321's batch
    n-way path, this does NOT halve qty on a CNC fallback -- matches
    run_entry_321's own manual_mode behavior, which this feature's
    per-symbol trigger shape mirrors. Returns None (caller must not write a
    position row, and must revert its state latch) if nothing filled."""
    margin_info  = _margin_check(sym, qty, limit_price)
    has_leverage = margin_info is not None and margin_info["leverage"] >= 2
    product      = "MTF" if has_leverage else "CNC"

    margin_required = margin_info["margin_required"] if has_leverage else qty * limit_price
    available = _available_balance()
    if available is None or available < margin_required:
        print(f"[uc_staged] {sym}: SKIP -- insufficient/unverifiable balance.")
        return None

    try:
        order_id = buy(sym, "NSE_EQ", qty, order_type="LIMIT", price=limit_price,
                       product=product, dry_run=dry_run)
    except Exception as exc:
        print(f"[uc_staged] {sym}: ORDER FAILED -- {exc}")
        return None

    if dry_run:
        return {"order_id": order_id, "product": product,
                "fill_price": limit_price, "fill_qty": qty}

    fill_price, fill_qty, rejected, reason = _poll_fill_strict(order_id)
    if (fill_qty == 0 and product == "MTF" and rejected
            and "mtf product is not allow" in reason.lower()):
        print(f"[uc_staged] {sym}: MTF-INELIGIBLE -- retrying as CNC.")
        try:
            order_id = buy(sym, "NSE_EQ", qty, order_type="LIMIT", price=limit_price,
                           product="CNC", dry_run=dry_run)
            fill_price, fill_qty, _, _ = _poll_fill_strict(order_id)
            product = "CNC"
        except Exception as exc:
            print(f"[uc_staged] {sym}: CNC retry ORDER FAILED -- {exc}")
            return None

    if fill_qty == 0:
        print(f"[uc_staged] {sym}: NOT FILLED.")
        return None

    return {"order_id": order_id, "product": product,
            "fill_price": fill_price, "fill_qty": fill_qty}


def execute_case_a_leg1(sym: str, state: UCState, ltp: float,
                        upper_circuit: float | None, dry_run: bool = False) -> None:
    leg_capital = state.capital_base / 2
    leg_qty     = compute_shares(leg_capital, ltp)
    if leg_qty == 0:
        print(f"[uc_staged] {sym}: Case A leg1 SKIP -- 0 shares at LTP {ltp:.2f}.")
        state.entry_status = "not_attempted"
        return

    limit_price = _capped_limit_price(sym, ltp, upper_circuit)
    result = _place_staged_buy(sym, leg_qty, limit_price, dry_run)
    if result is None:
        state.entry_status = "not_attempted"   # retry-eligible on a later tick this window
        return

    fill_amount = result["fill_price"] * result["fill_qty"]
    positions = _load_long_pos()
    positions.append({
        "broker": "dhan", "symbol": sym, "entry_date": date.today().isoformat(),
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
    state.case          = "A"
    state.entry_status  = "partially_filled"
    state.case_a_leg     = "leg1_filled_watching_retrace"
    state.filled_amount  = fill_amount
    try:
        notify.send_entry(broker="dhan", symbol=f"{sym} [Case A leg1]", ref_price=ltp,
                          shares=result["fill_qty"], order_id=result["order_id"], dry_run=dry_run)
    except Exception as exc:
        print(f"  [notify] Case A leg1 failed: {exc}", file=sys.stderr)


def execute_case_a_leg2(sym: str, state: UCState, ltp: float,
                        upper_circuit: float | None, dry_run: bool = False) -> None:
    positions = _load_long_pos()
    today = date.today().isoformat()
    pos = next((p for p in positions if p.get("symbol") == sym
               and p.get("entry_date") == today and p.get("case") == "A"
               and p.get("entry_status") == "partially_filled"), None)
    if pos is None:
        print(f"[uc_staged] {sym}: Case A leg2 fired but no leg1 row found -- skipping.")
        state.entry_status = "partially_filled"
        return

    remaining = pos["capital_base"] - pos["filled_amount"]
    leg_qty   = compute_shares(remaining, ltp)
    if leg_qty <= 0:
        state.entry_status = "filled"
        state.case_a_leg    = "leg2_filled"
        return

    limit_price = _capped_limit_price(sym, ltp, upper_circuit)
    result = _place_staged_buy(sym, leg_qty, limit_price, dry_run)
    if result is None:
        state.entry_status = "partially_filled"   # stay armed -- retry-eligible, or
        state.case_a_leg    = "leg1_filled_watching_retrace"  # Step 1 at 3:21pm completes it regardless
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
        notify.send_entry(broker="dhan", symbol=f"{sym} [Case A leg2]", ref_price=ltp,
                          shares=result["fill_qty"], order_id=result["order_id"], dry_run=dry_run)
    except Exception as exc:
        print(f"  [notify] Case A leg2 failed: {exc}", file=sys.stderr)


def execute_case_b(sym: str, state: UCState, ltp: float,
                   upper_circuit: float | None, dry_run: bool = False) -> None:
    qty = compute_shares(state.capital_base, ltp)
    if qty == 0:
        print(f"[uc_staged] {sym}: Case B SKIP -- 0 shares at LTP {ltp:.2f}.")
        state.entry_status = "not_attempted"
        return

    limit_price = _capped_limit_price(sym, ltp, upper_circuit)
    result = _place_staged_buy(sym, qty, limit_price, dry_run)
    if result is None:
        state.entry_status = "not_attempted"   # retry-eligible on a later tick this window
        return

    fill_amount = result["fill_price"] * result["fill_qty"]
    positions = _load_long_pos()
    positions.append({
        "broker": "dhan", "symbol": sym, "entry_date": date.today().isoformat(),
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
        notify.send_entry(broker="dhan", symbol=f"{sym} [Case B]", ref_price=ltp,
                          shares=result["fill_qty"], order_id=result["order_id"], dry_run=dry_run)
    except Exception as exc:
        print(f"  [notify] Case B failed: {exc}", file=sys.stderr)


# ── Monitor ────────────────────────────────────────────────────────────────────

class LiveMonitor:
    def __init__(self, client_id: str, access_token: str,
                enable_uc_staged_entry: bool = False, dry_run: bool = False):
        self._client_id    = client_id
        self._access_token = access_token
        self._states: dict[int, _SymState] = {}          # securityId -> state
        self._all_sids: list[int] = []
        self._lock    = threading.Lock()
        self._connect_count = 0   # 0 -> first connect fires send_monitor_started;
                                  # every call after that is a reconnect
        self._last_connect_at = None  # for _throttle_reconnect_ below
        self._reconnect_backoff = 2   # seconds; doubles on rapid reconnects, resets on a stable one

        # UC-based staged entry (Case A/B, see the section above) -- off by
        # default. When on, ticks also drive a second, independent state
        # machine per symbol, and firing an order goes through self._executor
        # so a slow broker call never blocks _on_message.
        self._enable_uc_staged_entry = enable_uc_staged_entry
        self._dry_run                = dry_run
        self._uc_states: dict[int, UCState] = {}
        self._executor = ThreadPoolExecutor(max_workers=4) if enable_uc_staged_entry else None
        # per_stock_capital = TOTAL_CAPITAL / qualified_count, snapshotted
        # EXACTLY ONCE at the first tick observed at/after 14:30 -- not
        # recomputed as more symbols qualify later in the day (confirmed).
        # None until that snapshot happens. Independent of this: the Case A
        # qualification latch (update_case_a_qualification) runs on every
        # tick from market open, well before this snapshot exists.
        self._per_stock_capital: float | None = None

    # ── Startup ───────────────────────────────────────────────────────────────

    def setup(self) -> None:
        print("Loading market cap…")
        universe, mcap_status = data_loader.load_market_cap()
        print(f"  {len(universe)} eligible symbols (mcap_status={mcap_status})")

        print(f"Precomputing volume baselines + VWAP thresholds for {len(universe)} symbols…")
        baselines = _compute_baselines(universe)
        print(f"  {len(baselines)} symbols have ≥{_MIN_PERIODS}-day candle history.")

        print("Resolving Dhan securityIds…")
        sid_to_sym, sym_to_sid = _build_security_id_maps(set(baselines))

        trackable = {s: b for s, b in baselines.items() if s in sym_to_sid}
        print(f"  Trackable (baseline + securityId): {len(trackable)}")

        n_chunks = (len(trackable) + _QUOTE_CHUNK - 1) // _QUOTE_CHUNK
        print(f"Fetching circuit limits via /marketfeed/quote [{n_chunks} chunk(s)]…")
        circuits = _fetch_circuit_limits({s: sym_to_sid[s] for s in trackable})
        print(f"  Circuit limits received for {len(circuits)}/{len(trackable)} symbols.")

        for sym, baseline in trackable.items():
            sid          = sym_to_sid[sym]
            upper, lower = circuits.get(sym, (0.0, 0.0))
            self._states[sid] = _SymState(
                symbol        = sym,
                vol_threshold = baseline.vol_threshold,
                prev_vwap     = baseline.prev_vwap,
                upper_circuit = upper,
                lower_circuit = lower,
            )

        self._all_sids = list(self._states)
        print(f"\nReady — monitoring {len(self._all_sids)} symbols via Dhan MarketFeed (Quote mode).")

        if self._enable_uc_staged_entry:
            print("UC-based staged entry ENABLED — loading previous-day closes…")
            n_have_prev_close = 0
            for sid, state in self._states.items():
                prev_close = load_prev_close(state.symbol)
                if prev_close is not None:
                    n_have_prev_close += 1
                self._uc_states[sid] = UCState(
                    symbol=state.symbol, prev_close=prev_close)
            print(f"  {n_have_prev_close}/{len(self._uc_states)} symbols have a usable "
                  f"prev_close (rest fall through to the normal 3:21pm entry untouched).")

    # ── WebSocket callback ───────────────────────────────────────────────────
    # dhanhq's MarketFeed calls on_message(feed, data) once per parsed message
    # (one instrument per call), unlike KiteTicker's on_ticks(ws, ticks: list)
    # which batches. See module docstring.

    def _on_message(self, feed, data: dict) -> None:
        if not data or data.get("type") != "Quote Data":
            return   # ignore OI/status/prev-close packets, not subscribed to Ticker/Full

        try:
            sid = int(data.get("security_id"))
        except (TypeError, ValueError):
            return
        state = self._states.get(sid)
        if state is None:
            return

        try:
            ltp     = float(data.get("LTP") or 0.0)
            cum_vol = float(data.get("volume") or 0.0)
        except (TypeError, ValueError):
            return

        with self._lock:
            fired = evaluate_tick(state, ltp, cum_vol)
            if "qualified"    in fired: self._fire_qualified(state)
            if "near_circuit" in fired: self._fire_near_circuit(state)

            if self._enable_uc_staged_entry:
                now = datetime.now(_IST)
                uc_state = self._uc_states.get(sid)

                # Case A qualification latch runs on EVERY tick from market
                # open onward -- "hit UC before 14:30" must be observed long
                # before the 14:30 window or the capital snapshot exist.
                if uc_state is not None:
                    update_case_a_qualification(
                        uc_state, ltp, state.upper_circuit, now=now)

                # One-time capital snapshot: TOTAL_CAPITAL / qualified_count,
                # taken at the first tick observed at/after 14:30 -- see
                # __init__. Every UC-staged-entry call after this reads it
                # back as a plain parameter, never recomputed later even as
                # more symbols qualify.
                if self._per_stock_capital is None and now.time() >= _CASE_A_START:
                    n_qualified = sum(1 for s in self._states.values() if s.qualified)
                    self._per_stock_capital = TOTAL_CAPITAL / max(n_qualified, 1)
                    print(f"[uc_staged] per_stock_capital snapshot: "
                          f"₹{TOTAL_CAPITAL:,.0f} / {n_qualified} qualified "
                          f"= ₹{self._per_stock_capital:,.2f}")

                if uc_state is not None and self._per_stock_capital is not None:
                    uc_event = uc_evaluate_tick(
                        uc_state, ltp, self._per_stock_capital, now=now)
                    if uc_event is not None:
                        self._fire_uc_staged(uc_event, uc_state, ltp, state.upper_circuit)

    # ── UC staged entry dispatch (called under self._lock; only submits to
    #    self._executor, never calls the slow order-placement function
    #    inline -- see module docstring) ──────────────────────────────────────

    def _fire_uc_staged(self, event: str, uc_state: "UCState",
                        ltp: float, upper_circuit: float) -> None:
        fn = {
            "case_a_leg1": execute_case_a_leg1,
            "case_a_leg2": execute_case_a_leg2,
            "case_b_fill": execute_case_b,
        }.get(event)
        if fn is None:
            return
        self._executor.submit(fn, uc_state.symbol, uc_state, ltp, upper_circuit, self._dry_run)

    # dhanhq's own internal reconnect loop (MarketFeed._run_async) retries on
    # a flat 1-second sleep with NO backoff -- fine for a single transient
    # blip, but if Dhan's WS server keeps killing the connection right back
    # (seen live 2026-08-19: ~1-second-apart reconnects for several minutes
    # straight), that hammers Dhan hard enough that THEY start rejecting new
    # connections outright with HTTP 429, which then just keeps the same
    # 1-second retry loop failing forever. There's no reconnect-delay knob on
    # MarketFeed itself, and patching the installed package isn't an option
    # -- so this blocks synchronously (time.sleep, safe here since dhanhq
    # calls on_connect as a plain sync function from inside its own asyncio
    # loop) with escalating backoff whenever reconnects arrive suspiciously
    # fast, which is the only lever this script has over the library's retry
    # cadence. Backoff resets once a connection has actually held for a
    # while, so a single blip still recovers in ~2s like before.
    def _throttle_reconnect_(self) -> None:
        now = time.monotonic()
        if self._last_connect_at is not None:
            held_for = now - self._last_connect_at
            if held_for < 30:
                print(f"[WebSocket] Previous connection lasted only {held_for:.1f}s — "
                      f"backing off {self._reconnect_backoff}s before letting dhanhq retry again.")
                time.sleep(self._reconnect_backoff)
                self._reconnect_backoff = min(self._reconnect_backoff * 2, 60)
            else:
                self._reconnect_backoff = 2
        self._last_connect_at = time.monotonic()

    def _on_connect(self, feed) -> None:
        n = len(self._all_sids)
        self._connect_count += 1
        if self._connect_count == 1:
            print(f"[WebSocket] Connected — subscribed {n} securityIds in Quote mode…")
            self._last_connect_at = time.monotonic()
            try:
                notify.send_monitor_started(n)
            except Exception:
                pass
        else:
            self._throttle_reconnect_()
            print(f"[WebSocket] Reconnected (attempt {self._connect_count})…")
            try:
                notify.send_monitor_reconnect(attempt=self._connect_count)
            except Exception:
                pass

    def _on_close(self, feed) -> None:
        # dhanhq's on_close carries no code/reason (unlike KiteTicker's) --
        # see module docstring caveat.
        print("[WebSocket] Closed")
        try:
            notify.send_monitor_disconnect(code="n/a", reason="Dhan feed closed")
        except Exception:
            pass

    def _on_error(self, feed, exc) -> None:
        print(f"[WebSocket] Error — {exc}", file=sys.stderr)

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
                symbol      = f"{state.symbol} [DHAN]",
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
                symbol        = f"{state.symbol} [DHAN]",
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

        instruments = [(MarketFeed.NSE, str(sid), MarketFeed.Quote) for sid in self._all_sids]
        dhan_context = DhanContext(self._client_id, self._access_token)

        hb = threading.Thread(target=self._heartbeat_loop, daemon=True)
        hb.start()

        print("Starting Dhan MarketFeed…")
        # Outer retry loop: MarketFeed.run() only auto-retries reconnects that
        # happen AFTER a successful first connect (see module docstring) -- a
        # failure on the very first attempt propagates out of run(), so this
        # wrapper re-instantiates and retries rather than letting the whole
        # script die on a transient startup failure.
        backoff = 5
        while True:
            feed = MarketFeed(
                dhan_context, instruments, "v2",
                on_connect = self._on_connect,
                on_message = self._on_message,
                on_close   = self._on_close,
                on_error   = self._on_error,
            )
            try:
                feed.run()   # blocks; internally retries reconnects once connected
                return       # clean shutdown (e.g. KeyboardInterrupt handled inside)
            except Exception as exc:
                print(f"[WebSocket] run() raised: {exc} — retrying in {backoff}s", file=sys.stderr)
                try:
                    notify.send_monitor_disconnect(code="n/a", reason=f"run() raised: {exc}")
                except Exception:
                    pass
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--enable-uc-staged-entry", action="store_true",
                         help="Enable Case A/B UC-based staged entry "
                              "(see the 'UC-based staged entry' section in this file). "
                              "Default off -- monitoring/"
                              "notification only, exactly like today, unless passed.")
    _parser.add_argument("--dry-run", action="store_true",
                         help="With --enable-uc-staged-entry: simulate staged-entry "
                              "orders (log, don't place) instead of real ones.")
    _args = _parser.parse_args()

    try:
        client_id, access_token = _get_dhan_credentials()
        LiveMonitor(client_id, access_token,
                   enable_uc_staged_entry=_args.enable_uc_staged_entry,
                   dry_run=_args.dry_run).run()
    except Exception as exc:
        print(f"dhan/live_monitor.py failed to start: {exc}", file=sys.stderr)
        try:
            notify.send_monitor_start_failure(f"[dhan] {exc}")
        except Exception:
            pass
        sys.exit(1)
