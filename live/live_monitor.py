#!/usr/bin/env python3
"""
live_monitor.py — KiteTicker live signal monitor for NSE mid-cap momentum
=========================================================================
Monitoring and notification only. No orders. No positions. No execution.

Run at or after 9:15 AM IST (market open):
    python live_monitor.py

Prerequisites:
    python -m zerodha.auth   # once per day, before market open
"""

# ── IPv4 monkeypatch — must precede any import that touches networking ────────
import socket as _socket
_orig_getaddrinfo = _socket.getaddrinfo
def _ipv4_only_getaddrinfo(*args, **kwargs):
    return [r for r in _orig_getaddrinfo(*args, **kwargs) if r[0] == _socket.AF_INET]
_socket.getaddrinfo = _ipv4_only_getaddrinfo
# ─────────────────────────────────────────────────────────────────────────────

import csv
import json
import os
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
_ROOT         = Path(__file__).resolve().parent.parent   # project root (one above live/)
_PIPELINE_DIR = _ROOT / "pipeline"
_CANDLES_DIR  = _ROOT / "data" / "candles"
_LOGS_DIR     = _ROOT / "logs"
_TOKEN_FILE   = _ROOT / "zerodha" / ".token.json"
_IST          = ZoneInfo("Asia/Kolkata")

# Add pipeline/ and live/ to path so pipeline modules and imbalance_tracker resolve
sys.path.insert(0, str(_PIPELINE_DIR))
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))  # live/ for imbalance_tracker

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
from imbalance_tracker import ImbalanceLogger, ImbalanceState, update_imbalance, make_label
from common.calc_utils import load_clean_candles, compute_36day_avg_volume, compute_prev_day_vwap

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


# ── CSV event log ─────────────────────────────────────────────────────────────

_LOG_FIELDS = [
    "timestamp", "symbol", "event",
    "ltp", "cum_vol", "vol_threshold", "vol_ratio",
    "prev_vwap", "vwap_target", "upper_circuit", "pct_to_upper",
]


class _CsvLog:
    def __init__(self):
        _LOGS_DIR.mkdir(exist_ok=True)
        self._path = _LOGS_DIR / f"live_monitor_{date.today().isoformat()}.csv"
        self._lock = threading.Lock()
        if not self._path.exists():
            with open(self._path, "w", newline="") as f:
                csv.DictWriter(f, fieldnames=_LOG_FIELDS).writeheader()

    def write(self, event: str, state: _SymState) -> None:
        vol_ratio    = round(state.cum_vol / state.vol_threshold, 4) if state.vol_threshold else 0.0
        vwap_target  = round(state.prev_vwap * _RETURN_MULT, 2)
        pct_to_upper = (
            round((state.upper_circuit - state.ltp) / state.ltp * 100, 2)
            if state.ltp > 0 and state.upper_circuit > 0 else ""
        )
        row = {
            "timestamp":     datetime.now(_IST).isoformat(timespec="seconds"),
            "symbol":        state.symbol,
            "event":         event,
            "ltp":           state.ltp,
            "cum_vol":       int(state.cum_vol),
            "vol_threshold": int(state.vol_threshold),
            "vol_ratio":     vol_ratio,
            "prev_vwap":     round(state.prev_vwap, 2),
            "vwap_target":   vwap_target,
            "upper_circuit": state.upper_circuit,
            "pct_to_upper":  pct_to_upper,
        }
        with self._lock:
            with open(self._path, "a", newline="") as f:
                csv.DictWriter(f, fieldnames=_LOG_FIELDS).writerow(row)


# ── Monitor ────────────────────────────────────────────────────────────────────

class LiveMonitor:
    def __init__(self, api_key: str, access_token: str):
        self._api_key      = api_key
        self._access_token = access_token
        self._states: dict[int, _SymState] = {}       # instrument_token -> state
        self._imb_states: dict[int, ImbalanceState] = {}  # instrument_token -> imbalance state
        self._all_tokens: list[int] = []
        self._log     = _CsvLog()
        self._imb_log = ImbalanceLogger(_LOGS_DIR)
        self._lock    = threading.Lock()
        self._started_notified = False   # fire send_monitor_started() once, on first connect only
        self._imbalance_snapshot_done = False   # log qualified-symbol imbalance once, at 15:20 IST

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
            self._imb_states[token] = ImbalanceState(symbol=sym)

        self._all_tokens = list(self._states)
        print(f"\nReady — monitoring {len(self._all_tokens)} symbols via KiteTicker MODE_FULL.")

    # ── WebSocket callbacks ───────────────────────────────────────────────────

    def _on_ticks(self, ws, ticks: list) -> None:
        for tick in ticks:
            token = tick.get("instrument_token")
            state = self._states.get(token)
            if state is None:
                continue

            ltp     = float(tick.get("last_price")   or 0.0)
            cum_vol = float(tick.get("volume_traded") or 0.0)

            # MODE_FULL-only field: 5-level bid/ask depth. Summed across all
            # levels on each side (not just best bid/ask) to smooth out a
            # single large resting order at one level.
            depth       = tick.get("depth") or {}
            bid_qty_tot = sum(int(lvl.get("quantity") or 0) for lvl in (depth.get("buy")  or []))
            ask_qty_tot = sum(int(lvl.get("quantity") or 0) for lvl in (depth.get("sell") or []))

            with self._lock:
                fired     = evaluate_tick(state, ltp, cum_vol)
                imb_state = self._imb_states.get(token)
                # Only accumulate depth-imbalance history for symbols that have
                # already qualified — no point tracking book pressure on the
                # other ~440 symbols that never fire a signal.
                if state.qualified and imb_state is not None:
                    update_imbalance(imb_state, bid_qty_tot, ask_qty_tot)
                if "qualified"    in fired: self._fire_qualified(state)
                if "near_circuit" in fired: self._fire_near_circuit(state)

        self._maybe_snapshot_imbalance()

    def _on_connect(self, ws, response) -> None:
        n = len(self._all_tokens)
        print(f"[WebSocket] Connected — subscribing {n} tokens in MODE_FULL…")
        ws.subscribe(self._all_tokens)
        ws.set_mode(ws.MODE_FULL, self._all_tokens)
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
                ws.set_mode(ws.MODE_FULL, self._all_tokens)
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
        self._log.write("qualified", state)
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
        self._log.write("near_circuit", state)
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

    # ── 15:20 imbalance snapshot ───────────────────────────────────────────────
    # Fires once, the first tick batch received at/after 15:20 IST: logs each
    # currently-qualified symbol's last-20min rolling book imbalance to
    # logs/imbalance_<date>.csv. Log only — no Telegram, no trading action.

    def _maybe_snapshot_imbalance(self) -> None:
        if self._imbalance_snapshot_done:
            return
        now = datetime.now(_IST)
        if now.hour * 100 + now.minute < 1520:
            return
        self._imbalance_snapshot_done = True

        n_logged = 0
        for token, state in self._states.items():
            if not state.qualified:
                continue
            imb_state = self._imb_states.get(token)
            if imb_state is None or not imb_state.rolling:
                continue
            rolling = sum(v for _, v in imb_state.rolling) / len(imb_state.rolling)
            self._imb_log.log_event(
                "snapshot_1520", imb_state,
                imb_state.last_imbalance, rolling, make_label(rolling),
            )
            n_logged += 1
        print(f"[{now.strftime('%H:%M:%S')}] 15:20 imbalance snapshot — "
              f"logged {n_logged} qualified symbol(s).")

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
    try:
        api_key, access_token = _get_kite_credentials()
        LiveMonitor(api_key, access_token).run()
    except Exception as exc:
        print(f"live_monitor.py failed to start: {exc}", file=sys.stderr)
        try:
            notify.send_monitor_start_failure(str(exc))
        except Exception:
            pass
        sys.exit(1)
