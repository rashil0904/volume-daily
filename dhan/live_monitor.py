#!/usr/bin/env python3
"""
dhan/live_monitor.py — Dhan MarketFeed live signal monitor for NSE mid-cap momentum
=====================================================================================
Monitoring and notification only. No orders. No positions. No execution.
Telegram is the only output -- no CSV event log, no local logs/ directory.
Independent of zerodha/live_monitor.py (Zerodha/KiteTicker) -- same strategy math
(evaluate_tick, thresholds), different data source, kept as a separate file per
this pipeline's pattern of not cross-wiring broker-specific execution paths.

Uses the official `dhanhq` package's MarketFeed WebSocket client (pip install
dhanhq) for ticks, and this project's own dhan/auth.py + dhan/instruments.py
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
from dataclasses import dataclass
from datetime import date, datetime
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
from common.calc_utils import load_clean_candles, compute_36day_avg_volume, compute_prev_day_vwap
from dhan.auth import BASE_URL as _DHAN_BASE, get_session as _dhan_session
from dhan.instruments import security_id

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
    (dhan/instruments.py). Returns (sid_to_sym, sym_to_sid)."""
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


# ── Monitor ────────────────────────────────────────────────────────────────────

class LiveMonitor:
    def __init__(self, client_id: str, access_token: str):
        self._client_id    = client_id
        self._access_token = access_token
        self._states: dict[int, _SymState] = {}          # securityId -> state
        self._all_sids: list[int] = []
        self._lock    = threading.Lock()
        self._connect_count = 0   # 0 -> first connect fires send_monitor_started;
                                  # every call after that is a reconnect

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

    def _on_connect(self, feed) -> None:
        n = len(self._all_sids)
        self._connect_count += 1
        if self._connect_count == 1:
            print(f"[WebSocket] Connected — subscribed {n} securityIds in Quote mode…")
            try:
                notify.send_monitor_started(n)
            except Exception:
                pass
        else:
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
    try:
        client_id, access_token = _get_dhan_credentials()
        LiveMonitor(client_id, access_token).run()
    except Exception as exc:
        print(f"dhan/live_monitor.py failed to start: {exc}", file=sys.stderr)
        try:
            notify.send_monitor_start_failure(f"[dhan] {exc}")
        except Exception:
            pass
        sys.exit(1)
