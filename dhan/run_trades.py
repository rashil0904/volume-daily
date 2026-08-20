"""
dhan/run_trades.py — 3-stage live trading via Dhan (independent of Zerodha)
============================================================================
Entry price : Upstox intraday V3 candle API (same source Zerodha's scripts use) —
              close of 15:20 candle, falls back to 15:19 if 15:20 isn't published yet.
Exit check  : Our own recorded entry fill price vs. current LTP from Dhan's
              /marketfeed/ltp (Stage 2) — pnl = (ltp - fill_price) * qty, computed
              directly rather than trusting Dhan's positions'
              unrealizedProfit field (same lesson learned on the Zerodha side:
              broker-side P&L fields can go stale same-day).
Orders      : Dhan API v2 (dhan/trade.py)
Positions   : results/positions_dhan.json  (independent from positions_zerodha.json)

Mirrors zerodha/run_trades_mtf.py's shape exactly: live per-symbol leverage
check before every entry -> product="MTF" when leverage is actually available
(>=2x), falls back to a resized product="CNC" buy (half the capital base)
otherwise. This is an INDEPENDENT capital pool and position file from the
Zerodha scripts -- same strategy signals, same schedule, separate broker,
separate money.

Usage:
    python dhan/run_trades.py --entry          [--capital AMOUNT] [--dry-run] [--date YYYY-MM-DD]
    python dhan/run_trades.py --exit-925       [--dry-run]
    python dhan/run_trades.py --exit-1159      [--dry-run]
    python dhan/run_trades.py --entry --symbol RELIANCE --capital 5000 --dry-run   (manual single-stock)
"""

import argparse
import csv
import json
import math
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

from dhan.auth import BASE_URL as _DHAN_BASE, get_session as _dhan_session
from dhan.trade import (buy, sell, place_order, order_status as _dhan_order_status,
                        cancel_order as _dhan_cancel_order)
from dhan.instruments import security_id, tick_size
from common.calc_utils import pick_reference_price, compute_allocation, compute_shares
import data_loader as _dl
import notify

# ── Config ─────────────────────────────────────────────────────────────────────

_IST          = ZoneInfo("Asia/Kolkata")
_BROKER       = "dhan"
_RESULTS_DIR  = _ROOT / "results"
_POS_FILE     = _RESULTS_DIR / "positions_dhan.json"
_INSTRUMENTS  = _ROOT / "data" / "instruments" / "upstox_instruments.csv"
_LOG_DIR      = _RESULTS_DIR / "trades"
TOTAL_CAPITAL = 1_500_000

_env = _ROOT / "pipeline" / ".env"
if _env.exists():
    for _ln in _env.read_text().splitlines():
        _ln = _ln.strip()
        if _ln and not _ln.startswith("#") and "=" in _ln:
            _k, _, _v = _ln.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

_sym_cache: dict[str, str] = {}


# ── Instrument key resolution (for Upstox candle API — same as zerodha/) ──────

def _ikey(symbol: str) -> str:
    if "|" in symbol:
        return symbol
    global _sym_cache
    if not _sym_cache and _INSTRUMENTS.exists():
        with open(_INSTRUMENTS, newline="") as f:
            for row in csv.DictReader(f):
                _sym_cache[row["symbol"].strip().upper()] = row["instrument_key"].strip()
    key = _sym_cache.get(symbol.upper())
    if not key:
        raise ValueError(
            f"[dhan] '{symbol}' not found in instruments CSV — "
            "pass the instrument_key directly (e.g. 'NSE_EQ|INE...')"
        )
    return key


def get_reference_price(symbol: str) -> tuple[float, int]:
    """Same reference-candle logic as the Zerodha scripts: close of 15:20 1-min
    candle, falling back to 15:19, then whichever candle at/before 15:20 is
    actually the most recent available."""
    matched        = [{"symbol": symbol, "instrument_key": _ikey(symbol)}]
    candles_by_sym = _dl.load_candles(matched, interval="1minute", mode="intraday")
    candles        = candles_by_sym.get(symbol, [])
    try:
        return pick_reference_price(candles, 1520, 1519)
    except ValueError:
        raise ValueError(
            f"[dhan] No candle data at all for {symbol} up to 15:20 "
            f"({len(candles)} candles). Run after 15:21 IST."
        )


# ── Trade list (same file, same parsing as zerodha/) ──────────────────────────

def _load_symbols(trade_date: date) -> list[str]:
    path = _RESULTS_DIR / "trades" / f"trade_list_{trade_date.isoformat()}.csv"
    if not path.exists():
        sys.exit(f"[dhan] No trade list: {path}")
    with open(path, newline="") as f:
        return [r["symbol"].strip().upper() for r in csv.DictReader(f)]


def _ts() -> str:
    return datetime.now(_IST).isoformat()


# ── Order fill polling ─────────────────────────────────────────────────────────

class OrderRejected(RuntimeError):
    """Order genuinely did not fill (REJECTED/CANCELLED)."""


def _poll_fill(order_id: str, retries: int = 12, delay: float = 1.0) -> tuple[float, int]:
    for _ in range(retries):
        time.sleep(delay)
        try:
            o      = _dhan_order_status(order_id)
            status = (o.get("orderStatus") or "").upper()
            if status == "TRADED":
                return float(o.get("averageTradedPrice") or 0), int(o.get("filledQty") or 0)
            if status in ("REJECTED", "CANCELLED", "EXPIRED"):
                raise OrderRejected(f"Order {order_id} {status}: {o.get('omsErrorDescription', '')}")
        except OrderRejected:
            raise
        except Exception:
            pass
    raise RuntimeError(f"Order {order_id} did not fill within {int(retries * delay)}s")


def _poll_fill_safe(order_id: str,
                    fallback_price: float, fallback_qty: int) -> tuple[float, int]:
    try:
        return _poll_fill(order_id)
    except OrderRejected as exc:
        print(f"[dhan]   ORDER REJECTED — {exc}")
        return 0.0, 0
    except Exception as exc:
        print(f"[dhan]   fill poll failed: {exc} — using fallback values")
        return fallback_price, fallback_qty


def _poll_fill_strict(order_id: str) -> tuple[float, int]:
    """Like _poll_fill_safe, but never guesses on an unconfirmed timeout --
    only a genuine broker-confirmed TRADED status counts as filled. Used for
    ENTRIES only: a MARKET order stuck PENDING with no matching liquidity
    (e.g. a circuit-locked stock) must never get written to positions_dhan.json
    as a phantom fill just because the poll gave up waiting (see STYLEBAAZA,
    2026-08-14 -- an order that sat PENDING for the full 12s poll window on a
    circuit-locked stock got recorded as filled at the reference price/qty,
    when nothing had actually traded). Returns (0.0, 0) for BOTH a genuine
    rejection and an unconfirmed timeout -- either way, nothing gets recorded."""
    try:
        return _poll_fill(order_id)
    except OrderRejected as exc:
        print(f"[dhan]   ORDER REJECTED — {exc}")
        return 0.0, 0
    except Exception as exc:
        print(f"[dhan]   fill poll failed: {exc} — NOT recording as filled "
              f"(order may still be pending at the broker; check manually)")
        return 0.0, 0


# ── Margin / funds checks ──────────────────────────────────────────────────────

def _parse_leverage(raw) -> float:
    """Dhan's /margincalculator returns leverage as a string like "2.41X" (a
    trailing X multiplier suffix), not a plain number. float("2.41X") raises
    ValueError, which _margin_check/_intraday_margin_check's broad except
    Exception silently swallowed -- meaning BOTH margin checks always
    returned None in practice, so entries never actually used MTF leverage
    (silently falling back to CNC every time) and shorts could never verify
    margin even once the LTP endpoint is fixed. Confirmed live 2026-08-17
    against a real /margincalculator response: {"leverage": "2.41X", ...}."""
    s = str(raw or "0").strip()
    if s and s[-1] in ("X", "x"):
        s = s[:-1]
    try:
        return float(s)
    except ValueError:
        return 0.0


def _margin_check(symbol: str, quantity: int, ref_price: float) -> dict | None:
    """POST /margincalculator with productType=MTF for this symbol+quantity.
    Returns {"leverage": float, "margin_required": float} on success, None on
    any lookup failure (caller must treat None as 'could not verify, skip')."""
    session, client_id = _dhan_session()
    try:
        sid = security_id(symbol)
    except ValueError:
        return None
    payload = {
        "dhanClientId":    client_id,
        "exchangeSegment": "NSE_EQ",
        "transactionType": "BUY",
        "quantity":        quantity,
        "productType":     "MTF",
        "securityId":      sid,
        "price":           ref_price,
    }
    try:
        resp = session.post(f"{_DHAN_BASE}/margincalculator", json=payload, timeout=15)
        if not resp.ok:
            return None
        data = resp.json()
        return {
            "leverage":        _parse_leverage(data.get("leverage")),
            "margin_required": float(data.get("totalMargin") or 0),
        }
    except Exception:
        return None


def _intraday_margin_check(symbol: str, quantity: int, price: float) -> dict | None:
    """POST /margincalculator with productType=INTRADAY, transactionType=SELL --
    margin required to open the mirrored intraday short for this symbol+quantity
    (see run_entry_321's shorting add-on in check_exit_925/force_exit_1159). Unlike
    Kite's margin endpoint, Dhan's docs show `price` as a required field (not
    tolerant of 0) -- caller passes the current LTP. Returns
    {"leverage": float, "margin_required": float} on success, None on any lookup
    failure (caller must treat None as 'could not verify, skip the short')."""
    session, client_id = _dhan_session()
    try:
        sid = security_id(symbol)
    except ValueError:
        return None
    payload = {
        "dhanClientId":    client_id,
        "exchangeSegment": "NSE_EQ",
        "transactionType": "SELL",
        "quantity":        quantity,
        "productType":     "INTRADAY",
        "securityId":      sid,
        "price":           price,
    }
    try:
        resp = session.post(f"{_DHAN_BASE}/margincalculator", json=payload, timeout=15)
        if not resp.ok:
            return None
        data = resp.json()
        return {
            "leverage":        _parse_leverage(data.get("leverage")),
            "margin_required": float(data.get("totalMargin") or 0),
        }
    except Exception:
        return None


def _available_balance() -> float | None:
    """GET /fundlimit -- live available balance. None on lookup failure."""
    session, _ = _dhan_session()
    try:
        resp = session.get(f"{_DHAN_BASE}/fundlimit", timeout=15)
        data = resp.json()
        return float(data.get("availabelBalance") or 0)
    except Exception:
        return None


def get_ltp(symbol: str) -> float:
    """Current LTP via Dhan's /marketfeed/ltp, used to compute Stage 2 exit P&L
    directly rather than trusting Dhan's own positions P&L field. Raises
    ValueError if no LTP is returned.

    Single-symbol ad-hoc use only (e.g. a lone manual check). Any call site
    that needs LTP for more than one symbol in the same run MUST use
    get_ltp_batch() instead -- Dhan's Quote APIs (which /marketfeed/ltp falls
    under) are rate-limited to 1 request/second, so N sequential single-symbol
    calls in the same run reliably 429 on the 2nd+ call (confirmed live
    2026-08-20 with 5 open positions: 4 of 5 calls back-to-back failed).
    get_ltp_batch() avoids this by fetching every symbol in ONE call."""
    session, _ = _dhan_session()
    sid = security_id(symbol)
    resp = session.post(f"{_DHAN_BASE}/marketfeed/ltp", json={"NSE_EQ": [int(sid)]}, timeout=15)
    resp.raise_for_status()
    entry = resp.json().get("data", {}).get("NSE_EQ", {}).get(str(sid))
    if not entry or entry.get("last_price") is None:
        raise ValueError(f"[dhan] No LTP found for {symbol}.")
    return float(entry["last_price"])


_LTP_CHUNK = 900  # Dhan docs: up to 1000 securityIds per /marketfeed/ltp call


def get_ltp_batch(symbols: list[str]) -> dict[str, float]:
    """Batch-fetch current LTP for every symbol in ONE call (chunked at 900
    securityIds per call, Dhan's documented cap is 1000) via POST
    /marketfeed/ltp -- same chunking pattern as live_monitor.py's
    _fetch_circuit_limits(). Use this instead of calling get_ltp() once per
    symbol in a loop (see get_ltp's docstring for why). Missing/failed
    symbols are simply absent from the returned dict rather than raising --
    callers should treat a missing key the same as get_ltp() raising (i.e.
    fall into their existing no-LTP-available branch)."""
    result: dict[str, float] = {}
    if not symbols:
        return result

    session, _ = _dhan_session()
    sym_to_sid: dict[str, int] = {}
    for sym in symbols:
        try:
            sym_to_sid[sym] = int(security_id(sym))
        except Exception as exc:
            print(f"[dhan]   LTP batch: could not resolve securityId for {sym}: {exc}")
    sid_to_sym = {sid: sym for sym, sid in sym_to_sid.items()}
    sids = list(sym_to_sid.values())

    for i in range(0, len(sids), _LTP_CHUNK):
        chunk = sids[i : i + _LTP_CHUNK]
        try:
            resp = session.post(f"{_DHAN_BASE}/marketfeed/ltp",
                                json={"NSE_EQ": chunk}, timeout=15)
            resp.raise_for_status()
            data = resp.json().get("data", {}).get("NSE_EQ", {})
        except Exception as exc:
            print(f"[dhan]   LTP batch chunk {i}-{i+len(chunk)-1} failed: {exc}")
            continue
        for sid_str, row in data.items():
            sym = sid_to_sym.get(int(sid_str))
            if sym is None or row.get("last_price") is None:
                continue
            result[sym] = float(row["last_price"])
        if i + _LTP_CHUNK < len(sids):
            time.sleep(1.0)

    return result


def _tick_round(symbol: str, price: float) -> float:
    """Rounds to the nearest valid tick for THIS symbol, per Dhan's own scrip
    master (dhan/instruments.py's tick_size()) -- tick size is NOT a flat ₹0.05
    across NSE equities as originally assumed (that assumption is what caused
    two real order rejections, EXCH:16283, on 2026-08-18): TVSSRICHAK/DREDGECORP/
    RELIANCE are ₹0.10, CAMLINFINE/MOTISONS/SHANTIGOLD are ₹0.01, UFLEX/TARSONS
    are ₹0.05 -- confirmed live, all different. A LIMIT price that isn't a
    multiple of the SYMBOL's real tick gets rejected outright by the exchange.
    Every LIMIT price this file computes should be passed through this instead
    of a plain round(). Raises if the symbol's tick can't be looked up -- never
    guesses a tick for a real order."""
    tick = tick_size(symbol)
    return round(round(price / tick) * tick, 2)


# ── Positions JSON ─────────────────────────────────────────────────────────────

def _load_pos() -> list:
    if not _POS_FILE.exists():
        return []
    try:
        return json.loads(_POS_FILE.read_text())
    except Exception:
        return []


def _save_pos(positions: list) -> None:
    _RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    _POS_FILE.write_text(json.dumps(positions, indent=2, ensure_ascii=False))


def _open_pos(positions: list) -> list:
    return [p for p in positions
            if p.get("broker") == _BROKER
            and p.get("direction") != "short"
            and p.get("status") in ("open", "partial_exit_925_nodata")]


def _open_short_pos(positions: list) -> list:
    """Short positions opened by _open_short() (mirroring a 925/1159 long exit) that
    still need squaring off at 2:39pm."""
    return [p for p in positions
            if p.get("broker") == _BROKER
            and p.get("direction") == "short"
            and p.get("status") == "short_open"]


# ── Broker quantity cross-check (product-aware) ───────────────────────────────

def _broker_qty(symbol: str, product: str) -> tuple[int, str]:
    """Confirm broker-held quantity AND the exchange segment it's actually held
    under, for this exact product, via Dhan's positions and holdings endpoints.
    Returns (quantity, exchange_segment), defaulting to (0, "NSE_EQ") if nothing
    is found.

    NOTE: totalQty is already the full holding -- dpQty and t1Qty are a
    settlement-status BREAKDOWN of totalQty (dpQty + t1Qty == totalQty), not
    additional to it. Confirmed live 2026-08-17: /holdings showed
    {"totalQty": 5, "dpQty": 0, "t1Qty": 5} for a T1-only position, and the old
    "totalQty + t1Qty" formula below double-counted it as 10 -- which then
    false-mismatched against our local qty=5 and blocked check_exit_925 from
    selling MOTISONS/SHANTIGOLD/TARSONS entirely that morning. Fixed to use
    totalQty alone.

    HISTORICAL NOTE (why the exchange segment below isn't just hardcoded):
    the Zerodha side of this pipeline was once bitten by a holding settling
    onto a different exchange than it was bought on overnight, which is why
    Kite's holdings API exposes a nested per-exchange breakdown at all. A
    position can genuinely shift exchange between entry and exit, so the
    /holdings fallback below honors Dhan's reported exchange when it's
    exchange-specific, rather than always assuming NSE_EQ.

    /positions and /holdings answer different questions and are COMBINED, not
    used as alternates. /holdings.totalQty is frozen as of the last
    settlement -- it does NOT decrement intraday after a sell. /positions
    .netQty is only today's net buy/sell delta -- it does NOT reflect
    anything carried over from before today, and for a same-day SELL with no
    matching same-day BUY, Dhan reports that delta as NEGATIVE (day-ledger
    accounting treats a sell-only day the same as opening a fresh short,
    tagging it positionType "SHORT" even though nothing was actually
    shorted). Confirmed live 2026-08-19: JINDRILL had 384 in /holdings
    (unchanged all day) and -192 in /positions (the 192 sold at the 9:25
    no-data fallback) -- the old code required /positions' netQty > 0 to
    trust it, so it discarded the -192 and fell back to /holdings' stale 384,
    false-mismatching against a local shares_remaining of 192 and blocking
    force_exit_1159 from selling the rest. Fixed to add the two together
    (384 + -192 = 192, matching reality) whenever a /holdings row exists,
    only falling back to /positions alone (requiring a positive netQty) when
    /holdings has no row yet -- e.g. a same-day buy that hasn't settled to
    /holdings."""
    session, _ = _dhan_session()
    product = product.upper()

    pos_delta = None
    pos_exch = None
    try:
        resp = session.get(f"{_DHAN_BASE}/positions", timeout=15)
        if resp.ok:
            for p in (resp.json() or []):
                if ((p.get("tradingSymbol") or "").upper() == symbol.upper()
                        and (p.get("productType") or "").upper() == product):
                    pos_delta = int(p.get("netQty") or 0)
                    pos_exch = (p.get("exchangeSegment") or "").upper() or None
                    break
    except Exception:
        pass

    hold_qty = None
    hold_exch = None
    try:
        resp = session.get(f"{_DHAN_BASE}/holdings", timeout=15)
        if resp.ok:
            for h in (resp.json() or []):
                if (h.get("tradingSymbol") or "").upper() != symbol.upper():
                    continue
                hold_qty = int(h.get("totalQty") or 0)
                # /holdings' own "exchange" field is usually the literal
                # string "ALL" (dual-listed/fungible, confirmed live
                # 2026-08-17 for every current holding) -- not itself a
                # placeable exchangeSegment, so it can't be passed straight
                # through as-is. But honor it when it IS exchange-specific
                # (see HISTORICAL NOTE above) -- only "ALL"/unrecognized falls
                # back to NSE_EQ (where this pipeline always places its
                # buys); a real "NSE" or "BSE" from Dhan is mapped to its
                # _EQ segment instead of being overridden.
                exch_raw = (h.get("exchange") or "").strip().upper()
                hold_exch = f"{exch_raw}_EQ" if exch_raw in ("NSE", "BSE") else "NSE_EQ"
                break
    except Exception:
        pass

    if hold_qty is not None:
        total = hold_qty + (pos_delta or 0)
        if total > 0:
            return total, (pos_exch or hold_exch or "NSE_EQ")
    elif pos_delta is not None and pos_delta > 0:
        return pos_delta, (pos_exch or "NSE_EQ")

    return 0, "NSE_EQ"


def _broker_short_qty(symbol: str) -> int:
    """Confirms broker-held short quantity for INTRADAY positions. Dhan's
    /positions reports netQty for a net short as NEGATIVE (mirrors Kite's sign
    convention) -- a separate helper rather than overloading _broker_qty() above,
    which only treats qty>0 as a match. Intraday shorts never settle into holdings
    (closed same day), so no holdings fallback needed. Returns 0 if not found."""
    session, _ = _dhan_session()
    try:
        resp = session.get(f"{_DHAN_BASE}/positions", timeout=15)
        if resp.ok:
            for p in (resp.json() or []):
                if ((p.get("tradingSymbol") or "").upper() == symbol.upper()
                        and (p.get("productType") or "").upper() == "INTRADAY"):
                    qty = int(p.get("netQty") or 0)
                    if qty < 0:
                        return abs(qty)
    except Exception:
        pass
    return 0


_SKIP_SHORTING_FILE = _RESULTS_DIR / ".skip_shorting.txt"


def _shorting_skipped_today() -> bool:
    """Self-expiring kill switch for the mirrored-short add-on -- the file
    holds a single date, and only skips shorting on THAT date, so there's
    nothing to remember to clean up before tomorrow. See _open_short."""
    if not _SKIP_SHORTING_FILE.exists():
        return False
    try:
        return _SKIP_SHORTING_FILE.read_text().strip() == date.today().isoformat()
    except Exception:
        return False


def _fetch_upper_circuit(symbol: str) -> float:
    """On-demand fetch of a single symbol's upper circuit limit via POST
    /marketfeed/quote, for the UC-based short stop-loss (see _open_short
    below). Same endpoint/response shape as dhan/live_monitor.py's own
    _fetch_circuit_limits() -- re-derived here as a single-symbol call since
    only one symbol's circuit is needed per short, rather than importing
    live_monitor's batch-chunking machinery. Circuit limits are an
    exchange-set daily price band, not a live tick value, so a mid-session
    ad-hoc call returns the same current value a batch call would. Raises if
    the fetch fails or no circuit data comes back for this symbol."""
    session, _ = _dhan_session()
    sid = security_id(symbol)
    resp = session.post(f"{_DHAN_BASE}/marketfeed/quote",
                        json={"NSE_EQ": [int(sid)]}, timeout=15)
    resp.raise_for_status()
    row = resp.json().get("data", {}).get("NSE_EQ", {}).get(str(sid))
    if not row or row.get("upper_circuit_limit") is None:
        raise ValueError(f"[dhan] No circuit data found for {symbol}.")
    return float(row["upper_circuit_limit"])


def _fetch_upper_circuit_batch(symbols: list[str]) -> dict[str, float]:
    """Batch-fetch upper circuit limits for every symbol in ONE call (chunked
    at 900 securityIds, Dhan's documented cap is 1000) via POST
    /marketfeed/quote -- same reasoning as get_ltp_batch(): Quote APIs are
    1 req/sec, so a caller looping over multiple symbols with one
    _fetch_upper_circuit() call each would 429 from the 2nd symbol on (see
    get_ltp's docstring for the confirmed-live version of this same bug).
    Missing/failed symbols are simply absent from the returned dict."""
    result: dict[str, float] = {}
    if not symbols:
        return result

    session, _ = _dhan_session()
    sym_to_sid: dict[str, int] = {}
    for sym in symbols:
        try:
            sym_to_sid[sym] = int(security_id(sym))
        except Exception as exc:
            print(f"[dhan]   UC batch: could not resolve securityId for {sym}: {exc}")
    sid_to_sym = {sid: sym for sym, sid in sym_to_sid.items()}
    sids = list(sym_to_sid.values())

    for i in range(0, len(sids), _LTP_CHUNK):
        chunk = sids[i : i + _LTP_CHUNK]
        try:
            resp = session.post(f"{_DHAN_BASE}/marketfeed/quote",
                                json={"NSE_EQ": chunk}, timeout=15)
            resp.raise_for_status()
            data = resp.json().get("data", {}).get("NSE_EQ", {})
        except Exception as exc:
            print(f"[dhan]   UC batch chunk {i}-{i+len(chunk)-1} failed: {exc}")
            continue
        for sid_str, row in data.items():
            sym = sid_to_sym.get(int(sid_str))
            if sym is None or row.get("upper_circuit_limit") is None:
                continue
            result[sym] = float(row["upper_circuit_limit"])
        if i + _LTP_CHUNK < len(sids):
            time.sleep(1.0)

    return result


def _open_short(sym: str, qty: int, source_stage: str, dry_run: bool = False,
                ltp: float | None = None) -> None:
    """Opens a same-quantity intraday short (productType=INTRADAY) mirroring a long
    exit that just filled at either the 9:25am or 11:59am stage -- see run_trades.py's
    module docstring for the shorting add-on. Skips (never raises) on any
    margin-check/balance/fill failure: the long exit that triggered this has already
    happened and is never reversed by a failed short.

    ltp: pre-fetched via get_ltp_batch() by the caller when opening multiple
    shorts in the same pass (see check_exit_925/force_exit_1159) -- avoids a
    separate single-symbol get_ltp() call per short, which would re-introduce
    the same Quote-API rate-limit risk get_ltp_batch() exists to avoid. Falls
    back to a live single-symbol fetch only when called standalone (ltp not
    supplied)."""
    if _shorting_skipped_today():
        print(f"[dhan]   SHORT SKIP — {sym}: shorting disabled for today "
              f"(see {_SKIP_SHORTING_FILE.name}).")
        return

    if ltp is None:
        try:
            ltp = get_ltp(sym)
        except Exception:
            ltp = 0.0

    margin_info = _intraday_margin_check(sym, qty, ltp) if ltp else None
    if margin_info is None:
        print(f"[dhan]   SHORT SKIP — {sym}: could not verify INTRADAY margin.")
        return
    available = _available_balance()
    if available is None or available < margin_info["margin_required"]:
        req = margin_info["margin_required"]
        print(f"[dhan]   SHORT SKIP — {sym}: insufficient balance "
              f"(available {'unknown' if available is None else f'₹{available:,.2f}'} "
              f"< required ₹{req:,.2f}).")
        return

    short_limit = _tick_round(sym, ltp * 0.995)
    try:
        oid = sell(sym, "NSE_EQ", qty, order_type="LIMIT", price=short_limit,
                  product="INTRADAY", dry_run=dry_run)
    except Exception as exc:
        print(f"[dhan]   SHORT FAILED — {sym}: {exc}")
        return

    ep, eq = (ltp, qty) if dry_run else _poll_fill_safe(oid, ltp, qty)
    if eq == 0:
        print(f"[dhan]   SHORT NOT FILLED — {sym} short order rejected.")
        return

    cover_price            = _tick_round(sym, ep * 0.95)
    cover_target_order_id  = None
    cover_target_price     = None
    try:
        cover_target_order_id = buy(sym, "NSE_EQ", eq, order_type="LIMIT",
                                    price=cover_price, product="INTRADAY", dry_run=dry_run)
        cover_target_price = cover_price
        print(f"[dhan]   cover target placed @ ₹{cover_price:,.2f} — order {cover_target_order_id}")
    except Exception as exc:
        print(f"[dhan]   !! SHORT OPENED but cover target placement failed for {sym}: {exc} "
              f"— manual review required (short is live, unprotected until square-off).")

    # UC-based stop-loss: buy-to-cover if price rises to within 0.5% of the
    # day's upper circuit. Limit price pinned AT the UC itself (the highest
    # price legally tradeable that day) rather than a market/SL-M order, so
    # it sits at the front of the book instead of risking a worse fill once
    # the stock is already ripping toward the circuit. Same
    # never-unwind-a-completed-action rule as the cover target above -- a
    # failed circuit fetch or failed order placement here never touches the
    # already-live short or cover target.
    upper_circuit = None
    try:
        upper_circuit = _fetch_upper_circuit(sym)
    except Exception as exc:
        print(f"[dhan]   !! circuit fetch failed for {sym}: {exc} — skipping stop-loss "
              f"(short + cover target remain live, unprotected by a stop-loss "
              f"until manual review).")
        try:
            notify.send_circuit_fetch_failed(broker=_BROKER, symbol=sym, error_msg=str(exc),
                                             dry_run=dry_run)
        except Exception as exc2:
            print(f"  [notify] circuit_fetch_failed failed: {exc2}", file=sys.stderr)

    stop_order_id      = None
    stop_trigger_price = None
    stop_limit_price   = None
    if upper_circuit is not None:
        try:
            stop_limit_price   = _tick_round(sym, upper_circuit)
            stop_trigger_price = _tick_round(sym, upper_circuit * 0.995)
            stop_order_id = buy(sym, "NSE_EQ", eq, order_type="STOP_LOSS",
                               price=stop_limit_price, trigger_price=stop_trigger_price,
                               product="INTRADAY", dry_run=dry_run)
            print(f"[dhan]   stop-loss placed @ trigger ₹{stop_trigger_price:,.2f} "
                  f"limit ₹{stop_limit_price:,.2f} (0.5% below UC ₹{upper_circuit:,.2f}) "
                  f"— order {stop_order_id}")
        except Exception as exc:
            print(f"[dhan]   !! SHORT OPENED (cover target live) but stop-loss placement "
                  f"failed for {sym}: {exc} — manual review required.")

    positions = _load_pos()
    positions.append({
        "broker":                _BROKER,
        "symbol":                sym,
        "direction":             "short",
        "product":               "INTRADAY",
        "source_exit_stage":     source_stage,
        "entry_date":            date.today().isoformat(),
        "entry_price":           round(ep, 4),
        "quantity":              eq,
        "entry_order_id":        oid,
        "cover_target_order_id": cover_target_order_id,
        "cover_target_price":    cover_target_price,
        "stop_order_id":         stop_order_id,
        "stop_trigger_price":    stop_trigger_price,
        "stop_limit_price":      stop_limit_price,
        "status":                "short_open",
        "entry_timestamp":       _ts(),
    })
    if not dry_run:
        _save_pos(positions)
    print(f"[dhan]   SHORT OPENED — {sym}  ₹{ep:,.2f} × {eq}  (from {source_stage} exit)")
    try:
        notify.send_short_open(broker=_BROKER, symbol=f"{sym} [SHORT INTRADAY]", entry_price=ep,
                               shares=eq, source_exit_stage=source_stage, order_id=oid,
                               dry_run=dry_run)
    except Exception as exc:
        print(f"  [notify] short_open failed: {exc}", file=sys.stderr)


# ── Entries log (separate from positions_dhan.json, mirrors mtf_entries_*.csv) ─

def _log_path(trade_date: date) -> Path:
    return _LOG_DIR / f"dhan_entries_{trade_date.isoformat()}.csv"


def _append_log(trade_date: date, row: dict) -> None:
    path       = _log_path(trade_date)
    fieldnames = ["timestamp", "symbol", "quantity", "ref_price", "fill_price",
                  "leverage", "margin_required", "order_id", "status", "product",
                  "capital_base"]
    write_header = not path.exists()
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def _sync_pnl_workbook() -> None:
    """Regenerates results/strategy_pnl_simple.xlsx from the latest
    positions_dhan.json -- called after every pipeline stage (321/925/1159/
    239) so the workbook stays current. Best-effort: loaded by file path
    (not a package import, results/ isn't one) and wrapped so a sync
    failure never blocks the actual trading stage that just ran."""
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "_build_pnl_simple", _RESULTS_DIR / "build_pnl_simple.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.main()
    except Exception as exc:
        print(f"[dhan]   !! P&L workbook sync failed: {exc}", file=sys.stderr)


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY — mirrors zerodha/run_trades_mtf.py's run_entry_321_mtf()
# ══════════════════════════════════════════════════════════════════════════════

def run_entry_321(trade_date: date | None = None, dry_run: bool = False,
                  capital: float | None = None, symbol: str | None = None,
                  shares_override: int | None = None, cnc_only: bool = False) -> None:
    if trade_date is None:
        trade_date = date.today()

    manual_mode = symbol is not None

    if manual_mode:
        symbols    = [symbol.strip().upper()]
        n          = 1
        capital    = capital if capital is not None else TOTAL_CAPITAL / 4
        allocation = capital
    else:
        symbols = _load_symbols(trade_date)
        if not symbols:
            print(f"[dhan] Trade list empty for {trade_date} — nothing to enter.")
            return
        n          = len(symbols)
        capital    = capital if capital is not None else TOTAL_CAPITAL
        allocation = compute_allocation(capital, n)

    print(f"\n{'='*60}")
    print(f"[dhan] Entry {trade_date}{'  DRY RUN' if dry_run else ''}"
          + ("  [MANUAL SINGLE-STOCK]" if manual_mode else ""))
    if manual_mode:
        print(f"[dhan] {symbols[0]}  ·  ₹{allocation:,.0f} allocated"
              + (f"  ·  shares override: {shares_override}" if shares_override else ""))
    else:
        print(f"[dhan] {n} signal(s)  ·  ₹{capital:,.0f} total  ·  ₹{allocation:,.0f} per position")
    print(f"{'='*60}")

    n_entered = 0
    n_skipped = 0

    positions     = _load_pos()
    entered_today = {
        p["symbol"] for p in positions
        if p.get("broker") == _BROKER and p.get("entry_date") == trade_date.isoformat()
    }

    # One batched call for every candidate symbol's LTP, not one call per
    # symbol inside the loop below -- see get_ltp's docstring for why (Quote
    # APIs are 1 req/sec; with N signals today, N sequential single-symbol
    # calls would 429 from the 2nd symbol on).
    ltp_cache = get_ltp_batch(list(symbols))

    for sym in symbols:
        if sym in entered_today:
            print(f"[dhan] {sym} — already entered today, skipping.")
            n_skipped += 1
            continue

        print(f"\n[dhan] {sym}")

        try:
            ref, ref_hhmm = get_reference_price(sym)
            print(f"[dhan]   ref price ({ref_hhmm//100:02d}:{ref_hhmm%100:02d} close): ₹{ref:,.2f}")
        except Exception as exc:
            print(f"[dhan]   SKIP — no reference price: {exc}")
            n_skipped += 1
            continue

        if manual_mode and shares_override is not None:
            shares = shares_override
        else:
            shares = compute_shares(allocation, ref)
        if shares == 0:
            print(f"[dhan]   SKIP — 0 shares at ₹{ref:,.2f} (allocation ₹{allocation:,.0f})")
            n_skipped += 1
            continue
        print(f"[dhan]   shares to buy: {shares}")

        try:
            security_id(sym)
        except ValueError as exc:
            print(f"[dhan]   SKIP — {exc}")
            n_skipped += 1
            continue

        if cnc_only:
            product         = "CNC"
            leverage        = 0.0
            margin_required = shares * ref
            capital_base    = capital
            print(f"[dhan]   --cnc-only — buying {shares} shares CNC (no leverage check)  "
                  f"·  margin required: ₹{margin_required:,.2f}")
        else:
            margin_info  = _margin_check(sym, shares, ref)
            has_leverage = margin_info is not None and margin_info["leverage"] >= 2

        if cnc_only:
            pass
        elif has_leverage:
            product         = "MTF"
            leverage        = margin_info["leverage"]
            margin_required = margin_info["margin_required"]
            capital_base    = capital
            print(f"[dhan]   leverage: {leverage:.2f}x  ·  margin required: ₹{margin_required:,.2f}")
        else:
            reason = ("margin check failed" if margin_info is None
                      else f"leverage below 2x ({margin_info['leverage']:.2f}x)")
            if manual_mode:
                resized_shares = shares
                capital_base   = capital
            else:
                fallback_capital    = capital / 2
                fallback_allocation = compute_allocation(fallback_capital, n)
                resized_shares      = compute_shares(fallback_allocation, ref)
                capital_base        = fallback_capital
            if resized_shares == 0:
                print(f"[dhan]   SKIP — {reason}; 0 shares at ₹{ref:,.2f} on "
                      f"₹{capital_base:,.0f}-based CNC fallback.")
                n_skipped += 1
                continue
            shares          = resized_shares
            product         = "CNC"
            leverage        = margin_info["leverage"] if margin_info else 0.0
            margin_required = shares * ref
            print(f"[dhan]   {reason} — falling back to CNC at {shares} shares "
                  f"(₹{capital_base:,.0f}-based)  ·  margin required: ₹{margin_required:,.2f}")

        available = _available_balance()
        if available is None:
            print(f"[dhan]   SKIP — could not verify available balance for {sym}.")
            n_skipped += 1
            continue
        if available < margin_required:
            print(f"[dhan]   SKIP — insufficient balance (available ₹{available:,.2f} "
                  f"< required ₹{margin_required:,.2f}).")
            n_skipped += 1
            continue
        print(f"[dhan]   balance confirmed: ₹{available:,.2f} available")

        # LIMIT, not MARKET: Dhan/NSE apply their own price-protection band to a
        # MARKET order (confirmed 2026-08-17 -- every "MARKET" order this pipeline
        # places actually comes back from Dhan as orderType=LIMIT near the
        # submission price), and that band is tight enough that CAMLINFINE/UFLEX
        # got 0-filled on ordinary same-second price movement, no circuit lock
        # involved. Placing our own LIMIT 0.5% above live LTP gives explicit,
        # wider headroom to fill instead of relying on Dhan's undocumented band.
        if sym in ltp_cache:
            entry_ltp = ltp_cache[sym]
        else:
            entry_ltp = ref
            print(f"[dhan]   live LTP unavailable — using ref price ₹{ref:,.2f} "
                  f"as the limit-price anchor instead.")
        limit_price = _tick_round(sym, entry_ltp * 1.005)
        print(f"[dhan]   LIMIT buy @ ₹{limit_price:,.2f}  (0.5% above LTP ₹{entry_ltp:,.2f})")

        try:
            order_id = buy(sym, "NSE_EQ", shares,
                          order_type="LIMIT", price=limit_price, product=product, dry_run=dry_run)
        except Exception as exc:
            print(f"[dhan]   ORDER FAILED: {exc}")
            _append_log(trade_date, {
                "timestamp": _ts(), "symbol": sym, "quantity": shares,
                "ref_price": round(ref, 4), "fill_price": "",
                "leverage": leverage, "margin_required": margin_required,
                "order_id": "", "status": "order_failed", "product": product,
                "capital_base": capital_base,
            })
            n_skipped += 1
            continue

        if dry_run:
            fill_price, fill_qty = ref, shares
            print(f"[dhan]   DRY RUN — simulated fill ₹{fill_price:,.2f} × {fill_qty}")
            status = "dry_run"
        else:
            fill_price, fill_qty = _poll_fill_strict(order_id)
            if fill_qty == 0:
                print(f"[dhan]   NOT FILLED — order rejected or unconfirmed "
                      f"(check broker manually, e.g. a circuit-locked stock).")
                _append_log(trade_date, {
                    "timestamp": _ts(), "symbol": sym, "quantity": shares,
                    "ref_price": round(ref, 4), "fill_price": "",
                    "leverage": leverage, "margin_required": margin_required,
                    "order_id": order_id, "status": "not_filled", "product": product,
                    "capital_base": capital_base,
                })
                n_skipped += 1
                continue
            print(f"[dhan]   filled ₹{fill_price:,.2f} × {fill_qty}")
            status = "filled"

        _append_log(trade_date, {
            "timestamp": _ts(), "symbol": sym, "quantity": fill_qty,
            "ref_price": round(ref, 4), "fill_price": round(fill_price, 4),
            "leverage": leverage, "margin_required": margin_required,
            "order_id": order_id, "status": status, "product": product,
            "capital_base": capital_base,
        })

        positions.append({
            "broker":               _BROKER,
            "symbol":               sym,
            "entry_date":           trade_date.isoformat(),
            "reference_price":      round(ref, 4),
            "shares_intended":      shares,
            "actual_fill_price":    round(fill_price, 4),
            "actual_fill_quantity": fill_qty,
            "entry_order_id":       order_id,
            "status":               "open",
            "entry_timestamp":      _ts(),
            "product":              product,
        })
        if not dry_run:
            _save_pos(positions)
        try:
            notify.send_entry(broker=_BROKER, symbol=f"{sym} [{product}]", ref_price=ref,
                              shares=fill_qty, order_id=order_id, dry_run=dry_run)
        except Exception as exc:
            print(f"  [notify] entry failed: {exc}", file=sys.stderr)

        n_entered += 1

    print(f"\n[dhan] Entry complete. Entered: {n_entered}  Skipped: {n_skipped}")
    print(f"[dhan] Log written to {_log_path(trade_date)}")
    _sync_pnl_workbook()


# ══════════════════════════════════════════════════════════════════════════════
# PROFIT TARGETS 9:15am — resting 17% LIMIT sell for every open long that doesn't
# already have one. Runs one cron tick before --exit-925 so target protection is
# live at the exchange before the 9:25 check runs. Shorts get their own 5% cover
# target placed inline at open time (see _open_short below) -- this step only
# concerns long entry-side targets.
# ══════════════════════════════════════════════════════════════════════════════

def place_targets_915(dry_run: bool = False) -> None:
    positions = _load_pos()
    open_ps   = _open_pos(positions)

    print(f"\n{'='*60}")
    print(f"[dhan] Place targets 9:15am{'  DRY RUN' if dry_run else ''}")
    print(f"[dhan] {len(open_ps)} open position(s)")
    print(f"{'='*60}")

    if not open_ps:
        print("[dhan] No open positions — nothing to place targets for.")
        return

    n_placed  = 0
    n_skipped = 0

    # One batched call for every open position's upper circuit -- needed to
    # cap the 17% target below the day's UC (see below); batched for the same
    # reason get_ltp_batch() exists (Quote APIs are 1 req/sec).
    uc_cache = _fetch_upper_circuit_batch([p["symbol"] for p in open_ps])

    for pos in open_ps:
        sym = pos["symbol"]
        if pos.get("target_order_id"):
            print(f"[dhan] {sym} — target already placed ({pos['target_order_id']}), skipping.")
            continue

        try:
            product      = pos.get("product", "MTF")
            is_partial   = pos["status"] == "partial_exit_925_nodata"
            qty          = (int(pos["shares_remaining"]) if is_partial
                            else int(pos["actual_fill_quantity"]))
            fill_price   = float(pos["actual_fill_price"] or 0)

            # Target is 17% above fill, OR 0.5% below the day's upper circuit,
            # WHICHEVER IS LOWER -- a plain 17% target can sit above the UC on
            # a stock that's already run hard (confirmed live 2026-08-20:
            # GOPAL's UC was Rs 317.25 but its 17% target computed to
            # Rs 338.60, and Dhan rejected the order outright since a LIMIT
            # sell can never legally trade above the circuit). Same 0.995
            # safety margin already used for the mirrored-short stop-loss
            # elsewhere in this file, not a new convention.
            seventeen_pct_target = _tick_round(sym, fill_price * 1.17)
            uc = uc_cache.get(sym)
            if uc is not None:
                uc_capped_target = _tick_round(sym, uc * 0.995)
                target_price = min(seventeen_pct_target, uc_capped_target)
                if target_price < seventeen_pct_target:
                    print(f"[dhan]   17% target ₹{seventeen_pct_target:,.2f} exceeds UC ₹{uc:,.2f} "
                          f"— capped to ₹{target_price:,.2f} (0.5% below UC) instead.")
            else:
                target_price = seventeen_pct_target
                print(f"[dhan]   !! UC unavailable for {sym} — using uncapped 17% target "
                      f"₹{target_price:,.2f}; may be rejected if it's above the real UC.")

            print(f"\n[dhan] {sym}  [{product}]  qty={qty}  target=₹{target_price:,.2f}")
            order_id = sell(sym, "NSE_EQ", qty, order_type="LIMIT",
                            price=target_price, product=product, dry_run=dry_run)

            pos["target_order_id"] = order_id
            pos["target_price"]    = target_price
            if not dry_run:
                _save_pos(positions)
            print(f"[dhan]   target placed — order {order_id}")
            try:
                notify.send_target_placed(broker=_BROKER, symbol=f"{sym} [{product}]",
                                          target_price=target_price, order_id=order_id,
                                          dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] target_placed failed: {exc}", file=sys.stderr)
            n_placed += 1
        except Exception as exc:
            print(f"[dhan]   !! target placement failed for {sym}: {exc}. Skipping.")
            n_skipped += 1

    print(f"\n[dhan] Place targets complete. Placed: {n_placed}  Skipped: {n_skipped}.")


# ══════════════════════════════════════════════════════════════════════════════
# EXIT — mirrors zerodha/run_trades_mtf.py's check_exit_925_mtf / force_exit_1159_mtf
# ══════════════════════════════════════════════════════════════════════════════

def check_exit_925(dry_run: bool = False) -> None:
    positions = _load_pos()
    open_ps   = _open_pos(positions)

    print(f"\n{'='*60}")
    print(f"[dhan] Exit check 9:25am{'  DRY RUN' if dry_run else ''}")
    print(f"[dhan] {len(open_ps)} open position(s)")
    print(f"{'='*60}")

    if not open_ps:
        print("[dhan] No open positions — nothing to check.")
        _sync_pnl_workbook()
        return

    # Collected here and opened in one pass AFTER every exit below has been
    # attempted -- all long exits complete first, then all mirrored shorts,
    # rather than interleaving short(A) between exit(A) and exit(B).
    to_short: list[tuple[str, int]] = []

    # One batched call for every open position's LTP, not one call per symbol
    # in the loop below -- Dhan's Quote APIs are 1 req/sec, so N sequential
    # single-symbol calls reliably 429 on the 2nd+ symbol (see get_ltp's
    # docstring). A symbol missing from this dict is treated identically to
    # get_ltp() raising -- falls into the existing no-data fallback branch.
    ltp_cache = get_ltp_batch([p["symbol"] for p in open_ps])

    for pos in open_ps:
        sym        = pos["symbol"]
        product    = pos.get("product", "MTF")
        fill_price = float(pos["actual_fill_price"] or 0)
        is_partial = pos["status"] == "partial_exit_925_nodata"
        qty        = (int(pos["shares_remaining"]) if is_partial
                      else int(pos["actual_fill_quantity"]))

        print(f"\n[dhan] {sym}  [{product}]  fill=₹{fill_price:,.2f}  qty={qty}")

        # Target-order status check, BEFORE the existing no_data/pnl_live branches.
        # If the resting 17% target already TRADED (placed by place_targets_915 at
        # 9:15am), close the position from the target's own fill and skip the rest
        # of this position's processing entirely -- no LTP check, no market sell.
        target_oid = pos.get("target_order_id")
        if target_oid:
            try:
                t_status = _dhan_order_status(target_oid)
            except Exception as exc:
                print(f"[dhan]   !! target status check failed for {sym}: {exc}. "
                      f"Skipping {sym} — manual review required.")
                continue
            if (t_status.get("orderStatus") or "").upper() == "TRADED":
                ep  = float(t_status.get("averageTradedPrice") or 0)
                eq  = int(t_status.get("filledQty") or 0) or qty
                pnl = (ep - fill_price) * eq
                ret = (ep - fill_price) / fill_price * 100 if fill_price else 0
                pos.update({
                    "status":              "exited_925",
                    "exit_price_925":      round(ep, 4),
                    "exit_order_id_925":   target_oid,
                    "exit_timestamp_925":  _ts(),
                    "realized_return_pct": round(ret, 4),
                    "realized_pnl":        round(pnl, 2),
                })
                if not dry_run:
                    _save_pos(positions)
                print(f"[dhan]   TARGET HIT — exited ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
                try:
                    notify.send_target_hit(broker=_BROKER, symbol=f"{sym} [{product}]", stage="925",
                                           exit_price=ep, return_pct=ret, pnl=pnl, dry_run=dry_run)
                except Exception as exc:
                    print(f"  [notify] target_hit failed: {exc}", file=sys.stderr)
                # No mirrored short here -- a stock that just ran +17% to hit its
                # resting target is exactly the one most likely to keep running
                # into an upper circuit, where a short can't be covered (no
                # sellers at UC). Only the no-data-fallback and forced/live-P&L
                # sell branches below still open a mirrored short.
                continue
            # Not traded -- falls through into the branches below, each of which
            # cancels this resting target before it does anything of its own.

        no_data  = False
        pnl_live = 0.0
        try:
            if sym not in ltp_cache:
                raise ValueError(f"[dhan] No LTP found for {sym}.")
            ltp      = ltp_cache[sym]
            pnl_live = (ltp - fill_price) * qty
            print(f"[dhan]   LTP: ₹{ltp:,.2f}  live P&L: ₹{pnl_live:+,.2f}")
        except Exception as exc:
            print(f"[dhan]   no LTP available: {exc}")
            no_data = True

        if no_data:
            if target_oid:
                try:
                    _dhan_cancel_order(target_oid)
                except Exception as exc:
                    print(f"[dhan]   target cancel failed (may already be filled/cancelled): {exc}")
            half   = math.floor(qty / 2)
            remain = qty - half
            if half == 0:
                print(f"[dhan]   qty too small to halve — holding until 11:59am.")
                continue
            exch = "NSE_EQ"
            if not dry_run:
                bqty, exch = _broker_qty(sym, product)
                if bqty != qty:
                    print(f"[dhan]   !! MISMATCH — local={qty} broker={bqty}. "
                          f"Skipping {sym} — manual review required.")
                    continue
            print(f"[dhan]   NO-DATA FALLBACK — selling {half} of {qty}")
            # LTP is unavailable in this branch by definition (that's why we're
            # here) -- anchor the LIMIT price on the recorded fill price instead,
            # same fallback pattern as the entry order's own LTP-unavailable case.
            fallback_limit = _tick_round(sym, fill_price * 0.995)
            try:
                oid = sell(sym, exch, half, order_type="LIMIT", price=fallback_limit,
                          product=product, dry_run=dry_run)
            except Exception as exc:
                print(f"[dhan]   fallback sell failed: {exc}")
                continue
            ep, eq = (fill_price, half) if dry_run else _poll_fill_safe(oid, fill_price, half)
            if eq == 0:
                print(f"[dhan]   NOT FILLED — fallback sell rejected, position left open.")
                continue
            pos.update({
                "status":             "partial_exit_925_nodata",
                "shares_exited_925":  half,
                "shares_remaining":   remain,
                "exit_price_925":     round(ep, 4),
                "exit_order_id_925":  oid,
                "exit_timestamp_925": _ts(),
            })
            if not dry_run:
                _save_pos(positions)
            if remain > 0 and target_oid:
                # Fresh target for the still-open remainder, at the SAME
                # target_price (not recomputed) -- carries target protection
                # into 11:59am for whatever didn't get sold in the half-sell.
                try:
                    new_target_oid = sell(sym, exch, remain, order_type="LIMIT",
                                          price=pos["target_price"], product=product, dry_run=dry_run)
                    pos["target_order_id"] = new_target_oid
                    if not dry_run:
                        _save_pos(positions)
                    print(f"[dhan]   fresh target placed for remaining {remain} "
                          f"@ ₹{pos['target_price']:,.2f} — order {new_target_oid}")
                except Exception as exc:
                    print(f"[dhan]   !! fresh target placement failed for {sym}: {exc}")
            try:
                notify.send_exit_925_nodata(broker=_BROKER, symbol=f"{sym} [{product}]",
                                            shares_exited=half, shares_remaining=remain,
                                            exit_price=ep, dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] exit_925_nodata failed: {exc}", file=sys.stderr)
            to_short.append((sym, eq))
            continue

        if pnl_live > 0:
            if target_oid:
                try:
                    _dhan_cancel_order(target_oid)
                except Exception as exc:
                    print(f"[dhan]   target cancel failed (may already be filled/cancelled): {exc}")
            exch = "NSE_EQ"
            if not dry_run:
                bqty, exch = _broker_qty(sym, product)
                if bqty != qty:
                    print(f"[dhan]   !! MISMATCH — local={qty} broker={bqty}. "
                          f"Skipping {sym} — manual review required.")
                    continue
            print(f"[dhan]   P&L positive — selling {qty}")
            sell_limit = _tick_round(sym, ltp * 0.995)
            try:
                oid = sell(sym, exch, qty, order_type="LIMIT", price=sell_limit,
                          product=product, dry_run=dry_run)
            except Exception as exc:
                print(f"[dhan]   sell failed: {exc}")
                continue
            ep, eq = (fill_price, qty) if dry_run else _poll_fill_safe(oid, fill_price, qty)
            if eq == 0:
                print(f"[dhan]   NOT FILLED — sell rejected, position left open.")
                continue
            pnl     = (ep - fill_price) * eq
            ret_act = (ep - fill_price) / fill_price * 100 if fill_price else 0
            pos.update({
                "status":              "exited_925",
                "exit_price_925":      round(ep, 4),
                "exit_order_id_925":   oid,
                "exit_timestamp_925":  _ts(),
                "realized_return_pct": round(ret_act, 4),
                "realized_pnl":        round(pnl, 2),
            })
            if not dry_run:
                _save_pos(positions)
            print(f"[dhan]   exited ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
            try:
                notify.send_exit_925(broker=_BROKER, symbol=f"{sym} [{product}]", exit_price=ep,
                                     return_pct=ret_act, pnl=pnl, dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] exit_925 failed: {exc}", file=sys.stderr)
            to_short.append((sym, eq))
        else:
            print(f"[dhan]   P&L ≤ 0 (₹{pnl_live:+,.2f}) — holding for 11:59am forced exit.")

    if to_short:
        print(f"\n[dhan] Opening {len(to_short)} mirrored short(s)...")
        short_ltp_cache = get_ltp_batch([sym for sym, _ in to_short])
        for sym, eq in to_short:
            _open_short(sym, eq, "925", dry_run=dry_run, ltp=short_ltp_cache.get(sym, 0.0))

    print(f"\n[dhan] Exit check 9:25am complete.")
    _sync_pnl_workbook()


def force_exit_1159(dry_run: bool = False) -> None:
    positions = _load_pos()
    open_ps   = _open_pos(positions)

    print(f"\n{'='*60}")
    print(f"[dhan] Force exit 11:59am{'  DRY RUN' if dry_run else ''}")
    print(f"[dhan] {len(open_ps)} position(s) still open")
    print(f"{'='*60}")

    if not open_ps:
        print("[dhan] All positions already exited — nothing to force-close.")
        try:
            notify.send_nothing_open_at_1159(broker=_BROKER)
        except Exception as exc:
            print(f"  [notify] nothing_open_at_1159 failed: {exc}", file=sys.stderr)
        _daily_summary(positions, 0, dry_run)
        _sync_pnl_workbook()
        return

    n_force = 0
    # Collected here and opened in one pass AFTER every forced exit below has
    # been attempted -- see the matching comment in check_exit_925.
    to_short: list[tuple[str, int]] = []

    # One batched call for every still-open position's LTP -- see the matching
    # comment in check_exit_925.
    ltp_cache = get_ltp_batch([p["symbol"] for p in open_ps])

    for pos in open_ps:
        sym        = pos["symbol"]
        product    = pos.get("product", "MTF")
        fill_price = float(pos["actual_fill_price"] or 0)
        is_partial = pos["status"] == "partial_exit_925_nodata"
        qty        = (int(pos["shares_remaining"]) if is_partial
                      else int(pos["actual_fill_quantity"]))

        print(f"\n[dhan] {sym}  [{product}]  qty={qty}")

        # Target-order status check, same pattern as check_exit_925: TRADED ->
        # close from the target's own fill and skip; not traded -> cancel it,
        # then proceed into the existing unconditional force-sell below (no
        # branching needed here since 11:59 has no P&L gate to begin with).
        target_oid = pos.get("target_order_id")
        if target_oid:
            try:
                t_status = _dhan_order_status(target_oid)
            except Exception as exc:
                print(f"[dhan]   !! target status check failed for {sym}: {exc}. "
                      f"Skipping {sym} — manual review required.")
                continue
            if (t_status.get("orderStatus") or "").upper() == "TRADED":
                ep = float(t_status.get("averageTradedPrice") or 0)
                eq = int(t_status.get("filledQty") or 0) or qty
                if is_partial:
                    s925 = int(pos.get("shares_exited_925") or 0)
                    p925 = float(pos.get("exit_price_925") or fill_price)
                    pnl  = (p925 - fill_price) * s925 + (ep - fill_price) * eq
                    tot  = int(pos["actual_fill_quantity"])
                    ret  = pnl / (fill_price * tot) * 100 if fill_price and tot else 0
                else:
                    pnl = (ep - fill_price) * eq
                    ret = (ep - fill_price) / fill_price * 100 if fill_price else 0
                pos.update({
                    "status":               "exited_1159",
                    "exit_price_1159":      round(ep, 4),
                    "exit_order_id_1159":   target_oid,
                    "exit_timestamp_1159":  _ts(),
                    "realized_return_pct":  round(ret, 4),
                    "realized_pnl":         round(pnl, 2),
                })
                if not dry_run:
                    _save_pos(positions)
                print(f"[dhan]   TARGET HIT — closed ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
                try:
                    notify.send_target_hit(broker=_BROKER, symbol=f"{sym} [{product}]", stage="1159",
                                           exit_price=ep, return_pct=ret, pnl=pnl, dry_run=dry_run)
                except Exception as exc:
                    print(f"  [notify] target_hit failed: {exc}", file=sys.stderr)
                # No mirrored short here -- see the matching comment in check_exit_925.
                n_force += 1
                continue
            try:
                _dhan_cancel_order(target_oid)
            except Exception as exc:
                print(f"[dhan]   target cancel failed (may already be filled/cancelled): {exc}")

        exch = "NSE_EQ"
        if not dry_run:
            bqty, exch = _broker_qty(sym, product)
            if bqty != qty:
                print(f"[dhan]   !! MISMATCH — local={qty} broker={bqty}. "
                      f"Skipping {sym} — manual review required.")
                continue
            print(f"[dhan]   broker confirmed: {bqty} shares [{exch}]")

        if sym in ltp_cache:
            exit_ltp = ltp_cache[sym]
        else:
            exit_ltp = fill_price
            print(f"[dhan]   live LTP unavailable — using fill price ₹{fill_price:,.2f} "
                  f"as the limit-price anchor instead.")
        sell_limit = _tick_round(sym, exit_ltp * 0.995)
        try:
            oid = sell(sym, exch, qty, order_type="LIMIT", price=sell_limit,
                      product=product, dry_run=dry_run)
        except Exception as exc:
            print(f"[dhan]   sell failed: {exc}")
            continue

        ep, eq = (fill_price, qty) if dry_run else _poll_fill_safe(oid, fill_price, qty)
        if eq == 0:
            print(f"[dhan]   !! NOT FILLED — force-exit sell rejected for {sym}. "
                  f"Position left as-is — manual review required.")
            continue

        if is_partial:
            s925 = int(pos.get("shares_exited_925") or 0)
            p925 = float(pos.get("exit_price_925") or fill_price)
            pnl  = (p925 - fill_price) * s925 + (ep - fill_price) * eq
            tot  = int(pos["actual_fill_quantity"])
            ret  = pnl / (fill_price * tot) * 100 if fill_price and tot else 0
        else:
            pnl = (ep - fill_price) * eq
            ret = (ep - fill_price) / fill_price * 100 if fill_price else 0

        pos.update({
            "status":               "exited_1159",
            "exit_price_1159":      round(ep, 4),
            "exit_order_id_1159":   oid,
            "exit_timestamp_1159":  _ts(),
            "realized_return_pct":  round(ret, 4),
            "realized_pnl":         round(pnl, 2),
        })
        if not dry_run:
            _save_pos(positions)
        print(f"[dhan]   force-exited ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
        try:
            notify.send_force_exit_1159(broker=_BROKER, symbol=f"{sym} [{product}]", exit_price=ep,
                                        return_pct=ret, pnl=pnl, dry_run=dry_run)
        except Exception as exc:
            print(f"  [notify] force_exit_1159 failed: {exc}", file=sys.stderr)
        to_short.append((sym, eq))
        n_force += 1

    if to_short:
        print(f"\n[dhan] Opening {len(to_short)} mirrored short(s)...")
        short_ltp_cache = get_ltp_batch([sym for sym, _ in to_short])
        for sym, eq in to_short:
            _open_short(sym, eq, "1159", dry_run=dry_run, ltp=short_ltp_cache.get(sym, 0.0))

    _daily_summary(positions, n_force, dry_run)
    print(f"\n[dhan] Force exit 11:59am complete. Force-exited: {n_force}.")
    _sync_pnl_workbook()


def _daily_summary(positions: list, n_force: int, dry_run: bool) -> None:
    today    = date.today().isoformat()
    today_ps = [p for p in positions
                if p.get("broker") == _BROKER and p.get("direction") != "short"
                and p.get("entry_date") == today]
    n_opened  = len(today_ps)
    n_925     = sum(1 for p in today_ps if p.get("status") == "exited_925")
    n_partial = sum(1 for p in today_ps
                    if p.get("status") == "exited_1159" and "exit_order_id_925" in p)
    total_pnl = sum(p.get("realized_pnl") or 0 for p in today_ps
                    if p.get("status") in ("exited_925", "exited_1159"))
    print(f"\n[dhan] Summary — opened={n_opened}  exited@925={n_925}  "
          f"partial_nodata={n_partial}  force@1159={n_force}  P&L=₹{total_pnl:+,.2f}")
    try:
        notify.send_daily_summary(broker=_BROKER, n_opened=n_opened, n_exited_925=n_925,
                                  n_partial_nodata=n_partial, n_force_1159=n_force,
                                  total_pnl=total_pnl, dry_run=dry_run)
    except Exception as exc:
        print(f"  [notify] daily_summary failed: {exc}", file=sys.stderr)


# ══════════════════════════════════════════════════════════════════════════════
# SHORT SQUARE-OFF 2:39pm — unconditional buy-to-cover for every short opened by
# _open_short() from either the 925 or 1159 long-exit stages (see module docstring).
# Mirrors force_exit_1159's unconditional-close shape, not check_exit_925's
# conditional one -- this always closes, regardless of P&L.
# ══════════════════════════════════════════════════════════════════════════════

def square_off_239(dry_run: bool = False) -> None:
    positions   = _load_pos()
    open_shorts = _open_short_pos(positions)

    print(f"\n{'='*60}")
    print(f"[dhan] Short square-off 2:39pm{'  DRY RUN' if dry_run else ''}")
    print(f"[dhan] {len(open_shorts)} open short position(s)")
    print(f"{'='*60}")

    if not open_shorts:
        print("[dhan] No open shorts — nothing to square off.")
        _sync_pnl_workbook()
        return

    n_closed = 0

    # One batched call for every open short's cover LTP -- see the matching
    # comment in check_exit_925. Not every short necessarily needs this (only
    # the neither-order-filled fallback path below does), but fetching once
    # up front for all of them is still a single call either way.
    ltp_cache = get_ltp_batch([p["symbol"] for p in open_shorts])

    for pos in open_shorts:
        sym         = pos["symbol"]
        entry_price = float(pos["entry_price"] or 0)
        qty         = int(pos["quantity"])

        print(f"\n[dhan] {sym}  [SHORT INTRADAY]  entry=₹{entry_price:,.2f}  qty={qty}  "
              f"(from {pos.get('source_exit_stage', '?')} exit)")

        # OCO status check -- a short now carries TWO live orders (cover_target,
        # stop-loss). Whichever traded closes the position from ITS OWN fill and
        # cancels the other; if neither traded, cancel BOTH before the existing
        # unconditional force-cover below. Every status check is wrapped
        # individually -- a failed check skips this position entirely for this
        # run rather than guessing filled/unfilled.
        cover_oid = pos.get("cover_target_order_id")
        stop_oid  = pos.get("stop_order_id")

        cover_status = None
        stop_status  = None

        if cover_oid:
            try:
                cover_status = _dhan_order_status(cover_oid)
            except Exception as exc:
                print(f"[dhan]   !! cover target status check failed for {sym}: {exc}. "
                      f"Skipping {sym} — manual review required.")
                continue

        if stop_oid:
            try:
                stop_status = _dhan_order_status(stop_oid)
            except Exception as exc:
                print(f"[dhan]   !! stop-loss status check failed for {sym}: {exc}. "
                      f"Skipping {sym} — manual review required.")
                continue

        cover_traded = bool(cover_status) and (cover_status.get("orderStatus") or "").upper() == "TRADED"
        stop_traded  = bool(stop_status)  and (stop_status.get("orderStatus")  or "").upper() == "TRADED"

        if cover_traded and stop_traded:
            print(f"[dhan]   !! BOTH cover target AND stop-loss show TRADED for {sym} -- "
                  f"OCO race, cannot determine which actually executed first. "
                  f"Skipping {sym} — manual review required.")
            continue

        if cover_traded:
            if stop_oid:
                try:
                    _dhan_cancel_order(stop_oid)
                except Exception as exc:
                    print(f"[dhan]   stop-loss cancel failed (may already be filled/cancelled): {exc}")
            ep  = float(cover_status.get("averageTradedPrice") or 0)
            eq  = int(cover_status.get("filledQty") or 0) or qty
            pnl = (entry_price - ep) * eq
            ret = (entry_price - ep) / entry_price * 100 if entry_price else 0
            pos.update({
                "status":              "short_closed",
                "exit_price_239":      round(ep, 4),
                "exit_order_id_239":   cover_oid,
                "exit_timestamp_239":  _ts(),
                "realized_return_pct": round(ret, 4),
                "realized_pnl":        round(pnl, 2),
            })
            if not dry_run:
                _save_pos(positions)
            print(f"[dhan]   COVER TARGET HIT — squared off ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
            try:
                notify.send_cover_target_hit(broker=_BROKER, symbol=f"{sym} [SHORT INTRADAY]",
                                             entry_price=entry_price, exit_price=ep,
                                             return_pct=ret, pnl=pnl, dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] cover_target_hit failed: {exc}", file=sys.stderr)
            n_closed += 1
            continue

        if stop_traded:
            if cover_oid:
                try:
                    _dhan_cancel_order(cover_oid)
                except Exception as exc:
                    print(f"[dhan]   cover target cancel failed (may already be filled/cancelled): {exc}")
            ep  = float(stop_status.get("averageTradedPrice") or 0)
            eq  = int(stop_status.get("filledQty") or 0) or qty
            pnl = (entry_price - ep) * eq
            ret = (entry_price - ep) / entry_price * 100 if entry_price else 0
            pos.update({
                "status":              "short_closed",
                "exit_price_239":      round(ep, 4),
                "exit_order_id_239":   stop_oid,
                "exit_timestamp_239":  _ts(),
                "realized_return_pct": round(ret, 4),
                "realized_pnl":        round(pnl, 2),
            })
            if not dry_run:
                _save_pos(positions)
            print(f"[dhan]   STOP-LOSS HIT — squared off ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
            try:
                notify.send_short_stoploss_hit(broker=_BROKER, symbol=f"{sym} [SHORT INTRADAY]",
                                               entry_price=entry_price, exit_price=ep,
                                               return_pct=ret, pnl=pnl, dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] short_stoploss_hit failed: {exc}", file=sys.stderr)
            n_closed += 1
            continue

        # Neither filled -- cancel BOTH pending orders before the existing
        # unconditional force-cover below.
        if cover_oid:
            try:
                _dhan_cancel_order(cover_oid)
            except Exception as exc:
                print(f"[dhan]   cover target cancel failed (may already be filled/cancelled): {exc}")
        if stop_oid:
            try:
                _dhan_cancel_order(stop_oid)
            except Exception as exc:
                print(f"[dhan]   stop-loss cancel failed (may already be filled/cancelled): {exc}")

        if not dry_run:
            bqty = _broker_short_qty(sym)
            if bqty != qty:
                print(f"[dhan]   !! MISMATCH — local={qty} broker_short={bqty}. "
                      f"Skipping {sym} — manual review required.")
                continue
            print(f"[dhan]   broker confirmed: {bqty} shares short")

        if sym in ltp_cache:
            cover_ltp = ltp_cache[sym]
        else:
            cover_ltp = entry_price
            print(f"[dhan]   live LTP unavailable — using short entry price "
                  f"₹{entry_price:,.2f} as the limit-price anchor instead.")
        buy_limit = _tick_round(sym, cover_ltp * 1.005)
        try:
            oid = buy(sym, "NSE_EQ", qty, order_type="LIMIT", price=buy_limit,
                     product="INTRADAY", dry_run=dry_run)
        except Exception as exc:
            print(f"[dhan]   cover buy failed: {exc}")
            continue

        ep, eq = (entry_price, qty) if dry_run else _poll_fill_safe(oid, entry_price, qty)
        if eq == 0:
            print(f"[dhan]   !! NOT FILLED — cover buy rejected for {sym}. "
                  f"Position left as-is — manual review required.")
            continue

        pnl = (entry_price - ep) * eq  # short economics: profit when price falls
        ret = (entry_price - ep) / entry_price * 100 if entry_price else 0

        pos.update({
            "status":              "short_closed",
            "exit_price_239":      round(ep, 4),
            "exit_order_id_239":   oid,
            "exit_timestamp_239":  _ts(),
            "realized_return_pct": round(ret, 4),
            "realized_pnl":        round(pnl, 2),
        })
        if not dry_run:
            _save_pos(positions)
        print(f"[dhan]   SQUARED OFF ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
        try:
            notify.send_square_off_239(broker=_BROKER, symbol=f"{sym} [SHORT INTRADAY]",
                                       entry_price=entry_price, exit_price=ep,
                                       return_pct=ret, pnl=pnl, dry_run=dry_run)
        except Exception as exc:
            print(f"  [notify] square_off_239 failed: {exc}", file=sys.stderr)
        n_closed += 1

    print(f"\n[dhan] Short square-off complete. Closed: {n_closed}.")
    _sync_pnl_workbook()


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dhan entry+exit (independent of the Zerodha scripts)")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--entry",         action="store_true", help="Run entry")
    grp.add_argument("--exit-925",      action="store_true", help="Exit check at 9:25am")
    grp.add_argument("--exit-1159",     action="store_true", help="Forced exit at 11:59am")
    grp.add_argument("--square-off-239", action="store_true",
                     help="Square off shorts opened from 925/1159 exits (unconditional, 2:39pm)")
    grp.add_argument("--place-targets", action="store_true",
                     help="Place 17%% profit-target LIMIT sells for open longs (9:15am)")
    parser.add_argument("--dry-run",  action="store_true", help="Simulate without placing orders")
    parser.add_argument("--date",     default=None, help="Trade date YYYY-MM-DD (--entry only; defaults to today)")
    parser.add_argument("--capital",  type=float, default=None,
                        help="Total capital for entries (defaults to TOTAL_CAPITAL if omitted). "
                             "In --symbol mode, this is the amount allocated to that one stock "
                             "(not divided by 4).")
    parser.add_argument("--symbol",   default=None,
                        help="Buy this one symbol instead of reading the day's trade list. "
                             "Shares are sized from --capital/ref-price unless --shares is given.")
    parser.add_argument("--shares",   type=int, default=None,
                        help="Exact quantity to buy in --symbol mode (skips allocation-based sizing).")
    parser.add_argument("--cnc-only", action="store_true",
                        help="Skip the MTF leverage check entirely -- buy every entry as CNC "
                             "using the full per-position allocation (--entry only).")
    args = parser.parse_args()

    if args.shares is not None and args.symbol is None:
        sys.exit("[dhan] --shares requires --symbol")

    td = date.fromisoformat(args.date) if args.date else date.today()

    try:
        if args.entry:
            run_entry_321(trade_date=td, dry_run=args.dry_run, capital=args.capital,
                          symbol=args.symbol, shares_override=args.shares, cnc_only=args.cnc_only)
        elif args.exit_925:
            check_exit_925(dry_run=args.dry_run)
        elif args.exit_1159:
            force_exit_1159(dry_run=args.dry_run)
        elif args.square_off_239:
            square_off_239(dry_run=args.dry_run)
        else:
            place_targets_915(dry_run=args.dry_run)
    except (EnvironmentError, RuntimeError, ValueError) as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)
