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
from dhan.trade import buy, sell, order_status as _dhan_order_status
from dhan.instruments import security_id
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
TOTAL_CAPITAL = 300_000

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
            "leverage":        float(data.get("leverage") or 0),
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
            "leverage":        float(data.get("leverage") or 0),
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
    ValueError if no LTP is returned."""
    session, _ = _dhan_session()
    sid = security_id(symbol)
    resp = session.post(f"{_DHAN_BASE}/marketfeed/ltp", json={"NSE_EQ": [int(sid)]}, timeout=15)
    resp.raise_for_status()
    entry = resp.json().get("data", {}).get("NSE_EQ", {}).get(str(sid))
    if not entry or entry.get("last_price") is None:
        raise ValueError(f"[dhan] No LTP found for {symbol}.")
    return float(entry["last_price"])


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

    NOTE: Dhan's published /holdings response doesn't document a separate MTF
    sub-quantity field the way Kite's does (Kite's holdings expose a nested
    "mtf": {quantity, used_quantity}, which is what the Zerodha side of this
    pipeline needed after a real bug where MTF holdings settled onto a
    different exchange overnight). This function checks /positions first (which
    DOES have an explicit productType field) and falls back to /holdings'
    totalQty only if nothing is found there -- but the CNC/MTF split in
    /holdings hasn't been verified against a real Dhan MTF fill yet. Re-verify
    this once the first real Dhan MTF position exists and needs exiting."""
    session, _ = _dhan_session()
    product = product.upper()
    try:
        resp = session.get(f"{_DHAN_BASE}/positions", timeout=15)
        if resp.ok:
            for p in (resp.json() or []):
                if ((p.get("tradingSymbol") or "").upper() == symbol.upper()
                        and (p.get("productType") or "").upper() == product):
                    qty = int(p.get("netQty") or 0)
                    if qty > 0:
                        return qty, (p.get("exchangeSegment") or "NSE_EQ").upper()
    except Exception:
        pass
    try:
        resp = session.get(f"{_DHAN_BASE}/holdings", timeout=15)
        if resp.ok:
            for h in (resp.json() or []):
                if (h.get("tradingSymbol") or "").upper() != symbol.upper():
                    continue
                qty = int(h.get("totalQty") or 0) + int(h.get("t1Qty") or 0)
                if qty > 0:
                    return qty, (h.get("exchange") or "NSE_EQ").upper()
    except Exception:
        pass
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


def _open_short(sym: str, qty: int, source_stage: str, dry_run: bool = False) -> None:
    """Opens a same-quantity intraday short (productType=INTRADAY) mirroring a long
    exit that just filled at either the 9:25am or 11:59am stage -- see run_trades.py's
    module docstring for the shorting add-on. Skips (never raises) on any
    margin-check/balance/fill failure: the long exit that triggered this has already
    happened and is never reversed by a failed short."""
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

    try:
        oid = sell(sym, "NSE_EQ", qty, order_type="MARKET", product="INTRADAY", dry_run=dry_run)
    except Exception as exc:
        print(f"[dhan]   SHORT FAILED — {sym}: {exc}")
        return

    ep, eq = (ltp, qty) if dry_run else _poll_fill_safe(oid, ltp, qty)
    if eq == 0:
        print(f"[dhan]   SHORT NOT FILLED — {sym} short order rejected.")
        return

    positions = _load_pos()
    positions.append({
        "broker":            _BROKER,
        "symbol":            sym,
        "direction":         "short",
        "product":           "INTRADAY",
        "source_exit_stage": source_stage,
        "entry_date":        date.today().isoformat(),
        "entry_price":       round(ep, 4),
        "quantity":          eq,
        "entry_order_id":    oid,
        "status":            "short_open",
        "entry_timestamp":   _ts(),
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

        try:
            order_id = buy(sym, "NSE_EQ", shares,
                          order_type="MARKET", product=product, dry_run=dry_run)
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
        return

    # Collected here and opened in one pass AFTER every exit below has been
    # attempted -- all long exits complete first, then all mirrored shorts,
    # rather than interleaving short(A) between exit(A) and exit(B).
    to_short: list[tuple[str, int]] = []

    for pos in open_ps:
        sym        = pos["symbol"]
        product    = pos.get("product", "MTF")
        fill_price = float(pos["actual_fill_price"] or 0)
        is_partial = pos["status"] == "partial_exit_925_nodata"
        qty        = (int(pos["shares_remaining"]) if is_partial
                      else int(pos["actual_fill_quantity"]))

        print(f"\n[dhan] {sym}  [{product}]  fill=₹{fill_price:,.2f}  qty={qty}")

        no_data  = False
        pnl_live = 0.0
        try:
            ltp      = get_ltp(sym)
            pnl_live = (ltp - fill_price) * qty
            print(f"[dhan]   LTP: ₹{ltp:,.2f}  live P&L: ₹{pnl_live:+,.2f}")
        except Exception as exc:
            print(f"[dhan]   no LTP available: {exc}")
            no_data = True

        if no_data:
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
            try:
                oid = sell(sym, exch, half,
                           order_type="MARKET", product=product, dry_run=dry_run)
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
            try:
                notify.send_exit_925_nodata(broker=_BROKER, symbol=f"{sym} [{product}]",
                                            shares_exited=half, shares_remaining=remain,
                                            exit_price=ep, dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] exit_925_nodata failed: {exc}", file=sys.stderr)
            to_short.append((sym, eq))
            continue

        if pnl_live > 0:
            exch = "NSE_EQ"
            if not dry_run:
                bqty, exch = _broker_qty(sym, product)
                if bqty != qty:
                    print(f"[dhan]   !! MISMATCH — local={qty} broker={bqty}. "
                          f"Skipping {sym} — manual review required.")
                    continue
            print(f"[dhan]   P&L positive — selling {qty}")
            try:
                oid = sell(sym, exch, qty,
                           order_type="MARKET", product=product, dry_run=dry_run)
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
        for sym, eq in to_short:
            _open_short(sym, eq, "925", dry_run=dry_run)

    print(f"\n[dhan] Exit check 9:25am complete.")


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
        return

    n_force = 0
    # Collected here and opened in one pass AFTER every forced exit below has
    # been attempted -- see the matching comment in check_exit_925.
    to_short: list[tuple[str, int]] = []

    for pos in open_ps:
        sym        = pos["symbol"]
        product    = pos.get("product", "MTF")
        fill_price = float(pos["actual_fill_price"] or 0)
        is_partial = pos["status"] == "partial_exit_925_nodata"
        qty        = (int(pos["shares_remaining"]) if is_partial
                      else int(pos["actual_fill_quantity"]))

        print(f"\n[dhan] {sym}  [{product}]  qty={qty}")

        exch = "NSE_EQ"
        if not dry_run:
            bqty, exch = _broker_qty(sym, product)
            if bqty != qty:
                print(f"[dhan]   !! MISMATCH — local={qty} broker={bqty}. "
                      f"Skipping {sym} — manual review required.")
                continue
            print(f"[dhan]   broker confirmed: {bqty} shares [{exch}]")

        try:
            oid = sell(sym, exch, qty,
                       order_type="MARKET", product=product, dry_run=dry_run)
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
        for sym, eq in to_short:
            _open_short(sym, eq, "1159", dry_run=dry_run)

    _daily_summary(positions, n_force, dry_run)
    print(f"\n[dhan] Force exit 11:59am complete. Force-exited: {n_force}.")


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
        return

    n_closed = 0

    for pos in open_shorts:
        sym         = pos["symbol"]
        entry_price = float(pos["entry_price"] or 0)
        qty         = int(pos["quantity"])

        print(f"\n[dhan] {sym}  [SHORT INTRADAY]  entry=₹{entry_price:,.2f}  qty={qty}  "
              f"(from {pos.get('source_exit_stage', '?')} exit)")

        if not dry_run:
            bqty = _broker_short_qty(sym)
            if bqty != qty:
                print(f"[dhan]   !! MISMATCH — local={qty} broker_short={bqty}. "
                      f"Skipping {sym} — manual review required.")
                continue
            print(f"[dhan]   broker confirmed: {bqty} shares short")

        try:
            oid = buy(sym, "NSE_EQ", qty, order_type="MARKET", product="INTRADAY", dry_run=dry_run)
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


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dhan entry+exit (independent of the Zerodha scripts)")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--entry",         action="store_true", help="Run entry")
    grp.add_argument("--exit-925",      action="store_true", help="Exit check at 9:25am")
    grp.add_argument("--exit-1159",     action="store_true", help="Forced exit at 11:59am")
    grp.add_argument("--square-off-239", action="store_true",
                     help="Square off shorts opened from 925/1159 exits (unconditional, 2:39pm)")
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
        else:
            square_off_239(dry_run=args.dry_run)
    except (EnvironmentError, RuntimeError, ValueError) as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)
