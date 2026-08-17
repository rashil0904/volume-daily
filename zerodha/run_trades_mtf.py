"""
zerodha/run_trades_mtf.py — Standalone MTF entry+exit flow (Zerodha Kite)
==========================================================================
Separate from the existing CNC flow in run_trades.py -- reads the same
day's trade list and computes quantity the same way (capital/4-or-n
allocation -> floor(allocation/ref_price), same Upstox 15:20 reference
candle, falling back to 15:19, then latest available). Places orders with product="MTF" when live leverage is
actually available; falls back to a resized product="CNC" buy (half the
capital base) when it isn't.

trade.py's buy()/sell() already accept `product` as a parameter, so this
reuses them directly -- no new function was needed there, and nothing in
run_trades.py or trade.py has been touched.

Before each entry this checks live per-symbol leverage and required
margin via POST /margins/orders (product=MTF), and confirms available
account margin covers it (GET /user/margins) -- skipping (not halting)
any symbol that doesn't fit. Per-symbol leverage/margin used is logged to
results/trades/mtf_entries_YYYY-MM-DD.csv for audit purposes.

Live (non-dry-run) fills are ALSO written into results/positions_zerodha.json
-- the same file run_trades.py uses -- tagged with a "product" field
(MTF or CNC) that run_trades.py never writes. This script's own exit
stages (--exit-925/--exit-1159, mirroring run_trades.py's Stage 2/3) use
that field's presence to scope themselves to ONLY the positions this
script entered; run_trades.py's own exit stages and its untagged legacy
positions are left alone.

MTF buys also require an email pledge approval (same day, by ~7pm) before
the position is actually established; this script cannot complete that
step, and prints a reminder at the end of every entry run.

Usage:
    python zerodha/run_trades_mtf.py --entry      [--capital AMOUNT] [--dry-run] [--date YYYY-MM-DD]
    python zerodha/run_trades_mtf.py --exit-925    [--dry-run]
    python zerodha/run_trades_mtf.py --exit-1159   [--dry-run]

Not wired into any cron job -- run manually until tested.
"""

import argparse
import csv
import json
import math
import sys
import time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

from zerodha.auth import BASE_URL as _KITE_BASE, get_session as _kite_session
from zerodha.trade import buy, sell, order_status as _kite_order_status
from common.calc_utils import pick_reference_price, compute_allocation, compute_shares
import data_loader as _dl
import notify

# ── Config ─────────────────────────────────────────────────────────────────────

_IST          = ZoneInfo("Asia/Kolkata")
_BROKER       = "zerodha"
_RESULTS_DIR  = _ROOT / "results"
_POS_FILE     = _RESULTS_DIR / "positions_zerodha.json"
_INSTRUMENTS  = _ROOT / "data" / "instruments" / "upstox_instruments.csv"
_MTF_LOG_DIR  = _RESULTS_DIR / "trades"
TOTAL_CAPITAL = 500_000

_sym_cache: dict[str, str] = {}


# ── Instrument key resolution (same as run_trades.py) ─────────────────────────

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
            f"[mtf] '{symbol}' not found in instruments CSV — "
            "pass the instrument_key directly (e.g. 'NSE_EQ|INE...')"
        )
    return key


def get_reference_price(symbol: str) -> tuple[float, int]:
    """Same reference candle logic as run_trades.py's get_reference_price: close of
    15:20 1-min candle, falling back to 15:19, then whichever candle at/before 15:20
    is actually the most recent available (not a fixed older point)."""
    matched        = [{"symbol": symbol, "instrument_key": _ikey(symbol)}]
    candles_by_sym = _dl.load_candles(matched, interval="1minute", mode="intraday")
    candles        = candles_by_sym.get(symbol, [])
    try:
        return pick_reference_price(candles, 1520, 1519)
    except ValueError:
        raise ValueError(
            f"[mtf] No candle data at all for {symbol} up to 15:20 "
            f"({len(candles)} candles). Run after 15:21 IST."
        )


# ── Trade list (same file, same parsing as run_trades.py) ────────────────────

def _load_symbols(trade_date: date) -> list[str]:
    path = _RESULTS_DIR / "trades" / f"trade_list_{trade_date.isoformat()}.csv"
    if not path.exists():
        sys.exit(f"[mtf] No trade list: {path}")
    with open(path, newline="") as f:
        return [r["symbol"].strip().upper() for r in csv.DictReader(f)]


def _ts() -> str:
    return datetime.now(_IST).isoformat()


# ── Order fill polling (same approach as run_trades.py) ───────────────────────

class OrderRejected(RuntimeError):
    """Order genuinely did not fill (REJECTED/CANCELLED)."""


def _poll_fill(order_id: str, retries: int = 12, delay: float = 3.0) -> tuple[float, int]:
    for _ in range(retries):
        time.sleep(delay)
        try:
            o      = _kite_order_status(order_id)
            status = (o.get("status") or "").upper()
            if status == "COMPLETE":
                return float(o.get("average_price") or 0), int(o.get("filled_quantity") or 0)
            if status in ("REJECTED", "CANCELLED"):
                raise OrderRejected(
                    f"Order {order_id} {status}: {o.get('status_message', '')}"
                )
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
        print(f"[mtf]   ORDER REJECTED — {exc}")
        return 0.0, 0
    except Exception as exc:
        print(f"[mtf]   fill poll failed: {exc} — using fallback values")
        return fallback_price, fallback_qty


# ── MTF margin checks ──────────────────────────────────────────────────────────

def _mtf_margin_check(symbol: str, quantity: int) -> dict | None:
    """POST /margins/orders with product=MTF for this symbol+quantity. Returns
    {"leverage": float, "margin_required": float} on success, None on any
    lookup failure (caller must treat None as 'could not verify, skip')."""
    session, _ = _kite_session()
    payload = [{
        "exchange":         "NSE",
        "tradingsymbol":    symbol,
        "transaction_type": "BUY",
        "variety":          "regular",
        "product":          "MTF",
        "order_type":       "MARKET",
        "quantity":         quantity,
        "price":            0,
        "trigger_price":    0,
    }]
    try:
        resp = session.post(f"{_KITE_BASE}/margins/orders", json=payload, timeout=15)
        data = resp.json().get("data", [])
        if not resp.ok or not data:
            return None
        row = data[0]
        return {
            "leverage":        float(row.get("leverage") or 0),
            "margin_required": float(row.get("mtf") or row.get("total") or 0),
        }
    except Exception:
        return None


def _mis_margin_check(symbol: str, quantity: int) -> dict | None:
    """POST /margins/orders with product=MIS, transaction_type=SELL -- margin required
    to open the mirrored intraday short for this symbol+quantity. Same shape as
    _mtf_margin_check but for the short leg (see run_entry_321_mtf's shorting add-on).
    Returns {"leverage": float, "margin_required": float} on success, None on any
    lookup failure (caller must treat None as 'could not verify, skip the short')."""
    session, _ = _kite_session()
    payload = [{
        "exchange":         "NSE",
        "tradingsymbol":    symbol,
        "transaction_type": "SELL",
        "variety":          "regular",
        "product":          "MIS",
        "order_type":       "MARKET",
        "quantity":         quantity,
        "price":            0,
        "trigger_price":    0,
    }]
    try:
        resp = session.post(f"{_KITE_BASE}/margins/orders", json=payload, timeout=15)
        data = resp.json().get("data", [])
        if not resp.ok or not data:
            return None
        row = data[0]
        return {
            "leverage":        float(row.get("leverage") or 0),
            "margin_required": float(row.get("total") or 0),
        }
    except Exception:
        return None


def _available_margin() -> float | None:
    """GET /user/margins -- equity segment live balance. None on lookup failure."""
    session, _ = _kite_session()
    try:
        resp = session.get(f"{_KITE_BASE}/user/margins", timeout=15)
        data = resp.json().get("data", {})
        return float(data.get("equity", {}).get("available", {}).get("live_balance") or 0)
    except Exception:
        return None


def get_ltp(symbol: str) -> float:
    """Same as run_trades.py's get_ltp: current LTP via Kite's /quote/ltp, used to
    compute Stage 2 exit P&L directly rather than trusting Kite's own positions/
    holdings pnl field (that field can go stale same-day). Raises ValueError if no
    LTP is returned."""
    session, _ = _kite_session()
    resp = session.get(f"{_KITE_BASE}/quote/ltp", params={"i": f"NSE:{symbol}"}, timeout=15)
    resp.raise_for_status()
    entry = resp.json().get("data", {}).get(f"NSE:{symbol}")
    if not entry or entry.get("last_price") is None:
        raise ValueError(f"[mtf] No LTP found for {symbol}.")
    return float(entry["last_price"])


# ── Positions JSON (shared with run_trades.py) ────────────────────────────────

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


def _open_pos_mtf(positions: list) -> list:
    """Positions this script itself entered and still needs to exit. Scoped to rows
    carrying a "product" key -- run_trades.py never writes that key, so this can
    never pick up (or touch) its legacy/CNC positions."""
    return [p for p in positions
            if p.get("broker") == _BROKER
            and "product" in p
            and p.get("direction") != "short"
            and p.get("status") in ("open", "partial_exit_925_nodata")]


def _open_short_pos_mtf(positions: list) -> list:
    """Short positions opened by _open_short() (mirroring a 925/1159 long exit) that
    still need squaring off at 2:39pm."""
    return [p for p in positions
            if p.get("broker") == _BROKER
            and p.get("direction") == "short"
            and p.get("status") == "short_open"]


# ── Broker quantity cross-check (product-aware) ───────────────────────────────

def _broker_qty(symbol: str, product: str) -> tuple[int, str]:
    """Confirm broker-held quantity AND the exchange it's actually held under, for
    this exact product, via Kite positions and holdings endpoints. Returns
    (quantity, exchange), defaulting to ("NSE", 0) if nothing is found.

    MTF positions settle into holdings overnight -- often under a DIFFERENT
    exchange than the one the buy order was placed on (confirmed live 2026-08-05:
    CAPILLARY/EPACKPEB/KKCL/PAUSHAKLTD were all bought on NSE but held under BSE
    by the next day, per Kite's holdings[].exchange). /portfolio/holdings is
    consulted for MTF too -- via the nested "mtf": {"quantity", "used_quantity"}
    fields -- not just CNC as before; a stale/netted "positions" entry alone
    caused false MISMATCH skips on real, sellable MTF holdings that morning."""
    session, _ = _kite_session()
    product = product.upper()
    try:
        resp = session.get(f"{_KITE_BASE}/portfolio/positions", timeout=15)
        if resp.ok:
            for p in (resp.json().get("data", {}).get("net") or []):
                if ((p.get("tradingsymbol") or "").upper() == symbol.upper()
                        and (p.get("product") or "").upper() == product):
                    qty = int(p.get("quantity") or 0)
                    if qty > 0:
                        return qty, (p.get("exchange") or "NSE").upper()
    except Exception:
        pass
    try:
        resp = session.get(f"{_KITE_BASE}/portfolio/holdings", timeout=15)
        if resp.ok:
            for h in (resp.json().get("data") or []):
                if (h.get("tradingsymbol") or "").upper() != symbol.upper():
                    continue
                if product == "CNC":
                    qty = int(h.get("quantity") or 0) + int(h.get("t1_quantity") or 0)
                else:
                    mtf = h.get("mtf") or {}
                    qty = int(mtf.get("quantity") or 0) + int(mtf.get("used_quantity") or 0)
                if qty > 0:
                    return qty, (h.get("exchange") or "NSE").upper()
    except Exception:
        pass
    return 0, "NSE"


def _broker_short_qty(symbol: str) -> int:
    """Confirms broker-held short quantity for MIS positions. Kite's
    /portfolio/positions reports a net short as a NEGATIVE quantity (buy_qty minus
    sell_qty going negative), the opposite sign of the long-side qty>0 check
    _broker_qty() does above -- a separate helper rather than overloading that one.
    Short MIS positions never settle into holdings overnight (intraday-only, closed
    same day), so there's no holdings fallback needed here. Returns 0 if not found."""
    session, _ = _kite_session()
    try:
        resp = session.get(f"{_KITE_BASE}/portfolio/positions", timeout=15)
        if resp.ok:
            for p in (resp.json().get("data", {}).get("net") or []):
                if ((p.get("tradingsymbol") or "").upper() == symbol.upper()
                        and (p.get("product") or "").upper() == "MIS"):
                    qty = int(p.get("quantity") or 0)
                    if qty < 0:
                        return abs(qty)
    except Exception:
        pass
    return 0


def _open_short(sym: str, qty: int, source_stage: str, dry_run: bool = False) -> None:
    """Opens a same-quantity intraday short (product=MIS) mirroring a long exit that
    just filled at either the 9:25am or 11:59am stage -- see run_trades_mtf.py's
    module docstring for the shorting add-on. Skips (never raises) on any
    margin-check/balance/fill failure: the long exit that triggered this has already
    happened and is never reversed by a failed short."""
    margin_info = _mis_margin_check(sym, qty)
    if margin_info is None:
        print(f"[mtf]   SHORT SKIP — {sym}: could not verify MIS margin.")
        return
    available = _available_margin()
    if available is None or available < margin_info["margin_required"]:
        req = margin_info["margin_required"]
        print(f"[mtf]   SHORT SKIP — {sym}: insufficient margin "
              f"(available {'unknown' if available is None else f'₹{available:,.2f}'} "
              f"< required ₹{req:,.2f}).")
        return

    try:
        ltp = get_ltp(sym)
    except Exception:
        ltp = 0.0

    try:
        oid = sell(sym, "NSE", qty, order_type="MARKET", product="MIS", dry_run=dry_run)
    except Exception as exc:
        print(f"[mtf]   SHORT FAILED — {sym}: {exc}")
        return

    ep, eq = (ltp, qty) if dry_run else _poll_fill_safe(oid, ltp, qty)
    if eq == 0:
        print(f"[mtf]   SHORT NOT FILLED — {sym} short order rejected.")
        return

    positions = _load_pos()
    positions.append({
        "broker":            _BROKER,
        "symbol":            sym,
        "direction":         "short",
        "product":           "MIS",
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
    print(f"[mtf]   SHORT OPENED — {sym}  ₹{ep:,.2f} × {eq}  (from {source_stage} exit)")
    try:
        notify.send_short_open(broker=_BROKER, symbol=f"{sym} [SHORT MIS]", entry_price=ep,
                               shares=eq, source_exit_stage=source_stage, order_id=oid,
                               dry_run=dry_run)
    except Exception as exc:
        print(f"  [notify] short_open failed: {exc}", file=sys.stderr)


# ── MTF entries log (separate from positions_zerodha.json) ───────────────────

def _mtf_log_path(trade_date: date) -> Path:
    return _MTF_LOG_DIR / f"mtf_entries_{trade_date.isoformat()}.csv"


def _append_mtf_log(trade_date: date, row: dict) -> None:
    path        = _mtf_log_path(trade_date)
    fieldnames  = ["timestamp", "symbol", "quantity", "ref_price", "fill_price",
                   "leverage", "margin_required", "order_id", "status", "product",
                   "capital_base"]
    write_header = not path.exists()
    _MTF_LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


# ══════════════════════════════════════════════════════════════════════════════
# MTF ENTRY — mirrors run_trades.py's run_entry_321() allocation logic
# (capital/4-or-n split); timing/reference-candle differ -- see module docstring.
# ══════════════════════════════════════════════════════════════════════════════

def run_entry_321_mtf(trade_date: date | None = None, dry_run: bool = False,
                      capital: float | None = None, symbol: str | None = None,
                      shares_override: int | None = None) -> None:
    if trade_date is None:
        trade_date = date.today()

    manual_mode = symbol is not None

    if manual_mode:
        symbols = [symbol.strip().upper()]
        n       = 1
        # Manual single-stock mode -- capital (if given) is the amount for THIS
        # stock, not divided by 4 the way the batch allocation rule does; that
        # rule exists to proportion across a multi-signal daily list, which
        # doesn't apply when you're deliberately picking one stock yourself.
        capital    = capital if capital is not None else TOTAL_CAPITAL / 4
        allocation = capital
    else:
        symbols = _load_symbols(trade_date)
        if not symbols:
            print(f"[mtf] Trade list empty for {trade_date} — nothing to enter.")
            return
        n = len(symbols)
        capital    = capital if capital is not None else TOTAL_CAPITAL
        # Same allocation rule as the CNC flow: capital/4 max per position for
        # n<=4 (idle capital allowed), full equal split once signals hit 5+.
        allocation = compute_allocation(capital, n)

    print(f"\n{'='*60}")
    print(f"[mtf] MTF Entry {trade_date}{'  DRY RUN' if dry_run else ''}"
          + ("  [MANUAL SINGLE-STOCK]" if manual_mode else ""))
    if manual_mode:
        print(f"[mtf] {symbols[0]}  ·  ₹{allocation:,.0f} allocated"
              + (f"  ·  shares override: {shares_override}" if shares_override else ""))
    else:
        print(f"[mtf] {n} signal(s)  ·  ₹{capital:,.0f} total  ·  ₹{allocation:,.0f} per position")
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
            print(f"[mtf] {sym} — already entered today (by this script or run_trades.py), skipping.")
            n_skipped += 1
            continue

        print(f"\n[mtf] {sym}")

        try:
            ref, ref_hhmm = get_reference_price(sym)
            print(f"[mtf]   ref price ({ref_hhmm//100:02d}:{ref_hhmm%100:02d} close): ₹{ref:,.2f}")
        except Exception as exc:
            print(f"[mtf]   SKIP — no reference price: {exc}")
            n_skipped += 1
            continue

        if manual_mode and shares_override is not None:
            shares = shares_override
        else:
            shares = compute_shares(allocation, ref)
        if shares == 0:
            print(f"[mtf]   SKIP — 0 shares at ₹{ref:,.2f} (allocation ₹{allocation:,.0f})")
            n_skipped += 1
            continue
        print(f"[mtf]   shares to buy: {shares}")

        # Live per-symbol leverage/margin check -- never assume a fixed leverage.
        # No usable MTF leverage (check failed, or came back below 2x) falls back to a
        # smaller CNC buy, resized down from half the original capital base
        # (not a fixed figure) -- single-stock --symbol mode is left untouched
        # (no resize) since it's an explicit, deliberately-sized order.
        margin_info  = _mtf_margin_check(sym, shares)
        has_leverage = margin_info is not None and margin_info["leverage"] >= 2
        if has_leverage:
            product         = "MTF"
            leverage        = margin_info["leverage"]
            margin_required = margin_info["margin_required"]
            capital_base    = capital
            print(f"[mtf]   leverage: {leverage:.2f}x  ·  margin required: ₹{margin_required:,.2f}")
        else:
            reason = ("MTF margin check failed" if margin_info is None
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
                print(f"[mtf]   SKIP — {reason}; 0 shares at ₹{ref:,.2f} on "
                      f"₹{capital_base:,.0f}-based CNC fallback.")
                n_skipped += 1
                continue
            shares          = resized_shares
            product         = "CNC"
            leverage        = margin_info["leverage"] if margin_info else 0.0
            margin_required = shares * ref
            print(f"[mtf]   {reason} — falling back to CNC at {shares} shares "
                  f"(₹{capital_base:,.0f}-based)  ·  margin required: ₹{margin_required:,.2f}")

        available = _available_margin()
        if available is None:
            print(f"[mtf]   SKIP — could not verify available margin for {sym}.")
            n_skipped += 1
            continue
        if available < margin_required:
            print(f"[mtf]   SKIP — insufficient margin (available ₹{available:,.2f} "
                  f"< required ₹{margin_required:,.2f}).")
            n_skipped += 1
            continue
        print(f"[mtf]   margin confirmed: ₹{available:,.2f} available")

        try:
            order_id = buy(sym, "NSE", shares,
                          order_type="MARKET", product=product, dry_run=dry_run)
        except Exception as exc:
            print(f"[mtf]   ORDER FAILED: {exc}")
            _append_mtf_log(trade_date, {
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
            print(f"[mtf]   DRY RUN — simulated fill ₹{fill_price:,.2f} × {fill_qty}")
            status = "dry_run"
        else:
            fill_price, fill_qty = _poll_fill_safe(order_id, ref, shares)
            if fill_qty == 0:
                print(f"[mtf]   NOT FILLED — order rejected.")
                _append_mtf_log(trade_date, {
                    "timestamp": _ts(), "symbol": sym, "quantity": shares,
                    "ref_price": round(ref, 4), "fill_price": "",
                    "leverage": leverage, "margin_required": margin_required,
                    "order_id": order_id, "status": "rejected", "product": product,
                    "capital_base": capital_base,
                })
                n_skipped += 1
                continue
            print(f"[mtf]   filled ₹{fill_price:,.2f} × {fill_qty}")
            status = "filled"

        _append_mtf_log(trade_date, {
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

    print(f"\n[mtf] MTF entry complete. Entered: {n_entered}  Skipped: {n_skipped}")
    print(f"[mtf] Log written to {_mtf_log_path(trade_date)}")
    if not dry_run and n_entered:
        print(f"[mtf] Live fills also recorded to {_POS_FILE} for this script's --exit-925/--exit-1159 stages.")
    print(
        "\n[mtf] REMINDER: MTF buy orders trigger a pledge request by email that must be "
        "approved (typically by ~7pm same day) for the position to actually be established. "
        "This script cannot complete that approval step -- check your email/Kite for the "
        "pledge request."
    )
    print("[mtf] run_trades.py and trade.py were not touched.")


# ══════════════════════════════════════════════════════════════════════════════
# EXIT — mirrors run_trades.py's Stage 2 (check_exit_925) / Stage 3 (force_exit_1159),
# scoped to only the positions this script itself entered (via _open_pos_mtf).
# ══════════════════════════════════════════════════════════════════════════════

def check_exit_925_mtf(dry_run: bool = False) -> None:
    positions = _load_pos()
    open_ps   = _open_pos_mtf(positions)

    print(f"\n{'='*60}")
    print(f"[mtf] Exit check 9:25am{'  DRY RUN' if dry_run else ''}")
    print(f"[mtf] {len(open_ps)} open MTF-script position(s)")
    print(f"{'='*60}")

    if not open_ps:
        print("[mtf] No open positions — nothing to check.")
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

        print(f"\n[mtf] {sym}  [{product}]  fill=₹{fill_price:,.2f}  qty={qty}")

        no_data  = False
        pnl_live = 0.0
        try:
            ltp      = get_ltp(sym)
            pnl_live = (ltp - fill_price) * qty
            print(f"[mtf]   LTP: ₹{ltp:,.2f}  live P&L: ₹{pnl_live:+,.2f}")
        except Exception as exc:
            print(f"[mtf]   no LTP available: {exc}")
            no_data = True

        if no_data:
            half   = math.floor(qty / 2)
            remain = qty - half
            if half == 0:
                print(f"[mtf]   qty too small to halve — holding until 11:59am.")
                continue
            exch = "NSE"
            if not dry_run:
                bqty, exch = _broker_qty(sym, product)
                if bqty != qty:
                    print(f"[mtf]   !! MISMATCH — local={qty} broker={bqty}. "
                          f"Skipping {sym} — manual review required.")
                    continue
            print(f"[mtf]   NO-DATA FALLBACK — selling {half} of {qty}")
            try:
                oid = sell(sym, exch, half,
                           order_type="MARKET", product=product, dry_run=dry_run)
            except Exception as exc:
                print(f"[mtf]   fallback sell failed: {exc}")
                continue
            ep, eq = (fill_price, half) if dry_run else _poll_fill_safe(oid, fill_price, half)
            if eq == 0:
                print(f"[mtf]   NOT FILLED — fallback sell rejected, position left open.")
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
            exch = "NSE"
            if not dry_run:
                bqty, exch = _broker_qty(sym, product)
                if bqty != qty:
                    print(f"[mtf]   !! MISMATCH — local={qty} broker={bqty}. "
                          f"Skipping {sym} — manual review required.")
                    continue
            print(f"[mtf]   P&L positive — selling {qty}")
            try:
                oid = sell(sym, exch, qty,
                           order_type="MARKET", product=product, dry_run=dry_run)
            except Exception as exc:
                print(f"[mtf]   sell failed: {exc}")
                continue
            ep, eq = (fill_price, qty) if dry_run else _poll_fill_safe(oid, fill_price, qty)
            if eq == 0:
                print(f"[mtf]   NOT FILLED — sell rejected, position left open.")
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
            print(f"[mtf]   exited ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
            try:
                notify.send_exit_925(broker=_BROKER, symbol=f"{sym} [{product}]", exit_price=ep,
                                     return_pct=ret_act, pnl=pnl, dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] exit_925 failed: {exc}", file=sys.stderr)
            to_short.append((sym, eq))
        else:
            print(f"[mtf]   P&L ≤ 0 (₹{pnl_live:+,.2f}) — holding for 11:59am forced exit.")

    if to_short:
        print(f"\n[mtf] Opening {len(to_short)} mirrored short(s)...")
        for sym, eq in to_short:
            _open_short(sym, eq, "925", dry_run=dry_run)

    print(f"\n[mtf] Exit check 9:25am complete.")


def force_exit_1159_mtf(dry_run: bool = False) -> None:
    positions = _load_pos()
    open_ps   = _open_pos_mtf(positions)

    print(f"\n{'='*60}")
    print(f"[mtf] Force exit 11:59am{'  DRY RUN' if dry_run else ''}")
    print(f"[mtf] {len(open_ps)} MTF-script position(s) still open")
    print(f"{'='*60}")

    if not open_ps:
        print("[mtf] All MTF-script positions already exited — nothing to force-close.")
        try:
            notify.send_nothing_open_at_1159(broker=_BROKER)
        except Exception as exc:
            print(f"  [notify] nothing_open_at_1159 failed: {exc}", file=sys.stderr)
        _daily_summary_mtf(positions, 0, dry_run)
        return

    n_force = 0
    # Collected here and opened in one pass AFTER every forced exit below has
    # been attempted -- see the matching comment in check_exit_925_mtf.
    to_short: list[tuple[str, int]] = []

    for pos in open_ps:
        sym        = pos["symbol"]
        product    = pos.get("product", "MTF")
        fill_price = float(pos["actual_fill_price"] or 0)
        is_partial = pos["status"] == "partial_exit_925_nodata"
        qty        = (int(pos["shares_remaining"]) if is_partial
                      else int(pos["actual_fill_quantity"]))

        print(f"\n[mtf] {sym}  [{product}]  qty={qty}")

        exch = "NSE"
        if not dry_run:
            bqty, exch = _broker_qty(sym, product)
            if bqty != qty:
                print(f"[mtf]   !! MISMATCH — local={qty} broker={bqty}. "
                      f"Skipping {sym} — manual review required.")
                continue
            print(f"[mtf]   broker confirmed: {bqty} shares [{exch}]")

        try:
            oid = sell(sym, exch, qty,
                       order_type="MARKET", product=product, dry_run=dry_run)
        except Exception as exc:
            print(f"[mtf]   sell failed: {exc}")
            continue

        ep, eq = (fill_price, qty) if dry_run else _poll_fill_safe(oid, fill_price, qty)
        if eq == 0:
            print(f"[mtf]   !! NOT FILLED — force-exit sell rejected for {sym}. "
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
        print(f"[mtf]   force-exited ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
        try:
            notify.send_force_exit_1159(broker=_BROKER, symbol=f"{sym} [{product}]", exit_price=ep,
                                        return_pct=ret, pnl=pnl, dry_run=dry_run)
        except Exception as exc:
            print(f"  [notify] force_exit_1159 failed: {exc}", file=sys.stderr)
        to_short.append((sym, eq))
        n_force += 1

    if to_short:
        print(f"\n[mtf] Opening {len(to_short)} mirrored short(s)...")
        for sym, eq in to_short:
            _open_short(sym, eq, "1159", dry_run=dry_run)

    _daily_summary_mtf(positions, n_force, dry_run)
    print(f"\n[mtf] Force exit 11:59am complete. Force-exited: {n_force}.")


def _daily_summary_mtf(positions: list, n_force: int, dry_run: bool) -> None:
    today    = date.today().isoformat()
    today_ps = [p for p in positions
                if p.get("broker") == _BROKER and "product" in p
                and p.get("direction") != "short" and p.get("entry_date") == today]
    n_opened  = len(today_ps)
    n_925     = sum(1 for p in today_ps if p.get("status") == "exited_925")
    n_partial = sum(1 for p in today_ps
                    if p.get("status") == "exited_1159" and "exit_order_id_925" in p)
    total_pnl = sum(p.get("realized_pnl") or 0 for p in today_ps
                    if p.get("status") in ("exited_925", "exited_1159"))
    print(f"\n[mtf] Summary — opened={n_opened}  exited@925={n_925}  "
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
# Mirrors force_exit_1159_mtf's unconditional-close shape, not check_exit_925_mtf's
# conditional one -- this always closes, regardless of P&L.
# ══════════════════════════════════════════════════════════════════════════════

def square_off_239_mtf(dry_run: bool = False) -> None:
    positions   = _load_pos()
    open_shorts = _open_short_pos_mtf(positions)

    print(f"\n{'='*60}")
    print(f"[mtf] Short square-off 2:39pm{'  DRY RUN' if dry_run else ''}")
    print(f"[mtf] {len(open_shorts)} open short position(s)")
    print(f"{'='*60}")

    if not open_shorts:
        print("[mtf] No open shorts — nothing to square off.")
        return

    n_closed = 0

    for pos in open_shorts:
        sym         = pos["symbol"]
        entry_price = float(pos["entry_price"] or 0)
        qty         = int(pos["quantity"])

        print(f"\n[mtf] {sym}  [SHORT MIS]  entry=₹{entry_price:,.2f}  qty={qty}  "
              f"(from {pos.get('source_exit_stage', '?')} exit)")

        if not dry_run:
            bqty = _broker_short_qty(sym)
            if bqty != qty:
                print(f"[mtf]   !! MISMATCH — local={qty} broker_short={bqty}. "
                      f"Skipping {sym} — manual review required.")
                continue
            print(f"[mtf]   broker confirmed: {bqty} shares short")

        try:
            oid = buy(sym, "NSE", qty, order_type="MARKET", product="MIS", dry_run=dry_run)
        except Exception as exc:
            print(f"[mtf]   cover buy failed: {exc}")
            continue

        ep, eq = (entry_price, qty) if dry_run else _poll_fill_safe(oid, entry_price, qty)
        if eq == 0:
            print(f"[mtf]   !! NOT FILLED — cover buy rejected for {sym}. "
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
        print(f"[mtf]   SQUARED OFF ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
        try:
            notify.send_square_off_239(broker=_BROKER, symbol=f"{sym} [SHORT MIS]",
                                       entry_price=entry_price, exit_price=ep,
                                       return_pct=ret, pnl=pnl, dry_run=dry_run)
        except Exception as exc:
            print(f"  [notify] square_off_239 failed: {exc}", file=sys.stderr)
        n_closed += 1

    print(f"\n[mtf] Short square-off complete. Closed: {n_closed}.")


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Zerodha MTF entry+exit (standalone, not wired into any cron job)")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--entry",         action="store_true", help="Run MTF entry")
    grp.add_argument("--exit-925",      action="store_true", help="Exit check at 9:25am (this script's positions only)")
    grp.add_argument("--exit-1159",     action="store_true", help="Forced exit at 11:59am (this script's positions only)")
    grp.add_argument("--square-off-239", action="store_true",
                     help="Square off shorts opened from 925/1159 exits (unconditional, 2:39pm)")
    parser.add_argument("--dry-run",  action="store_true", help="Simulate without placing orders")
    parser.add_argument("--date",     default=None, help="Trade date YYYY-MM-DD (--entry only; defaults to today)")
    parser.add_argument("--capital",  type=float, default=None,
                        help="Total capital for MTF entries (independent of the CNC script's "
                             "--capital; defaults to TOTAL_CAPITAL if omitted). In --symbol mode, "
                             "this is the amount allocated to that one stock (not divided by 4).")
    parser.add_argument("--symbol",   default=None,
                        help="Buy this one symbol on MTF instead of reading the day's trade list. "
                             "Shares are sized from --capital/ref-price unless --shares is given.")
    parser.add_argument("--shares",   type=int, default=None,
                        help="Exact quantity to buy in --symbol mode (skips allocation-based sizing).")
    args = parser.parse_args()

    if args.shares is not None and args.symbol is None:
        sys.exit("[mtf] --shares requires --symbol")

    td = date.fromisoformat(args.date) if args.date else date.today()

    try:
        if args.entry:
            run_entry_321_mtf(trade_date=td, dry_run=args.dry_run, capital=args.capital,
                              symbol=args.symbol, shares_override=args.shares)
        elif args.exit_925:
            check_exit_925_mtf(dry_run=args.dry_run)
        elif args.exit_1159:
            force_exit_1159_mtf(dry_run=args.dry_run)
        else:
            square_off_239_mtf(dry_run=args.dry_run)
    except (EnvironmentError, RuntimeError, ValueError) as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)
