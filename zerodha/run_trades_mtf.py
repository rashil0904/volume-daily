"""
zerodha/run_trades_mtf.py — Standalone MTF entry script (Zerodha Kite)
=======================================================================
Separate from the existing CNC flow in run_trades.py -- reads the same
day's trade list and computes quantity the same way (capital/4-or-n
allocation -> floor(allocation/ref_price), same Upstox 15:14/15:13/14:45
reference candle), but places orders with product="MTF" instead of "CNC".

trade.py's buy() already accepts `product` as a parameter, so this reuses
it directly -- no new function was needed there, and nothing in
run_trades.py or trade.py has been touched.

Before each order this checks live per-symbol leverage and required
margin via POST /margins/orders (product=MTF), and confirms available
account margin covers it (GET /user/margins) -- skipping (not halting)
any symbol that doesn't fit. Per-symbol leverage/margin used is logged to
results/trades/mtf_entries_YYYY-MM-DD.csv, separate from positions_zerodha.json
so it can never collide with or be picked up by the CNC exit stages.

This script only places entry orders -- there is no MTF exit stage here.
MTF buys also require an email pledge approval (same day, by ~7pm) before
the position is actually established; this script cannot complete that
step, and prints a reminder at the end of every run.

Usage:
    python zerodha/run_trades_mtf.py --entry [--capital AMOUNT] [--dry-run] [--date YYYY-MM-DD]

Not wired into any cron job -- run manually until tested.
"""

import argparse
import csv
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
from zerodha.trade import buy, order_status as _kite_order_status
import data_loader as _dl

# ── Config ─────────────────────────────────────────────────────────────────────

_IST          = ZoneInfo("Asia/Kolkata")
_RESULTS_DIR  = _ROOT / "results"
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


def _close_at(candles: list, hhmm: int) -> float | None:
    for c in candles:
        try:
            dt = datetime.fromisoformat(str(c[0]))
            if dt.hour * 100 + dt.minute == hhmm:
                return float(c[4])
        except (IndexError, ValueError, TypeError):
            continue
    return None


def get_reference_price(symbol: str) -> tuple[float, int]:
    """Same reference candle logic as run_trades.py's get_reference_price:
    close of 15:14 1-min candle, falling back to 15:13, then 14:45."""
    matched        = [{"symbol": symbol, "instrument_key": _ikey(symbol)}]
    candles_by_sym = _dl.load_candles(matched, interval="1minute", mode="intraday")
    candles        = candles_by_sym.get(symbol, [])
    for hhmm in (1514, 1513, 1445):
        price = _close_at(candles, hhmm)
        if price is not None:
            return price, hhmm
    raise ValueError(
        f"[mtf] No 15:14, 15:13, or 14:45 candle found for {symbol} "
        f"({len(candles)} candles). Run after 15:15 IST."
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


def _available_margin() -> float | None:
    """GET /user/margins -- equity segment live balance. None on lookup failure."""
    session, _ = _kite_session()
    try:
        resp = session.get(f"{_KITE_BASE}/user/margins", timeout=15)
        data = resp.json().get("data", {})
        return float(data.get("equity", {}).get("available", {}).get("live_balance") or 0)
    except Exception:
        return None


# ── MTF entries log (separate from positions_zerodha.json) ───────────────────

def _mtf_log_path(trade_date: date) -> Path:
    return _MTF_LOG_DIR / f"mtf_entries_{trade_date.isoformat()}.csv"


def _append_mtf_log(trade_date: date, row: dict) -> None:
    path        = _mtf_log_path(trade_date)
    fieldnames  = ["timestamp", "symbol", "quantity", "ref_price", "fill_price",
                   "leverage", "margin_required", "order_id", "status"]
    write_header = not path.exists()
    _MTF_LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


# ══════════════════════════════════════════════════════════════════════════════
# MTF ENTRY — mirrors run_trades.py's run_entry_315() timing/allocation logic
# ══════════════════════════════════════════════════════════════════════════════

def run_entry_315_mtf(trade_date: date | None = None, dry_run: bool = False,
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
        allocation = capital / 4 if n <= 4 else capital / n

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

    for sym in symbols:
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
            shares = math.floor(allocation / ref) if ref > 0 else 0
        if shares == 0:
            print(f"[mtf]   SKIP — 0 shares at ₹{ref:,.2f} (allocation ₹{allocation:,.0f})")
            n_skipped += 1
            continue
        print(f"[mtf]   shares to buy: {shares}")

        # Live per-symbol leverage/margin check -- never assume a fixed leverage.
        margin_info = _mtf_margin_check(sym, shares)
        if margin_info is None:
            print(f"[mtf]   SKIP — could not verify MTF margin/leverage for {sym}.")
            n_skipped += 1
            continue

        leverage        = margin_info["leverage"]
        margin_required = margin_info["margin_required"]
        print(f"[mtf]   leverage: {leverage:.2f}x  ·  margin required: ₹{margin_required:,.2f}")

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
                          order_type="MARKET", product="MTF", dry_run=dry_run)
        except Exception as exc:
            print(f"[mtf]   ORDER FAILED: {exc}")
            _append_mtf_log(trade_date, {
                "timestamp": _ts(), "symbol": sym, "quantity": shares,
                "ref_price": round(ref, 4), "fill_price": "",
                "leverage": leverage, "margin_required": margin_required,
                "order_id": "", "status": "order_failed",
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
                    "order_id": order_id, "status": "rejected",
                })
                n_skipped += 1
                continue
            print(f"[mtf]   filled ₹{fill_price:,.2f} × {fill_qty}")
            status = "filled"

        _append_mtf_log(trade_date, {
            "timestamp": _ts(), "symbol": sym, "quantity": fill_qty,
            "ref_price": round(ref, 4), "fill_price": round(fill_price, 4),
            "leverage": leverage, "margin_required": margin_required,
            "order_id": order_id, "status": status,
        })
        n_entered += 1

    print(f"\n[mtf] MTF entry complete. Entered: {n_entered}  Skipped: {n_skipped}")
    print(f"[mtf] Log written to {_mtf_log_path(trade_date)}")
    print(
        "\n[mtf] REMINDER: MTF buy orders trigger a pledge request by email that must be "
        "approved (typically by ~7pm same day) for the position to actually be established. "
        "This script cannot complete that approval step -- check your email/Kite for the "
        "pledge request."
    )
    print("[mtf] This was the MTF-only script. run_trades.py and the CNC pipeline were not touched.")


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Zerodha MTF entry (standalone, not wired into any cron job)")
    parser.add_argument("--entry",    action="store_true", required=True, help="Run MTF entry")
    parser.add_argument("--dry-run",  action="store_true", help="Simulate without placing orders")
    parser.add_argument("--date",     default=None, help="Trade date YYYY-MM-DD (defaults to today)")
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
        run_entry_315_mtf(trade_date=td, dry_run=args.dry_run, capital=args.capital,
                          symbol=args.symbol, shares_override=args.shares)
    except (EnvironmentError, RuntimeError, ValueError) as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)
