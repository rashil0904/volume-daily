"""
zerodha/run_trades.py — 3-stage live trading via Zerodha Kite
=============================================================
Entry price : Upstox intraday V3 candle API  (UPSTOX_ACCESS_TOKEN from pipeline/.env) —
              close of 15:14 candle, falls back to 15:13 if 15:14 isn't published yet.
Exit check  : Zerodha Kite's own computed pnl from /portfolio/positions or /portfolio/holdings
              (Stage 2) — not candle-based, so it isn't affected by whether a specific
              candle has been published yet.
Orders      : Zerodha Kite API  (zerodha/trade.py)
Positions   : results/positions_zerodha.json  (full persistent trade book)

Usage:
    python zerodha/run_trades.py --entry          [--dry-run] [--date YYYY-MM-DD]
    python zerodha/run_trades.py --exit-945       [--dry-run]
    python zerodha/run_trades.py --exit-1200      [--dry-run]
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

from zerodha.auth import BASE_URL as _KITE_BASE, get_session as _kite_session
from zerodha.trade import buy, sell
from zerodha.trade import order_status as _kite_order_status
import data_loader as _dl

_notify = None
try:
    import notify as _notify
except Exception as _ne:
    print(f"[zerodha] WARNING: Telegram unavailable: {_ne}", file=sys.stderr)

# ── Config ─────────────────────────────────────────────────────────────────────

_IST          = ZoneInfo("Asia/Kolkata")
_BROKER       = "zerodha"
_RESULTS_DIR  = _ROOT / "results"
_POS_FILE     = _RESULTS_DIR / "positions_zerodha.json"
_INSTRUMENTS  = _ROOT / "data" / "instruments" / "upstox_instruments.csv"
TOTAL_CAPITAL = 500_000

_env = _ROOT / "pipeline" / ".env"
if _env.exists():
    for _ln in _env.read_text().splitlines():
        _ln = _ln.strip()
        if _ln and not _ln.startswith("#") and "=" in _ln:
            _k, _, _v = _ln.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

_sym_cache: dict[str, str] = {}


# ── Instrument key resolution (for Upstox candle API) ─────────────────────────

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
            f"[zerodha] '{symbol}' not found in instruments CSV — "
            "pass the instrument_key directly (e.g. 'NSE_EQ|INE...')"
        )
    return key


# ── Candle price fetch (1-minute, via data_loader) ────────────────────────────

def _close_at(candles: list, hhmm: int) -> float | None:
    """Return close of candle whose start time (IST) matches hhmm (e.g. 1513 for 15:13)."""
    for c in candles:
        try:
            dt = datetime.fromisoformat(str(c[0]))
            if dt.hour * 100 + dt.minute == hhmm:
                return float(c[4])
        except (IndexError, ValueError, TypeError):
            continue
    return None


def get_reference_price(symbol: str) -> tuple[float, int]:
    """Close of 15:14 1-min candle — Stage 1 entry sizing. Falls back to 15:13 if 15:14
    isn't available yet. Returns (price, hhmm_used). Raises ValueError if neither is found."""
    matched       = [{"symbol": symbol, "instrument_key": _ikey(symbol)}]
    candles_by_sym = _dl.load_candles(matched, interval="1minute", mode="intraday")
    candles       = candles_by_sym.get(symbol, [])
    price = _close_at(candles, 1514)
    if price is not None:
        return price, 1514
    price = _close_at(candles, 1513)
    if price is not None:
        return price, 1513
    raise ValueError(
        f"[zerodha] Neither 15:14 nor 15:13 candle found for {symbol} "
        f"({len(candles)} candles). Run after 15:15 IST."
    )


def get_live_pnl(symbol: str) -> float:
    """Kite's own computed P&L for this symbol from positions (same-day) or holdings
    (settled) — Stage 2 exit decision. Uses the broker's pnl field directly rather than
    computing return from a fetched price, and isn't candle-based at all, so it isn't at
    the mercy of whether a specific candle has been published yet. Raises ValueError if
    the symbol isn't found in either endpoint (e.g. already exited, or a lookup failure)."""
    session, _ = _kite_session()
    try:
        resp = session.get(f"{_KITE_BASE}/portfolio/positions", timeout=15)
        if resp.ok:
            for p in (resp.json().get("data", {}).get("net") or []):
                if (p.get("tradingsymbol") or "").upper() == symbol.upper():
                    pnl = p.get("pnl")
                    if pnl is not None:
                        return float(pnl)
    except Exception:
        pass
    try:
        resp = session.get(f"{_KITE_BASE}/portfolio/holdings", timeout=15)
        if resp.ok:
            for h in (resp.json().get("data") or []):
                if (h.get("tradingsymbol") or "").upper() == symbol.upper():
                    pnl = h.get("pnl")
                    if pnl is not None:
                        return float(pnl)
    except Exception:
        pass
    raise ValueError(f"[zerodha] No P&L found for {symbol} in Kite positions or holdings.")


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
    """Positions still requiring an exit: fully open OR partial no-data exits from Stage 2."""
    return [p for p in positions
            if p.get("broker") == _BROKER
            and p.get("status") in ("open", "partial_exit_945_nodata")]


def _ts() -> str:
    return datetime.now(_IST).isoformat()


# ── Order fill polling (Zerodha Kite) ─────────────────────────────────────────

class OrderRejected(RuntimeError):
    """Order genuinely did not fill (REJECTED/CANCELLED) — distinct from a poll
    timeout, where the order may well have filled and we just don't know yet."""


def _poll_fill(order_id: str, retries: int = 12, delay: float = 3.0) -> tuple[float, int]:
    """Poll until market order fills. Returns (avg_fill_price, filled_qty)."""
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
    """Returns (price, filled_qty). filled_qty is 0 if the order was genuinely
    rejected/cancelled -- callers MUST check for that and not record it as a
    real fill. For a poll timeout (status still unknown), falls back to the
    intended price/qty as a best-effort guess, same as before."""
    try:
        return _poll_fill(order_id)
    except OrderRejected as exc:
        print(f"[zerodha]   ORDER REJECTED — {exc}")
        return 0.0, 0
    except Exception as exc:
        print(f"[zerodha]   fill poll failed: {exc} — using fallback values")
        return fallback_price, fallback_qty


# ── Broker quantity cross-check (Stage 3, Zerodha Kite) ───────────────────────

def _broker_qty(symbol: str) -> int:
    """Confirm broker-held quantity via Kite positions and holdings endpoints."""
    session, _ = _kite_session()
    # Same-day positions
    try:
        resp = session.get(f"{_KITE_BASE}/portfolio/positions", timeout=15)
        if resp.ok:
            for p in (resp.json().get("data", {}).get("net") or []):
                if (p.get("tradingsymbol") or "").upper() == symbol.upper():
                    qty = int(p.get("quantity") or 0)
                    if qty > 0:
                        return qty
    except Exception:
        pass
    # Overnight holdings
    try:
        resp = session.get(f"{_KITE_BASE}/portfolio/holdings", timeout=15)
        if resp.ok:
            for h in (resp.json().get("data") or []):
                if (h.get("tradingsymbol") or "").upper() == symbol.upper():
                    qty = int(h.get("quantity") or 0)
                    if qty > 0:
                        return qty
    except Exception:
        pass
    return 0


# ── Trade list ─────────────────────────────────────────────────────────────────

def _load_symbols(trade_date: date) -> list[str]:
    path = _RESULTS_DIR / "trades" / f"trade_list_{trade_date.isoformat()}.csv"
    if not path.exists():
        sys.exit(f"[zerodha] No trade list: {path}")
    with open(path, newline="") as f:
        return [r["symbol"].strip().upper() for r in csv.DictReader(f)]


# ── Telegram helper ────────────────────────────────────────────────────────────

def _tg(fn: str, *args, **kwargs) -> None:
    if _notify is None:
        return
    try:
        getattr(_notify, fn)(*args, **kwargs)
    except Exception as exc:
        print(f"[zerodha] Telegram ({fn}) failed: {exc}", file=sys.stderr)


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 1 — Entry at 3:15pm
# ══════════════════════════════════════════════════════════════════════════════

def run_entry_315(trade_date: date | None = None, dry_run: bool = False,
                  capital: float | None = None) -> None:
    if trade_date is None:
        trade_date = date.today()

    symbols = _load_symbols(trade_date)
    if not symbols:
        print(f"[zerodha] Trade list empty for {trade_date} — nothing to enter.")
        return

    n = len(symbols)
    if capital is not None:
        # Testing mode — simple equal split across every signal, regardless of n.
        # (The >=5-signal-only split rule below is a ₹5L-specific design; doesn't
        # apply when running against an arbitrary reduced capital pool.)
        allocation = capital / n
    else:
        capital    = TOTAL_CAPITAL
        allocation = 125_000 if n <= 4 else TOTAL_CAPITAL // n

    print(f"\n{'='*60}")
    print(f"[zerodha] Stage 1 — Entry {trade_date}{'  DRY RUN' if dry_run else ''}")
    print(f"[zerodha] {n} signal(s)  ·  ₹{capital:,.0f} total  ·  ₹{allocation:,.0f} per position")
    print(f"{'='*60}")

    positions     = _load_pos()
    entered_today = {
        p["symbol"] for p in positions
        if p.get("broker") == _BROKER and p.get("entry_date") == trade_date.isoformat()
    }

    for sym in symbols:
        if sym in entered_today:
            print(f"[zerodha] {sym} — already entered today, skipping.")
            continue

        print(f"\n[zerodha] {sym}")

        try:
            ref, ref_hhmm = get_reference_price(sym)
            print(f"[zerodha]   ref price ({ref_hhmm//100:02d}:{ref_hhmm%100:02d} close): ₹{ref:,.2f}")
        except Exception as exc:
            print(f"[zerodha]   SKIP — no reference price: {exc}")
            continue

        shares = math.floor(allocation / ref) if ref > 0 else 0
        if shares == 0:
            print(f"[zerodha]   SKIP — 0 shares at ₹{ref:,.2f} (allocation ₹{allocation:,.0f})")
            continue
        print(f"[zerodha]   shares to buy: {shares}")

        try:
            order_id = buy(sym, "NSE", shares,
                           order_type="MARKET", product="CNC", dry_run=dry_run)
        except Exception as exc:
            print(f"[zerodha]   ORDER FAILED: {exc}")
            continue

        if dry_run:
            fill_price, fill_qty = ref, shares
            print(f"[zerodha]   DRY RUN — simulated fill ₹{fill_price:,.2f} × {fill_qty}")
        else:
            fill_price, fill_qty = _poll_fill_safe(order_id, ref, shares)
            if fill_qty == 0:
                print(f"[zerodha]   NOT FILLED — order rejected, no position recorded.")
                continue
            print(f"[zerodha]   filled ₹{fill_price:,.2f} × {fill_qty}")

        _tg("send_entry", _BROKER, sym, ref, shares, order_id, dry_run=dry_run)

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
        })
        _save_pos(positions)

    print(f"\n[zerodha] Stage 1 complete.")


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 2 — Exit check at 9:45am
# ══════════════════════════════════════════════════════════════════════════════

def check_exit_945(dry_run: bool = False) -> None:
    positions = _load_pos()
    open_ps   = _open_pos(positions)

    print(f"\n{'='*60}")
    print(f"[zerodha] Stage 2 — Exit check 9:45am{'  DRY RUN' if dry_run else ''}")
    print(f"[zerodha] {len(open_ps)} open position(s)")
    print(f"{'='*60}")

    if not open_ps:
        print("[zerodha] No open positions — nothing to check.")
        return

    for pos in open_ps:
        sym        = pos["symbol"]
        fill_price = float(pos["actual_fill_price"] or 0)
        is_partial = pos["status"] == "partial_exit_945_nodata"
        qty        = (int(pos["shares_remaining"]) if is_partial
                      else int(pos["actual_fill_quantity"]))

        print(f"\n[zerodha] {sym}  fill=₹{fill_price:,.2f}  qty={qty}")

        no_data  = False
        pnl_live = 0.0
        try:
            pnl_live = get_live_pnl(sym)
            print(f"[zerodha]   live P&L: ₹{pnl_live:+,.2f}")
        except Exception as exc:
            print(f"[zerodha]   no live P&L available: {exc}")
            no_data = True

        if no_data:
            # Sell half as a precautionary fallback
            half   = math.floor(qty / 2)
            remain = qty - half
            if half == 0:
                print(f"[zerodha]   qty too small to halve — holding until 12pm.")
                continue
            print(f"[zerodha]   NO-DATA FALLBACK — selling {half} of {qty}")
            try:
                oid = sell(sym, "NSE", half,
                           order_type="MARKET", product="CNC", dry_run=dry_run)
            except Exception as exc:
                print(f"[zerodha]   fallback sell failed: {exc}")
                continue
            ep, eq = (fill_price, half) if dry_run else _poll_fill_safe(oid, fill_price, half)
            if eq == 0:
                print(f"[zerodha]   NOT FILLED — fallback sell rejected, position left open.")
                continue
            _tg("send_exit_945_nodata", _BROKER, sym, half, remain, ep, dry_run=dry_run)
            pos.update({
                "status":             "partial_exit_945_nodata",
                "shares_exited_945":  half,
                "shares_remaining":   remain,
                "exit_price_945":     round(ep, 4),
                "exit_order_id_945":  oid,
                "exit_timestamp_945": _ts(),
            })
            _save_pos(positions)
            continue

        if pnl_live > 0:
            # Positive P&L (Kite's own figure) — exit full remaining quantity
            print(f"[zerodha]   P&L positive — selling {qty}")
            try:
                oid = sell(sym, "NSE", qty,
                           order_type="MARKET", product="CNC", dry_run=dry_run)
            except Exception as exc:
                print(f"[zerodha]   sell failed: {exc}")
                continue
            ep, eq = (fill_price, qty) if dry_run else _poll_fill_safe(oid, fill_price, qty)
            if eq == 0:
                print(f"[zerodha]   NOT FILLED — sell rejected, position left open.")
                continue
            pnl     = (ep - fill_price) * eq
            ret_act = (ep - fill_price) / fill_price * 100 if fill_price else 0
            _tg("send_exit_945", _BROKER, sym, ep, ret_act, pnl, dry_run=dry_run)
            pos.update({
                "status":              "exited_945",
                "exit_price_945":      round(ep, 4),
                "exit_order_id_945":   oid,
                "exit_timestamp_945":  _ts(),
                "realized_return_pct": round(ret_act, 4),
                "realized_pnl":        round(pnl, 2),
            })
            _save_pos(positions)
            print(f"[zerodha]   exited ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
        else:
            print(f"[zerodha]   P&L ≤ 0 (₹{pnl_live:+,.2f}) — holding for 12pm forced exit.")

    print(f"\n[zerodha] Stage 2 complete.")


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 3 — Forced exit at 12:00pm
# ══════════════════════════════════════════════════════════════════════════════

def force_exit_1200(dry_run: bool = False) -> None:
    positions = _load_pos()
    open_ps   = _open_pos(positions)

    print(f"\n{'='*60}")
    print(f"[zerodha] Stage 3 — Force exit 12pm{'  DRY RUN' if dry_run else ''}")
    print(f"[zerodha] {len(open_ps)} position(s) still open")
    print(f"{'='*60}")

    if not open_ps:
        print("[zerodha] All positions already exited — nothing to force-close.")
        _tg("send_nothing_open_at_1200", _BROKER)
        _daily_summary(positions, 0, dry_run)
        return

    n_force = 0

    for pos in open_ps:
        sym        = pos["symbol"]
        fill_price = float(pos["actual_fill_price"] or 0)
        is_partial = pos["status"] == "partial_exit_945_nodata"
        qty        = (int(pos["shares_remaining"]) if is_partial
                      else int(pos["actual_fill_quantity"]))

        print(f"\n[zerodha] {sym}  qty={qty}")

        if not dry_run:
            bqty = _broker_qty(sym)
            if bqty != qty:
                print(f"[zerodha]   !! MISMATCH — local={qty} broker={bqty}. "
                      f"Skipping {sym} — manual review required.")
                continue
            print(f"[zerodha]   broker confirmed: {bqty} shares")

        try:
            oid = sell(sym, "NSE", qty,
                       order_type="MARKET", product="CNC", dry_run=dry_run)
        except Exception as exc:
            print(f"[zerodha]   sell failed: {exc}")
            continue

        ep, eq = (fill_price, qty) if dry_run else _poll_fill_safe(oid, fill_price, qty)
        if eq == 0:
            print(f"[zerodha]   !! NOT FILLED — force-exit sell rejected for {sym}. "
                  f"Position left as-is — manual review required.")
            continue

        # Blended P&L: partial 9:45am exit + this 12pm remainder
        if is_partial:
            s945 = int(pos.get("shares_exited_945") or 0)
            p945 = float(pos.get("exit_price_945") or fill_price)
            pnl  = (p945 - fill_price) * s945 + (ep - fill_price) * eq
            tot  = int(pos["actual_fill_quantity"])
            ret  = pnl / (fill_price * tot) * 100 if fill_price and tot else 0
        else:
            pnl = (ep - fill_price) * eq
            ret = (ep - fill_price) / fill_price * 100 if fill_price else 0

        _tg("send_force_exit_1200", _BROKER, sym, ep, ret, pnl, dry_run=dry_run)
        pos.update({
            "status":               "exited_1200",
            "exit_price_1200":      round(ep, 4),
            "exit_order_id_1200":   oid,
            "exit_timestamp_1200":  _ts(),
            "realized_return_pct":  round(ret, 4),
            "realized_pnl":         round(pnl, 2),
        })
        _save_pos(positions)
        print(f"[zerodha]   force-exited ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
        n_force += 1

    _daily_summary(positions, n_force, dry_run)
    print(f"\n[zerodha] Stage 3 complete. Force-exited: {n_force}.")


def _daily_summary(positions: list, n_force: int, dry_run: bool) -> None:
    today     = date.today().isoformat()
    today_ps  = [p for p in positions
                 if p.get("broker") == _BROKER and p.get("entry_date") == today]
    n_opened  = len(today_ps)
    n_945     = sum(1 for p in today_ps if p.get("status") == "exited_945")
    n_partial = sum(1 for p in today_ps
                    if p.get("status") == "exited_1200" and "exit_order_id_945" in p)
    total_pnl = sum(p.get("realized_pnl") or 0 for p in today_ps
                    if p.get("status") in ("exited_945", "exited_1200"))
    _tg("send_daily_summary", _BROKER,
        n_opened, n_945, n_partial, n_force, total_pnl, dry_run=dry_run)
    print(f"\n[zerodha] Summary — opened={n_opened}  exited@945={n_945}  "
          f"partial_nodata={n_partial}  force@1200={n_force}  P&L=₹{total_pnl:+,.2f}")


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Zerodha 3-stage live trading")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--entry",     action="store_true", help="Stage 1: entry at 3:15pm")
    grp.add_argument("--exit-945",  action="store_true", help="Stage 2: exit check at 9:45am")
    grp.add_argument("--exit-1200", action="store_true", help="Stage 3: forced exit at 12pm")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without placing orders")
    parser.add_argument("--date",    default=None,
                        help="Trade date YYYY-MM-DD (--entry only; defaults to today)")
    parser.add_argument("--capital", type=float, default=None,
                        help="Override total capital for --entry (equal split across all "
                             "signals, ignoring the >=5-signal split rule). For testing with "
                             "a reduced capital pool; defaults to the real TOTAL_CAPITAL.")
    args = parser.parse_args()

    td = date.fromisoformat(args.date) if args.date else date.today()

    try:
        if args.entry:
            run_entry_315(trade_date=td, dry_run=args.dry_run, capital=args.capital)
        elif args.exit_945:
            check_exit_945(dry_run=args.dry_run)
        else:
            force_exit_1200(dry_run=args.dry_run)
    except (EnvironmentError, RuntimeError, ValueError) as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)
