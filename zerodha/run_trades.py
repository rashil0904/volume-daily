"""
zerodha/run_trades.py — 3-stage live trading via Zerodha Kite
=============================================================
Entry price : Upstox intraday V3 candle API  (UPSTOX_ACCESS_TOKEN from pipeline/.env) —
              close of 15:20 candle, falls back to 15:19 if 15:20 isn't published yet.
Exit check  : Our own recorded entry fill price vs. current LTP from Kite's /quote/ltp
              (Stage 2) — pnl = (ltp - fill_price) * qty, computed directly rather than
              trusting Kite's positions/holdings pnl field, which can go stale same-day.
Orders      : Zerodha Kite API  (zerodha/trade.py)
Positions   : results/positions_zerodha.json  (full persistent trade book)

Usage:
    python zerodha/run_trades.py --entry          [--dry-run] [--date YYYY-MM-DD]
    python zerodha/run_trades.py --exit-925       [--dry-run]
    python zerodha/run_trades.py --exit-1159      [--dry-run]
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
from common.calc_utils import pick_reference_price, compute_allocation, compute_shares
import data_loader as _dl
import notify

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

def get_reference_price(symbol: str) -> tuple[float, int]:
    """Close of 15:20 1-min candle — Stage 1 entry sizing. Falls back to 15:19, then to
    whichever candle at/before 15:20 is actually the most recent available (not a fixed
    older point) if neither of those posted -- e.g. an illiquid stock with no trades in
    that exact minute. Returns (price, hhmm_used). Raises ValueError only if there's no
    candle data at all up to 15:20."""
    matched        = [{"symbol": symbol, "instrument_key": _ikey(symbol)}]
    candles_by_sym = _dl.load_candles(matched, interval="1minute", mode="intraday")
    candles        = candles_by_sym.get(symbol, [])
    try:
        return pick_reference_price(candles, 1520, 1519)
    except ValueError:
        raise ValueError(
            f"[zerodha] No candle data at all for {symbol} up to 15:20 "
            f"({len(candles)} candles). Run after 15:21 IST."
        )


def get_ltp(symbol: str) -> float:
    """Current last-traded price via Kite's lightweight /quote/ltp endpoint — Stage 2
    exit decision computes P&L as (ltp - our recorded fill_price) * qty directly, rather
    than trusting Kite's own positions/holdings pnl field. That field can go stale: e.g.
    holdings' average_price doesn't always refresh same-day when a symbol is sold and
    then re-bought before settlement (seen live on ASIANTILES, 2026-07-30), which would
    silently corrupt the exit decision. Raises ValueError if no LTP is returned."""
    session, _ = _kite_session()
    resp = session.get(f"{_KITE_BASE}/quote/ltp", params={"i": f"NSE:{symbol}"}, timeout=15)
    resp.raise_for_status()
    entry = resp.json().get("data", {}).get(f"NSE:{symbol}")
    if not entry or entry.get("last_price") is None:
        raise ValueError(f"[zerodha] No LTP found for {symbol}.")
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
    """Positions still requiring an exit: fully open OR partial no-data exits from Stage 2."""
    return [p for p in positions
            if p.get("broker") == _BROKER
            and p.get("status") in ("open", "partial_exit_925_nodata")]


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
    # Overnight holdings — "quantity" is fully-settled shares only; shares bought
    # the previous trading day still sit in "t1_quantity" until Kite completes
    # T+1 settlement (confirmed live 2026-07-22: quantity=0, t1_quantity=N the day
    # after a same-week buy). Both are sellable, so they must be summed.
    try:
        resp = session.get(f"{_KITE_BASE}/portfolio/holdings", timeout=15)
        if resp.ok:
            for h in (resp.json().get("data") or []):
                if (h.get("tradingsymbol") or "").upper() == symbol.upper():
                    qty = int(h.get("quantity") or 0) + int(h.get("t1_quantity") or 0)
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


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 1 — Entry at 3:21pm
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
    capital    = capital if capital is not None else TOTAL_CAPITAL
    # Max ₹ per position is capital/4 (idle capital allowed when n<=4); once
    # signals hit 5+, split the full capital equally instead of over-allocating.
    allocation = compute_allocation(capital, n)

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

        shares = compute_shares(allocation, ref)
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
        if not dry_run:
            _save_pos(positions)
        try:
            notify.send_entry(broker=_BROKER, symbol=sym, ref_price=ref,
                              shares=fill_qty, order_id=order_id, dry_run=dry_run)
        except Exception as exc:
            print(f"  [notify] entry failed: {exc}", file=sys.stderr)

    print(f"\n[zerodha] Stage 1 complete.")


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 2 — Exit check at 9:25am
# ══════════════════════════════════════════════════════════════════════════════

def check_exit_925(dry_run: bool = False) -> None:
    positions = _load_pos()
    open_ps   = _open_pos(positions)

    print(f"\n{'='*60}")
    print(f"[zerodha] Stage 2 — Exit check 9:25am{'  DRY RUN' if dry_run else ''}")
    print(f"[zerodha] {len(open_ps)} open position(s)")
    print(f"{'='*60}")

    if not open_ps:
        print("[zerodha] No open positions — nothing to check.")
        return

    for pos in open_ps:
        sym        = pos["symbol"]
        fill_price = float(pos["actual_fill_price"] or 0)
        is_partial = pos["status"] == "partial_exit_925_nodata"
        qty        = (int(pos["shares_remaining"]) if is_partial
                      else int(pos["actual_fill_quantity"]))

        print(f"\n[zerodha] {sym}  fill=₹{fill_price:,.2f}  qty={qty}")

        no_data  = False
        pnl_live = 0.0
        try:
            ltp      = get_ltp(sym)
            pnl_live = (ltp - fill_price) * qty
            print(f"[zerodha]   LTP: ₹{ltp:,.2f}  live P&L: ₹{pnl_live:+,.2f}")
        except Exception as exc:
            print(f"[zerodha]   no LTP available: {exc}")
            no_data = True

        if no_data:
            # Sell half as a precautionary fallback
            half   = math.floor(qty / 2)
            remain = qty - half
            if half == 0:
                print(f"[zerodha]   qty too small to halve — holding until 11:59am.")
                continue
            if not dry_run:
                bqty = _broker_qty(sym)
                if bqty != qty:
                    print(f"[zerodha]   !! MISMATCH — local={qty} broker={bqty}. "
                          f"Skipping {sym} — manual review required.")
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
                notify.send_exit_925_nodata(broker=_BROKER, symbol=sym,
                                            shares_exited=half, shares_remaining=remain,
                                            exit_price=ep, dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] exit_925_nodata failed: {exc}", file=sys.stderr)
            continue

        if pnl_live > 0:
            # Positive P&L (Kite's own figure) — exit full remaining quantity
            if not dry_run:
                bqty = _broker_qty(sym)
                if bqty != qty:
                    print(f"[zerodha]   !! MISMATCH — local={qty} broker={bqty}. "
                          f"Skipping {sym} — manual review required.")
                    continue
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
            print(f"[zerodha]   exited ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
            try:
                notify.send_exit_925(broker=_BROKER, symbol=sym, exit_price=ep,
                                     return_pct=ret_act, pnl=pnl, dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] exit_925 failed: {exc}", file=sys.stderr)
        else:
            print(f"[zerodha]   P&L ≤ 0 (₹{pnl_live:+,.2f}) — holding for 11:59am forced exit.")

    print(f"\n[zerodha] Stage 2 complete.")


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 3 — Forced exit at 11:59am
# ══════════════════════════════════════════════════════════════════════════════

def force_exit_1159(dry_run: bool = False) -> None:
    positions = _load_pos()
    open_ps   = _open_pos(positions)

    print(f"\n{'='*60}")
    print(f"[zerodha] Stage 3 — Force exit 11:59am{'  DRY RUN' if dry_run else ''}")
    print(f"[zerodha] {len(open_ps)} position(s) still open")
    print(f"{'='*60}")

    if not open_ps:
        print("[zerodha] All positions already exited — nothing to force-close.")
        try:
            notify.send_nothing_open_at_1159(broker=_BROKER)
        except Exception as exc:
            print(f"  [notify] nothing_open_at_1159 failed: {exc}", file=sys.stderr)
        _daily_summary(positions, 0, dry_run)
        return

    n_force = 0

    for pos in open_ps:
        sym        = pos["symbol"]
        fill_price = float(pos["actual_fill_price"] or 0)
        is_partial = pos["status"] == "partial_exit_925_nodata"
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

        # Blended P&L: partial 9:25am exit + this 11:59am remainder
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
        print(f"[zerodha]   force-exited ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
        try:
            notify.send_force_exit_1159(broker=_BROKER, symbol=sym, exit_price=ep,
                                        return_pct=ret, pnl=pnl, dry_run=dry_run)
        except Exception as exc:
            print(f"  [notify] force_exit_1159 failed: {exc}", file=sys.stderr)
        n_force += 1

    _daily_summary(positions, n_force, dry_run)
    print(f"\n[zerodha] Stage 3 complete. Force-exited: {n_force}.")


def _daily_summary(positions: list, n_force: int, dry_run: bool) -> None:
    today     = date.today().isoformat()
    today_ps  = [p for p in positions
                 if p.get("broker") == _BROKER and p.get("entry_date") == today]
    n_opened  = len(today_ps)
    n_925     = sum(1 for p in today_ps if p.get("status") == "exited_925")
    n_partial = sum(1 for p in today_ps
                    if p.get("status") == "exited_1159" and "exit_order_id_925" in p)
    total_pnl = sum(p.get("realized_pnl") or 0 for p in today_ps
                    if p.get("status") in ("exited_925", "exited_1159"))
    print(f"\n[zerodha] Summary — opened={n_opened}  exited@925={n_925}  "
          f"partial_nodata={n_partial}  force@1159={n_force}  P&L=₹{total_pnl:+,.2f}")
    try:
        notify.send_daily_summary(broker=_BROKER, n_opened=n_opened, n_exited_925=n_925,
                                  n_partial_nodata=n_partial, n_force_1159=n_force,
                                  total_pnl=total_pnl, dry_run=dry_run)
    except Exception as exc:
        print(f"  [notify] daily_summary failed: {exc}", file=sys.stderr)


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Zerodha 3-stage live trading")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--entry",     action="store_true", help="Stage 1: entry at 3:21pm")
    grp.add_argument("--exit-925",  action="store_true", help="Stage 2: exit check at 9:25am")
    grp.add_argument("--exit-1159", action="store_true", help="Stage 3: forced exit at 11:59am")
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
        elif args.exit_925:
            check_exit_925(dry_run=args.dry_run)
        else:
            force_exit_1159(dry_run=args.dry_run)
    except (EnvironmentError, RuntimeError, ValueError) as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)
