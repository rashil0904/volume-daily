"""
upstox/run_trades.py — 3-stage live trading via Upstox
=======================================================
Price data  : Upstox intraday V3 candle API  (UPSTOX_ACCESS_TOKEN from pipeline/.env)
Orders      : Upstox order API  (upstox/trade.py)
Positions   : results/positions_upstox.json  (full persistent trade book)

Usage:
    python upstox/run_trades.py --entry          [--dry-run] [--date YYYY-MM-DD] [--mode sandbox]
    python upstox/run_trades.py --exit-945       [--dry-run] [--mode sandbox]
    python upstox/run_trades.py --exit-1200      [--dry-run] [--mode sandbox]
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
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

from upstox.auth import get_session
from upstox.trade import buy, sell, order_status

_notify = None
try:
    import notify as _notify
except Exception as _ne:
    print(f"[upstox] WARNING: Telegram unavailable: {_ne}", file=sys.stderr)

# ── Config ─────────────────────────────────────────────────────────────────────

_IST          = ZoneInfo("Asia/Kolkata")
_BROKER       = "upstox"
_RESULTS_DIR  = _ROOT / "results"
_POS_FILE     = _RESULTS_DIR / "positions_upstox.json"
_INSTRUMENTS  = _ROOT / "data" / "instruments" / "upstox_instruments.csv"
_BASE_V3      = "https://api.upstox.com/v3"
TOTAL_CAPITAL = 500_000

_env = _ROOT / "pipeline" / ".env"
if _env.exists():
    for _ln in _env.read_text().splitlines():
        _ln = _ln.strip()
        if _ln and not _ln.startswith("#") and "=" in _ln:
            _k, _, _v = _ln.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())
_DATA_TOKEN = (os.environ.get("UPSTOX_ACCESS_TOKEN") or "").strip()

_sym_cache: dict[str, str] = {}


# ── Instrument key resolution ──────────────────────────────────────────────────

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
            f"[upstox] '{symbol}' not found in instruments CSV — "
            "pass the instrument_key directly (e.g. 'NSE_EQ|INE...')"
        )
    return key


# ── Candle price fetches (Upstox V3 intraday, 1-minute) ───────────────────────

def _fetch_1min(symbol: str) -> list:
    """Fetch today's 1-min intraday candles for symbol. Returns list of candle arrays."""
    if not _DATA_TOKEN:
        raise EnvironmentError("[upstox] UPSTOX_ACCESS_TOKEN not set in pipeline/.env")
    encoded = quote(_ikey(symbol), safe="")
    url     = f"{_BASE_V3}/historical-candle/intraday/{encoded}/minutes/1"
    headers = {"Authorization": f"Bearer {_DATA_TOKEN}", "Accept": "application/json"}
    for attempt in range(1, 4):
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 429:
                time.sleep(30 * attempt)
                continue
            resp.raise_for_status()
            return resp.json().get("data", {}).get("candles", [])
        except requests.RequestException as exc:
            if attempt == 3:
                raise RuntimeError(f"[upstox] Candle fetch failed for {symbol}: {exc}") from exc
            time.sleep(5 * attempt)
    return []


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


def get_reference_price(symbol: str) -> float:
    """Close of 15:13 1-min candle — Stage 1 entry sizing. Raises ValueError if not found."""
    candles = _fetch_1min(symbol)
    price   = _close_at(candles, 1513)
    if price is None:
        raise ValueError(
            f"[upstox] 15:13 candle not found for {symbol} "
            f"({len(candles)} candles). Run after 15:14 IST."
        )
    return price


def get_exit_check_price(symbol: str) -> float:
    """Close of 09:43 1-min candle — Stage 2 exit decision. Raises ValueError if not found."""
    candles = _fetch_1min(symbol)
    price   = _close_at(candles, 943)
    if price is None:
        raise ValueError(
            f"[upstox] 09:43 candle not found for {symbol} "
            f"({len(candles)} candles). Run after 09:44 IST."
        )
    return price


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


# ── Order fill polling ─────────────────────────────────────────────────────────

def _poll_fill(order_id: str, mode: str = "live",
               retries: int = 12, delay: float = 3.0) -> tuple[float, int]:
    """Poll until market order fills. Returns (avg_fill_price, filled_qty)."""
    for _ in range(retries):
        time.sleep(delay)
        try:
            o      = order_status(order_id, mode=mode)
            status = (o.get("status") or "").lower()
            if status == "complete":
                return float(o.get("average_price") or 0), int(o.get("filled_quantity") or 0)
            if status in ("rejected", "cancelled"):
                raise RuntimeError(
                    f"Order {order_id} {status}: {o.get('status_message', '')}"
                )
        except RuntimeError:
            raise
        except Exception:
            pass
    raise RuntimeError(f"Order {order_id} did not fill within {int(retries * delay)}s")


def _poll_fill_safe(order_id: str, mode: str,
                    fallback_price: float, fallback_qty: int) -> tuple[float, int]:
    try:
        return _poll_fill(order_id, mode=mode)
    except Exception as exc:
        print(f"[upstox]   fill poll failed: {exc} — using fallback values")
        return fallback_price, fallback_qty


# ── Broker quantity cross-check (Stage 3) ──────────────────────────────────────

def _broker_qty(session, base_url: str, symbol: str) -> int:
    """Confirm broker-held quantity via Upstox positions/holdings endpoints."""
    for endpoint in ("portfolio/short-term-positions", "portfolio/long-term-holdings"):
        try:
            resp = session.get(f"{base_url}/{endpoint}", timeout=15)
            if resp.ok:
                for item in (resp.json().get("data") or []):
                    if (item.get("trading_symbol") or "").upper() == symbol.upper():
                        qty = int(item.get("quantity") or 0)
                        if qty > 0:
                            return qty
        except Exception:
            continue
    return 0


# ── Trade list ─────────────────────────────────────────────────────────────────

def _load_symbols(trade_date: date) -> list[str]:
    path = _RESULTS_DIR / f"trade_list_{trade_date.isoformat()}.csv"
    if not path.exists():
        sys.exit(f"[upstox] No trade list: {path}")
    with open(path, newline="") as f:
        return [r["symbol"].strip().upper() for r in csv.DictReader(f)]


# ── Telegram helper ────────────────────────────────────────────────────────────

def _tg(fn: str, *args, **kwargs) -> None:
    if _notify is None:
        return
    try:
        getattr(_notify, fn)(*args, **kwargs)
    except Exception as exc:
        print(f"[upstox] Telegram ({fn}) failed: {exc}", file=sys.stderr)


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 1 — Entry at 3:15pm
# ══════════════════════════════════════════════════════════════════════════════

def run_entry_315(trade_date: date | None = None,
                  dry_run: bool = False,
                  mode: str = "live") -> None:
    if trade_date is None:
        trade_date = date.today()

    symbols = _load_symbols(trade_date)
    if not symbols:
        print(f"[upstox] Trade list empty for {trade_date} — nothing to enter.")
        return

    n          = len(symbols)
    allocation = 125_000 if n <= 4 else TOTAL_CAPITAL // n

    print(f"\n{'='*60}")
    print(f"[upstox] Stage 1 — Entry {trade_date}  mode={mode}{'  DRY RUN' if dry_run else ''}")
    print(f"[upstox] {n} signal(s)  ·  ₹{allocation:,.0f} per position")
    print(f"{'='*60}")

    positions     = _load_pos()
    entered_today = {
        p["symbol"] for p in positions
        if p.get("broker") == _BROKER and p.get("entry_date") == trade_date.isoformat()
    }

    for sym in symbols:
        if sym in entered_today:
            print(f"[upstox] {sym} — already entered today, skipping.")
            continue

        print(f"\n[upstox] {sym}")

        try:
            ref = get_reference_price(sym)
            print(f"[upstox]   ref price (15:13 close): ₹{ref:,.2f}")
        except Exception as exc:
            print(f"[upstox]   SKIP — no reference price: {exc}")
            continue

        shares = math.floor(allocation / ref) if ref > 0 else 0
        if shares == 0:
            print(f"[upstox]   SKIP — 0 shares at ₹{ref:,.2f} (allocation ₹{allocation:,.0f})")
            continue
        print(f"[upstox]   shares to buy: {shares}")

        try:
            order_id = buy(sym, "NSE", shares,
                           order_type="MARKET", product="D",
                           dry_run=dry_run, mode=mode)
        except Exception as exc:
            print(f"[upstox]   ORDER FAILED: {exc}")
            continue

        if dry_run:
            fill_price, fill_qty = ref, shares
            print(f"[upstox]   DRY RUN — simulated fill ₹{fill_price:,.2f} × {fill_qty}")
        else:
            fill_price, fill_qty = _poll_fill_safe(order_id, mode, ref, shares)
            print(f"[upstox]   filled ₹{fill_price:,.2f} × {fill_qty}")

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

    print(f"\n[upstox] Stage 1 complete.")


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 2 — Exit check at 9:45am
# ══════════════════════════════════════════════════════════════════════════════

def check_exit_945(dry_run: bool = False, mode: str = "live") -> None:
    positions = _load_pos()
    open_ps   = _open_pos(positions)

    print(f"\n{'='*60}")
    print(f"[upstox] Stage 2 — Exit check 9:45am  mode={mode}{'  DRY RUN' if dry_run else ''}")
    print(f"[upstox] {len(open_ps)} open position(s)")
    print(f"{'='*60}")

    if not open_ps:
        print("[upstox] No open positions — nothing to check.")
        return

    for pos in open_ps:
        sym        = pos["symbol"]
        fill_price = float(pos["actual_fill_price"] or 0)
        is_partial = pos["status"] == "partial_exit_945_nodata"
        qty        = (int(pos["shares_remaining"]) if is_partial
                      else int(pos["actual_fill_quantity"]))

        print(f"\n[upstox] {sym}  fill=₹{fill_price:,.2f}  qty={qty}")

        no_data = False
        chk = 0.0
        return_pct = 0.0
        try:
            chk        = get_exit_check_price(sym)
            return_pct = (chk - fill_price) / fill_price * 100 if fill_price else 0
            print(f"[upstox]   check price (09:43 close): ₹{chk:,.2f}  return={return_pct:+.2f}%")
        except Exception as exc:
            print(f"[upstox]   no exit check price: {exc}")
            no_data = True

        if no_data:
            # Sell half as a precautionary fallback
            half   = math.floor(qty / 2)
            remain = qty - half
            if half == 0:
                print(f"[upstox]   qty too small to halve — holding until 12pm.")
                continue
            print(f"[upstox]   NO-DATA FALLBACK — selling {half} of {qty}")
            try:
                oid = sell(sym, "NSE", half,
                           order_type="MARKET", product="D",
                           dry_run=dry_run, mode=mode)
            except Exception as exc:
                print(f"[upstox]   fallback sell failed: {exc}")
                continue
            ep, _ = (fill_price, half) if dry_run else _poll_fill_safe(oid, mode, fill_price, half)
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

        if return_pct > 0:
            # Positive return — exit full remaining quantity
            print(f"[upstox]   return > 0 — selling {qty}")
            try:
                oid = sell(sym, "NSE", qty,
                           order_type="MARKET", product="D",
                           dry_run=dry_run, mode=mode)
            except Exception as exc:
                print(f"[upstox]   sell failed: {exc}")
                continue
            ep, eq = (chk, qty) if dry_run else _poll_fill_safe(oid, mode, chk, qty)
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
            print(f"[upstox]   exited ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
        else:
            print(f"[upstox]   return ≤ 0 ({return_pct:+.2f}%) — holding for 12pm forced exit.")

    print(f"\n[upstox] Stage 2 complete.")


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 3 — Forced exit at 12:00pm
# ══════════════════════════════════════════════════════════════════════════════

def force_exit_1200(dry_run: bool = False, mode: str = "live") -> None:
    positions = _load_pos()
    open_ps   = _open_pos(positions)

    print(f"\n{'='*60}")
    print(f"[upstox] Stage 3 — Force exit 12pm  mode={mode}{'  DRY RUN' if dry_run else ''}")
    print(f"[upstox] {len(open_ps)} position(s) still open")
    print(f"{'='*60}")

    if not open_ps:
        print("[upstox] All positions already exited — nothing to force-close.")
        _tg("send_nothing_open_at_1200", _BROKER)
        _daily_summary(positions, 0, dry_run)
        return

    session, base_url = get_session(mode)
    n_force = 0

    for pos in open_ps:
        sym        = pos["symbol"]
        fill_price = float(pos["actual_fill_price"] or 0)
        is_partial = pos["status"] == "partial_exit_945_nodata"
        qty        = (int(pos["shares_remaining"]) if is_partial
                      else int(pos["actual_fill_quantity"]))

        print(f"\n[upstox] {sym}  qty={qty}")

        if not dry_run:
            bqty = _broker_qty(session, base_url, sym)
            if bqty != qty:
                print(f"[upstox]   !! MISMATCH — local={qty} broker={bqty}. "
                      f"Skipping {sym} — manual review required.")
                continue
            print(f"[upstox]   broker confirmed: {bqty} shares")

        try:
            oid = sell(sym, "NSE", qty,
                       order_type="MARKET", product="D",
                       dry_run=dry_run, mode=mode)
        except Exception as exc:
            print(f"[upstox]   sell failed: {exc}")
            continue

        ep, eq = (fill_price, qty) if dry_run else _poll_fill_safe(oid, mode, fill_price, qty)

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
        print(f"[upstox]   force-exited ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
        n_force += 1

    _daily_summary(positions, n_force, dry_run)
    print(f"\n[upstox] Stage 3 complete. Force-exited: {n_force}.")


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
    print(f"\n[upstox] Summary — opened={n_opened}  exited@945={n_945}  "
          f"partial_nodata={n_partial}  force@1200={n_force}  P&L=₹{total_pnl:+,.2f}")


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upstox 3-stage live trading")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--entry",     action="store_true", help="Stage 1: entry at 3:15pm")
    grp.add_argument("--exit-945",  action="store_true", help="Stage 2: exit check at 9:45am")
    grp.add_argument("--exit-1200", action="store_true", help="Stage 3: forced exit at 12pm")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without placing orders")
    parser.add_argument("--mode",    default="live", choices=["live", "sandbox"])
    parser.add_argument("--date",    default=None,
                        help="Trade date YYYY-MM-DD (--entry only; defaults to today)")
    args = parser.parse_args()

    td = date.fromisoformat(args.date) if args.date else date.today()

    try:
        if args.entry:
            run_entry_315(trade_date=td, dry_run=args.dry_run, mode=args.mode)
        elif args.exit_945:
            check_exit_945(dry_run=args.dry_run, mode=args.mode)
        else:
            force_exit_1200(dry_run=args.dry_run, mode=args.mode)
    except (EnvironmentError, RuntimeError, ValueError) as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)
