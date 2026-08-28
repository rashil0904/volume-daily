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
Positions   : results/positions_dhan_long.json + results/positions_dhan_short.json
              -- separate files (not one combined file with a "direction" field) so
              a same-day mirrored short can never block a fresh same-day long entry
              on the same symbol; see the "Positions JSON" section below for why.
              Independent from positions_zerodha_long.json/positions_zerodha_short.json.

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
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait as _wait_futures
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

from dhan.auth import BASE_URL as _DHAN_BASE, get_session as _dhan_session
from dhan.trade import (buy, sell, place_order, order_status as _dhan_order_status,
                        cancel_order as _dhan_cancel_order, get_orders as _dhan_get_orders,
                        security_id, tick_size)
from common.calc_utils import pick_reference_price, compute_allocation, compute_shares
import data_loader as _dl
import notify

# ── Config ─────────────────────────────────────────────────────────────────────

_IST          = ZoneInfo("Asia/Kolkata")
_BROKER       = "dhan"
_RESULTS_DIR  = _ROOT / "results"
_POS_FILE_LONG  = _RESULTS_DIR / "positions_dhan_long.json"
_POS_FILE_SHORT = _RESULTS_DIR / "positions_dhan_short.json"
_INSTRUMENTS  = _ROOT / "data" / "instruments" / "upstox_instruments.csv"
_LOG_DIR      = _RESULTS_DIR / "trades"
TOTAL_CAPITAL = 1_400_000

# ── Batched-concurrent execution for check_exit_925 / force_exit_1159 /
# _open_short / square_off_239 ──────────────────────────────────────────────
# Dhan's confirmed rate ceilings (separate budgets):
#   Order APIs (place/modify/cancel/status-check) : 10 req/sec
#   Data APIs  (historical/candle data)            :  5 req/sec
#   Quote APIs (/marketfeed/ltp|quote|ohlc)        :  1 req/sec, up to 1,000
#                                                     instruments per call
# MAX_ORDER_CALLS_PER_SECOND is set to HALF the confirmed Order-API ceiling,
# not the full 10 -- margin against the ceiling being an aggregate across
# every order call this process makes in that second (placements, status
# checks, cancels), not just the ones this file's chunking directly controls
# (dhan/trade.py's rate_limiter separately caps place_order()/cancel_order()
# at 5/sec already; this constant governs how many POSITIONS worth of
# concurrent order-flow -- exit sell, fill poll, and any short it triggers --
# run_trades.py itself allows in flight at once, see run_entry_321's model
# pattern above this being adapted here with explicit chunk+sleep pacing
# instead of one flat unbounded pool).
MAX_ORDER_CALLS_PER_SECOND = 5
BATCH_SLEEP_SECONDS        = 1.0
# Wave-based redesign (2026-08-27): check_exit_925/force_exit_1159/
# square_off_239 no longer run a position's full cancel->sell->short->
# target/SL sequence in one worker thread -- every position in a batch now
# performs the SAME action before any position in that batch moves to the
# next action (cancel-all, then sell-all; short-all; target/SL-all), so a
# concurrent Order-API burst is always homogeneous by call type, never a mix
# of e.g. one position's cancel landing alongside another's cover-target buy.
# Wave 1 cancels BEFORE selling (reverted 2026-08-28 -- a "sell first" version
# was tried and broke live: the broker's RMS rejects a new sell for the same
# shares a still-resting target order already committed, "you are trying to
# sell more than the quantity you currently hold" -- confirmed live on
# BOMDYEING/GKENERGY/RAMRAT's 11:59 force-exit, all 3 rejected twice and left
# open until manually cancelled+sold). Cancel-then-sell avoids that collision
# entirely since the target is gone before the new sell is even placed.
# SHORT_SETTLE_BUFFER_SECONDS is a ONE-TIME pause between the short-open wave
# and the target/stop-loss wave (not between every batch -- BATCH_SLEEP_SECONDS
# already covers that) giving the exchange a moment to register the new short
# before orders referencing it are placed.
SHORT_SETTLE_BUFFER_SECONDS = 2.5

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


# ── Entry staging — hold until exactly 15:21:00 IST before firing, same as
# zerodha/trade.py's stage_entry_orders() ──────────────────────────────────────

_LTP_FETCH_AT = (15, 20, 57)   # wall-clock (H, M, S) IST -- fresh LTP fetched here
_FIRE_AT      = (15, 21, 0)    # wall-clock (H, M, S) IST -- orders fire exactly here

def _seconds_until(hh: int, mm: int, ss: int, now: datetime | None = None) -> float:
    now    = now or datetime.now(_IST)
    target = now.replace(hour=hh, minute=mm, second=ss, microsecond=0)
    if target < now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


# ── Order fill polling ─────────────────────────────────────────────────────────

class OrderRejected(RuntimeError):
    """Order genuinely did not fill (REJECTED/CANCELLED)."""


def _poll_fill(order_id: str, retries: int = 12, delay: float = 1.0) -> tuple[float, int]:
    # Check immediately on the first attempt -- only sleep before retries 2+.
    # By the time an order_id comes back from the broker, the order has
    # already been sitting there for the round-trip of the placement call
    # itself, so an immediate first check isn't a wasted no-op; sleeping
    # BEFORE it (the old behavior) just threw away a full second on every
    # single order, entry or exit, for no benefit. retries stays at 12 --
    # only the pointless first sleep is removed (worst-case wait drops from
    # ~12s to ~11s of actual sleeping, same number of status checks).
    for attempt in range(retries):
        if attempt > 0:
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


def _sell_margin_safe(sym: str, exch: str, qty: int, price: float, product: str,
                      dry_run: bool, order_type: str = "LIMIT") -> str:
    """Places a SELL order; retries once if the broker rejects it for a
    specific, confirmed-live settlement/pledge-ledger lag -- NOT on any
    rejection (a genuine circuit-limit or other rejection would fail
    identically on retry too, so retrying there just burns another order for
    nothing). Two confirmed patterns share the same root cause -- a
    reporting API (/positions, /holdings) already shows the full quantity,
    but the broker's real-time RMS eligibility check for that SPECIFIC
    product hasn't caught up yet -- but they need DIFFERENT remedies:

      - product == "MARGIN" (T+5): "Sell orders under T+5 are permitted only
        against existing T+5 buy positions. No eligible T+5 quantity found"
        -- confirmed live 2026-08-24 (KLBRENG-B/WELSPLSOL). Retries almost
        immediately as CNC -- CNC sells against plain settled DEMAT holdings
        and sidesteps the T+5-specific eligibility check entirely.

      - product == "MTF": "...You are trying to sell more than the quantity
        you currently hold" -- confirmed live 2026-08-25 (OPTIEMUS, bought
        the previous session, sold at 9:25am the next morning). UNLIKE the
        T+5 case, a CNC retry does NOT fix this -- the shares are pledged as
        collateral for the MTF loan itself, so the block sits at the
        depository/pledge level, not a product-routing quirk; CNC would hit
        the exact same pledge block and fail identically (confirmed by
        reasoning through the mechanism, not just guessed). The only thing
        that actually resolved it live was waiting for the pledge-linkage
        reconciliation with CDSL/NSDL to finish, then retrying at the SAME
        product -- a manual retry as plain MTF, a few minutes later,
        succeeded on its own. So this retries as MTF again, not CNC, after a
        30s wait -- long enough to give the reconciliation a real chance,
        short enough not to badly stall every OTHER symbol still waiting in
        the same exit-check loop (this call blocks synchronously). NOT
        guaranteed to succeed -- the real case took ~4 minutes wall-clock to
        clear -- if this retry is ALSO rejected, its order_id is returned
        as-is; the caller's normal fill-confirmation polling correctly
        reports it as not filled, and the position is picked up again,
        unconditionally, by the 11:59am force exit -- the real safety net
        for whatever this 30s retry doesn't catch.

    Gated on the SPECIFIC rejection text for MTF (unlike MARGIN, which
    retries on any REJECTED/CANCELLED) -- an MTF sell can also legitimately
    fail for unrelated reasons (e.g. a circuit-limit breach), and blindly
    retrying THOSE would be pointless and could mask a real issue.

    Returns the order_id actually used (original, or the retry's). Not used
    for CNC/INTRADAY sells -- no known ledger-lag quirk there, and a blind
    retry could mask a real problem."""
    order_id = sell(sym, exch, qty, order_type=order_type, price=price,
                    product=product, dry_run=dry_run)
    if dry_run or product not in ("MARGIN", "MTF"):
        return order_id
    time.sleep(2)
    try:
        o      = _dhan_order_status(order_id)
        status = (o.get("orderStatus") or "").upper()
    except Exception:
        return order_id

    if product == "MARGIN" and status in ("REJECTED", "CANCELLED"):
        print(f"[dhan]   MARGIN SELL REJECTED — retrying as CNC.")
        return sell(sym, exch, qty, order_type=order_type, price=price,
                   product="CNC", dry_run=dry_run)

    if product == "MTF" and status in ("REJECTED", "CANCELLED"):
        reason = (o.get("omsErrorDescription") or "").lower()
        if "trying to sell more than" in reason:
            print(f"[dhan]   MTF SELL REJECTED (pledge-ledger lag) — waiting 30s "
                  f"then retrying as MTF again (CNC would hit the same pledge block).")
            time.sleep(30)
            return sell(sym, exch, qty, order_type=order_type, price=price,
                       product="MTF", dry_run=dry_run)

    return order_id


def _poll_fill_strict(order_id: str) -> tuple[float, int, bool, str]:
    """Like _poll_fill_safe, but never guesses on an unconfirmed timeout --
    only a genuine broker-confirmed TRADED status counts as filled. Used for
    ENTRIES only: a MARKET order stuck PENDING with no matching liquidity
    (e.g. a circuit-locked stock) must never get written to
    positions_dhan_long.json as a phantom fill just because the poll gave up
    waiting (see STYLEBAAZA,
    2026-08-14 -- an order that sat PENDING for the full 12s poll window on a
    circuit-locked stock got recorded as filled at the reference price/qty,
    when nothing had actually traded). Returns (0.0, 0, rejected, reason) for
    BOTH a genuine rejection and an unconfirmed timeout -- either way,
    nothing gets recorded -- but callers that need to tell the two apart
    (e.g. an MTF-ineligibility retry, which must NOT fire on a plain
    no-liquidity timeout, NOR on a genuine rejection for an unrelated reason
    like a circuit-limit breach -- see the reason string) can check the
    third element: True only for a genuine broker-confirmed
    REJECTED/CANCELLED/EXPIRED, False for an unconfirmed timeout."""
    try:
        price, qty = _poll_fill(order_id)
        return price, qty, False, ""
    except OrderRejected as exc:
        print(f"[dhan]   ORDER REJECTED — {exc}")
        return 0.0, 0, True, str(exc)
    except Exception as exc:
        print(f"[dhan]   fill poll failed: {exc} — NOT recording as filled "
              f"(order may still be pending at the broker; check manually)")
        return 0.0, 0, False, ""


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
    master (dhan/trade.py's tick_size()) -- tick size is NOT a flat ₹0.05
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
# Long and short positions live in SEPARATE files (positions_dhan_long.json /
# positions_dhan_short.json), not one combined file keyed by a "direction"
# field. This is deliberate, not just a naming split: with one shared file,
# a fresh long entry's "already entered today" check (see run_entry_321) has
# to scan for any same-day row regardless of direction -- which means a
# mirrored short opened THIS MORNING from a 9:25/11:59 exit silently blocks
# a legitimate fresh long re-entry on that same symbol later the same day.
# Confirmed live 2026-08-20: BAJAJHIND and ZAGGLE both had fresh long signals
# today, and both got skipped ("already entered today") purely because their
# mirrored shorts from this morning's exits carried today's entry_date --
# nothing to do with today's actual long signal. Separate files make that
# structurally impossible: run_entry_321 only ever looks at the long file.

def _load_json_positions(path: Path) -> list:
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text())
    except Exception:
        return []


def _save_json_positions(path: Path, positions: list) -> None:
    _RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(positions, indent=2, ensure_ascii=False))


def _load_long_pos() -> list:
    return _load_json_positions(_POS_FILE_LONG)


def _save_long_pos(positions: list) -> None:
    _save_json_positions(_POS_FILE_LONG, positions)


def _load_short_pos() -> list:
    return _load_json_positions(_POS_FILE_SHORT)


def _save_short_pos(positions: list) -> None:
    _save_json_positions(_POS_FILE_SHORT, positions)


def _open_pos(positions: list) -> list:
    """positions must already be from _load_long_pos() -- the long file never
    contains a short-direction row, so no direction filter is needed here."""
    return [p for p in positions
            if p.get("broker") == _BROKER
            and p.get("status") in ("open", "partial_exit_925_nodata")]


def _open_short_pos(positions: list) -> list:
    """Short positions opened by _open_short() (mirroring a 925/1159 long exit) that
    still need squaring off at 2:39pm. positions must already be from
    _load_short_pos() -- the short file only ever contains direction="short" rows."""
    return [p for p in positions
            if p.get("broker") == _BROKER
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


class _BalanceTracker:
    """Thread-safe shared balance pool for concurrent _open_short_core() calls
    within the same chunk -- lock around the check-and-decrement so two
    threads can never both observe "room for this short" and both proceed
    (the concurrent equivalent of run_entry_321's plain-float
    available_balance threaded sequentially through its Phase 1 loop; that
    approach only works single-threaded, hence the lock here). `value` is
    None when the balance couldn't be verified at all -- every reservation
    then fails closed (never guesses a balance is fine)."""

    def __init__(self, initial: float | None):
        self._lock  = threading.Lock()
        self._value = initial

    def try_reserve(self, amount: float) -> bool:
        """Atomically checks then decrements. Returns False (no mutation) if
        the balance is unknown or insufficient."""
        with self._lock:
            if self._value is None or self._value < amount:
                return False
            self._value -= amount
            return True

    @property
    def value(self) -> float | None:
        with self._lock:
            return self._value


def _run_batch(items: list, worker_fn) -> list:
    """Runs worker_fn(item) concurrently for EVERY item in `items` as one
    single burst -- a bounded ThreadPoolExecutor (max_workers == len(items))
    -- and blocks until all of them complete, returning the results as a
    list. This is the single-burst primitive both _run_in_chunks (one
    worker per batch) and _run_exit_wave1 (two workers per batch -- cancel,
    then sell) are built from, so a multi-sub-step batch can compose more
    than one burst within the same batch boundary without going through
    separate _run_in_chunks calls.

    worker_fn must never let an exception escape (wrap its own body in
    try/except and return an error dict/marker instead) -- one item's
    failure must not affect its batch-mates; this helper adds no additional
    per-item exception handling. Returns [] immediately for an empty list
    (no ThreadPoolExecutor spun up for nothing)."""
    if not items:
        return []
    with ThreadPoolExecutor(max_workers=len(items)) as executor:
        futures = {executor.submit(worker_fn, item): item for item in items}
        _wait_futures(futures.keys())
        return [fut.result() for fut in futures]


def _run_in_chunks(items: list, worker_fn, chunk_size: int | None = None,
                   sleep_between: float | None = None):
    """Generator: yields one list of results per chunk of `items`
    (chunk_size at a time, default MAX_ORDER_CALLS_PER_SECOND), running
    worker_fn(item) concurrently within each chunk via _run_batch (waiting
    for the WHOLE chunk to finish before yielding -- same "fire the batch,
    wait for all of it, only then move on" shape as run_entry_321's Phase 2,
    `_wait_futures` -- never proceed on partial completion), and paced with
    `sleep_between` seconds (default BATCH_SLEEP_SECONDS) between chunks
    (skipped after the last one) to keep combined Order-API call volume
    under Dhan's confirmed ceiling.

    A generator rather than a single list-returning function specifically so
    callers can apply each chunk's results -- including any position-file
    write -- BEFORE the next chunk's threads start, per the "no position-file
    write from inside a worker thread" rule (see check_exit_925 /
    force_exit_1159 / square_off_239, all of which write their results this
    way).

    worker_fn must never let an exception escape -- see _run_batch."""
    chunk_size    = chunk_size or MAX_ORDER_CALLS_PER_SECOND
    sleep_between = BATCH_SLEEP_SECONDS if sleep_between is None else sleep_between
    for i in range(0, len(items), chunk_size):
        chunk = items[i : i + chunk_size]
        yield _run_batch(chunk, worker_fn)
        if i + chunk_size < len(items):
            time.sleep(sleep_between)


def _run_exit_wave1(tasks: list, cancel_fn, sell_fn, chunk_size: int | None = None,
                    sleep_between: float | None = None):
    """Generator shared by check_exit_925/force_exit_1159's Wave 1: for each
    batch of `tasks`, first cancels every task's stale target order (one
    concurrent burst via _run_batch), THEN -- only once every cancel in the
    batch has completed -- sells every task in the batch (a second, separate
    concurrent burst; includes fill polling and, for a no-data fallback
    task, its fresh-target-remainder placement). The two sub-steps within a
    batch never overlap with each other, so every concurrent Order-API burst
    stays homogeneous by call type (all cancels together, then all sells
    together) -- the core invariant the wave design trades the old
    per-position cascade-in-one-thread design for. Cancel MUST precede sell,
    not the other way around: a still-resting target order and a fresh sell
    for the same shares can't both be live at once -- the broker's RMS
    rejects the new sell as "trying to sell more than the quantity you
    currently hold" (confirmed live 2026-08-28). `sleep_between` (default
    BATCH_SLEEP_SECONDS) is paced between BATCHES, not between a batch's own
    cancel and sell sub-steps.

    Yields one batch's SELL-step results at a time -- cancel-step results
    are never surfaced (a cancel failure is non-fatal, same as today, and
    only printed inline by cancel_fn itself)."""
    chunk_size    = chunk_size or MAX_ORDER_CALLS_PER_SECOND
    sleep_between = BATCH_SLEEP_SECONDS if sleep_between is None else sleep_between
    for i in range(0, len(tasks), chunk_size):
        batch = tasks[i : i + chunk_size]
        _run_batch(batch, cancel_fn)
        yield _run_batch(batch, sell_fn)
        if i + chunk_size < len(tasks):
            time.sleep(sleep_between)


def _open_short_place(sym: str, qty: int, source_stage: str, dry_run: bool,
                      ltp: float | None, balance: "_BalanceTracker") -> dict | None:
    """Wave 2 of the mirrored-short open (check_exit_925/force_exit_1159):
    shorting kill-switch check, INTRADAY margin check, thread-safe balance
    reservation (_BalanceTracker -- safe to call concurrently from multiple
    Wave-2 workers at once), places the short SELL, polls the fill, fires
    send_short_open. Returns a row dict with cover_target_order_id/
    cover_target_price/stop_order_id/stop_trigger_price/stop_limit_price all
    None -- _open_short_protect() (Wave 3) fills those in as a SEPARATE
    concurrent burst, after SHORT_SETTLE_BUFFER_SECONDS, so a cover-target/
    stop-loss placement never lands in the same Order-API burst as a
    different position's short-open sell (see the module note on the
    wave-based redesign above MAX_ORDER_CALLS_PER_SECOND's definition).

    Never raises: the long exit that triggered this has already happened
    and is never reversed by a failed short. Wrapped in its own top-level
    try/except so an unexpected error here can never propagate out of a
    batch worker and affect sibling positions in the same batch."""
    try:
        if _shorting_skipped_today():
            print(f"[dhan]   SHORT SKIP — {sym}: shorting disabled for today "
                  f"(see {_SKIP_SHORTING_FILE.name}).")
            return None

        if ltp is None:
            try:
                ltp = get_ltp(sym)
            except Exception:
                ltp = 0.0

        margin_info = _intraday_margin_check(sym, qty, ltp) if ltp else None
        if margin_info is None:
            print(f"[dhan]   SHORT SKIP — {sym}: could not verify INTRADAY margin.")
            return None
        if not balance.try_reserve(margin_info["margin_required"]):
            bal = balance.value
            print(f"[dhan]   SHORT SKIP — {sym}: insufficient balance "
                  f"(available {'unknown' if bal is None else f'₹{bal:,.2f}'} "
                  f"< required ₹{margin_info['margin_required']:,.2f}).")
            return None

        short_limit = _tick_round(sym, ltp * 0.995)
        try:
            oid = sell(sym, "NSE_EQ", qty, order_type="LIMIT", price=short_limit,
                      product="INTRADAY", dry_run=dry_run)
        except Exception as exc:
            print(f"[dhan]   SHORT FAILED — {sym}: {exc}")
            return None

        ep, eq = (ltp, qty) if dry_run else _poll_fill_safe(oid, ltp, qty)
        if eq == 0:
            print(f"[dhan]   SHORT NOT FILLED — {sym} short order rejected.")
            return None

        print(f"[dhan]   SHORT OPENED — {sym}  ₹{ep:,.2f} × {eq}  (from {source_stage} exit)")
        try:
            notify.send_short_open(broker=_BROKER, symbol=f"{sym} [SHORT INTRADAY]", entry_price=ep,
                                   shares=eq, source_exit_stage=source_stage, order_id=oid,
                                   dry_run=dry_run)
        except Exception as exc:
            print(f"  [notify] short_open failed: {exc}", file=sys.stderr)

        return {
            "broker":                _BROKER,
            "symbol":                sym,
            "direction":             "short",
            "product":               "INTRADAY",
            "source_exit_stage":     source_stage,
            "entry_date":            date.today().isoformat(),
            "entry_price":           round(ep, 4),
            "quantity":              eq,
            "entry_order_id":        oid,
            "cover_target_order_id": None,
            "cover_target_price":    None,
            "stop_order_id":         None,
            "stop_trigger_price":    None,
            "stop_limit_price":      None,
            "status":                "short_open",
            "entry_timestamp":       _ts(),
        }
    except Exception as exc:
        print(f"[dhan]   !! SHORT open sequence crashed unexpectedly for {sym}: {exc} "
              f"— manual review required.")
        return None


def _open_short_protect(row: dict, dry_run: bool, circuit: float | None = None) -> dict:
    """Wave 3 of the mirrored-short open: places the 5%-below cover target
    and the UC-based stop-loss for a short _open_short_place() already
    opened, mutating and returning the SAME row dict with those fields
    filled in (safe -- each Wave-3 worker only ever touches its own
    position's row, never a shared one). The short itself is already live
    by this point; a failure here never unwinds it -- same
    never-unwind-a-completed-action rule as before the wave split, just
    spread across two functions now instead of one.

    circuit: pre-fetched via _fetch_upper_circuit_batch() by the caller, one
    call covering every symbol that might trigger a short THIS stage-run
    (Part 1 batching -- see module notes). None falls back to a live
    single-symbol fetch (matches the pre-wave-split behavior, used when
    this is called standalone -- see _open_short_core below).

    Never raises: wrapped in its own top-level try/except so an unexpected
    error here can never propagate out of a batch worker and affect
    sibling positions in the same batch."""
    sym, eq, ep = row["symbol"], row["quantity"], row["entry_price"]
    try:
        cover_price = _tick_round(sym, ep * 0.95)
        try:
            cover_target_order_id = buy(sym, "NSE_EQ", eq, order_type="LIMIT",
                                        price=cover_price, product="INTRADAY", dry_run=dry_run)
            row["cover_target_order_id"] = cover_target_order_id
            row["cover_target_price"]    = cover_price
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
        upper_circuit = circuit
        if upper_circuit is None:
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

        if upper_circuit is not None:
            try:
                stop_limit_price   = _tick_round(sym, upper_circuit)
                stop_trigger_price = _tick_round(sym, upper_circuit * 0.995)
                stop_order_id = buy(sym, "NSE_EQ", eq, order_type="STOP_LOSS",
                                   price=stop_limit_price, trigger_price=stop_trigger_price,
                                   product="INTRADAY", dry_run=dry_run)
                row["stop_order_id"]      = stop_order_id
                row["stop_trigger_price"] = stop_trigger_price
                row["stop_limit_price"]   = stop_limit_price
                print(f"[dhan]   stop-loss placed @ trigger ₹{stop_trigger_price:,.2f} "
                      f"limit ₹{stop_limit_price:,.2f} (0.5% below UC ₹{upper_circuit:,.2f}) "
                      f"— order {stop_order_id}")
            except Exception as exc:
                print(f"[dhan]   !! SHORT OPENED (cover target live) but stop-loss placement "
                      f"failed for {sym}: {exc} — manual review required.")
    except Exception as exc:
        print(f"[dhan]   !! target/stop-loss placement crashed unexpectedly for {sym}: {exc} "
              f"— manual review required (short is live).")
    return row


def _open_short_core(sym: str, qty: int, source_stage: str, dry_run: bool,
                     ltp: float | None, balance: "_BalanceTracker",
                     circuit: float | None = None) -> dict | None:
    """Combines _open_short_place() (Wave 2) + _open_short_protect() (Wave 3)
    into the single call standalone/manual callers expect -- see
    _open_short() below, kept as the thin backward-compatible wrapper it
    already was. check_exit_925/force_exit_1159 call the two pieces
    separately instead, as two independently-batched waves (see the module
    note on the wave-based redesign above MAX_ORDER_CALLS_PER_SECOND's
    definition) -- this function's own external behavior/signature is
    unchanged by that split."""
    row = _open_short_place(sym, qty, source_stage, dry_run, ltp, balance)
    if row is None:
        return None
    return _open_short_protect(row, dry_run, circuit)


def _open_short(sym: str, qty: int, source_stage: str, dry_run: bool = False,
                ltp: float | None = None,
                available_balance: float | None = None) -> float | None:
    """Opens a same-quantity intraday short (productType=INTRADAY) mirroring a long
    exit that just filled at either the 9:25am or 11:59am stage -- see run_trades.py's
    module docstring for the shorting add-on. Skips (never raises) on any
    margin-check/balance/fill failure: the long exit that triggered this has already
    happened and is never reversed by a failed short.

    Thin backward-compatible wrapper around _open_short_core() -- same
    external signature/behavior as before this file's stages were made
    batched-concurrent (still does its own position-file load+append+save,
    still takes/returns a plain float), for standalone/manual calls and
    existing single-call-site tests. check_exit_925/force_exit_1159's
    concurrent chunk workers call _open_short_core() directly instead, with
    a shared _BalanceTracker across the whole chunk -- see that function's
    docstring.

    ltp: pre-fetched via get_ltp_batch() by the caller when opening multiple
    shorts in the same pass -- avoids a separate single-symbol get_ltp()
    call per short, which would re-introduce the same Quote-API rate-limit
    risk get_ltp_batch() exists to avoid. Falls back to a live single-symbol
    fetch only when called standalone (ltp not supplied).

    available_balance: same idea, for /fundlimit -- None means "fetch it
    fresh" (fine for a standalone call). Returns the balance remaining after
    this short's margin commitment (decremented at ORDER PLACEMENT, not fill
    confirmation -- matches how a broker actually blocks funds), or whatever
    value was passed in, unchanged, on any branch that skips before an order
    is actually placed."""
    if available_balance is None:
        available_balance = _available_balance()
    balance = _BalanceTracker(available_balance)
    row = _open_short_core(sym, qty, source_stage, dry_run, ltp, balance)
    if row is not None:
        positions = _load_short_pos()
        positions.append(row)
        if not dry_run:
            _save_short_pos(positions)
    return balance.value


# ── Entries log (separate from positions_dhan_long.json, mirrors mtf_entries_*.csv) ─

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
    positions_dhan_long.json + positions_dhan_short.json -- called after
    every pipeline stage (321/925/1159/239) so the workbook stays current.
    Best-effort: loaded by file path (not a package import, results/ isn't
    one) and wrapped so a sync failure never blocks the actual trading
    stage that just ran."""
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

    positions     = _load_long_pos()
    positions_today = {
        p["symbol"]: p for p in positions
        if p.get("broker") == _BROKER and p.get("entry_date") == trade_date.isoformat()
    }

    # Step 1/2/3 priority ordering (dhan/uc_staged_entry.py's Case A/B may
    # already have partially or fully filled some of today's symbols, or
    # even symbols outside today's trade_list.csv entirely -- Case A/B
    # watches live_monitor.py's broader qualified universe, not the strict
    # shortlist `symbols` is otherwise drawn from):
    #   Step 1 (highest priority): entry_status=="partially_filled" -- Case A
    #     leg 1 fired but leg 2 never retraced by its window close. Completed
    #     FIRST, buying only the remaining balance of that symbol's own
    #     capital_base (not the batch allocation below).
    #   Step 2: any other existing row for today (entry_status=="filled" via
    #     Case A/B, or an ordinary already-filled entry) -- skip entirely,
    #     same dedup as always.
    #   Step 3: no row at all -- completely unchanged today's full-allocation
    #     entry.
    step1_syms = [sym for sym, pos in positions_today.items()
                 if pos.get("entry_status") == "partially_filled"]
    step2_syms = [sym for sym in positions_today if sym not in step1_syms]
    step3_syms = [sym for sym in symbols if sym not in positions_today]

    for sym in step2_syms:
        print(f"[dhan] {sym} — already entered today, skipping.")
        n_skipped += 1

    ordered_symbols = step1_syms + step3_syms
    if step1_syms:
        print(f"[dhan] Step 1 priority completion(s): {step1_syms}")

    # One-time balance fetch for this whole run -- see the matching note on
    # _open_short()'s available_balance param. Decremented locally in Phase 1
    # below as each order is DECIDED (not on fill confirmation, and now not
    # even on order placement -- placement itself moves to Phase 2's thread
    # pool, so the pool can never oversubscribe the same balance two threads
    # both still see as available), instead of a fresh /fundlimit REST call
    # per symbol.
    available_balance = _available_balance()

    # ── Phase 1 (sequential): resolve reference price, shares, product/
    # leverage/margin, and the balance-availability decision for every
    # symbol, decrementing the shared available_balance pool as we go. Must
    # stay sequential -- these are the /margincalculator leverage checks and
    # the capital-pool bookkeeping that is NOT safe to run concurrently
    # against the same pool. Nothing here places an order yet.
    ready: list[dict] = []
    for sym in ordered_symbols:
        existing_pos = positions_today.get(sym)   # present only for Step 1 symbols
        is_partial_fill = existing_pos is not None

        print(f"\n[dhan] {sym}")

        try:
            ref, ref_hhmm = get_reference_price(sym)
            print(f"[dhan]   ref price ({ref_hhmm//100:02d}:{ref_hhmm%100:02d} close): ₹{ref:,.2f}")
        except Exception as exc:
            print(f"[dhan]   SKIP — no reference price: {exc}")
            n_skipped += 1
            continue

        if is_partial_fill:
            remaining_capital = existing_pos["capital_base"] - existing_pos["filled_amount"]
            shares = compute_shares(remaining_capital, ref)
            print(f"[dhan]   Step 1 priority — Case A left ₹{existing_pos['filled_amount']:,.2f} "
                  f"of ₹{existing_pos['capital_base']:,.2f} filled — completing remaining "
                  f"₹{remaining_capital:,.2f} ({shares} shares).")
        elif manual_mode and shares_override is not None:
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

        if is_partial_fill:
            # Reuse leg 1's product as-is rather than running a fresh
            # leverage decision -- avoids ending up with a mixed-product
            # position (leg 1 MTF, completion CNC or vice versa) that
            # dhan/charges.py's single per-position `product` field can't
            # represent cleanly. If leg 1's product genuinely isn't
            # available anymore, the existing MTF-ineligibility CNC-retry
            # further below still catches that the same way it always does.
            product         = existing_pos["product"]
            leverage        = 0.0
            margin_required = shares * ref
            capital_base    = existing_pos.get("capital_base", capital)
            print(f"[dhan]   Case A completion — reusing product {product} from leg 1  "
                  f"·  margin required: ₹{margin_required:,.2f}")
        elif cnc_only:
            product         = "CNC"
            leverage        = 0.0
            margin_required = shares * ref
            capital_base    = capital
            print(f"[dhan]   --cnc-only — buying {shares} shares CNC (no leverage check)  "
                  f"·  margin required: ₹{margin_required:,.2f}")
        else:
            margin_info  = _margin_check(sym, shares, ref)
            has_leverage = margin_info is not None and margin_info["leverage"] >= 2

        if is_partial_fill or cnc_only:
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

        if available_balance is None:
            print(f"[dhan]   SKIP — could not verify available balance for {sym}.")
            n_skipped += 1
            continue
        if available_balance < margin_required:
            print(f"[dhan]   SKIP — insufficient balance (available ₹{available_balance:,.2f} "
                  f"< required ₹{margin_required:,.2f}).")
            n_skipped += 1
            continue
        available_balance -= margin_required
        print(f"[dhan]   balance confirmed: ₹{available_balance:,.2f} remaining after this order")

        # LIMIT price itself isn't resolved yet -- see the staging hold right
        # below Phase 1 for why (computing it here, before the 15:21:00 hold,
        # would anchor it to an LTP up to ~a minute stale by the time the
        # order actually fires).
        ready.append({
            "symbol": sym, "is_partial_fill": is_partial_fill, "existing_pos": existing_pos,
            "ref": ref, "shares": shares, "product": product, "leverage": leverage,
            "margin_required": margin_required, "capital_base": capital_base,
        })

    # ── Staging hold, part 1: block until 15:20:57 IST -- 3s ahead of fire
    # time, leaving just enough room for the LTP fetch + limit-price calc
    # below to finish before part 2's hold takes over for the final stretch.
    if ready:
        hold1_s = _seconds_until(*_LTP_FETCH_AT)
        if 0 < hold1_s <= 60:
            print(f"\n[dhan] Staged {len(ready)}/{len(ordered_symbols)} symbol(s) — "
                  f"holding {hold1_s:.1f}s until 15:20:57 to fetch LTP…")
            time.sleep(hold1_s)

    # LIMIT, not MARKET: Dhan/NSE apply their own price-protection band to a
    # MARKET order (confirmed 2026-08-17 -- every "MARKET" order this pipeline
    # places actually comes back from Dhan as orderType=LIMIT near the
    # submission price), and that band is tight enough that CAMLINFINE/UFLEX
    # got 0-filled on ordinary same-second price movement, no circuit lock
    # involved. Placing our own LIMIT 0.5% above live LTP gives explicit,
    # wider headroom to fill instead of relying on Dhan's undocumented band.
    #
    # Fetched fresh HERE, at 15:20:57, not earlier -- one batched call for
    # every ready symbol's LTP, not one call per symbol (see get_ltp_batch's
    # docstring: Quote APIs are 1 req/sec, N sequential single-symbol calls
    # would 429 from the 2nd symbol on).
    ltp_cache = get_ltp_batch([item["symbol"] for item in ready])
    for item in ready:
        sym, ref = item["symbol"], item["ref"]
        if sym in ltp_cache:
            entry_ltp = ltp_cache[sym]
        else:
            entry_ltp = ref
            print(f"[dhan]   {sym}: live LTP unavailable — using ref price ₹{ref:,.2f} "
                  f"as the limit-price anchor instead.")
        item["limit_price"] = _tick_round(sym, entry_ltp * 1.005)
        print(f"[dhan]   {sym}: LIMIT buy @ ₹{item['limit_price']:,.2f}  "
              f"(0.5% above LTP ₹{entry_ltp:,.2f})")

    # ── Staging hold, part 2: block until exactly 15:21:00 IST, same as
    # zerodha/trade.py's stage_entry_orders() -- so this cron's dry-run test
    # (scheduled at 15:20 to leave prep time) fires at the SAME wall-clock
    # moment as the real production entry, rather than ~a minute early. The
    # LTP fetch + limit-price calc above should only eat ~1s of the 2s left
    # after part 1's hold, so this is normally a short ~1s wait, not another
    # full 2s one. Only sleeps if the wait is short (called mid-window as
    # intended); a late or manual run skips the wait and fires immediately.
    if ready:
        hold2_s = _seconds_until(*_FIRE_AT)
        if 0 < hold2_s <= 60:
            print(f"[dhan] Priced {len(ready)} order(s) — holding {hold2_s:.1f}s "
                  f"until 15:21:00…")
            time.sleep(hold2_s)

    # ── Phase 2 (parallel): fire every resolved order at once. Each worker
    # places the order, polls its own fill, and (on a confirmed MTF-
    # ineligibility rejection) retries once as CNC -- exactly the same retry
    # this file has always done, just now running inside the worker instead
    # of inline in a sequential loop. No position-file access, no shared-
    # state mutation beyond the rate limiter (already thread-safe on its own)
    # -- available_balance is already fully consumed by Phase 1 above, so
    # there's nothing left for two workers to race over. A single symbol's
    # exception/rejection cannot block or delay any other symbol's task or
    # the eventual position-file write below.
    def _place_and_poll(item: dict) -> dict:
        sym, shares, product = item["symbol"], item["shares"], item["product"]
        ref, limit_price     = item["ref"], item["limit_price"]
        print(f"[dhan]   {_ts()} — placing {product} LIMIT BUY {shares}× {sym} @ ₹{limit_price:,.2f}"
              + ("  (DRY RUN)" if dry_run else ""))
        try:
            order_id = buy(sym, "NSE_EQ", shares,
                          order_type="LIMIT", price=limit_price, product=product, dry_run=dry_run)
        except Exception as exc:
            return {**item, "error": f"ORDER FAILED: {exc}", "log_status": "order_failed",
                    "order_id": ""}

        if dry_run:
            # Report the ACTUAL LIMIT price that would be submitted, not the
            # reference candle close (ref is only used for share sizing) --
            # this is the exact price a dry run should show as "what would
            # be placed."
            return {**item, "order_id": order_id, "fill_price": limit_price, "fill_qty": shares}

        fill_price, fill_qty, rejected, reject_reason = _poll_fill_strict(order_id)

        # The /margincalculator pre-check above can't detect MTF-ineligible
        # scrips -- confirmed live 2026-08-21: KLBRENG-B/WELSPLSOL both
        # reported 4-5x leverage there, yet the real order came back
        # REJECTED "Mtf Product Is Not Allowed For This Scrip". Only an
        # actual order attempt surfaces this, so retry once as CNC -- but
        # ONLY when the rejection is specifically MTF-ineligibility, not
        # any genuine rejection: a circuit-limit breach (e.g. SHANTIGEAR,
        # 2026-08-21, "Rate Not Within Ckt Limit 309.55 To 464.25") would
        # reject a CNC retry at the same price identically, so retrying
        # there just burns another order for nothing. Also not on a plain
        # unfilled/timeout (no seller matched yet -- CNC wouldn't fix that
        # either).
        if (fill_qty == 0 and product == "MTF" and rejected
                and "mtf product is not allow" in reject_reason.lower()):
            print(f"[dhan]   MTF-INELIGIBLE — retrying as CNC.")
            if manual_mode:
                cnc_capital_base = capital
                cnc_shares       = shares
            else:
                cnc_capital_base = capital / 2
                cnc_allocation   = compute_allocation(cnc_capital_base, n)
                cnc_shares       = compute_shares(cnc_allocation, ref)
            if cnc_shares == 0:
                print(f"[dhan]   SKIP — 0 shares at ₹{ref:,.2f} on "
                      f"₹{cnc_capital_base:,.0f}-based CNC retry.")
            else:
                try:
                    cnc_order_id = buy(sym, "NSE_EQ", cnc_shares, order_type="LIMIT",
                                        price=limit_price, product="CNC", dry_run=dry_run)
                    cnc_fill_price, cnc_fill_qty, _, _ = _poll_fill_strict(cnc_order_id)
                except Exception as exc:
                    print(f"[dhan]   CNC retry ORDER FAILED: {exc}")
                    cnc_fill_qty = 0
                if cnc_fill_qty > 0:
                    print(f"[dhan]   CNC retry filled ₹{cnc_fill_price:,.2f} × {cnc_fill_qty}")
                    return {**item, "order_id": cnc_order_id, "product": "CNC", "leverage": 0.0,
                            "shares": cnc_shares, "capital_base": cnc_capital_base,
                            "margin_required": cnc_shares * ref,
                            "fill_price": cnc_fill_price, "fill_qty": cnc_fill_qty}

        if fill_qty == 0:
            return {**item, "order_id": order_id,
                    "error": "NOT FILLED — order rejected or unconfirmed "
                             "(check broker manually, e.g. a circuit-locked stock).",
                    "log_status": "not_filled"}
        return {**item, "order_id": order_id, "fill_price": fill_price, "fill_qty": fill_qty}

    results: list[dict] = []
    if ready:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(_place_and_poll, item): item for item in ready}
            _wait_futures(futures.keys())   # do not proceed on partial completion
            for fut in futures:
                results.append(fut.result())

    # ── Phase 3 (sequential): one pass over every result, one save at the end.
    for res in results:
        sym, product = res["symbol"], res["product"]
        print(f"\n[dhan] {sym}  [{product}]")

        if "error" in res:
            print(f"[dhan]   {res['error']}")
            _append_log(trade_date, {
                "timestamp": _ts(), "symbol": sym, "quantity": res["shares"],
                "ref_price": round(res["ref"], 4), "fill_price": "",
                "leverage": res["leverage"], "margin_required": res["margin_required"],
                "order_id": res.get("order_id", ""), "status": res["log_status"],
                "product": product, "capital_base": res["capital_base"],
            })
            n_skipped += 1
            continue

        order_id, fill_price, fill_qty = res["order_id"], res["fill_price"], res["fill_qty"]
        status = "dry_run" if dry_run else "filled"
        print(f"[dhan]   filled ₹{fill_price:,.2f} × {fill_qty}"
              + ("  (DRY RUN)" if dry_run else ""))

        _append_log(trade_date, {
            "timestamp": _ts(), "symbol": sym, "quantity": fill_qty,
            "ref_price": round(res["ref"], 4), "fill_price": round(fill_price, 4),
            "leverage": res["leverage"], "margin_required": res["margin_required"],
            "order_id": order_id, "status": status, "product": product,
            "capital_base": res["capital_base"],
        })

        if res["is_partial_fill"]:
            # Fold the completion fill into the SAME row leg 1 already wrote
            # (weighted-average price, summed quantity) rather than a second
            # row, so exit logic (place_targets_915/check_exit_925/
            # force_exit_1159) still sees exactly one row per position --
            # they already only match status in ("open",
            # "partial_exit_925_nodata"), so flipping this to "open" is all
            # that's needed for them to pick it up with zero changes.
            existing_pos = res["existing_pos"]
            total_qty   = existing_pos["actual_fill_quantity"] + fill_qty
            avg_price   = ((existing_pos["actual_fill_price"] * existing_pos["actual_fill_quantity"]
                           + fill_price * fill_qty) / total_qty)
            fill_amount = fill_price * fill_qty
            existing_pos.update({
                "status":                    "open",
                "entry_status":              "filled",
                "case_a_leg":                "leg2_filled",
                "filled_amount":             round(existing_pos["filled_amount"] + fill_amount, 2),
                "actual_fill_price":         round(avg_price, 4),
                "actual_fill_quantity":      total_qty,
                "completion_order_id":       order_id,
                "completion_fill_price":     round(fill_price, 4),
                "completion_fill_quantity":  fill_qty,
                "completion_timestamp":      _ts(),
            })
        else:
            positions.append({
                "broker":               _BROKER,
                "symbol":               sym,
                "entry_date":           trade_date.isoformat(),
                "reference_price":      round(res["ref"], 4),
                "shares_intended":      res["shares"],
                "actual_fill_price":    round(fill_price, 4),
                "actual_fill_quantity": fill_qty,
                "entry_order_id":       order_id,
                "status":               "open",
                "entry_timestamp":      _ts(),
                "product":              product,
            })
        try:
            notify.send_entry(broker=_BROKER, symbol=f"{sym} [{product}]", fill_price=fill_price,
                              shares=fill_qty, order_id=order_id, dry_run=dry_run)
        except Exception as exc:
            print(f"  [notify] entry failed: {exc}", file=sys.stderr)

        n_entered += 1

    if not dry_run and results:
        _save_long_pos(positions)   # single write for the whole batch

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
    positions = _load_long_pos()
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
            order_id = _sell_margin_safe(sym, "NSE_EQ", qty, target_price, product, dry_run)

            pos["target_order_id"] = order_id
            pos["target_price"]    = target_price
            if not dry_run:
                _save_long_pos(positions)
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
    positions = _load_long_pos()
    open_ps   = _open_pos(positions)

    print(f"\n{'='*60}")
    print(f"[dhan] Exit check 9:25am{'  DRY RUN' if dry_run else ''}")
    print(f"[dhan] {len(open_ps)} open position(s)")
    print(f"{'='*60}")

    if not open_ps:
        print("[dhan] No open positions — nothing to check.")
        _sync_pnl_workbook()
        return

    # One batched call for every open position's LTP, not one call per symbol
    # in the loop below -- Dhan's Quote APIs are 1 req/sec, so N sequential
    # single-symbol calls reliably 429 on the 2nd+ symbol (see get_ltp's
    # docstring). A symbol missing from this dict is treated identically to
    # get_ltp() raising -- falls into the existing no-data fallback branch.
    ltp_cache = get_ltp_batch([p["symbol"] for p in open_ps])

    dirty = False   # any in-memory mutation this run -- decides whether to save at the end

    # ── Phase 1 (sequential): target-hit checks resolve immediately in place
    # (no new order -- just recording an already-completed fill), and the
    # no-data/pnl-gate decision for everything else. Builds a list of
    # per-symbol exit tasks to run in parallel below; positions that hit
    # their target or are held for 11:59 never enter that list.
    tasks: list[dict] = []
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
                dirty = True
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
            half   = math.floor(qty / 2)
            remain = qty - half
            if half == 0:
                print(f"[dhan]   qty too small to halve — holding until 11:59am.")
                continue
            # LTP is unavailable in this branch by definition (that's why we're
            # here) -- anchor the LIMIT price on the recorded fill price instead,
            # same fallback pattern as the entry order's own LTP-unavailable case.
            fallback_limit = _tick_round(sym, fill_price * 0.995)
            tasks.append({"kind": "fallback", "pos": pos, "sym": sym, "product": product,
                         "fill_price": fill_price, "check_qty": qty, "qty": half, "remain": remain,
                         "sell_limit": fallback_limit,
                         "target_oid": target_oid, "target_price": pos.get("target_price")})
            continue

        if pnl_live > 0:
            print(f"[dhan]   P&L positive — queuing sell of {qty}")
            sell_limit = _tick_round(sym, ltp * 0.995)
            tasks.append({"kind": "full", "pos": pos, "sym": sym, "product": product,
                         "fill_price": fill_price, "check_qty": qty, "qty": qty,
                         "sell_limit": sell_limit, "target_oid": target_oid})
        else:
            print(f"[dhan]   P&L ≤ 0 (₹{pnl_live:+,.2f}) — holding for 11:59am forced exit.")

    # Phase 1's target-hit updates (if any) are complete now -- persist them
    # before any chunk work begins below, independent of whether `tasks` ends
    # up empty (e.g. every open position hit its target -- there would be no
    # chunk to piggyback a save on otherwise).
    if not dry_run and dirty:
        _save_long_pos(positions)

    # Batched circuit-limit pre-fetch (Part 1) for every task candidate, ONE
    # call covering the whole stage-run, fetched upfront before any chunk
    # runs -- not every task will actually reach the mirrored-short step
    # below, but we don't know which will until each chunk's exit fires, so
    # every candidate is covered now rather than guessing.
    circuit_cache: dict[str, float] = {}
    short_balance = _BalanceTracker(None)
    if tasks:
        circuit_cache = _fetch_upper_circuit_batch([t["sym"] for t in tasks])
        # One-time INTRADAY-short balance fetch for this whole stage-run,
        # tracked thread-safely across every chunk below (Part 3) -- the
        # concurrent equivalent of the old sequential available_balance
        # threading, now safe for multiple _open_short_core() calls to share
        # at once (see _BalanceTracker).
        short_balance = _BalanceTracker(_available_balance())

    # ── Wave 1 (batched-concurrent): cancel every task's stale target (one
    # concurrent burst per batch), THEN -- only once that batch's cancels are
    # all confirmed -- sell every task in that same batch (a second,
    # separate concurrent burst; includes fill polling and, for a no-data
    # fallback task, its fresh-target-remainder placement). See
    # _run_exit_wave1's docstring and the module note on the wave-based
    # redesign above MAX_ORDER_CALLS_PER_SECOND's definition -- every
    # position in a batch performs the SAME action before any position moves
    # to the next, so a concurrent Order-API burst never mixes call types
    # from different positions. No position-file access inside either
    # worker; the sequential apply step below does every field update and
    # this wave's one write.
    def _cancel_fn(task: dict) -> None:
        sym, target_oid = task["sym"], task["target_oid"]
        if target_oid:
            try:
                _dhan_cancel_order(target_oid)
            except Exception as exc:
                print(f"[dhan]   target cancel failed for {sym} (may already be filled/cancelled): {exc}")
        return None

    def _sell_fn(task: dict) -> dict:
        try:
            sym, product, fill_price, qty = task["sym"], task["product"], task["fill_price"], task["qty"]
            target_oid = task["target_oid"]

            exch = "NSE_EQ"
            if not dry_run:
                bqty, exch = _broker_qty(sym, product)
                if bqty != task["check_qty"]:
                    return {**task, "error": f"!! MISMATCH — local={task['check_qty']} broker={bqty}. "
                                              f"Skipping {sym} — manual review required."}

            try:
                oid = _sell_margin_safe(sym, exch, qty, task["sell_limit"], product, dry_run)
            except Exception as exc:
                kind_label = "fallback sell" if task["kind"] == "fallback" else "sell"
                return {**task, "error": f"{kind_label} failed: {exc}"}

            ep, eq = (fill_price, qty) if dry_run else _poll_fill_safe(oid, fill_price, qty)
            if eq == 0:
                err = ("NOT FILLED — fallback sell rejected, position left open." if task["kind"] == "fallback"
                       else "NOT FILLED — sell rejected, position left open.")
                return {**task, "error": err}

            result = {**task, "oid": oid, "ep": ep, "eq": eq, "exch": exch}

            # No-data-fallback remainder: fresh target at the SAME price,
            # placed here (still the sell sub-step -- a fresh SELL-side
            # target order is the same call TYPE as the exit sell itself,
            # not a different one this wave split needs to separate out).
            if task["kind"] == "fallback" and task["remain"] > 0 and target_oid and task.get("target_price"):
                try:
                    result["new_target_oid"] = _sell_margin_safe(sym, exch, task["remain"],
                                                                  task["target_price"], product, dry_run)
                except Exception as exc:
                    result["fresh_target_error"] = str(exc)

            return result
        except Exception as exc:
            return {**task, "error": f"!! task crashed unexpectedly: {exc}"}

    wave1_results: list[dict] = []
    for batch_results in _run_exit_wave1(tasks, _cancel_fn, _sell_fn):
        wave1_results.extend(batch_results)

    # ── Sequential apply for Wave 1 (once per whole run, not per batch): every
    # field update + notify call, then exactly ONE _save_long_pos() -- the
    # first of check_exit_925's 3 total write-points this run (Wave 1 sold
    # status, Wave 2 short-open rows, Wave 3 target/SL order IDs).
    wave1_dirty = False
    sold_tasks: list[dict] = []   # feeds Wave 2 -- everything actually sold in Wave 1
    for res in wave1_results:
        sym, product, pos, fill_price = res["sym"], res["product"], res["pos"], res["fill_price"]
        print(f"\n[dhan] {sym}  [{product}]")

        if "error" in res:
            print(f"[dhan]   {res['error']}")
            continue

        ep, eq = res["ep"], res["eq"]
        wave1_dirty = True

        if res["kind"] == "fallback":
            remain = res["remain"]
            pos.update({
                "status":             "partial_exit_925_nodata",
                "shares_exited_925":  eq,
                "shares_remaining":   remain,
                "exit_price_925":     round(ep, 4),
                "exit_order_id_925":  res["oid"],
                "exit_timestamp_925": _ts(),
            })
            print(f"[dhan]   NO-DATA FALLBACK — sold {eq}  ₹{ep:,.2f}")
            if "new_target_oid" in res:
                pos["target_order_id"] = res["new_target_oid"]
                print(f"[dhan]   fresh target placed for remaining {remain} "
                      f"@ ₹{res['target_price']:,.2f} — order {res['new_target_oid']}")
            elif "fresh_target_error" in res:
                print(f"[dhan]   !! fresh target placement failed for {sym}: {res['fresh_target_error']}")
            try:
                notify.send_exit_925_nodata(broker=_BROKER, symbol=f"{sym} [{product}]",
                                            shares_exited=eq, shares_remaining=remain,
                                            exit_price=ep, dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] exit_925_nodata failed: {exc}", file=sys.stderr)
        else:
            pnl     = (ep - fill_price) * eq
            ret_act = (ep - fill_price) / fill_price * 100 if fill_price else 0
            pos.update({
                "status":              "exited_925",
                "exit_price_925":      round(ep, 4),
                "exit_order_id_925":   res["oid"],
                "exit_timestamp_925":  _ts(),
                "realized_return_pct": round(ret_act, 4),
                "realized_pnl":        round(pnl, 2),
            })
            print(f"[dhan]   exited ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
            try:
                notify.send_exit_925(broker=_BROKER, symbol=f"{sym} [{product}]", exit_price=ep,
                                     return_pct=ret_act, pnl=pnl, dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] exit_925 failed: {exc}", file=sys.stderr)

        sold_tasks.append(res)

    if not dry_run and wave1_dirty:
        _save_long_pos(positions)

    # ── Wave 2 (batched-concurrent): open the mirrored short for everything
    # sold in Wave 1 -- one Order-API call TYPE (short-open sell) per burst,
    # nothing else. Shares the same _BalanceTracker across every batch this
    # wave, same as before the wave split (Part 3).
    def _short_place_fn(res: dict) -> dict | None:
        return _open_short_place(res["sym"], res["eq"], "925", dry_run,
                                 ltp=ltp_cache.get(res["sym"]), balance=short_balance)

    wave2_rows: list[dict] = []
    for batch_results in _run_in_chunks(sold_tasks, _short_place_fn):
        wave2_rows.extend([r for r in batch_results if r is not None])

    short_positions = None
    if wave2_rows:
        short_positions = _load_short_pos()
        short_positions.extend(wave2_rows)
        if not dry_run:
            _save_short_pos(short_positions)
        print(f"\n[dhan] Opened {len(wave2_rows)} mirrored short(s).")

    # ── Settle buffer: one pause between the short-open wave and the
    # target/stop-loss wave, giving the exchange a moment to register the
    # new short before orders referencing it are placed. Only paid once per
    # run (not per batch -- BATCH_SLEEP_SECONDS already covers that), and
    # only if anything actually opened.
    if wave2_rows:
        time.sleep(SHORT_SETTLE_BUFFER_SECONDS)

    # ── Wave 3 (batched-concurrent): place the cover-target + stop-loss for
    # every short Wave 2 opened -- one Order-API call TYPE (target/SL buys)
    # per burst. Each worker mutates and returns the SAME row object Wave 2
    # already appended to short_positions, so the single save below already
    # reflects every batch's updates without needing to re-load the file.
    def _protect_fn(row: dict) -> dict:
        return _open_short_protect(row, dry_run, circuit=circuit_cache.get(row["symbol"]))

    for _ in _run_in_chunks(wave2_rows, _protect_fn):
        pass   # each worker mutates its row in place; nothing further to apply here

    if wave2_rows and not dry_run:
        _save_short_pos(short_positions)

    print(f"\n[dhan] Exit check 9:25am complete.")
    _sync_pnl_workbook()


def force_exit_1159(dry_run: bool = False) -> None:
    positions = _load_long_pos()
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
    dirty   = False

    # One batched call for every still-open position's LTP -- see the matching
    # comment in check_exit_925.
    ltp_cache = get_ltp_batch([p["symbol"] for p in open_ps])

    # ── Phase 1 (sequential): target-hit checks resolve immediately in place;
    # everything else queues an unconditional force-sell task (no P&L gate at
    # this stage -- unlike 9:25, every remaining open position sells here).
    tasks: list[dict] = []
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
                dirty = True
                print(f"[dhan]   TARGET HIT — closed ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
                try:
                    notify.send_target_hit(broker=_BROKER, symbol=f"{sym} [{product}]", stage="1159",
                                           exit_price=ep, return_pct=ret, pnl=pnl, dry_run=dry_run)
                except Exception as exc:
                    print(f"  [notify] target_hit failed: {exc}", file=sys.stderr)
                # No mirrored short here -- see the matching comment in check_exit_925.
                n_force += 1
                continue
            # Not traded -- the actual cancel moves into Wave 1's concurrent
            # cancel sub-step below (batched with every other task's cancel
            # in the same burst), not fired here sequentially -- see the
            # module note on the wave-based redesign above
            # MAX_ORDER_CALLS_PER_SECOND's definition.

        if sym in ltp_cache:
            exit_ltp = ltp_cache[sym]
        else:
            exit_ltp = fill_price
            print(f"[dhan]   live LTP unavailable — using fill price ₹{fill_price:,.2f} "
                  f"as the limit-price anchor instead.")
        sell_limit = _tick_round(sym, exit_ltp * 0.995)

        tasks.append({"pos": pos, "sym": sym, "product": product, "fill_price": fill_price,
                      "qty": qty, "is_partial": is_partial, "sell_limit": sell_limit,
                      "target_oid": target_oid})

    # Phase 1's target-hit updates (if any) are complete now -- persist them
    # before any chunk work begins, independent of whether `tasks` ends up
    # empty (see the matching note in check_exit_925).
    if not dry_run and dirty:
        _save_long_pos(positions)

    # Batched circuit-limit pre-fetch (Part 1) + one-time INTRADAY-short
    # balance fetch (Part 3), same reasoning as check_exit_925.
    circuit_cache: dict[str, float] = {}
    short_balance = _BalanceTracker(None)
    if tasks:
        circuit_cache = _fetch_upper_circuit_batch([t["sym"] for t in tasks])
        short_balance = _BalanceTracker(_available_balance())

    # ── Wave 1 (batched-concurrent): cancel every task's stale target (one
    # concurrent burst per batch), THEN -- only once that batch's cancels are
    # all confirmed -- force-sell every task in that same batch (a second,
    # separate concurrent burst; includes fill polling). No P&L gate here
    # (unlike 9:25, no "kind"/fallback distinction -- every task is a plain
    # unconditional sell). See check_exit_925's matching Wave 1 for the full
    # reasoning; identical shape, reused via _run_exit_wave1.
    def _cancel_fn(task: dict) -> None:
        sym, target_oid = task["sym"], task["target_oid"]
        if target_oid:
            try:
                _dhan_cancel_order(target_oid)
            except Exception as exc:
                print(f"[dhan]   target cancel failed for {sym} (may already be filled/cancelled): {exc}")
        return None

    def _sell_fn(task: dict) -> dict:
        try:
            sym, product, fill_price, qty = task["sym"], task["product"], task["fill_price"], task["qty"]

            exch = "NSE_EQ"
            if not dry_run:
                bqty, exch = _broker_qty(sym, product)
                if bqty != qty:
                    return {**task, "error": f"!! MISMATCH — local={qty} broker={bqty}. "
                                              f"Skipping {sym} — manual review required."}

            try:
                oid = _sell_margin_safe(sym, exch, qty, task["sell_limit"], product, dry_run)
            except Exception as exc:
                return {**task, "error": f"sell failed: {exc}"}

            ep, eq = (fill_price, qty) if dry_run else _poll_fill_safe(oid, fill_price, qty)
            if eq == 0:
                return {**task, "error": f"!! NOT FILLED — force-exit sell rejected for {sym}. "
                                          f"Position left as-is — manual review required."}

            return {**task, "oid": oid, "ep": ep, "eq": eq}
        except Exception as exc:
            return {**task, "error": f"!! task crashed unexpectedly: {exc}"}

    wave1_results: list[dict] = []
    for batch_results in _run_exit_wave1(tasks, _cancel_fn, _sell_fn):
        wave1_results.extend(batch_results)

    # ── Sequential apply for Wave 1 (once per whole run): every field
    # update + notify call, then exactly ONE _save_long_pos().
    wave1_dirty = False
    sold_tasks: list[dict] = []   # feeds Wave 2 -- everything actually force-sold in Wave 1
    for res in wave1_results:
        sym, product, pos = res["sym"], res["product"], res["pos"]
        fill_price, is_partial = res["fill_price"], res["is_partial"]
        print(f"\n[dhan] {sym}  [{product}]")

        if "error" in res:
            print(f"[dhan]   {res['error']}")
            continue

        ep, eq = res["ep"], res["eq"]
        wave1_dirty = True

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
            "exit_order_id_1159":   res["oid"],
            "exit_timestamp_1159":  _ts(),
            "realized_return_pct":  round(ret, 4),
            "realized_pnl":         round(pnl, 2),
        })
        print(f"[dhan]   force-exited ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
        try:
            notify.send_force_exit_1159(broker=_BROKER, symbol=f"{sym} [{product}]", exit_price=ep,
                                        return_pct=ret, pnl=pnl, dry_run=dry_run)
        except Exception as exc:
            print(f"  [notify] force_exit_1159 failed: {exc}", file=sys.stderr)
        n_force += 1

        sold_tasks.append(res)

    if not dry_run and wave1_dirty:
        _save_long_pos(positions)

    # ── Wave 2 (batched-concurrent): open the mirrored short for everything
    # force-sold in Wave 1 -- see check_exit_925's matching Wave 2.
    def _short_place_fn(res: dict) -> dict | None:
        return _open_short_place(res["sym"], res["eq"], "1159", dry_run,
                                 ltp=ltp_cache.get(res["sym"]), balance=short_balance)

    wave2_rows: list[dict] = []
    for batch_results in _run_in_chunks(sold_tasks, _short_place_fn):
        wave2_rows.extend([r for r in batch_results if r is not None])

    short_positions = None
    if wave2_rows:
        short_positions = _load_short_pos()
        short_positions.extend(wave2_rows)
        if not dry_run:
            _save_short_pos(short_positions)
        print(f"\n[dhan] Opened {len(wave2_rows)} mirrored short(s).")

    # ── Settle buffer -- see check_exit_925's matching comment.
    if wave2_rows:
        time.sleep(SHORT_SETTLE_BUFFER_SECONDS)

    # ── Wave 3 (batched-concurrent): place cover-target + stop-loss for
    # every short Wave 2 opened -- see check_exit_925's matching Wave 3.
    def _protect_fn(row: dict) -> dict:
        return _open_short_protect(row, dry_run, circuit=circuit_cache.get(row["symbol"]))

    for _ in _run_in_chunks(wave2_rows, _protect_fn):
        pass

    if wave2_rows and not dry_run:
        _save_short_pos(short_positions)

    _daily_summary(positions, n_force, dry_run)
    print(f"\n[dhan] Force exit 11:59am complete. Force-exited: {n_force}.")
    _sync_pnl_workbook()


def _daily_summary(positions: list, n_force: int, dry_run: bool) -> None:
    """positions must already be from _load_long_pos() -- see _open_pos()."""
    today    = date.today().isoformat()
    today_ps = [p for p in positions
                if p.get("broker") == _BROKER and p.get("entry_date") == today]
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
    positions   = _load_short_pos()
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

    # ── Pre-check (single call): the whole run's cover-target/stop-loss OCO
    # status now comes from ONE GET /orders (Order Book) call, not one
    # GET /orders/{id} per order per position -- every order this account
    # placed today, looked up locally by orderId from here on. If this one
    # call itself fails, nothing about ANY position's status can be trusted,
    # so every open short is skipped for manual review rather than guessed
    # (same fail-closed reasoning as _BalanceTracker's None-balance case).
    try:
        order_by_id = {o.get("orderId"): o for o in _dhan_get_orders()}
        orders_ok   = True
    except Exception as exc:
        print(f"[dhan]   !! Order Book fetch failed: {exc} -- cannot verify any cover/stop "
              f"status this run. Skipping all {len(open_shorts)} short(s) — manual review required.")
        order_by_id = {}
        orders_ok   = False

    # Classify every position from that ONE pre-fetched snapshot -- no
    # per-order API calls anywhere in this loop.
    classified: list[dict] = []
    for pos in open_shorts:
        sym         = pos["symbol"]
        entry_price = float(pos["entry_price"] or 0)
        qty         = int(pos["quantity"])
        cover_oid   = pos.get("cover_target_order_id")
        stop_oid    = pos.get("stop_order_id")

        if not orders_ok:
            try:
                notify.send_square_off_manual_review(broker=_BROKER, symbol=sym,
                    error_msg="Order Book fetch failed this run -- status unverifiable.",
                    dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] square_off_manual_review failed: {exc}", file=sys.stderr)
            continue

        cover_status = order_by_id.get(cover_oid) if cover_oid else None
        stop_status  = order_by_id.get(stop_oid) if stop_oid else None

        # An order_id the position record says it has, but that's genuinely
        # missing from today's Order Book, is NOT the same thing as "not
        # filled yet" -- treating it as neither_filled could force-cover a
        # short whose cover/stop already closed it, buying back twice. Skip
        # for manual review instead of guessing.
        if cover_oid and cover_status is None:
            msg = f"cover_target_order_id {cover_oid} not found in today's Order Book."
            print(f"[dhan]   !! {sym}: {msg} Skipping — manual review required.")
            try:
                notify.send_square_off_manual_review(broker=_BROKER, symbol=sym, error_msg=msg, dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] square_off_manual_review failed: {exc}", file=sys.stderr)
            continue
        if stop_oid and stop_status is None:
            msg = f"stop_order_id {stop_oid} not found in today's Order Book."
            print(f"[dhan]   !! {sym}: {msg} Skipping — manual review required.")
            try:
                notify.send_square_off_manual_review(broker=_BROKER, symbol=sym, error_msg=msg, dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] square_off_manual_review failed: {exc}", file=sys.stderr)
            continue

        cover_traded = bool(cover_status) and (cover_status.get("orderStatus") or "").upper() == "TRADED"
        stop_traded  = bool(stop_status)  and (stop_status.get("orderStatus")  or "").upper() == "TRADED"

        if cover_traded and stop_traded:
            msg = ("BOTH cover target AND stop-loss show TRADED -- OCO race, "
                   "cannot determine which actually executed first.")
            print(f"[dhan]   !! {sym}: {msg} Skipping — manual review required.")
            try:
                notify.send_square_off_manual_review(broker=_BROKER, symbol=sym, error_msg=msg, dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] square_off_manual_review failed: {exc}", file=sys.stderr)
            continue

        kind = "cover_filled" if cover_traded else "stop_filled" if stop_traded else "neither_filled"
        classified.append({"pos": pos, "sym": sym, "kind": kind, "entry_price": entry_price,
                           "qty": qty, "cover_oid": cover_oid, "stop_oid": stop_oid,
                           "cover_status": cover_status, "stop_status": stop_status})

    # ── Batches of MAX_ORDER_CALLS_PER_SECOND: cancel sub-step, then a
    # force-cover sub-step (only for whichever of THIS batch's positions were
    # neither_filled -- cover_filled/stop_filled resolve from the pre-fetched
    # status alone, no further order call needed), save this batch's results
    # before the next batch's cancels begin. Per-batch saves (not one save
    # for the whole run, unlike check_exit_925/force_exit_1159's waves) --
    # if this run is interrupted, some batches end up fully resolved
    # (cancelled + closed) and others fully untouched, never a half-cancelled
    # position in between.
    def _cancel_fn(item: dict) -> None:
        sym, kind = item["sym"], item["kind"]
        if kind == "cover_filled" and item["stop_oid"]:
            try:
                _dhan_cancel_order(item["stop_oid"])
            except Exception as exc:
                print(f"[dhan]   stop-loss cancel failed for {sym} (may already be filled/cancelled): {exc}")
        elif kind == "stop_filled" and item["cover_oid"]:
            try:
                _dhan_cancel_order(item["cover_oid"])
            except Exception as exc:
                print(f"[dhan]   cover target cancel failed for {sym} (may already be filled/cancelled): {exc}")
        elif kind == "neither_filled":
            if item["cover_oid"]:
                try:
                    _dhan_cancel_order(item["cover_oid"])
                except Exception as exc:
                    print(f"[dhan]   cover target cancel failed for {sym} (may already be filled/cancelled): {exc}")
            if item["stop_oid"]:
                try:
                    _dhan_cancel_order(item["stop_oid"])
                except Exception as exc:
                    print(f"[dhan]   stop-loss cancel failed for {sym} (may already be filled/cancelled): {exc}")
        return None

    def _force_cover_fn(item: dict) -> dict:
        sym, entry_price, qty = item["sym"], item["entry_price"], item["qty"]
        try:
            print(f"\n[dhan] {sym}  [SHORT INTRADAY]  entry=₹{entry_price:,.2f}  qty={qty}  "
                  f"(from {item['pos'].get('source_exit_stage', '?')} exit)")
            if not dry_run:
                bqty = _broker_short_qty(sym)
                if bqty != qty:
                    return {"pos": item["pos"], "sym": sym,
                            "error": f"!! MISMATCH — local={qty} broker_short={bqty}. "
                                     f"Skipping {sym} — manual review required."}
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
                return {"pos": item["pos"], "sym": sym, "error": f"cover buy failed: {exc}"}

            ep, eq = (entry_price, qty) if dry_run else _poll_fill_safe(oid, entry_price, qty)
            if eq == 0:
                return {"pos": item["pos"], "sym": sym,
                        "error": f"!! NOT FILLED — cover buy rejected for {sym}. "
                                 f"Position left as-is — manual review required."}

            return {"pos": item["pos"], "sym": sym, "kind": "force_cover",
                    "ep": ep, "eq": eq, "oid": oid, "entry_price": entry_price}
        except Exception as exc:
            return {"pos": item["pos"], "sym": sym, "error": f"!! task crashed unexpectedly: {exc}"}

    chunk_size = MAX_ORDER_CALLS_PER_SECOND
    for i in range(0, len(classified), chunk_size):
        batch = classified[i : i + chunk_size]

        _run_batch(batch, _cancel_fn)   # sub-step 1: cancels, non-fatal, printed inline

        # sub-step 2: force-cover -- ONLY the neither_filled items in this
        # batch make an actual Order-API call; cover_filled/stop_filled
        # resolve immediately below from the pre-fetched status, no thread
        # needed for those.
        neither = [item for item in batch if item["kind"] == "neither_filled"]
        force_cover_by_sym = {r["sym"]: r for r in _run_batch(neither, _force_cover_fn)}

        batch_results: list[dict] = []
        for item in batch:
            sym, kind = item["sym"], item["kind"]
            if kind == "cover_filled":
                cs = item["cover_status"]
                ep = float(cs.get("averageTradedPrice") or 0)
                eq = int(cs.get("filledQty") or 0) or item["qty"]
                batch_results.append({"pos": item["pos"], "sym": sym, "kind": "cover_target_hit",
                                      "ep": ep, "eq": eq, "oid": item["cover_oid"],
                                      "entry_price": item["entry_price"]})
            elif kind == "stop_filled":
                ss = item["stop_status"]
                ep = float(ss.get("averageTradedPrice") or 0)
                eq = int(ss.get("filledQty") or 0) or item["qty"]
                batch_results.append({"pos": item["pos"], "sym": sym, "kind": "stop_loss_hit",
                                      "ep": ep, "eq": eq, "oid": item["stop_oid"],
                                      "entry_price": item["entry_price"]})
            else:
                batch_results.append(force_cover_by_sym[sym])

        # ── Sequential apply for THIS batch: field updates + notify, then
        # this batch's ONE save -- never begin the next batch's cancels
        # until this save has completed (Part 2's "no half-cancelled state
        # on interruption" guarantee).
        chunk_dirty = False
        for res in batch_results:
            sym, pos = res["sym"], res["pos"]

            if "error" in res:
                print(f"[dhan]   {res['error']}")
                continue

            ep, eq, oid, kind = res["ep"], res["eq"], res["oid"], res["kind"]
            entry_price = res["entry_price"]
            pnl = (entry_price - ep) * eq   # short economics: profit when price falls
            ret = (entry_price - ep) / entry_price * 100 if entry_price else 0
            pos.update({
                "status":              "short_closed",
                "exit_price_239":      round(ep, 4),
                "exit_order_id_239":   oid,
                "exit_timestamp_239":  _ts(),
                "realized_return_pct": round(ret, 4),
                "realized_pnl":        round(pnl, 2),
            })
            chunk_dirty = True
            n_closed += 1

            if kind == "cover_target_hit":
                print(f"[dhan]   COVER TARGET HIT — squared off ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
                try:
                    notify.send_cover_target_hit(broker=_BROKER, symbol=f"{sym} [SHORT INTRADAY]",
                                                 entry_price=entry_price, exit_price=ep,
                                                 return_pct=ret, pnl=pnl, dry_run=dry_run)
                except Exception as exc:
                    print(f"  [notify] cover_target_hit failed: {exc}", file=sys.stderr)
            elif kind == "stop_loss_hit":
                print(f"[dhan]   STOP-LOSS HIT — squared off ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
                try:
                    notify.send_short_stoploss_hit(broker=_BROKER, symbol=f"{sym} [SHORT INTRADAY]",
                                                   entry_price=entry_price, exit_price=ep,
                                                   return_pct=ret, pnl=pnl, dry_run=dry_run)
                except Exception as exc:
                    print(f"  [notify] short_stoploss_hit failed: {exc}", file=sys.stderr)
            else:  # force_cover
                print(f"[dhan]   SQUARED OFF ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
                try:
                    notify.send_square_off_239(broker=_BROKER, symbol=f"{sym} [SHORT INTRADAY]",
                                               entry_price=entry_price, exit_price=ep,
                                               return_pct=ret, pnl=pnl, dry_run=dry_run)
                except Exception as exc:
                    print(f"  [notify] square_off_239 failed: {exc}", file=sys.stderr)

        if not dry_run and chunk_dirty:
            _save_short_pos(positions)

        if i + chunk_size < len(classified):
            time.sleep(BATCH_SLEEP_SECONDS)

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
