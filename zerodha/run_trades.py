"""
zerodha/run_trades.py — the single live trading pipeline (Zerodha Kite)
========================================================================
Supersedes the old CNC-only run_trades.py + the standalone run_trades_mtf.py
split -- one leverage-branch entry path instead of two separate scripts (see
the zerodha/ 4-file redesign). Every entry checks live per-symbol MTF
leverage via /margins/orders first: product="MTF" when leverage is actually
>=2x, falls back to a resized product="CNC" buy (half the capital base)
otherwise. No more separate CNC-only entry point -- this IS the entry point.

Entry price : Zerodha's OWN 15:20 1-minute candle close, via trade.py's
              stage_entry_orders() (Kite historical-candle endpoint, LTP
              fallback) -- no external broker dependency any more (the old
              Upstox reference-price fetch is gone).
Exit check  : Our own recorded entry fill price vs. current LTP from Kite's
              /quote/ltp (Stage 2) -- pnl = (ltp - fill_price) * qty, computed
              directly rather than trusting Kite's positions/holdings pnl
              field, which can go stale same-day.
Orders      : Zerodha Kite API, all through trade.py's rate-limited
              place_order()/buy()/sell()/cancel_order().
Positions   : results/positions_zerodha_long.json (all long positions, MTF
              or CNC, tagged by a "product" field) + results/
              positions_zerodha_short.json (mirrored shorts opened from a
              925/1159 long exit) -- kept in SEPARATE files specifically so a
              same-day mirrored short can never block a fresh same-day long
              re-entry on that symbol (confirmed live on the Dhan side of
              this pipeline, 2026-08-20 -- BAJAJHIND/ZAGGLE both had fresh
              long signals skipped purely because a same-day mirrored short
              carried today's entry_date in a combined file).

Entry priority: Step 1/2/3
---------------------------
live_monitor.py's UC-based staged-entry ladder (Case A/B, --enable-uc-
staged-entry) may already have partially or fully filled some of today's
symbols before this ever runs, or even symbols outside today's trade_list.csv
entirely (the ladder watches live_monitor's broader qualified universe, not
this strict shortlist). This function reconciles with whatever the ladder
already did before running its own batch logic:
  Step 1 (highest priority): entry_status=="partially_filled" -- a Case A
    leg 1 that fired but never got its leg 2 retrace before the ladder's
    window closed. Completed FIRST, buying only the remaining balance of
    that symbol's own capital_base (the snapshot live_monitor.py wrote at
    leg 1, not a fresh batch allocation), reusing leg 1's exact product
    (MTF/CNC) rather than re-running the leverage decision -- avoids ending
    up with a mixed-product position that a single `product` field per row
    can't represent cleanly. The fill is folded into leg 1's existing row
    (weighted-average price, summed quantity), not written as a second row.
  Step 2: any other row already filled today (via the ladder or an ordinary
    entry) -- skipped, same dedup as always.
  Step 3: no row at all -- the untouched, full-allocation normal entry.

Usage:
    python zerodha/run_trades.py --entry          [--capital AMOUNT] [--dry-run] [--date YYYY-MM-DD]
    python zerodha/run_trades.py --place-targets  [--dry-run]
    python zerodha/run_trades.py --exit-925       [--dry-run]
    python zerodha/run_trades.py --exit-1159      [--dry-run]
    python zerodha/run_trades.py --square-off-239 [--dry-run]
    python zerodha/run_trades.py --entry --symbol RELIANCE --capital 5000 --dry-run   (manual single-stock)
"""

import argparse
import csv
import json
import math
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait as _wait_futures
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

from zerodha.auth import BASE_URL as _KITE_BASE, get_session as _kite_session
from zerodha.trade import (buy, sell, place_order, order_status as _kite_order_status,
                           cancel_order as _kite_cancel_order, get_orders as _kite_get_orders,
                           get_reference_price, get_ltp, stage_entry_orders)
from common.calc_utils import compute_allocation, compute_shares
import notify

# ── Config ─────────────────────────────────────────────────────────────────────

_IST          = ZoneInfo("Asia/Kolkata")
_BROKER       = "zerodha"
_RESULTS_DIR  = _ROOT / "results"
_POS_FILE_LONG  = _RESULTS_DIR / "positions_zerodha_long.json"
_POS_FILE_SHORT = _RESULTS_DIR / "positions_zerodha_short.json"
_MTF_LOG_DIR  = _RESULTS_DIR / "trades"
TOTAL_CAPITAL = 500_000

# ── Wave-based batching (check_exit_925 / force_exit_1159 / square_off_239) ─────
# Mirrors the dhan/run_trades.py redesign: every position in a batch performs
# the SAME Order-API call type before any position in that batch moves to the
# next one (cancel-all, then sell-all; short-open-all; protect-all), so a
# concurrent burst can never mix call types. Wave 1 cancels BEFORE selling
# (reverted 2026-08-28 -- a "sell first" version was tried and broke live on
# the Dhan side: the broker's RMS rejects a new sell for the same shares a
# still-resting target order already committed, "you are trying to sell more
# than the quantity you currently hold" -- confirmed on real positions, all
# rejected twice and left open until manually cancelled+sold). Cancel-then-
# sell avoids that collision entirely since the target is gone before the
# new sell is even placed. trade.py's `rate_limiter` is ALSO a 5-orders/sec
# sliding window (see trade.py), so this batch size lines up with the same
# real ceiling that limiter already enforces -- the batching
# here isn't a second independent rate limit, it's what keeps each concurrent
# burst homogeneous by call type within that limiter's own window.
MAX_ORDER_CALLS_PER_SECOND  = 5
BATCH_SLEEP_SECONDS         = 1.0
SHORT_SETTLE_BUFFER_SECONDS = 2.5   # one-time pause between the short-open wave
                                    # and the cover-target/stop-loss wave


def _load_symbols(trade_date: date) -> list[str]:
    path = _RESULTS_DIR / "trades" / f"trade_list_{trade_date.isoformat()}.csv"
    if not path.exists():
        sys.exit(f"[zerodha] No trade list: {path}")
    with open(path, newline="") as f:
        return [r["symbol"].strip().upper() for r in csv.DictReader(f)]


def _ts() -> str:
    return datetime.now(_IST).isoformat()


# ── Order fill polling ─────────────────────────────────────────────────────────

class OrderRejected(RuntimeError):
    """Order genuinely did not fill (REJECTED/CANCELLED) -- distinct from a poll
    timeout, where the order may well have filled and we just don't know yet."""


def _poll_fill(order_id: str, retries: int = 12, delay: float = 1.0) -> tuple[float, int]:
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
    intended price/qty as a best-effort guess."""
    try:
        return _poll_fill(order_id)
    except OrderRejected as exc:
        print(f"[zerodha]   ORDER REJECTED — {exc}")
        return 0.0, 0
    except Exception as exc:
        print(f"[zerodha]   fill poll failed: {exc} — using fallback values")
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
    """POST /margins/orders with product=MIS, transaction_type=SELL -- margin
    required to open the mirrored intraday short for this symbol+quantity."""
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


# ── Positions JSON ─────────────────────────────────────────────────────────────
# Long and short positions live in SEPARATE files -- see module docstring for
# why (a mirrored short opened THIS MORNING from a 925/1159 exit must never
# block a fresh same-day long re-entry on that symbol).

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
    """Positions still requiring an exit: fully open OR partial no-data exits."""
    return [p for p in positions
            if p.get("broker") == _BROKER
            and p.get("status") in ("open", "partial_exit_925_nodata")]


def _open_short_pos(positions: list) -> list:
    """Short positions opened by _open_short() that still need squaring off."""
    return [p for p in positions
            if p.get("broker") == _BROKER and p.get("status") == "short_open"]


# ── Broker quantity cross-check (product-aware) ─────────────────────────────────

def _broker_qty(symbol: str, product: str) -> tuple[int, str]:
    """Confirm broker-held quantity AND the exchange it's actually held under,
    for this exact product, via Kite positions and holdings endpoints. Returns
    (quantity, exchange), defaulting to ("NSE", 0) if nothing is found."""
    session, _ = _kite_session()
    if product in ("MIS",):
        try:
            resp = session.get(f"{_KITE_BASE}/portfolio/positions", timeout=15)
            if resp.ok:
                for p in (resp.json().get("data", {}).get("net") or []):
                    if ((p.get("tradingsymbol") or "").upper() == symbol.upper()
                            and p.get("product") == product):
                        qty = int(p.get("quantity") or 0)
                        if qty != 0:
                            return abs(qty), p.get("exchange", "NSE")
        except Exception:
            pass
        return 0, "NSE"

    # CNC/MTF: same-day positions first, then overnight holdings (T1 quantity
    # still sellable but not yet in "quantity" until Kite completes T+1
    # settlement -- confirmed live 2026-07-22).
    try:
        resp = session.get(f"{_KITE_BASE}/portfolio/positions", timeout=15)
        if resp.ok:
            for p in (resp.json().get("data", {}).get("net") or []):
                if (p.get("tradingsymbol") or "").upper() == symbol.upper():
                    qty = int(p.get("quantity") or 0)
                    if qty > 0:
                        return qty, p.get("exchange", "NSE")
    except Exception:
        pass
    try:
        resp = session.get(f"{_KITE_BASE}/portfolio/holdings", timeout=15)
        if resp.ok:
            for h in (resp.json().get("data") or []):
                if (h.get("tradingsymbol") or "").upper() == symbol.upper():
                    qty = int(h.get("quantity") or 0) + int(h.get("t1_quantity") or 0)
                    if qty > 0:
                        return qty, h.get("exchange", "NSE")
    except Exception:
        pass
    return 0, "NSE"


def _broker_short_qty(symbol: str) -> int:
    """Confirm broker-held short (net negative MIS) quantity for a mirrored short."""
    session, _ = _kite_session()
    try:
        resp = session.get(f"{_KITE_BASE}/portfolio/positions", timeout=15)
        if resp.ok:
            for p in (resp.json().get("data", {}).get("net") or []):
                if ((p.get("tradingsymbol") or "").upper() == symbol.upper()
                        and p.get("product") == "MIS"):
                    qty = int(p.get("quantity") or 0)
                    if qty < 0:
                        return abs(qty)
    except Exception:
        pass
    return 0


def _fetch_upper_circuit(symbol: str) -> float:
    """On-demand fetch of a single symbol's upper circuit limit via Kite's
    /quote, for the UC-based short stop-loss. Raises if the fetch fails or no
    circuit data comes back for this symbol."""
    session, _ = _kite_session()
    resp = session.get(f"{_KITE_BASE}/quote", params={"i": f"NSE:{symbol}"}, timeout=15)
    resp.raise_for_status()
    row = resp.json().get("data", {}).get(f"NSE:{symbol}")
    if not row or row.get("upper_circuit_limit") is None:
        raise ValueError(f"[zerodha] No circuit data found for {symbol}.")
    return float(row["upper_circuit_limit"])


def _run_batch(items: list, worker_fn) -> list:
    """Runs worker_fn(item) concurrently for EVERY item in `items` as one
    single burst -- a bounded ThreadPoolExecutor (max_workers == len(items))
    -- and blocks until all of them complete, returning the results as a
    list. This is the single-burst primitive both _run_in_chunks (one worker
    per batch) and _run_exit_wave1 (two workers per batch -- cancel, then
    sell) are built from, so a multi-sub-step batch can compose more than one
    burst within the same batch boundary without going through separate
    _run_in_chunks calls.

    worker_fn must never let an exception escape (wrap its own body in
    try/except and return an error dict/marker instead) -- one item's failure
    must not affect its batch-mates; this helper adds no additional per-item
    exception handling. Returns [] immediately for an empty list (no
    ThreadPoolExecutor spun up for nothing)."""
    if not items:
        return []
    with ThreadPoolExecutor(max_workers=len(items)) as executor:
        futures = {executor.submit(worker_fn, item): item for item in items}
        _wait_futures(futures.keys())
        return [fut.result() for fut in futures]


def _run_in_chunks(items: list, worker_fn, chunk_size: int | None = None,
                   sleep_between: float | None = None):
    """Generator: yields one list of results per chunk of `items` (chunk_size
    at a time, default MAX_ORDER_CALLS_PER_SECOND), running worker_fn(item)
    concurrently within each chunk via _run_batch (waiting for the WHOLE
    chunk to finish before yielding -- never proceed on partial completion),
    paced with `sleep_between` seconds (default BATCH_SLEEP_SECONDS) between
    chunks (skipped after the last one).

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
    concurrent burst; includes fill polling and, for a no-data fallback task,
    its fresh-target-remainder placement). The two sub-steps within a batch
    never overlap with each other, so every concurrent Order-API burst stays
    homogeneous by call type (all cancels together, then all sells together).
    Cancel MUST precede sell, not the other way around: a still-resting
    target order and a fresh sell for the same shares can't both be live at
    once -- the broker rejects the new sell as "trying to sell more than the
    quantity you currently hold" (confirmed live on the Dhan side 2026-08-28).
    `sleep_between` (default BATCH_SLEEP_SECONDS) is paced between BATCHES,
    not between a batch's own cancel and sell sub-steps.

    Yields one batch's SELL-step results at a time -- cancel-step results are
    never surfaced (a cancel failure is non-fatal, same as today, and only
    printed inline by cancel_fn itself)."""
    chunk_size    = chunk_size or MAX_ORDER_CALLS_PER_SECOND
    sleep_between = BATCH_SLEEP_SECONDS if sleep_between is None else sleep_between
    for i in range(0, len(tasks), chunk_size):
        batch = tasks[i : i + chunk_size]
        _run_batch(batch, cancel_fn)
        yield _run_batch(batch, sell_fn)
        if i + chunk_size < len(tasks):
            time.sleep(sleep_between)


class _BalanceTracker:
    """Thread-safe shared margin pool for concurrent _open_short_place()
    calls within the same batch -- lock around the check-and-decrement so two
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


def _open_short_place(sym: str, qty: int, source_stage: str, dry_run: bool,
                      balance: "_BalanceTracker") -> dict | None:
    """Wave 2 of the mirrored-short open (check_exit_925/force_exit_1159):
    MIS margin check, thread-safe balance reservation (_BalanceTracker --
    safe to call concurrently from multiple Wave-2 workers at once), places
    the short SELL, polls the fill, fires send_short_open. Returns a row
    dict with cover_target_order_id/cover_target_price/stop_order_id/
    stop_trigger_price/stop_limit_price all None -- _open_short_protect()
    (Wave 3) fills those in as a SEPARATE concurrent burst, after
    SHORT_SETTLE_BUFFER_SECONDS, so a cover-target/stop-loss placement never
    lands in the same Order-API burst as a different position's short-open
    sell.

    Never raises: the long exit that triggered this has already happened and
    is never reversed by a failed short. Wrapped in its own top-level
    try/except so an unexpected error here can never propagate out of a
    batch worker and affect sibling positions in the same batch."""
    try:
        margin_info = _mis_margin_check(sym, qty)
        if margin_info is None:
            print(f"[zerodha]   SHORT SKIP — {sym}: could not verify MIS margin.")
            return None
        if not balance.try_reserve(margin_info["margin_required"]):
            bal = balance.value
            print(f"[zerodha]   SHORT SKIP — {sym}: insufficient margin "
                  f"(available {'unknown' if bal is None else f'₹{bal:,.2f}'} "
                  f"< required ₹{margin_info['margin_required']:,.2f}).")
            return None

        try:
            ltp = get_ltp(sym)
        except Exception:
            ltp = 0.0

        try:
            oid = sell(sym, "NSE", qty, order_type="MARKET", product="MIS", dry_run=dry_run)
        except Exception as exc:
            print(f"[zerodha]   SHORT FAILED — {sym}: {exc}")
            return None

        ep, eq = (ltp, qty) if dry_run else _poll_fill_safe(oid, ltp, qty)
        if eq == 0:
            print(f"[zerodha]   SHORT NOT FILLED — {sym} short order rejected.")
            return None

        print(f"[zerodha]   SHORT OPENED — {sym}  ₹{ep:,.2f} × {eq}  (from {source_stage} exit)")
        try:
            notify.send_short_open(broker=_BROKER, symbol=f"{sym} [SHORT MIS]", entry_price=ep,
                                   shares=eq, source_exit_stage=source_stage, order_id=oid,
                                   dry_run=dry_run)
        except Exception as exc:
            print(f"  [notify] short_open failed: {exc}", file=sys.stderr)

        return {
            "broker":                _BROKER,
            "symbol":                sym,
            "direction":             "short",
            "product":               "MIS",
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
        print(f"[zerodha]   !! SHORT open sequence crashed unexpectedly for {sym}: {exc} "
              f"— manual review required.")
        return None


def _open_short_protect(row: dict, dry_run: bool) -> dict:
    """Wave 3 of the mirrored-short open: places the 5%-below cover target
    and the UC-based stop-loss for a short _open_short_place() already
    opened, mutating and returning the SAME row dict with those fields
    filled in (safe -- each Wave-3 worker only ever touches its own
    position's row, never a shared one). The short itself is already live by
    this point; a failure here never unwinds it.

    Never raises: wrapped in its own top-level try/except so an unexpected
    error here can never propagate out of a batch worker and affect sibling
    positions in the same batch."""
    sym, eq, ep = row["symbol"], row["quantity"], row["entry_price"]
    try:
        cover_price = round(ep * 0.95, 4)
        try:
            cover_target_order_id = buy(sym, "NSE", eq, order_type="LIMIT",
                                        price=cover_price, product="MIS", dry_run=dry_run)
            row["cover_target_order_id"] = cover_target_order_id
            row["cover_target_price"]    = cover_price
            print(f"[zerodha]   cover target placed @ ₹{cover_price:,.2f} — order {cover_target_order_id}")
        except Exception as exc:
            print(f"[zerodha]   !! SHORT OPENED but cover target placement failed for {sym}: {exc} "
                  f"— manual review required (short is live, unprotected until square-off).")

        # UC-based stop-loss: buy-to-cover if price rises to within 0.5% of
        # the day's upper circuit. Resting LIMIT (not SL-M) pinned AT the UC
        # itself (the highest price legally tradeable that day), so it sits
        # at the front of the book instead of risking a worse fill once the
        # stock is already ripping toward the circuit -- a trigger-based stop
        # can convert to a market order that finds no liquidity at all once
        # locked at UC.
        upper_circuit = None
        try:
            upper_circuit = _fetch_upper_circuit(sym)
        except Exception as exc:
            print(f"[zerodha]   !! circuit fetch failed for {sym}: {exc} — skipping stop-loss "
                  f"(short + cover target remain live, unprotected until manual review).")
            try:
                notify.send_circuit_fetch_failed(broker=_BROKER, symbol=sym, error_msg=str(exc),
                                                 dry_run=dry_run)
            except Exception as exc2:
                print(f"  [notify] circuit_fetch_failed failed: {exc2}", file=sys.stderr)

        if upper_circuit is not None:
            try:
                stop_limit_price   = round(upper_circuit, 4)
                stop_trigger_price = round(upper_circuit * 0.995, 4)
                stop_order_id = place_order(sym, "NSE", "BUY", eq,
                                            order_type="SL",
                                            price=stop_limit_price,
                                            trigger_price=stop_trigger_price,
                                            product="MIS", dry_run=dry_run)
                row["stop_order_id"]      = stop_order_id
                row["stop_trigger_price"] = stop_trigger_price
                row["stop_limit_price"]   = stop_limit_price
                print(f"[zerodha]   stop-loss placed @ trigger ₹{stop_trigger_price:,.2f} "
                      f"limit ₹{stop_limit_price:,.2f} (0.5% below UC ₹{upper_circuit:,.2f}) "
                      f"— order {stop_order_id}")
            except Exception as exc:
                print(f"[zerodha]   !! SHORT OPENED (cover target live) but stop-loss placement "
                      f"failed for {sym}: {exc} — manual review required.")
    except Exception as exc:
        print(f"[zerodha]   !! target/stop-loss placement crashed unexpectedly for {sym}: {exc} "
              f"— manual review required (short is live).")
    return row


def _open_short_execute(sym: str, qty: int, source_stage: str, dry_run: bool = False) -> dict | None:
    """Combines _open_short_place() (Wave 2) + _open_short_protect() (Wave 3)
    into the single call standalone/manual callers expect -- check_exit_925/
    force_exit_1159 call the two pieces separately instead, as two
    independently-batched waves. This function's own external
    behavior/signature is unchanged by that split -- still does NOT touch
    results/positions_zerodha_short.json, still returns the finished
    position dict (or None if nothing filled)."""
    row = _open_short_place(sym, qty, source_stage, dry_run, _BalanceTracker(_available_margin()))
    return None if row is None else _open_short_protect(row, dry_run)


# ── Entries log ──────────────────────────────────────────────────────────────

def _log_path(trade_date: date) -> Path:
    return _MTF_LOG_DIR / f"entries_{trade_date.isoformat()}.csv"


def _append_log(trade_date: date, row: dict) -> None:
    path        = _log_path(trade_date)
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
# ENTRY — 15:21, single leverage-branch path (MTF if >=2x leverage, else CNC)
# ══════════════════════════════════════════════════════════════════════════════

def run_entry_321(trade_date: date | None = None, dry_run: bool = False,
                  capital: float | None = None, symbol: str | None = None,
                  shares_override: int | None = None) -> None:
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
            print(f"[zerodha] Trade list empty for {trade_date} — nothing to enter.")
            return
        n          = len(symbols)
        capital    = capital if capital is not None else TOTAL_CAPITAL
        allocation = compute_allocation(capital, n)

    print(f"\n{'='*60}")
    print(f"[zerodha] Entry {trade_date}{'  DRY RUN' if dry_run else ''}"
          + ("  [MANUAL SINGLE-STOCK]" if manual_mode else ""))
    if manual_mode:
        print(f"[zerodha] {symbols[0]}  ·  ₹{allocation:,.0f} allocated"
              + (f"  ·  shares override: {shares_override}" if shares_override else ""))
    else:
        print(f"[zerodha] {n} signal(s)  ·  ₹{capital:,.0f} total  ·  ₹{allocation:,.0f} per position")
    print(f"{'='*60}")

    n_entered = 0
    n_skipped = 0

    positions       = _load_long_pos()
    positions_today = {
        p["symbol"]: p for p in positions
        if p.get("broker") == _BROKER and p.get("entry_date") == trade_date.isoformat()
    }

    # Step 1/2/3 priority ordering -- see module docstring. In manual mode
    # there's no batch trade_list and no ladder to reconcile with, so this
    # collapses to the single requested symbol untouched.
    if manual_mode:
        ordered_symbols = symbols
        step1_syms: list[str] = []
    else:
        step1_syms = [sym for sym, pos in positions_today.items()
                     if pos.get("entry_status") == "partially_filled"]
        step2_syms = [sym for sym in positions_today if sym not in step1_syms]
        step3_syms = [sym for sym in symbols if sym not in positions_today]
        for sym in step2_syms:
            print(f"[zerodha] {sym} — already entered today, skipping.")
            n_skipped += 1
        ordered_symbols = step1_syms + step3_syms
        if step1_syms:
            print(f"[zerodha] Step 1 priority completion(s): {step1_syms}")

    # trade.py bundles the 15:20:00-15:21:00 staging window: resolves every
    # symbol's reference price now, holds internally until exactly 15:21:00,
    # then returns -- so the loop below fires every order back to back the
    # instant this call returns, rather than mixing reference-price/margin
    # lookups into the 5-orders/sec budget while orders are going out.
    if manual_mode:
        try:
            ref, ref_hhmm = get_reference_price(symbols[0])
            staged = [{"symbol": symbols[0], "ref_price": ref, "ref_hhmm": ref_hhmm,
                      "shares": shares_override or compute_shares(allocation, ref)}]
        except Exception as exc:
            print(f"[zerodha]   SKIP — no reference price: {exc}")
            staged = []
    else:
        staged = stage_entry_orders(ordered_symbols, capital)
    staged_by_sym = {s["symbol"]: s for s in staged}

    available_balance = _available_margin()

    # ── Phase 1 (sequential): resolve product/shares/margin for every symbol,
    # decrementing the shared available_balance pool as we go. Must stay
    # sequential -- these are the /margins/orders leverage checks and the
    # capital-pool bookkeeping the task itself calls out as NOT safe to run
    # concurrently for the same pool. Nothing here places an order yet.
    ready: list[dict] = []
    for sym in ordered_symbols:
        existing_pos    = positions_today.get(sym)
        is_partial_fill = existing_pos is not None

        print(f"\n[zerodha] {sym}")

        staged_row = staged_by_sym.get(sym)
        if staged_row is None:
            print(f"[zerodha]   SKIP — no reference price available.")
            n_skipped += 1
            continue
        ref = staged_row["ref_price"]

        if is_partial_fill:
            remaining_capital = existing_pos["capital_base"] - existing_pos["filled_amount"]
            shares = compute_shares(remaining_capital, ref)
            print(f"[zerodha]   Step 1 priority — Case A left ₹{existing_pos['filled_amount']:,.2f} "
                  f"of ₹{existing_pos['capital_base']:,.2f} filled — completing remaining "
                  f"₹{remaining_capital:,.2f} ({shares} shares).")
        else:
            shares = staged_row["shares"]
        if shares == 0:
            print(f"[zerodha]   SKIP — 0 shares at ₹{ref:,.2f}.")
            n_skipped += 1
            continue

        if is_partial_fill:
            # Reuse leg 1's product as-is -- avoids a mixed-product position
            # (leg 1 MTF, completion CNC or vice versa) that a single
            # `product` field per row can't represent.
            product         = existing_pos["product"]
            leverage        = 0.0
            margin_required = shares * ref
            capital_base    = existing_pos.get("capital_base", capital)
            print(f"[zerodha]   Case A completion — reusing product {product} from leg 1  "
                  f"·  margin required: ₹{margin_required:,.2f}")
        else:
            margin_info  = _mtf_margin_check(sym, shares)
            has_leverage = margin_info is not None and margin_info["leverage"] >= 2
            if has_leverage:
                product         = "MTF"
                leverage        = margin_info["leverage"]
                margin_required = margin_info["margin_required"]
                capital_base    = capital
                print(f"[zerodha]   leverage: {leverage:.2f}x  ·  margin required: ₹{margin_required:,.2f}")
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
                    print(f"[zerodha]   SKIP — {reason}; 0 shares at ₹{ref:,.2f} on "
                          f"₹{capital_base:,.0f}-based CNC fallback.")
                    n_skipped += 1
                    continue
                shares          = resized_shares
                product         = "CNC"
                leverage        = margin_info["leverage"] if margin_info else 0.0
                margin_required = shares * ref
                print(f"[zerodha]   {reason} — falling back to CNC at {shares} shares "
                      f"(₹{capital_base:,.0f}-based)  ·  margin required: ₹{margin_required:,.2f}")

        if available_balance is None:
            print(f"[zerodha]   SKIP — could not verify available margin for {sym}.")
            n_skipped += 1
            continue
        if available_balance < margin_required:
            print(f"[zerodha]   SKIP — insufficient margin (available ₹{available_balance:,.2f} "
                  f"< required ₹{margin_required:,.2f}).")
            n_skipped += 1
            continue
        available_balance -= margin_required
        print(f"[zerodha]   margin confirmed  ·  ₹{available_balance:,.2f} remaining after this order")

        ready.append({
            "symbol": sym, "is_partial_fill": is_partial_fill, "existing_pos": existing_pos,
            "ref": ref, "shares": shares, "product": product, "leverage": leverage,
            "margin_required": margin_required, "capital_base": capital_base,
        })

    # ── Phase 2 (parallel): fire every resolved order at once. Each worker
    # only places the order and polls its own fill -- no position-file access,
    # no shared-state mutation beyond the rate limiter (already thread-safe on
    # its own). A single symbol's exception/rejection cannot block or delay
    # any other symbol's task or the eventual position-file write below.
    def _place_and_poll(item: dict) -> dict:
        sym, ref, shares, product = item["symbol"], item["ref"], item["shares"], item["product"]
        # MARKET order -- no predetermined price the way Dhan's LIMIT entry
        # has. ref (the 15:20 candle close) is only ever used for share
        # sizing; the real fill price is whatever the market gives, subject
        # to the 0.5% market_protection collar in trade.py's place_order().
        print(f"[zerodha]   {_ts()} — placing {product} MARKET BUY {shares}× {sym}  "
              f"(sizing ref ₹{ref:,.2f} — MARKET orders have no predetermined fill price)"
              + ("  (DRY RUN)" if dry_run else ""))
        try:
            order_id = buy(sym, "NSE", shares, order_type="MARKET", product=product, dry_run=dry_run)
        except Exception as exc:
            return {**item, "error": f"ORDER FAILED: {exc}"}

        if dry_run:
            return {**item, "order_id": order_id, "fill_price": ref, "fill_qty": shares}

        fill_price, fill_qty = _poll_fill_safe(order_id, ref, shares)
        if fill_qty == 0:
            return {**item, "order_id": order_id, "error": "NOT FILLED — order rejected."}
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
        print(f"\n[zerodha] {sym}  [{product}]")

        if "error" in res:
            print(f"[zerodha]   {res['error']}")
            _append_log(trade_date, {
                "timestamp": _ts(), "symbol": sym, "quantity": res["shares"],
                "ref_price": round(res["ref"], 4), "fill_price": "",
                "leverage": res["leverage"], "margin_required": res["margin_required"],
                "order_id": res.get("order_id", ""),
                "status": "order_failed" if "ORDER FAILED" in res["error"] else "rejected",
                "product": product, "capital_base": res["capital_base"],
            })
            n_skipped += 1
            continue

        order_id, fill_price, fill_qty = res["order_id"], res["fill_price"], res["fill_qty"]
        status = "dry_run" if dry_run else "filled"
        print(f"[zerodha]   filled ₹{fill_price:,.2f} × {fill_qty}"
              + ("  (DRY RUN)" if dry_run else ""))

        _append_log(trade_date, {
            "timestamp": _ts(), "symbol": sym, "quantity": fill_qty,
            "ref_price": round(res["ref"], 4), "fill_price": round(fill_price, 4),
            "leverage": res["leverage"], "margin_required": res["margin_required"],
            "order_id": order_id, "status": status, "product": product,
            "capital_base": res["capital_base"],
        })

        if res["is_partial_fill"]:
            existing_pos = res["existing_pos"]
            total_qty = existing_pos["actual_fill_quantity"] + fill_qty
            avg_price = ((existing_pos["actual_fill_price"] * existing_pos["actual_fill_quantity"]
                         + fill_price * fill_qty) / total_qty)
            existing_pos.update({
                "status": "open", "entry_status": "filled", "case_a_leg": "leg2_filled",
                "filled_amount": round(existing_pos["filled_amount"] + fill_price * fill_qty, 2),
                "actual_fill_price": round(avg_price, 4), "actual_fill_quantity": total_qty,
                "leg2_order_id": order_id, "leg2_fill_price": round(fill_price, 4),
                "leg2_fill_quantity": fill_qty, "leg2_timestamp": _ts(),
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

    print(f"\n[zerodha] Entry complete. Entered: {n_entered}  Skipped: {n_skipped}")
    print(f"[zerodha] Log written to {_log_path(trade_date)}")
    print(
        "\n[zerodha] REMINDER: MTF buy orders trigger a pledge request by email that must be "
        "approved (typically by ~7pm same day) for the position to actually be established."
    )


# ══════════════════════════════════════════════════════════════════════════════
# PROFIT TARGETS 9:15am — resting 17% LIMIT sell for every open long that
# doesn't already have one. Shorts get their own 5% cover target placed
# inline at open time (see _open_short above) -- this step only concerns
# long entry-side targets.
# ══════════════════════════════════════════════════════════════════════════════

def place_targets_915(dry_run: bool = False) -> None:
    positions = _load_long_pos()
    open_ps   = _open_pos(positions)

    print(f"\n{'='*60}")
    print(f"[zerodha] Place targets 9:15am{'  DRY RUN' if dry_run else ''}")
    print(f"[zerodha] {len(open_ps)} open position(s)")
    print(f"{'='*60}")

    if not open_ps:
        print("[zerodha] No open positions — nothing to place targets for.")
        return

    n_placed  = 0
    n_skipped = 0

    for pos in open_ps:
        sym = pos["symbol"]
        if pos.get("target_order_id"):
            print(f"[zerodha] {sym} — target already placed ({pos['target_order_id']}), skipping.")
            continue

        try:
            product      = pos.get("product", "CNC")
            is_partial   = pos["status"] == "partial_exit_925_nodata"
            qty          = (int(pos["shares_remaining"]) if is_partial
                            else int(pos["actual_fill_quantity"]))
            fill_price   = float(pos["actual_fill_price"] or 0)
            target_price = round(fill_price * 1.17, 4)

            print(f"\n[zerodha] {sym}  [{product}]  qty={qty}  target=₹{target_price:,.2f}")
            order_id = sell(sym, "NSE", qty, order_type="LIMIT",
                            price=target_price, product=product, dry_run=dry_run)

            pos["target_order_id"] = order_id
            pos["target_price"]    = target_price
            if not dry_run:
                _save_long_pos(positions)
            print(f"[zerodha]   target placed — order {order_id}")
            try:
                notify.send_target_placed(broker=_BROKER, symbol=f"{sym} [{product}]",
                                          target_price=target_price, order_id=order_id,
                                          dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] target_placed failed: {exc}", file=sys.stderr)
            n_placed += 1
        except Exception as exc:
            print(f"[zerodha]   !! target placement failed for {sym}: {exc}. Skipping.")
            n_skipped += 1

    print(f"\n[zerodha] Place targets complete. Placed: {n_placed}  Skipped: {n_skipped}.")


# ══════════════════════════════════════════════════════════════════════════════
# EXIT — Stage 2 (9:25am, P&L-gated) / Stage 3 (11:59am, unconditional)
# ══════════════════════════════════════════════════════════════════════════════

def check_exit_925(dry_run: bool = False) -> None:
    positions = _load_long_pos()
    open_ps   = _open_pos(positions)

    print(f"\n{'='*60}")
    print(f"[zerodha] Exit check 9:25am{'  DRY RUN' if dry_run else ''}")
    print(f"[zerodha] {len(open_ps)} open position(s)")
    print(f"{'='*60}")

    if not open_ps:
        print("[zerodha] No open positions — nothing to check.")
        return

    dirty = False   # any in-memory mutation this run -- decides whether to save at the end

    # ── Phase 1 (sequential): target-hit checks resolve immediately in place
    # (no new order -- just recording an already-completed fill), and the
    # no-data/pnl-gate decision for everything else. Builds a list of
    # per-symbol exit tasks to run in parallel below; positions that hit
    # their target or are held for 11:59 never enter that list.
    tasks: list[dict] = []
    for pos in open_ps:
        sym        = pos["symbol"]
        product    = pos.get("product", "CNC")
        fill_price = float(pos["actual_fill_price"] or 0)
        is_partial = pos["status"] == "partial_exit_925_nodata"
        qty        = (int(pos["shares_remaining"]) if is_partial
                      else int(pos["actual_fill_quantity"]))

        print(f"\n[zerodha] {sym}  [{product}]  fill=₹{fill_price:,.2f}  qty={qty}")

        target_oid = pos.get("target_order_id")
        if target_oid:
            try:
                t_status = _kite_order_status(target_oid)
            except Exception as exc:
                print(f"[zerodha]   !! target status check failed for {sym}: {exc}. "
                      f"Skipping {sym} — manual review required.")
                continue
            if (t_status.get("status") or "").upper() == "COMPLETE":
                ep  = float(t_status.get("average_price") or 0)
                eq  = int(t_status.get("filled_quantity") or 0) or qty
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
                print(f"[zerodha]   TARGET HIT — exited ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
                try:
                    notify.send_target_hit(broker=_BROKER, symbol=f"{sym} [{product}]", stage="925",
                                           exit_price=ep, return_pct=ret, pnl=pnl, dry_run=dry_run)
                except Exception as exc:
                    print(f"  [notify] target_hit failed: {exc}", file=sys.stderr)
                # No mirrored short here -- a stock that just ran +17% to hit
                # its resting target is exactly the one most likely to keep
                # running into an upper circuit, where a short can't be
                # covered (no sellers at UC).
                continue

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
            half   = math.floor(qty / 2)
            remain = qty - half
            if half == 0:
                print(f"[zerodha]   qty too small to halve — holding until 11:59am.")
                continue
            tasks.append({"kind": "fallback", "pos": pos, "sym": sym, "product": product,
                          "fill_price": fill_price, "qty": half, "remain": remain,
                          "target_oid": target_oid, "target_price": pos.get("target_price")})
            continue

        if pnl_live > 0:
            print(f"[zerodha]   P&L positive — queuing sell of {qty}")
            tasks.append({"kind": "full", "pos": pos, "sym": sym, "product": product,
                          "fill_price": fill_price, "qty": qty, "target_oid": target_oid})
        else:
            print(f"[zerodha]   P&L ≤ 0 (₹{pnl_live:+,.2f}) — holding for 11:59am forced exit.")

    # Phase 1's target-hit updates (if any) are complete now -- persist them
    # before any wave work begins below, independent of whether `tasks` ends
    # up empty (e.g. every open position hit its target -- there would be no
    # wave to piggyback a save on otherwise).
    if not dry_run and dirty:
        _save_long_pos(positions)

    # One-time MIS-short balance fetch for this whole stage-run, tracked
    # thread-safely across every batch below (Wave 2) -- the concurrent
    # equivalent of a fresh _available_margin() read per short, now safe for
    # multiple _open_short_place() calls to share at once (see
    # _BalanceTracker).
    short_balance = _BalanceTracker(_available_margin()) if tasks else _BalanceTracker(None)

    # ── Wave 1 (batched-concurrent): cancel every task's stale target (one
    # concurrent burst per batch), THEN -- only once that batch's cancels are
    # all confirmed -- sell every task in that same batch (a second, separate
    # concurrent burst; includes fill polling and, for a no-data fallback
    # task, its fresh-target-remainder placement). Every position in a batch
    # performs the SAME action before any position moves to the next, so a
    # concurrent Order-API burst never mixes call types from different
    # positions. No position-file access inside either worker; the
    # sequential apply step below does every field update and this wave's
    # one write.
    def _cancel_fn(task: dict) -> None:
        sym, target_oid = task["sym"], task["target_oid"]
        if target_oid:
            try:
                _kite_cancel_order(target_oid)
            except Exception as exc:
                print(f"[zerodha]   target cancel failed for {sym} (may already be filled/cancelled): {exc}")
        return None

    def _sell_fn(task: dict) -> dict:
        try:
            sym, product, fill_price, qty = task["sym"], task["product"], task["fill_price"], task["qty"]
            target_oid = task["target_oid"]

            exch = "NSE"
            if not dry_run:
                bqty, exch = _broker_qty(sym, product)
                if bqty != qty:
                    return {**task, "error": f"!! MISMATCH — local={qty} broker={bqty}. "
                                              f"Skipping {sym} — manual review required."}

            try:
                oid = sell(sym, exch, qty, order_type="MARKET", product=product, dry_run=dry_run)
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
                    result["new_target_oid"] = sell(sym, exch, task["remain"], order_type="LIMIT",
                                                     price=task["target_price"], product=product,
                                                     dry_run=dry_run)
                except Exception as exc:
                    result["fresh_target_error"] = str(exc)

            return result
        except Exception as exc:
            return {**task, "error": f"!! task crashed unexpectedly: {exc}"}

    wave1_results: list[dict] = []
    for batch_results in _run_exit_wave1(tasks, _cancel_fn, _sell_fn):
        wave1_results.extend(batch_results)

    # ── Sequential apply for Wave 1 (once per whole run, not per batch):
    # every field update + notify call, then exactly ONE _save_long_pos() --
    # the first of check_exit_925's 3 total write-points this run (Wave 1
    # sold status, Wave 2 short-open rows, Wave 3 target/SL order IDs).
    wave1_dirty = False
    sold_tasks: list[dict] = []   # feeds Wave 2 -- everything actually sold in Wave 1
    for res in wave1_results:
        sym, product, pos, fill_price = res["sym"], res["product"], res["pos"], res["fill_price"]
        print(f"\n[zerodha] {sym}  [{product}]")

        if "error" in res:
            print(f"[zerodha]   {res['error']}")
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
            print(f"[zerodha]   NO-DATA FALLBACK — sold {eq}  ₹{ep:,.2f}")
            if "new_target_oid" in res:
                pos["target_order_id"] = res["new_target_oid"]
                print(f"[zerodha]   fresh target placed for remaining {remain} "
                      f"@ ₹{res['target_price']:,.2f} — order {res['new_target_oid']}")
            elif "fresh_target_error" in res:
                print(f"[zerodha]   !! fresh target placement failed for {sym}: {res['fresh_target_error']}")
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
            print(f"[zerodha]   exited ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
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
    # wave.
    def _short_place_fn(res: dict) -> dict | None:
        return _open_short_place(res["sym"], res["eq"], "925", dry_run, balance=short_balance)

    wave2_rows: list[dict] = []
    for batch_results in _run_in_chunks(sold_tasks, _short_place_fn):
        wave2_rows.extend([r for r in batch_results if r is not None])

    short_positions = None
    if wave2_rows:
        short_positions = _load_short_pos()
        short_positions.extend(wave2_rows)
        if not dry_run:
            _save_short_pos(short_positions)
        print(f"\n[zerodha] Opened {len(wave2_rows)} mirrored short(s).")

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
        return _open_short_protect(row, dry_run)

    for _ in _run_in_chunks(wave2_rows, _protect_fn):
        pass   # each worker mutates its row in place; nothing further to apply here

    if wave2_rows and not dry_run:
        _save_short_pos(short_positions)

    print(f"\n[zerodha] Exit check 9:25am complete.")


def force_exit_1159(dry_run: bool = False) -> None:
    positions = _load_long_pos()
    open_ps   = _open_pos(positions)

    print(f"\n{'='*60}")
    print(f"[zerodha] Force exit 11:59am{'  DRY RUN' if dry_run else ''}")
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
    dirty   = False

    # ── Phase 1 (sequential): target-hit checks resolve immediately in place;
    # everything else queues an unconditional force-sell task (no P&L gate at
    # this stage -- unlike 9:25, every remaining open position sells here).
    tasks: list[dict] = []
    for pos in open_ps:
        sym        = pos["symbol"]
        product    = pos.get("product", "CNC")
        fill_price = float(pos["actual_fill_price"] or 0)
        is_partial = pos["status"] == "partial_exit_925_nodata"
        qty        = (int(pos["shares_remaining"]) if is_partial
                      else int(pos["actual_fill_quantity"]))

        print(f"\n[zerodha] {sym}  [{product}]  qty={qty}")

        target_oid = pos.get("target_order_id")
        if target_oid:
            try:
                t_status = _kite_order_status(target_oid)
            except Exception as exc:
                print(f"[zerodha]   !! target status check failed for {sym}: {exc}. "
                      f"Skipping {sym} — manual review required.")
                continue
            if (t_status.get("status") or "").upper() == "COMPLETE":
                ep = float(t_status.get("average_price") or 0)
                eq = int(t_status.get("filled_quantity") or 0) or qty
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
                print(f"[zerodha]   TARGET HIT — closed ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
                try:
                    notify.send_target_hit(broker=_BROKER, symbol=f"{sym} [{product}]", stage="1159",
                                           exit_price=ep, return_pct=ret, pnl=pnl, dry_run=dry_run)
                except Exception as exc:
                    print(f"  [notify] target_hit failed: {exc}", file=sys.stderr)
                n_force += 1
                continue
            # Not traded -- the actual cancel moves into Wave 1's concurrent
            # cancel sub-step below (batched with every other task's cancel
            # in the same burst), not fired here sequentially.

        tasks.append({"pos": pos, "sym": sym, "product": product, "fill_price": fill_price,
                      "qty": qty, "is_partial": is_partial, "target_oid": target_oid})

    # Phase 1's target-hit updates (if any) are complete now -- persist them
    # before any wave work begins, independent of whether `tasks` ends up
    # empty (see the matching note in check_exit_925).
    if not dry_run and dirty:
        _save_long_pos(positions)

    # One-time MIS-short balance fetch for this whole stage-run -- same
    # reasoning as check_exit_925.
    short_balance = _BalanceTracker(_available_margin()) if tasks else _BalanceTracker(None)

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
                _kite_cancel_order(target_oid)
            except Exception as exc:
                print(f"[zerodha]   target cancel failed for {sym} (may already be filled/cancelled): {exc}")
        return None

    def _sell_fn(task: dict) -> dict:
        try:
            sym, product, fill_price, qty = task["sym"], task["product"], task["fill_price"], task["qty"]

            exch = "NSE"
            if not dry_run:
                bqty, exch = _broker_qty(sym, product)
                if bqty != qty:
                    return {**task, "error": f"!! MISMATCH — local={qty} broker={bqty}. "
                                              f"Skipping {sym} — manual review required."}

            try:
                oid = sell(sym, exch, qty, order_type="MARKET", product=product, dry_run=dry_run)
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
        print(f"\n[zerodha] {sym}  [{product}]")

        if "error" in res:
            print(f"[zerodha]   {res['error']}")
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
        print(f"[zerodha]   force-exited ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
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
        return _open_short_place(res["sym"], res["eq"], "1159", dry_run, balance=short_balance)

    wave2_rows: list[dict] = []
    for batch_results in _run_in_chunks(sold_tasks, _short_place_fn):
        wave2_rows.extend([r for r in batch_results if r is not None])

    short_positions = None
    if wave2_rows:
        short_positions = _load_short_pos()
        short_positions.extend(wave2_rows)
        if not dry_run:
            _save_short_pos(short_positions)
        print(f"\n[zerodha] Opened {len(wave2_rows)} mirrored short(s).")

    # ── Settle buffer -- see check_exit_925's matching comment.
    if wave2_rows:
        time.sleep(SHORT_SETTLE_BUFFER_SECONDS)

    # ── Wave 3 (batched-concurrent): place cover-target + stop-loss for
    # every short Wave 2 opened -- see check_exit_925's matching Wave 3.
    def _protect_fn(row: dict) -> dict:
        return _open_short_protect(row, dry_run)

    for _ in _run_in_chunks(wave2_rows, _protect_fn):
        pass

    if wave2_rows and not dry_run:
        _save_short_pos(short_positions)

    _daily_summary(positions, n_force, dry_run)
    print(f"\n[zerodha] Force exit 11:59am complete. Force-exited: {n_force}.")


def _daily_summary(positions: list, n_force: int, dry_run: bool) -> None:
    today    = date.today().isoformat()
    today_ps = [p for p in positions
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


# ══════════════════════════════════════════════════════════════════════════════
# SHORT SQUARE-OFF 2:39pm — unconditional buy-to-cover for every short opened
# by _open_short() from either the 925 or 1159 long-exit stages.
# ══════════════════════════════════════════════════════════════════════════════

def square_off_239(dry_run: bool = False) -> None:
    positions   = _load_short_pos()
    open_shorts = _open_short_pos(positions)

    print(f"\n{'='*60}")
    print(f"[zerodha] Short square-off 2:39pm{'  DRY RUN' if dry_run else ''}")
    print(f"[zerodha] {len(open_shorts)} open short position(s)")
    print(f"{'='*60}")

    if not open_shorts:
        print("[zerodha] No open shorts — nothing to square off.")
        return

    n_closed = 0

    # ── Pre-check (single call): the whole run's cover-target/stop-loss OCO
    # status now comes from ONE GET /orders (Order Book) call, not one
    # GET /orders/{id} per order per position -- every order this account
    # placed today, looked up locally by order_id from here on. If this one
    # call itself fails, nothing about ANY position's status can be trusted,
    # so every open short is skipped for manual review rather than guessed
    # (same fail-closed reasoning as _BalanceTracker's None-balance case).
    try:
        order_by_id = {o.get("order_id"): o for o in _kite_get_orders()}
        orders_ok   = True
    except Exception as exc:
        print(f"[zerodha]   !! Order Book fetch failed: {exc} -- cannot verify any cover/stop "
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
            print(f"[zerodha]   !! {sym}: {msg} Skipping — manual review required.")
            try:
                notify.send_square_off_manual_review(broker=_BROKER, symbol=sym, error_msg=msg, dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] square_off_manual_review failed: {exc}", file=sys.stderr)
            continue
        if stop_oid and stop_status is None:
            msg = f"stop_order_id {stop_oid} not found in today's Order Book."
            print(f"[zerodha]   !! {sym}: {msg} Skipping — manual review required.")
            try:
                notify.send_square_off_manual_review(broker=_BROKER, symbol=sym, error_msg=msg, dry_run=dry_run)
            except Exception as exc:
                print(f"  [notify] square_off_manual_review failed: {exc}", file=sys.stderr)
            continue

        cover_traded = bool(cover_status) and (cover_status.get("status") or "").upper() == "COMPLETE"
        stop_traded  = bool(stop_status)  and (stop_status.get("status")  or "").upper() == "COMPLETE"

        if cover_traded and stop_traded:
            msg = ("BOTH cover target AND stop-loss show COMPLETE -- OCO race, "
                   "cannot determine which actually executed first.")
            print(f"[zerodha]   !! {sym}: {msg} Skipping — manual review required.")
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
                _kite_cancel_order(item["stop_oid"])
            except Exception as exc:
                print(f"[zerodha]   stop-loss cancel failed for {sym} (may already be filled/cancelled): {exc}")
        elif kind == "stop_filled" and item["cover_oid"]:
            try:
                _kite_cancel_order(item["cover_oid"])
            except Exception as exc:
                print(f"[zerodha]   cover target cancel failed for {sym} (may already be filled/cancelled): {exc}")
        elif kind == "neither_filled":
            if item["cover_oid"]:
                try:
                    _kite_cancel_order(item["cover_oid"])
                except Exception as exc:
                    print(f"[zerodha]   cover target cancel failed for {sym} (may already be filled/cancelled): {exc}")
            if item["stop_oid"]:
                try:
                    _kite_cancel_order(item["stop_oid"])
                except Exception as exc:
                    print(f"[zerodha]   stop-loss cancel failed for {sym} (may already be filled/cancelled): {exc}")
        return None

    def _force_cover_fn(item: dict) -> dict:
        sym, entry_price, qty = item["sym"], item["entry_price"], item["qty"]
        try:
            print(f"\n[zerodha] {sym}  [SHORT MIS]  entry=₹{entry_price:,.2f}  qty={qty}  "
                  f"(from {item['pos'].get('source_exit_stage', '?')} exit)")
            if not dry_run:
                bqty = _broker_short_qty(sym)
                if bqty != qty:
                    return {"pos": item["pos"], "sym": sym,
                            "error": f"!! MISMATCH — local={qty} broker_short={bqty}. "
                                     f"Skipping {sym} — manual review required."}
                print(f"[zerodha]   broker confirmed: {bqty} shares short")

            try:
                oid = buy(sym, "NSE", qty, order_type="MARKET", product="MIS", dry_run=dry_run)
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
                ep = float(cs.get("average_price") or 0)
                eq = int(cs.get("filled_quantity") or 0) or item["qty"]
                batch_results.append({"pos": item["pos"], "sym": sym, "kind": "cover_target_hit",
                                      "ep": ep, "eq": eq, "oid": item["cover_oid"],
                                      "entry_price": item["entry_price"]})
            elif kind == "stop_filled":
                ss = item["stop_status"]
                ep = float(ss.get("average_price") or 0)
                eq = int(ss.get("filled_quantity") or 0) or item["qty"]
                batch_results.append({"pos": item["pos"], "sym": sym, "kind": "stop_loss_hit",
                                      "ep": ep, "eq": eq, "oid": item["stop_oid"],
                                      "entry_price": item["entry_price"]})
            else:
                batch_results.append(force_cover_by_sym[sym])

        # ── Sequential apply for THIS batch: field updates + notify, then
        # this batch's ONE save -- never begin the next batch's cancels
        # until this save has completed (the "no half-cancelled state on
        # interruption" guarantee described above).
        chunk_dirty = False
        for res in batch_results:
            sym, pos = res["sym"], res["pos"]

            if "error" in res:
                print(f"[zerodha]   {res['error']}")
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
                print(f"[zerodha]   COVER TARGET HIT — squared off ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
                try:
                    notify.send_cover_target_hit(broker=_BROKER, symbol=f"{sym} [SHORT MIS]",
                                                 entry_price=entry_price, exit_price=ep,
                                                 return_pct=ret, pnl=pnl, dry_run=dry_run)
                except Exception as exc:
                    print(f"  [notify] cover_target_hit failed: {exc}", file=sys.stderr)
            elif kind == "stop_loss_hit":
                print(f"[zerodha]   STOP-LOSS HIT — squared off ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
                try:
                    notify.send_short_stoploss_hit(broker=_BROKER, symbol=f"{sym} [SHORT MIS]",
                                                   entry_price=entry_price, exit_price=ep,
                                                   return_pct=ret, pnl=pnl, dry_run=dry_run)
                except Exception as exc:
                    print(f"  [notify] short_stoploss_hit failed: {exc}", file=sys.stderr)
            else:  # force_cover
                print(f"[zerodha]   SQUARED OFF ₹{ep:,.2f}  P&L ₹{pnl:+,.2f}")
                try:
                    notify.send_square_off_239(broker=_BROKER, symbol=f"{sym} [SHORT MIS]",
                                               entry_price=entry_price, exit_price=ep,
                                               return_pct=ret, pnl=pnl, dry_run=dry_run)
                except Exception as exc:
                    print(f"  [notify] square_off_239 failed: {exc}", file=sys.stderr)

        if not dry_run and chunk_dirty:
            _save_short_pos(positions)

        if i + chunk_size < len(classified):
            time.sleep(BATCH_SLEEP_SECONDS)

    print(f"\n[zerodha] Short square-off complete. Closed: {n_closed}.")


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Zerodha live trading — single leverage-branch pipeline")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--entry",          action="store_true", help="Entry at 15:21")
    grp.add_argument("--place-targets",  action="store_true", help="Place 17%% profit-target LIMIT sells (9:15am)")
    grp.add_argument("--exit-925",       action="store_true", help="Exit check at 9:25am")
    grp.add_argument("--exit-1159",      action="store_true", help="Forced exit at 11:59am")
    grp.add_argument("--square-off-239", action="store_true", help="Square off mirrored shorts (2:39pm)")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without placing orders")
    parser.add_argument("--date",    default=None,
                        help="Trade date YYYY-MM-DD (--entry only; defaults to today)")
    parser.add_argument("--capital", type=float, default=None,
                        help="Override total capital for --entry. In --symbol mode, this is the "
                             "amount allocated to that one stock (not divided by 4).")
    parser.add_argument("--symbol",  default=None,
                        help="Buy this one symbol instead of reading the day's trade list.")
    parser.add_argument("--shares",  type=int, default=None,
                        help="Exact quantity to buy in --symbol mode (skips allocation-based sizing).")
    args = parser.parse_args()

    if args.shares is not None and args.symbol is None:
        sys.exit("[zerodha] --shares requires --symbol")

    td = date.fromisoformat(args.date) if args.date else date.today()

    try:
        if args.entry:
            run_entry_321(trade_date=td, dry_run=args.dry_run, capital=args.capital,
                          symbol=args.symbol, shares_override=args.shares)
        elif args.place_targets:
            place_targets_915(dry_run=args.dry_run)
        elif args.exit_925:
            check_exit_925(dry_run=args.dry_run)
        elif args.exit_1159:
            force_exit_1159(dry_run=args.dry_run)
        else:
            square_off_239(dry_run=args.dry_run)
    except (EnvironmentError, RuntimeError, ValueError) as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)
