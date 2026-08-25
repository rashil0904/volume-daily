"""
Zerodha trading functions — buy, sell, cancel, order status, entry scheduling.
================================================================================
The only file that talks to Kite's order/historical endpoints directly.
Three responsibilities (per the zerodha/ 4-file design):

  1. Order placement  — place_order()/buy()/sell()/cancel_order()/order_status()/
     get_orders(), same shapes as before.
  2. Rate limiting    — every order-placing/cancelling call goes through a single
     process-wide sliding-window token bucket capped at 5 orders/sec (see
     RateLimiter/rate_limiter below), so run_trades.py's parallel entry/exit
     batches (each symbol's order fired from its own thread-pool worker) and
     live_monitor.py's UC-staged-entry ladder (its own thread pool) never
     combine to exceed Kite's order-endpoint rate limit no matter how many
     threads or callers import this module.
  3. Entry scheduling — stage_entry_orders() replaces the old Upstox-based
     reference-price fetch entirely: Zerodha's OWN 15:20 1-minute candle close,
     via Kite's historical-candle REST endpoint (GET /instruments/historical),
     falling back to a live LTP snapshot if the historical endpoint comes back
     empty for today (same "don't trust one source blindly" posture as the old
     Upstox fallback chain — see get_reference_price's docstring for exactly why
     this fallback exists and what it protects against). Bundles the whole
     "resolve every symbol's reference price, compute allocation, size shares"
     prep into one function so run_trades.py's entry stage calls one thing and
     gets back a ready-to-fire order list.

Requires a valid session from zerodha.auth. Run `python -m zerodha.auth` once
each morning to authenticate, then import these functions freely.

Quick CLI usage:
    python zerodha/trade.py buy  INFY NSE 1 MARKET
    python zerodha/trade.py sell INFY NSE 1 MARKET
    python zerodha/trade.py buy  RELIANCE NSE 1 LIMIT 2850.50
    python zerodha/trade.py status <order_id>
    python zerodha/trade.py cancel <order_id>
    python zerodha/trade.py orders

Supported values:
    exchange     : NSE, BSE
    order_type   : MARKET, LIMIT, SL, SL-M
    product      : CNC (delivery), MIS (intraday), NRML (F&O)
    variety      : regular (default), amo (after-market)
"""

import csv
import sys
import threading
import time
from collections import deque
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from zerodha.auth import BASE_URL, get_session
from common.calc_utils import pick_reference_price, compute_allocation, compute_shares

_ROOT              = Path(__file__).resolve().parent.parent
_IST               = ZoneInfo("Asia/Kolkata")
_INSTRUMENTS_CACHE = _ROOT / "data" / "instruments" / "kite_nse_instruments.csv"
_INSTRUMENTS_MAX_AGE = timedelta(days=1)   # instrument tokens can churn on corporate actions


# ── Rate limiter (process-wide, 5 orders/sec, sliding window) ───────────────────
# A single token bucket that EVERY order-placing/cancelling call in this module
# goes through -- run_trades.py's now-parallel entry/exit batches (each symbol's
# order fired from its own ThreadPoolExecutor worker) and live_monitor.py's
# UC-staged-entry ladder (its own 4-worker executor) both end up calling
# buy()/sell()/place_order()/cancel_order() from HERE, so this is the one place
# that caps COMBINED throughput across every thread in the process, not each
# caller/thread pacing itself independently -- there is exactly one `rate_limiter`
# instance (module-level singleton below), never one per call site or per pool.
#
# True sliding window (deque of the last 5 call timestamps), not a flat
# min-interval spacer: a batch of <=5 orders must fire back to back with NO
# artificial delay between them (that's the whole point of parallelizing the
# entry/exit batches -- a fixed per-call spacing would silently re-serialize
# them). Only the 6th-or-later acquire() within any trailing 1-second window
# blocks, and only for as long as it takes the oldest of those 5 calls to age
# out of the window. Thread-safe: every mutation of the deque happens under
# `_lock`, held for the full check-or-wait-and-record sequence so two threads
# can never both observe "room for one more" and both proceed.

class RateLimiter:
    def __init__(self, max_per_sec: int):
        self._max_per_sec = max_per_sec
        self._calls: deque[float] = deque()   # timestamps of the last <=max_per_sec calls
        self._lock  = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                # Drop anything that's already aged out of the trailing 1s window.
                while self._calls and now - self._calls[0] >= 1.0:
                    self._calls.popleft()
                if len(self._calls) < self._max_per_sec:
                    self._calls.append(now)
                    return
                # At capacity -- compute how long until the oldest call ages out.
                sleep_for = 1.0 - (now - self._calls[0])
            if sleep_for > 0:
                time.sleep(sleep_for)
            # Loop back and re-check under the lock -- another thread may have
            # grabbed the freed slot in the meantime.


rate_limiter = RateLimiter(max_per_sec=5)


# ── Core order functions ───────────────────────────────────────────────────────

def _disclosed_qty(quantity: int, order_type: str, disclosed_pct: float) -> int:
    """NSE/BSE's Disclosed Quantity (DQ) feature -- only the returned amount
    shows on the public order book, the rest fills silently as it's matched.
    Kite only accepts DQ for regular-variety equity LIMIT orders, and it must
    be strictly less than the order quantity (0 disables it, which is also
    what a MARKET/SL/SL-M order type or disclosed_pct<=0 falls back to
    here)."""
    if order_type != "LIMIT" or disclosed_pct <= 0 or quantity <= 1:
        return 0
    dq = int(round(quantity * disclosed_pct))
    return max(0, min(dq, quantity - 1))


def place_order(
    symbol: str,
    exchange: str,
    transaction_type: str,          # "BUY" or "SELL"
    quantity: int,
    order_type: str  = "MARKET",    # MARKET | LIMIT | SL | SL-M
    price: float     = 0,           # required for LIMIT and SL
    trigger_price: float = 0,       # required for SL and SL-M
    product: str     = "CNC",       # CNC (delivery) | MIS (intraday) | NRML
    variety: str     = "regular",   # regular | amo
    market_protection: float = 0.5,   # required by Kite for MARKET/SL-M; 0 is REJECTED (treated as no
                                     # value at all -- confirmed live 2026-07-21). -1 (system default)
                                     # is NOT a fixed rate -- confirmed live 2026-07-23 via the order's
                                     # "mktp:X.XX" tag: 2.00% for one order, 1.00% for another, 0.50% for
                                     # a third, same day. That variability is what caused two same-day
                                     # entries (ASIANTILES, GANDHAR) to be falsely rejected as "outside
                                     # circuit limits" despite their reference price sitting ~1.3-1.4%
                                     # inside the real exchange band -- the dynamic default happened to
                                     # pick 1-2%, wide enough to push the protected ceiling past the band.
                                     # Pinned at 0.5% (deliberate choice, 2026-08-25 zerodha/ 4-file
                                     # redesign) so the collar stays predictable across every MARKET
                                     # order this file places -- entry, forced exits, AND the
                                     # UC-staged-entry ladder's buys (see live_monitor.py). NOTE: an
                                     # earlier version of this file pinned 0.75% specifically because a
                                     # too-wide DYNAMIC default (1-2%) caused the ASIANTILES/GANDHAR
                                     # rejections above; 0.5% is TIGHTER than that, not wider, so it does
                                     # not reintroduce that failure mode by itself -- but it does leave
                                     # less headroom than 0.75% did if a stock gaps hard in the single
                                     # second between LTP snapshot and order placement. Worth
                                     # re-widening if a same-shape false rejection shows up again.
    tag: str         = "",          # optional identifier (max 20 chars)
    disclosed_pct: float = 0.20,    # fraction of quantity shown on the book (LIMIT only); rest hidden
    dry_run: bool    = False,       # print payload only, no real order
) -> str:
    """
    Places an order and returns the order_id.
    Pass dry_run=True to validate params and print the payload without sending.
    Raises RuntimeError on failure with the exact error from Kite.
    """
    transaction_type = transaction_type.upper()
    exchange         = exchange.upper()
    order_type       = order_type.upper()
    product          = product.upper()

    if transaction_type not in ("BUY", "SELL"):
        raise ValueError(f"transaction_type must be BUY or SELL, got: {transaction_type!r}")
    if order_type not in ("MARKET", "LIMIT", "SL", "SL-M"):
        raise ValueError(f"order_type must be MARKET/LIMIT/SL/SL-M, got: {order_type!r}")
    if order_type == "LIMIT" and not price:
        raise ValueError("price is required for LIMIT orders")
    if order_type in ("SL", "SL-M") and not trigger_price:
        raise ValueError("trigger_price is required for SL/SL-M orders")

    dq = _disclosed_qty(quantity, order_type, disclosed_pct)

    payload = {
        "tradingsymbol":    symbol.upper(),
        "exchange":         exchange,
        "transaction_type": transaction_type,
        "order_type":       order_type,
        "quantity":         quantity,
        "product":          product,
        "price":            price,
        "trigger_price":    trigger_price,
        "validity":         "DAY",
    }
    if order_type in ("MARKET", "SL-M"):
        payload["market_protection"] = market_protection
    if dq:
        payload["disclosed_quantity"] = dq
    if tag:
        payload["tag"] = tag[:20]

    if dry_run:
        print("[trade] ── DRY RUN — no real order placed ──────────────────")
        print(f"[trade] POST {BASE_URL}/orders/{variety}")
        print("[trade] Payload:")
        for k, v in payload.items():
            print(f"  {k:20} = {v}")
        print("[trade] ───────────────────────────────────────────────────")
        return "DRY_RUN"

    session, _ = get_session()

    print(f"[trade] Placing {transaction_type} {quantity}× {symbol} @ {order_type}"
          + (f" ₹{price}" if price else "")
          + f"  [{exchange} · {product} · {variety}]"
          + (f"  (disclosed {dq}/{quantity})" if dq else ""))

    rate_limiter.acquire()
    resp = session.post(f"{BASE_URL}/orders/{variety}", data=payload, timeout=15)

    body = resp.json()
    if not resp.ok or body.get("status") == "error":
        raise RuntimeError(
            f"[trade] Order failed: {body.get('message') or body.get('error_type') or body}"
        )

    order_id = body["data"]["order_id"]
    print(f"[trade] Order placed — order_id: {order_id}")
    return order_id


def buy(symbol: str, exchange: str, quantity: int,
        order_type: str = "MARKET", price: float = 0,
        product: str = "CNC", **kwargs) -> str:
    """Shorthand for place_order with transaction_type=BUY."""
    return place_order(symbol, exchange, "BUY", quantity,
                       order_type=order_type, price=price, product=product, **kwargs)


def sell(symbol: str, exchange: str, quantity: int,
         order_type: str = "MARKET", price: float = 0,
         product: str = "CNC", **kwargs) -> str:
    """Shorthand for place_order with transaction_type=SELL."""
    return place_order(symbol, exchange, "SELL", quantity,
                       order_type=order_type, price=price, product=product, **kwargs)


def cancel_order(order_id: str, variety: str = "regular") -> str:
    """Cancels an open order. Returns the order_id on success."""
    session, _ = get_session()
    rate_limiter.acquire()
    resp = session.delete(f"{BASE_URL}/orders/{variety}/{order_id}", timeout=15)
    body = resp.json()
    if not resp.ok or body.get("status") == "error":
        raise RuntimeError(
            f"[trade] Cancel failed: {body.get('message') or body}"
        )
    print(f"[trade] Order {order_id} cancelled.")
    return order_id


def order_status(order_id: str) -> dict:
    """Returns the latest status dict for a specific order. Not rate-limited --
    read-only status polling isn't the order-placement throughput Kite's limit
    (and this module's throttle) is protecting."""
    session, _ = get_session()
    resp = session.get(f"{BASE_URL}/orders/{order_id}", timeout=15)
    body = resp.json()
    if not resp.ok or body.get("status") == "error":
        raise RuntimeError(
            f"[trade] Could not fetch order {order_id}: {body.get('message') or body}"
        )
    history = body.get("data", [])
    if not history:
        raise RuntimeError(f"[trade] No data returned for order {order_id}")
    latest = history[-1]
    print(f"[trade] {order_id}: {latest.get('status')}  "
          f"filled={latest.get('filled_quantity')}/{latest.get('quantity')}  "
          f"avg_price={latest.get('average_price')}")
    return latest


def get_orders() -> list[dict]:
    """Returns all orders placed today."""
    session, _ = get_session()
    resp = session.get(f"{BASE_URL}/orders", timeout=15)
    body = resp.json()
    if not resp.ok or body.get("status") == "error":
        raise RuntimeError(f"[trade] Could not fetch orders: {body.get('message') or body}")
    orders = body.get("data", [])
    print(f"[trade] {len(orders)} order(s) today:")
    for o in orders:
        print(f"  {o.get('order_id')}  {o.get('transaction_type'):4}  "
              f"{o.get('tradingsymbol'):20}  qty={o.get('quantity')}  "
              f"status={o.get('status')}  avg_price={o.get('average_price')}")
    return orders


# ── Kite instrument-token resolution ────────────────────────────────────────────
# Kite's historical-candle endpoint needs a numeric instrument_token per symbol
# (not the plain tradingsymbol every order call above uses). GET /instruments/NSE
# returns the WHOLE NSE instrument dump as a raw CSV body (not JSON, unlike every
# other Kite endpoint this file touches) -- cached locally and refreshed daily
# (instrument tokens can change on corporate actions, same reasoning as Dhan's
# weekly scrip-master cache in dhan/instruments.py, just a tighter window since
# this is the ONLY source this file has for tokens, with no per-symbol fallback
# lookup API any more than Dhan has one).

_token_cache: dict[str, int] = {}


def _refresh_instrument_cache_if_stale() -> None:
    _INSTRUMENTS_CACHE.parent.mkdir(parents=True, exist_ok=True)
    if _INSTRUMENTS_CACHE.exists():
        age = datetime.now() - datetime.fromtimestamp(_INSTRUMENTS_CACHE.stat().st_mtime)
        if age < _INSTRUMENTS_MAX_AGE:
            return
    session, _ = get_session()
    resp = session.get(f"{BASE_URL}/instruments/NSE", timeout=60)
    resp.raise_for_status()
    _INSTRUMENTS_CACHE.write_bytes(resp.content)
    print(f"[trade] Kite NSE instrument master saved → {_INSTRUMENTS_CACHE} "
          f"({len(resp.content):,} bytes).")


def _load_token_cache() -> None:
    if _token_cache:
        return
    _refresh_instrument_cache_if_stale()
    with open(_INSTRUMENTS_CACHE, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("instrument_type") in ("EQ", "BE"):
                sym = (row.get("tradingsymbol") or "").strip().upper()
                tok = (row.get("instrument_token") or "").strip()
                if sym and tok and sym not in _token_cache:  # first EQ/BE match wins
                    _token_cache[sym] = int(tok)


def instrument_token(symbol: str) -> int:
    """Returns the Kite instrument_token for a plain NSE equity symbol.
    Raises ValueError if not found (never guesses for a real historical-data
    request, same philosophy as dhan/instruments.py's security_id())."""
    _load_token_cache()
    tok = _token_cache.get(symbol.strip().upper())
    if not tok:
        raise ValueError(
            f"[trade] '{symbol}' not found in Kite NSE instrument master "
            f"({_INSTRUMENTS_CACHE}). Symbol may be delisted/renamed, or the cache "
            "is stale -- delete the cache file to force a re-download."
        )
    return tok


# ── Reference price (replaces the old Upstox-based fetch) ──────────────────────

def _fetch_historical_minute_candles(symbol: str, on_date: date) -> list:
    """GET /instruments/historical/:token/minute for the given calendar date.
    Returns Kite's raw candle rows: [timestamp, open, high, low, close, volume, oi].

    UNVERIFIED ASSUMPTION, flagged deliberately rather than silently trusted:
    this codebase already learned the hard way (see README's data_loader.py
    note) that Upstox's historical endpoint returns ZERO rows for same-day
    dates -- only its separate intraday endpoint has today's candles. Kite
    Connect does not document a comparable historical/intraday split (its
    historical-candle endpoint is generally understood to serve same-day
    intraday data too), but that has NOT been confirmed live against this
    account. If it turns out Kite's historical endpoint also comes back empty
    for today, get_reference_price()'s LTP fallback below still produces a
    usable (if less precise) reference price -- but this needs a real
    same-day check on the next trading day before trusting it unattended."""
    token      = instrument_token(symbol)
    session, _ = get_session()
    day_str    = on_date.isoformat()
    resp = session.get(
        f"{BASE_URL}/instruments/historical/{token}/minute",
        params={"from": f"{day_str} 09:15:00", "to": f"{day_str} 15:30:00", "oi": 0},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("data", {}).get("candles", [])


def get_reference_price(symbol: str) -> tuple[float, int]:
    """Close of 15:20 1-min candle — entry sizing. Falls back to 15:19, then to
    whichever candle at/before 15:20 is actually the most recent available (not a
    fixed older point) if neither of those posted -- e.g. an illiquid stock with
    no trades in that exact minute -- exactly the same fallback chain the old
    Upstox-based version used (common.calc_utils.pick_reference_price). If Kite's
    historical endpoint returns no candles at all for today (see the unverified-
    assumption note on _fetch_historical_minute_candles above), falls back one
    step further to a live LTP snapshot via /quote/ltp -- a same-moment price is
    a strictly worse substitute for "the 15:20 close" than a real candle, but a
    live LTP taken after 15:20 is still a far better reference than raising and
    aborting the entry outright. Returns (price, hhmm_used); hhmm_used is 0 when
    the LTP fallback was the one that fired, so callers/logs can tell the two
    apart. Raises ValueError only if BOTH the candle fetch and the LTP fallback
    fail."""
    try:
        candles = _fetch_historical_minute_candles(symbol, date.today())
    except Exception as exc:
        candles = []
        print(f"[trade]   historical candle fetch failed for {symbol}: {exc}")

    if candles:
        try:
            return pick_reference_price(candles, 1520, 1519)
        except ValueError:
            pass   # falls through to the LTP fallback below

    print(f"[trade]   no usable 15:20/15:19 candle for {symbol} — falling back to live LTP.")
    ltp = get_ltp(symbol)
    return ltp, 0


def get_ltp(symbol: str) -> float:
    """Current last-traded price via Kite's lightweight /quote/ltp endpoint.
    Raises ValueError if no LTP is returned."""
    session, _ = get_session()
    resp = session.get(f"{BASE_URL}/quote/ltp", params={"i": f"NSE:{symbol}"}, timeout=15)
    resp.raise_for_status()
    entry = resp.json().get("data", {}).get(f"NSE:{symbol}")
    if not entry or entry.get("last_price") is None:
        raise ValueError(f"[trade] No LTP found for {symbol}.")
    return float(entry["last_price"])


# ── Entry scheduling — the 15:20:00-15:21:00 staging window ────────────────────

_STAGE_START = (15, 20, 0)   # wall-clock (H, M, S) IST -- prep work begins here
_FIRE_AT     = (15, 21, 0)   # wall-clock (H, M, S) IST -- returns exactly here

def _seconds_until(hh: int, mm: int, ss: int, now: datetime | None = None) -> float:
    now    = now or datetime.now(_IST)
    target = now.replace(hour=hh, minute=mm, second=ss, microsecond=0)
    if target < now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def stage_entry_orders(symbols: list[str], capital: float) -> list[dict]:
    """The 15:20:00-15:21:00 prep window, bundled into one call: for every
    symbol, fetch its reference price and size shares off the capital/4-or-n
    allocation -- then block until exactly 15:21:00 IST before returning the
    finished list, so run_trades.py's entry stage can fire every order back to
    back the instant this returns, through the SAME throttled place_order()
    above, rather than computing while sending (which would eat into the
    5-orders/sec budget with reference-price/margin lookups mixed in).

    If called before 15:20:00, sleeps until then first (assumes the candle
    isn't posted yet regardless of Kite's actual publish timing -- fetching
    early just means more retries below, not a correctness problem). If
    called after 15:21:00 already, skips both waits and returns immediately
    -- e.g. a manual/late run, where waiting further would just delay entry
    for no benefit.

    Returns a list of {"symbol", "shares", "ref_price", "ref_hhmm"} dicts,
    already filtered to shares > 0 and a successfully resolved reference
    price -- callers can iterate straight into buy() without re-checking
    either.
    """
    wait_s = _seconds_until(*_STAGE_START)
    if 0 < wait_s <= 60:
        print(f"[trade] Staging: waiting {wait_s:.1f}s for the 15:20:00 window to open…")
        time.sleep(wait_s)

    n          = len(symbols)
    allocation = compute_allocation(capital, n)
    print(f"[trade] Staging entry for {n} symbol(s) — ₹{capital:,.0f} total, "
          f"₹{allocation:,.0f}/position.")

    staged = []
    for sym in symbols:
        try:
            ref, ref_hhmm = get_reference_price(sym)
        except Exception as exc:
            print(f"[trade]   {sym}: SKIP — no reference price ({exc}).")
            continue
        shares = compute_shares(allocation, ref)
        if shares == 0:
            print(f"[trade]   {sym}: SKIP — 0 shares at ₹{ref:,.2f} (allocation ₹{allocation:,.0f}).")
            continue
        print(f"[trade]   {sym}: ref ₹{ref:,.2f}"
              + (f" ({ref_hhmm//100:02d}:{ref_hhmm%100:02d})" if ref_hhmm else " (LTP fallback)")
              + f"  ·  {shares} shares")
        staged.append({"symbol": sym, "shares": shares, "ref_price": ref, "ref_hhmm": ref_hhmm})

    hold_s = _seconds_until(*_FIRE_AT)
    if 0 < hold_s <= 60:
        print(f"[trade] Staged {len(staged)}/{n} symbol(s) — holding {hold_s:.1f}s until 15:21:00…")
        time.sleep(hold_s)

    return staged


# ── CLI entry point ────────────────────────────────────────────────────────────

def _usage():
    print(__doc__)
    sys.exit(1)


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        _usage()

    dry_run = "--dry-run" in args
    args = [a for a in args if a != "--dry-run"]
    cmd = args[0].lower()

    try:
        if cmd in ("buy", "sell") and len(args) >= 5:
            # buy/sell SYMBOL EXCHANGE QTY ORDER_TYPE [PRICE] [--dry-run]
            symbol    = args[1]
            exchange  = args[2]
            qty       = int(args[3])
            otype     = args[4].upper()
            price     = float(args[5]) if len(args) > 5 else 0
            product   = args[6].upper() if len(args) > 6 else "CNC"
            place_order(symbol, exchange, cmd.upper(), qty,
                        order_type=otype, price=price, product=product, dry_run=dry_run)

        elif cmd == "cancel" and len(args) == 2:
            cancel_order(args[1])

        elif cmd == "status" and len(args) == 2:
            order_status(args[1])

        elif cmd == "orders":
            get_orders()

        else:
            _usage()

    except (RuntimeError, EnvironmentError, ValueError) as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        sys.exit(1)
