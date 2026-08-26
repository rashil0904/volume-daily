"""
Dhan trading functions — buy, sell, cancel, check orders, rate limiting, and
symbol -> securityId/tick-size resolution.
================================================================================
The only file that talks to Dhan's order endpoints directly. Three
responsibilities (per the dhan/ 4-file redesign, mirroring zerodha/trade.py):

  1. Order placement  — place_order()/buy()/sell()/cancel_order()/order_status()/
     get_orders(), same shapes as before.
  2. Rate limiting    — every order-placing/cancelling call goes through a single
     process-wide sliding-window token bucket capped at 5 orders/sec (see
     RateLimiter/rate_limiter below), so run_trades.py's parallel entry/exit
     batches (each symbol's order fired from its own thread-pool worker) never
     exceed Dhan's order-endpoint rate limit no matter how many threads import
     this module.
  3. Instrument resolution — security_id()/tick_size() (absorbed from the old
     dhan/instruments.py, folded in here rather than kept as a separate file --
     see the dhan/ 4-file redesign). Dhan requires a numeric securityId per
     order (not a plain tradingsymbol like Kite accepts) and a per-symbol tick
     size for any LIMIT price (NOT a flat ₹0.05 across NSE equities -- see
     tick_size()'s docstring). Both are sourced from Dhan's own ~200k-row NSE
     scrip master CSV, cached locally and refreshed weekly.

Requires a valid session from dhan.auth. Run `python -m dhan.auth
<ACCESS_TOKEN>` once each morning to authenticate, then import these
functions freely.

Note: unlike Kite, Dhan's order API has no "market_protection"-style
collar param for MARKET orders (confirmed against DhanHQ docs, 2026-08-14)
-- there's nothing to set here, and consequently no risk of a Kite-style
false rejection from a too-tight auto-picked collar either.

Quick CLI usage:
    python -m dhan.trade buy  RELIANCE 1 MARKET
    python -m dhan.trade sell RELIANCE 1 MARKET
    python -m dhan.trade buy  RELIANCE 1 LIMIT 2850.50
    python -m dhan.trade status <order_id>
    python -m dhan.trade cancel <order_id>
    python -m dhan.trade orders

Supported values:
    exchange_segment : NSE_EQ (only equity is wired up here)
    order_type        : MARKET, LIMIT, STOP_LOSS, STOP_LOSS_MARKET
    product           : CNC (delivery), MTF (margin-funded delivery), INTRADAY
"""

import csv
import sys
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dhan.auth import BASE_URL, get_session

_ROOT       = Path(__file__).resolve().parent.parent
_CACHE_FILE = _ROOT / "data" / "instruments" / "dhan_scrip_master.csv"
_SOURCE_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
_MAX_AGE    = timedelta(days=7)


# ── Rate limiter (process-wide, 5 orders/sec, sliding window) ───────────────────
# A single token bucket that EVERY order-placing/cancelling call in this module
# goes through -- run_trades.py's parallel entry/exit batches (each symbol's
# order fired from its own ThreadPoolExecutor worker) all end up calling
# buy()/sell()/place_order()/cancel_order() from HERE, so this is the one place
# that caps COMBINED throughput across every thread in the process, not each
# caller/thread pacing itself independently -- there is exactly one
# `rate_limiter` instance (module-level singleton below), never one per call
# site or per pool.
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


# ── Instrument resolution (symbol -> Dhan securityId / tick size) ──────────────
# Kite accepts a plain tradingsymbol on every order; Dhan requires a numeric
# securityId instead. There's no per-symbol lookup API for this -- Dhan
# publishes a single instrument/scrip master CSV covering every exchange and
# segment (~200k rows) and expects callers to filter it themselves.
#
# Verified live (2026-08-14) against https://images.dhan.co/api-data/api-scrip-master.csv:
# filtering SEM_EXM_EXCH_ID=='NSE', SEM_SEGMENT=='E', SEM_SERIES=='EQ' and
# matching SEM_TRADING_SYMBOL gives exactly one row per plain NSE equity
# symbol, with SEM_SMST_SECURITY_ID as the securityId Dhan's order/margin/quote
# APIs expect. Spot-checked: RELIANCE->2885, CAPILLARY->759867, KKCL->13381,
# EPACKPEB->759251, ARVSMART->10457.

_cache: dict[str, str] = {}
_tick_cache: dict[str, float] = {}


def _refresh_if_stale() -> None:
    _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    if _CACHE_FILE.exists():
        age = datetime.now() - datetime.fromtimestamp(_CACHE_FILE.stat().st_mtime)
        if age < _MAX_AGE:
            return
    print(f"[dhan] Downloading instrument master → {_CACHE_FILE} ...")
    resp = requests.get(_SOURCE_URL, timeout=60)
    resp.raise_for_status()
    _CACHE_FILE.write_bytes(resp.content)
    print(f"[dhan] Instrument master saved ({len(resp.content):,} bytes).")


def _load_cache() -> None:
    global _cache, _tick_cache
    if _cache:
        return
    _refresh_if_stale()
    with open(_CACHE_FILE, newline="") as f:
        for row in csv.DictReader(f):
            if (row.get("SEM_EXM_EXCH_ID") == "NSE"
                    and row.get("SEM_SEGMENT") == "E"
                    and row.get("SEM_SERIES") == "EQ"):
                sym = (row.get("SEM_TRADING_SYMBOL") or "").strip().upper()
                sid = (row.get("SEM_SMST_SECURITY_ID") or "").strip()
                if sym and sid:
                    _cache[sym] = sid
                tick_raw = (row.get("SEM_TICK_SIZE") or "").strip()
                if sym and tick_raw:
                    try:
                        # SEM_TICK_SIZE is in paise, not rupees -- confirmed via Dhan's
                        # own field description ("Minimum decimal point at which an
                        # instrument can be priced") plus the raw value distribution
                        # across the whole NSE-EQ master (1/5/10/50/100/500), which only
                        # makes sense as paise (0.01/0.05/0.10/0.50/1.00/5.00 rupees) --
                        # a literal "500 rupee tick" would be absurd for any equity.
                        # Confirmed live 2026-08-18: TVSSRICHAK's raw tick is 10.0000
                        # (-> ₹0.10), NOT the ₹0.05 every NSE equity was previously
                        # assumed to share -- that wrong assumption is what caused two
                        # real order rejections (EXCH:16283) earlier that day.
                        _tick_cache[sym] = float(tick_raw) / 100
                    except ValueError:
                        pass


def security_id(symbol: str) -> str:
    """Returns the Dhan securityId for a plain NSE equity symbol.
    Raises ValueError if the symbol isn't found (never guesses for a real order)."""
    _load_cache()
    sid = _cache.get(symbol.strip().upper())
    if not sid:
        raise ValueError(
            f"[dhan] '{symbol}' not found in NSE equity instrument master "
            f"({_CACHE_FILE}). Symbol may be delisted/renamed, or the cache is stale "
            "-- delete the cache file to force a re-download."
        )
    return sid


def tick_size(symbol: str) -> float:
    """Returns the minimum valid LIMIT-price increment (in rupees) for a plain NSE
    equity symbol, per Dhan's own scrip master (SEM_TICK_SIZE, paise). Raises
    ValueError if the symbol/tick isn't found -- never guesses a tick for a real
    order, same philosophy as security_id() above."""
    _load_cache()
    tick = _tick_cache.get(symbol.strip().upper())
    if not tick:
        raise ValueError(
            f"[dhan] '{symbol}' has no tick size in the NSE equity instrument master "
            f"({_CACHE_FILE}). Symbol may be delisted/renamed, or the cache is stale "
            "-- delete the cache file to force a re-download."
        )
    return tick


# ── Core order functions ───────────────────────────────────────────────────────

def _disclosed_qty(quantity: int, order_type: str, disclosed_pct: float) -> int:
    """NSE's Disclosed Quantity (DQ) feature -- only the returned amount shows
    on the public order book, the rest fills silently as it's matched.
    Exchange-side rules (both NSE and BSE cash segment): only valid for LIMIT
    orders, and DQ must be strictly less than the order quantity (0 disables
    it, which is also what a MARKET/STOP_LOSS* order type or disclosed_pct<=0
    falls back to here)."""
    if order_type != "LIMIT" or disclosed_pct <= 0 or quantity <= 1:
        return 0
    dq = int(round(quantity * disclosed_pct))
    return max(0, min(dq, quantity - 1))


def place_order(
    symbol: str,
    exchange_segment: str,
    transaction_type: str,          # "BUY" or "SELL"
    quantity: int,
    order_type: str  = "MARKET",    # MARKET | LIMIT | STOP_LOSS | STOP_LOSS_MARKET
    price: float     = 0,           # required for LIMIT and STOP_LOSS
    trigger_price: float = 0,       # required for STOP_LOSS / STOP_LOSS_MARKET
    product: str     = "CNC",       # CNC | MTF | INTRADAY
    validity: str    = "DAY",
    tag: str         = "",          # correlationId, max 30 chars
    disclosed_pct: float = 0.20,    # fraction of quantity shown on the book (LIMIT only); rest hidden
    dry_run: bool    = False,       # print payload only, no real order
) -> str:
    """
    Places an order and returns the orderId.
    Pass dry_run=True to validate params and print the payload without sending.
    Raises RuntimeError on failure with the exact error from Dhan.
    """
    transaction_type = transaction_type.upper()
    exchange_segment = exchange_segment.upper()
    order_type       = order_type.upper()
    product          = product.upper()

    if transaction_type not in ("BUY", "SELL"):
        raise ValueError(f"transaction_type must be BUY or SELL, got: {transaction_type!r}")
    if order_type not in ("MARKET", "LIMIT", "STOP_LOSS", "STOP_LOSS_MARKET"):
        raise ValueError(f"order_type must be MARKET/LIMIT/STOP_LOSS/STOP_LOSS_MARKET, got: {order_type!r}")
    if order_type == "LIMIT" and not price:
        raise ValueError("price is required for LIMIT orders")
    if order_type in ("STOP_LOSS", "STOP_LOSS_MARKET") and not trigger_price:
        raise ValueError("trigger_price is required for STOP_LOSS/STOP_LOSS_MARKET orders")

    sid = security_id(symbol)
    dq  = _disclosed_qty(quantity, order_type, disclosed_pct)

    session, client_id = get_session()

    payload = {
        "dhanClientId":     client_id,
        "transactionType":  transaction_type,
        "exchangeSegment":  exchange_segment,
        "productType":      product,
        "orderType":        order_type,
        "validity":         validity,
        "securityId":       sid,
        "quantity":         quantity,
        "disclosedQuantity": dq,
        "price":            price,
        "triggerPrice":     trigger_price,
    }
    if tag:
        payload["correlationId"] = tag[:30]

    if dry_run:
        print("[trade] ── DRY RUN — no real order placed ──────────────────")
        print(f"[trade] POST {BASE_URL}/orders")
        print("[trade] Payload:")
        for k, v in payload.items():
            print(f"  {k:20} = {v}")
        print("[trade] ───────────────────────────────────────────────────")
        return "DRY_RUN"

    print(f"[trade] Placing {transaction_type} {quantity}× {symbol} (securityId {sid}) @ {order_type}"
          + (f" ₹{price}" if price else "")
          + f"  [{exchange_segment} · {product}]"
          + (f"  (disclosed {dq}/{quantity})" if dq else ""))

    rate_limiter.acquire()
    resp = session.post(f"{BASE_URL}/orders", json=payload, timeout=15)

    body = resp.json()
    if not resp.ok or body.get("orderStatus") == "REJECTED":
        raise RuntimeError(f"[trade] Order failed: {body}")

    order_id = body.get("orderId")
    if not order_id:
        raise RuntimeError(f"[trade] No orderId in response: {body}")
    print(f"[trade] Order placed — orderId: {order_id}  status: {body.get('orderStatus')}")
    return order_id


def buy(symbol: str, exchange_segment: str, quantity: int,
        order_type: str = "MARKET", price: float = 0,
        product: str = "CNC", **kwargs) -> str:
    """Shorthand for place_order with transaction_type=BUY."""
    return place_order(symbol, exchange_segment, "BUY", quantity,
                       order_type=order_type, price=price, product=product, **kwargs)


def sell(symbol: str, exchange_segment: str, quantity: int,
         order_type: str = "MARKET", price: float = 0,
         product: str = "CNC", **kwargs) -> str:
    """Shorthand for place_order with transaction_type=SELL."""
    return place_order(symbol, exchange_segment, "SELL", quantity,
                       order_type=order_type, price=price, product=product, **kwargs)


def cancel_order(order_id: str) -> str:
    """Cancels an open order. Returns the order_id on success."""
    session, _ = get_session()
    rate_limiter.acquire()
    resp = session.delete(f"{BASE_URL}/orders/{order_id}", timeout=15)
    body = resp.json()
    if not resp.ok:
        raise RuntimeError(f"[trade] Cancel failed: {body}")
    print(f"[trade] Order {order_id} cancelled.")
    return order_id


def order_status(order_id: str) -> dict:
    """Returns the latest status dict for a specific order."""
    session, _ = get_session()
    resp = session.get(f"{BASE_URL}/orders/{order_id}", timeout=15)
    body = resp.json()
    if not resp.ok:
        raise RuntimeError(f"[trade] Could not fetch order {order_id}: {body}")
    latest = body[-1] if isinstance(body, list) else body
    print(f"[trade] {order_id}: {latest.get('orderStatus')}  "
          f"filled={latest.get('filledQty')}/{latest.get('quantity')}  "
          f"avg_price={latest.get('averageTradedPrice')}")
    return latest


def get_orders() -> list[dict]:
    """Returns all orders placed today."""
    session, _ = get_session()
    resp = session.get(f"{BASE_URL}/orders", timeout=15)
    orders = resp.json()
    if not resp.ok:
        raise RuntimeError(f"[trade] Could not fetch orders: {orders}")
    print(f"[trade] {len(orders)} order(s) today:")
    for o in orders:
        print(f"  {o.get('orderId')}  {o.get('transactionType'):4}  "
              f"{o.get('tradingSymbol', ''):20}  qty={o.get('quantity')}  "
              f"status={o.get('orderStatus')}  avg_price={o.get('averageTradedPrice')}")
    return orders


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
        if cmd in ("buy", "sell") and len(args) >= 3:
            # buy/sell SYMBOL QTY ORDER_TYPE [PRICE] [PRODUCT] [--dry-run]
            symbol  = args[1]
            qty     = int(args[2])
            otype   = args[3].upper() if len(args) > 3 else "MARKET"
            price   = float(args[4]) if len(args) > 4 else 0
            product = args[5].upper() if len(args) > 5 else "CNC"
            place_order(symbol, "NSE_EQ", cmd.upper(), qty,
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
