"""
Upstox trading functions — buy, sell, cancel, and check orders.

Supports live and sandbox modes. Pass --mode sandbox to use sandbox credentials.

Symbol lookup uses data/instruments/upstox_instruments.csv (your tracked universe).
For stocks outside the universe, pass the instrument_token directly instead of a symbol:
    python upstox/trade.py buy "NSE_EQ|INE062A01020" NSE 100 MARKET

Quick CLI usage:
    python upstox/trade.py buy  CYIENTDLM NSE 10 MARKET
    python upstox/trade.py sell CYIENTDLM NSE 10 MARKET
    python upstox/trade.py buy  CYIENTDLM NSE 10 LIMIT 580.00
    python upstox/trade.py status <order_id>
    python upstox/trade.py cancel <order_id>
    python upstox/trade.py orders
    python upstox/trade.py buy CYIENTDLM NSE 10 MARKET --dry-run
    python upstox/trade.py buy CYIENTDLM NSE 10 MARKET --mode sandbox

Supported values:
    order_type : MARKET, LIMIT, SL, SL-M
    product    : D (delivery/CNC), I (intraday/MIS)
"""

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from upstox.auth import get_session

_INSTRUMENTS_FILE = Path(__file__).resolve().parent.parent / "data" / "instruments" / "upstox_instruments.csv"

_instrument_cache: dict[str, str] = {}


def _resolve_instrument_token(symbol_or_token: str) -> str:
    """
    If the input already looks like an instrument_token (contains '|'), return it directly.
    Otherwise look up the symbol in our instruments CSV.
    """
    if "|" in symbol_or_token:
        return symbol_or_token

    global _instrument_cache
    if not _instrument_cache and _INSTRUMENTS_FILE.exists():
        with open(_INSTRUMENTS_FILE, newline="") as f:
            for row in csv.DictReader(f):
                _instrument_cache[row["symbol"].strip().upper()] = row["instrument_key"].strip()

    token = _instrument_cache.get(symbol_or_token.upper())
    if not token:
        raise ValueError(
            f"[upstox] '{symbol_or_token}' not found in instruments file.\n"
            f"Pass the instrument_token directly instead, e.g. 'NSE_EQ|INE062A01020'.\n"
            f"Find tokens at: https://account.upstox.com/developer/apps"
        )
    return token


# ── Core order functions ───────────────────────────────────────────────────────

def place_order(
    symbol: str,
    exchange: str,
    transaction_type: str,           # "BUY" or "SELL"
    quantity: int,
    order_type: str    = "MARKET",   # MARKET | LIMIT | SL | SL-M
    price: float       = 0,
    trigger_price: float = 0,
    product: str       = "D",        # D (delivery) | I (intraday)
    tag: str           = "",
    is_amo: bool       = False,
    dry_run: bool      = False,
    mode: str          = "live",     # "live" or "sandbox"
) -> str:
    """
    Places an order and returns the order_id.
    `symbol` can be a trading symbol (looked up in instruments CSV) or a raw instrument_token.
    Pass dry_run=True to print the payload without placing a real order.
    """
    transaction_type = transaction_type.upper()
    order_type       = order_type.upper()
    product          = product.upper()

    if transaction_type not in ("BUY", "SELL"):
        raise ValueError(f"transaction_type must be BUY or SELL, got: {transaction_type!r}")
    if order_type not in ("MARKET", "LIMIT", "SL", "SL-M"):
        raise ValueError(f"order_type must be MARKET/LIMIT/SL/SL-M, got: {order_type!r}")
    if product not in ("D", "I"):
        raise ValueError(f"product must be D (delivery) or I (intraday), got: {product!r}")
    if order_type == "LIMIT" and not price:
        raise ValueError("price is required for LIMIT orders")
    if order_type in ("SL", "SL-M") and not trigger_price:
        raise ValueError("trigger_price is required for SL/SL-M orders")

    instrument_token = _resolve_instrument_token(symbol)

    payload = {
        "instrument_token":   instrument_token,
        "transaction_type":   transaction_type,
        "order_type":         order_type,
        "quantity":           quantity,
        "product":            product,
        "price":              price,
        "trigger_price":      trigger_price,
        "disclosed_quantity": 0,
        "validity":           "DAY",
        "is_amo":             is_amo,
    }
    if tag:
        payload["tag"] = tag[:20]

    if dry_run:
        print(f"[upstox/{mode}] ── DRY RUN — no real order placed ──────────")
        print(f"[upstox/{mode}] Symbol          : {symbol.upper()} ({exchange.upper()}) → {instrument_token}")
        print(f"[upstox/{mode}] Payload:")
        for k, v in payload.items():
            print(f"  {k:22} = {v}")
        print(f"[upstox/{mode}] ────────────────────────────────────────────")
        return "DRY_RUN"

    session, base_url = get_session(mode)
    print(f"[upstox/{mode}] Placing {transaction_type} {quantity}× {symbol} @ {order_type}"
          + (f" ₹{price}" if price else ""))

    resp = session.post(f"{base_url}/order/place", json=payload, timeout=15)
    body = resp.json()
    if not resp.ok or body.get("status") == "error":
        raise RuntimeError(
            f"[upstox/{mode}] Order failed: {body.get('message') or body.get('errors') or body}"
        )

    order_id = body["data"]["order_id"]
    print(f"[upstox/{mode}] Order placed — order_id: {order_id}")
    return order_id


def buy(symbol: str, exchange: str, quantity: int,
        order_type: str = "MARKET", price: float = 0,
        product: str = "D", **kwargs) -> str:
    return place_order(symbol, exchange, "BUY", quantity,
                       order_type=order_type, price=price, product=product, **kwargs)


def sell(symbol: str, exchange: str, quantity: int,
         order_type: str = "MARKET", price: float = 0,
         product: str = "D", **kwargs) -> str:
    return place_order(symbol, exchange, "SELL", quantity,
                       order_type=order_type, price=price, product=product, **kwargs)


def cancel_order(order_id: str, mode: str = "live") -> str:
    session, base_url = get_session(mode)
    resp = session.delete(f"{base_url}/order/cancel", params={"order_id": order_id}, timeout=15)
    body = resp.json()
    if not resp.ok or body.get("status") == "error":
        raise RuntimeError(f"[upstox/{mode}] Cancel failed: {body.get('message') or body}")
    print(f"[upstox/{mode}] Order {order_id} cancelled.")
    return order_id


def order_status(order_id: str, mode: str = "live") -> dict:
    session, base_url = get_session(mode)
    resp = session.get(f"{base_url}/order/details", params={"order_id": order_id}, timeout=15)
    body = resp.json()
    if not resp.ok or body.get("status") == "error":
        raise RuntimeError(f"[upstox/{mode}] Could not fetch order: {body.get('message') or body}")
    o = body.get("data", {})
    print(f"[upstox/{mode}] {order_id}: {o.get('status')}  "
          f"filled={o.get('filled_quantity')}/{o.get('quantity')}  "
          f"avg_price={o.get('average_price')}")
    return o


def get_orders(mode: str = "live") -> list[dict]:
    session, base_url = get_session(mode)
    resp = session.get(f"{base_url}/order/retrieve-all", timeout=15)
    body = resp.json()
    if not resp.ok or body.get("status") == "error":
        raise RuntimeError(f"[upstox/{mode}] Could not fetch orders: {body.get('message') or body}")
    orders = body.get("data", [])
    print(f"[upstox/{mode}] {len(orders)} order(s) today:")
    for o in orders:
        print(f"  {o.get('order_id')}  {o.get('transaction_type'):4}  "
              f"{o.get('trading_symbol'):20}  qty={o.get('quantity')}  "
              f"status={o.get('status')}  avg_price={o.get('average_price')}")
    return orders


# ── CLI ────────────────────────────────────────────────────────────────────────

def _usage():
    print(__doc__)
    sys.exit(1)


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        _usage()

    dry_run = "--dry-run" in args
    args    = [a for a in args if a != "--dry-run"]

    mode = "live"
    if "--mode" in args:
        idx  = args.index("--mode")
        mode = args[idx + 1].lower()
        args = args[:idx] + args[idx + 2:]

    cmd = args[0].lower()

    try:
        if cmd in ("buy", "sell") and len(args) >= 5:
            symbol   = args[1]
            exchange = args[2]
            qty      = int(args[3])
            otype    = args[4].upper()
            price    = float(args[5]) if len(args) > 5 else 0
            product  = args[6].upper() if len(args) > 6 else "D"
            place_order(symbol, exchange, cmd.upper(), qty,
                        order_type=otype, price=price, product=product,
                        dry_run=dry_run, mode=mode)

        elif cmd == "cancel" and len(args) == 2:
            cancel_order(args[1], mode=mode)

        elif cmd == "status" and len(args) == 2:
            order_status(args[1], mode=mode)

        elif cmd == "orders":
            get_orders(mode=mode)

        else:
            _usage()

    except (RuntimeError, EnvironmentError, ValueError) as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        sys.exit(1)
