"""
Zerodha trading functions — buy, sell, cancel, and check orders.

Requires a valid session from zerodha_auth. Run zerodha_auth.py once
each morning to authenticate, then import these functions freely.

Quick CLI usage:
    python zerodha_trade.py buy  INFY NSE 1 MARKET
    python zerodha_trade.py sell INFY NSE 1 MARKET
    python zerodha_trade.py buy  RELIANCE NSE 1 LIMIT 2850.50
    python zerodha_trade.py status <order_id>
    python zerodha_trade.py cancel <order_id>
    python zerodha_trade.py orders

Supported values:
    exchange     : NSE, BSE
    order_type   : MARKET, LIMIT, SL, SL-M
    product      : CNC (delivery), MIS (intraday), NRML (F&O)
    variety      : regular (default), amo (after-market)
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from zerodha.auth import BASE_URL, get_session

# ── Core order functions ───────────────────────────────────────────────────────

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
    market_protection: float = 0,   # required by Kite for MARKET/SL-M; 0 = no extra protection band
    tag: str         = "",          # optional identifier (max 20 chars)
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
          + f"  [{exchange} · {product} · {variety}]")

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
    resp = session.delete(f"{BASE_URL}/orders/{variety}/{order_id}", timeout=15)
    body = resp.json()
    if not resp.ok or body.get("status") == "error":
        raise RuntimeError(
            f"[trade] Cancel failed: {body.get('message') or body}"
        )
    print(f"[trade] Order {order_id} cancelled.")
    return order_id


def order_status(order_id: str) -> dict:
    """Returns the latest status dict for a specific order."""
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
