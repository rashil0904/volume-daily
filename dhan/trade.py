"""
Dhan trading functions — buy, sell, cancel, and check orders.

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

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dhan.auth import BASE_URL, get_session
from dhan.instruments import security_id

# ── Core order functions ───────────────────────────────────────────────────────

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
          + f"  [{exchange_segment} · {product}]")

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
