"""
Dhan trade charges — per-trade brokerage/STT/exchange/SEBI/stamp/GST breakdown,
plus fixed DP/pledge-unpledge charges kept as their own separate total (see
print_report: "TRADE LEG CHARGES" vs "FIXED POSITION-LEVEL CHARGES" below).

Pulls actual executed-trade charge fields from Dhan's historical trade-book
endpoint (GET /trades/{from-date}/{to-date}/{page-number}) -- unlike the plain
/trades endpoint (today only, no charges breakdown by itself beyond what's on
the trade record), this one works for any date range and every trade record
already carries brokerageCharges/stt/exchangeTransactionCharges/sebiTax/
stampDuty/serviceTax individually.

A single symbol can generate up to FOUR separate trade legs in one day
through run_trades.py, each with a genuinely different charge profile:
    1. 3:21pm entry BUY       -- delivery (CNC or MTF), full charge set
    2. 9:25am/11:59am SELL    -- closes #1, same delivery position, own
                                 charge numbers (e.g. no stamp duty on sells)
    3. 9:25am/11:59am SELL    -- the shorting add-on's short-OPEN, a brand
                                 new INTRADAY position in the same symbol
    4. 2:39pm BUY             -- covers #3, INTRADAY, same day squared off
Two fixed costs Dhan doesn't return per-trade are added on top of the API
figures, and ONLY on leg #1 (the delivery entry BUY):
    - DP (Depository Participant) charge: ₹14.75.
    - Pledge/unpledge charge: ₹35.40, MTF trades only (MTF positions are
      auto-pledged as collateral for the funded leg and unpledged on exit).
These are one-time-per-position costs, not per-leg, so they must NOT be
re-added on leg #2 (matching SELL close) nor on legs #3/#4 (the shorting
add-on's INTRADAY legs never touch the demat account or MTF collateral at
all, even though leg #4 is also a BUY). See is_delivery_buy() -- it gates on
both transactionType=="BUY" AND the order's product (from
positions_dhan.json) being CNC/MTF specifically, which is what correctly
excludes leg #4 despite also being a BUY. Every leg's own 6 API charge
fields (brokerage/STT/exchange/SEBI/stamp/GST) are always read as-is
regardless of which of the 4 legs it is -- those numbers are already
genuinely different per leg/product, nothing to fix there.
MTF-ness is read from results/positions_dhan.json's "product" field (keyed
by entry_order_id) since the trade record's own productType is usually null
for MTF fills (only reliably populated for CNC, confirmed 2026-08-19). Trades
with no matching position record (not placed by this pipeline, or an
INTRADAY short/cover leg keyed under a different id than entry_order_id)
are treated as non-delivery -- no DP or pledge/unpledge charge is added.

MTF interest (--mtf-interest) is calculated separately from the trade-book
charges above, since it isn't a per-trade fee -- it accrues daily on the
funded (borrowed) amount of every currently-open MTF position, at Dhan's
slab rate for that funded amount:
    Up to ₹500                    : no interest
    ₹500.01     – ₹5,00,000       : 12.49% p.a. (0.0342% per day)
    ₹5,00,000.01 – ₹10,00,000     : 13.49% p.a. (0.0369% per day)
    ₹10,00,000.01 – ₹25,00,000    : 14.49% p.a. (0.0397% per day)
    ₹25,00,000.01 – ₹5,00,00,000  : 15.49% p.a. (0.0425% per day)
The whole funded amount is charged at whichever single slab it falls into
(not a marginal/progressive bracket). Days held is a plain calendar-day
difference between entry_date and the as-of date -- e.g. bought Friday, sold
Monday is 3 days, since that's 3 calendar days apart, weekends included same
as any other day.

Quick CLI usage:
    python -m dhan.charges 2026-08-18 2026-08-19
    python -m dhan.charges 2026-08-18 2026-08-19 --tracked-only
    python -m dhan.charges 2026-08-18 2026-08-19 --symbol "TVS Srichakra"
    python -m dhan.charges --mtf-interest [AS_OF_DATE, default today]

--tracked-only restricts the report to orderIds found in
results/positions_dhan.json (i.e. trades this pipeline itself placed),
filtering out any other activity on the account in the same date range.
"""

import sys
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import json
from zoneinfo import ZoneInfo
from datetime import datetime
from dhan.auth import BASE_URL, get_session
from dhan.instruments import security_id

_CHARGE_FIELDS = [
    ("brokerageCharges",          "Brokerage"),
    ("stt",                       "STT"),
    ("exchangeTransactionCharges","Exchange"),
    ("sebiTax",                   "SEBI"),
    ("stampDuty",                 "Stamp"),
    ("serviceTax",                "GST"),
]

_DP_CHARGE                = 14.75  # fixed, every trade
_MTF_PLEDGE_UNPLEDGE_CHARGE = 35.40  # fixed, MTF trades only

# (upper bound inclusive, daily rate %) -- whole funded amount taxed at one slab
_MTF_INTEREST_SLABS = [
    (500,        0.0),
    (500_000,    0.0342),
    (1_000_000,  0.0369),
    (2_500_000,  0.0397),
    (50_000_000, 0.0425),
]

_IST      = ZoneInfo("Asia/Kolkata")
_POS_FILE = Path(__file__).resolve().parent.parent / "results" / "positions_dhan.json"


def product_map() -> dict[str, str]:
    """entry_order_id -> product ("MTF"/"CNC"/...) from positions_dhan.json."""
    if not _POS_FILE.exists():
        return {}
    positions = json.loads(_POS_FILE.read_text())
    return {p["entry_order_id"]: p["product"] for p in positions
            if p.get("entry_order_id") and p.get("product")}


def is_delivery_buy(trade: dict, products: dict[str, str]) -> bool:
    """True only for the 3:21pm entry BUY on a CNC/MTF position -- i.e. an
    actual delivery trade that lands in the demat account. Deliberately
    False for:
      - any SELL (the 9:25/11:59 close of that same delivery position --
        different charge fields, no DP/pledge re-charge, see trade_charges)
      - the shorting add-on's INTRADAY legs: the 9:25/11:59 short-open SELL
        and the 2:39 cover BUY. Both are same-day squared-off intraday
        trades that never touch the demat account or MTF collateral, so
        DP/pledge must never apply to them even though the cover leg is a
        BUY. Matched by requiring the order's product (from
        positions_dhan.json) to be CNC/MTF specifically, not just "found".
    """
    if trade.get("transactionType") != "BUY":
        return False
    return products.get(trade.get("orderId")) in ("CNC", "MTF")


def is_mtf_trade(trade: dict, products: dict[str, str]) -> bool:
    """MTF-ness for a trade record -- see module docstring for why this reads
    positions_dhan.json rather than trusting the trade record's own
    (usually-null) productType field."""
    return products.get(trade.get("orderId")) == "MTF"


def trade_api_charges(trade: dict) -> float:
    """Sums ONLY the 6 API charge fields for this leg (read as-is regardless
    of side/product -- they're genuinely different numbers for delivery vs
    intraday and for BUY vs SELL). Does NOT include DP/pledge-unpledge --
    those are one-time-per-position fixed costs, kept as their own separate
    total (see fixed_charges_for_trades) rather than folded into either leg's
    total, per-trade or per-position."""
    return sum(trade.get(field, 0.0) or 0.0 for field, _ in _CHARGE_FIELDS)


def estimate_trade_charges(quantity: int, price: float, product: str,
                           transaction_type: str) -> float:
    """Estimates the same 6 API charge fields (brokerage/STT/exchange/SEBI/
    stamp/GST) for one trade leg from Dhan's PUBLISHED rate card, for use
    ONLY when the real per-trade figures can't be pulled from the trade-book
    API (outage, or a same-day trade not yet indexed there) -- see
    position_charge_summary's api-then-estimate fallback. NSE only (this
    pipeline never trades BSE). Rates verified live against
    https://dhan.co/pricing/ on 2026-08-20:

        CNC       brokerage 0; STT 0.1% both sides; stamp 0.015% buy-side only
        INTRADAY  brokerage min(Rs20, 0.03% of turnover); STT 0.025%
                  sell-side only; stamp 0.003% buy-side only
        MTF       brokerage same formula as INTRADAY; STT/stamp same as CNC

    Exchange transaction charges (0.0030699%) and SEBI turnover fees
    (0.0001%) are identical across all three products. GST is 18% on
    (brokerage + exchange charges + SEBI charges) only -- STT and stamp
    duty are government taxes, not GST-able fees, same convention Dhan's
    own charge sheet uses."""
    product          = product.upper()
    transaction_type = transaction_type.upper()
    turnover         = quantity * price

    if product == "CNC":
        brokerage = 0.0
        stt = turnover * 0.001                                    # 0.1%, both sides
        stamp = turnover * 0.00015 if transaction_type == "BUY" else 0.0   # 0.015%, buy-side only
    elif product == "MTF":
        brokerage = min(20.0, turnover * 0.0003)                  # Rs20 or 0.03%, whichever lower
        stt = turnover * 0.001                                    # 0.1%, both sides (same as CNC)
        stamp = turnover * 0.00015 if transaction_type == "BUY" else 0.0   # 0.015%, buy-side only (same as CNC)
    else:  # INTRADAY
        brokerage = min(20.0, turnover * 0.0003)                  # Rs20 or 0.03%, whichever lower
        stt = turnover * 0.00025 if transaction_type == "SELL" else 0.0    # 0.025%, sell-side only
        stamp = turnover * 0.00003 if transaction_type == "BUY" else 0.0   # 0.003%, buy-side only

    exchange_txn = turnover * 0.000030699   # NSE 0.0030699%
    sebi         = turnover * 0.000001      # 0.0001%
    gst          = (brokerage + exchange_txn + sebi) * 0.18

    return brokerage + stt + exchange_txn + sebi + stamp + gst


def fixed_charges(trade: dict, products: dict[str, str]) -> tuple[float, float]:
    """(dp_charge, pledge_unpledge_charge) for this trade -- both non-zero
    only on the genuine delivery entry BUY leg (see is_delivery_buy)."""
    if not is_delivery_buy(trade, products):
        return 0.0, 0.0
    dp = _DP_CHARGE
    pledge = _MTF_PLEDGE_UNPLEDGE_CHARGE if is_mtf_trade(trade, products) else 0.0
    return dp, pledge


def get_trades(from_date: str, to_date: str) -> list[dict]:
    """Fetches every trade in [from_date, to_date] (inclusive), paginating
    until an empty page comes back."""
    session, _ = get_session()
    trades = []
    page = 0
    while True:
        resp = session.get(f"{BASE_URL}/trades/{from_date}/{to_date}/{page}", timeout=15)
        if not resp.ok:
            raise RuntimeError(f"[dhan] trade-book fetch failed (page {page}): {resp.text}")
        batch = resp.json()
        if not batch:
            break
        trades.extend(batch)
        page += 1
    return trades


def trade_by_order_id(order_id: str) -> dict | None:
    """Fetches today's trade record for a single order via GET
    /trades/{order-id}. Only works for orders placed TODAY (confirmed
    2026-08-19: returns [] for a prior day's order, even though the order id
    itself is still valid/lookupable elsewhere) -- the fast path for the
    normal case (push_charges.py calling this right after a same-day fill).
    Returns None on any miss; callers needing to also cover older positions
    should fall back to trade_by_order_id_since()."""
    session, _ = get_session()
    resp = session.get(f"{BASE_URL}/trades/{order_id}", timeout=15)
    if not resp.ok:
        return None
    body = resp.json()
    if not body:
        return None
    return body[0] if isinstance(body, list) else body


def _has_charge_fields(trade: dict) -> bool:
    """True if this trade record actually carries at least one of the 6
    charge fields (brokerage/STT/exchange/SEBI/stamp/GST) as a KEY, not just
    a zero value. trade_by_order_id()'s single-order endpoint (today-only)
    returns a real trade record but confirmed live 2026-08-20 (BAJAJHIND
    exit) to omit these keys ENTIRELY -- summing 6 missing fields via
    trade_api_charges()'s .get(field, 0.0) silently produces 0.0 and gets
    mislabelled "entry_source"/"exit_source": "api" (a real API hit, just
    the wrong endpoint for charges) instead of falling through to an
    estimate. Only the historical range endpoint (get_trades) actually
    carries these -- see module docstring."""
    return any(field in trade for field, _ in _CHARGE_FIELDS)


def trade_by_order_id_since(order_id: str, since_date_str: str) -> dict | None:
    """Same-day fast path (trade_by_order_id) first; on a miss OR a hit that
    lacks charge data (see _has_charge_fields), falls back to a historical
    trade-book pull (get_trades) from since_date_str through today and scans
    for a matching orderId. Needed because positions in positions_dhan.json
    can be queried well after their entry day (e.g. a position entered
    yesterday still sitting open this morning) -- the single-order endpoint
    alone would silently report 0 charges for those, which is wrong, not
    just incomplete.

    Treats an outright API failure on the historical pull (get_trades
    raises RuntimeError -- confirmed live 2026-08-20: the trade-book
    endpoint was returning a 500 TRADE_RESOURCE_ERROR for every date range)
    the same as "not found": returns None rather than propagating. Callers
    like position_charge_summary() already treat None as "fall back to an
    estimate," which is the whole point -- a real API outage shouldn't
    behave any differently from a not-yet-indexed trade."""
    trade = trade_by_order_id(order_id)
    if trade is not None and _has_charge_fields(trade):
        return trade
    today = datetime.now(_IST).date().isoformat()
    try:
        trades = get_trades(since_date_str, today)
    except RuntimeError:
        return None
    for t in trades:
        if t.get("orderId") == order_id:
            return t
    return None


def tracked_order_ids() -> set[str]:
    """Every order ID this pipeline placed, per results/positions_dhan.json --
    not just entry_order_id. A position can carry up to 7 different
    order-id fields across its lifecycle (entry_order_id, target_order_id,
    exit_order_id_925, exit_order_id_1159, cover_target_order_id,
    stop_order_id, exit_order_id_239 -- the last 3 only on the shorting
    add-on's mirrored short position). Scanning every "*order_id*"-named key
    on every position, rather than hardcoding the field list, means new
    stages/fields picked up automatically without editing this function
    again -- and it's what makes --tracked-only actually show the shorting
    leg's 925/1159 short-open SELL and 2:39 cover BUY, not just the original
    3:21 entry BUY."""
    if not _POS_FILE.exists():
        return set()
    positions = json.loads(_POS_FILE.read_text())
    ids = set()
    for p in positions:
        for key, val in p.items():
            if "order_id" in key and val:
                ids.add(val)
    return ids


def mtf_daily_rate_pct(funded_amount: float) -> float:
    """The single slab rate (% per day) that applies to the WHOLE funded
    amount -- see module docstring for the slab table."""
    for upper, rate in _MTF_INTEREST_SLABS:
        if funded_amount <= upper:
            return rate
    return _MTF_INTEREST_SLABS[-1][1]


def funded_amount(symbol: str, quantity: int, price: float) -> float:
    """POST /margincalculator (productType=MTF) for this symbol+quantity and
    returns the borrowed/funded portion: trade value minus the client's own
    margin contribution (totalMargin). Raises on any lookup failure -- unlike
    run_trades.py's _margin_check, this is an on-demand report, not something
    that must degrade gracefully mid-pipeline."""
    session, client_id = get_session()
    sid = security_id(symbol)
    payload = {
        "dhanClientId":    client_id,
        "exchangeSegment": "NSE_EQ",
        "transactionType": "BUY",
        "quantity":        quantity,
        "productType":     "MTF",
        "securityId":      sid,
        "price":           price,
    }
    resp = session.post(f"{BASE_URL}/margincalculator", json=payload, timeout=15)
    if not resp.ok:
        raise RuntimeError(f"[dhan] margin calculator failed for {symbol}: {resp.text}")
    data = resp.json()
    trade_value    = quantity * price
    margin_required = float(data.get("totalMargin") or 0)
    return max(trade_value - margin_required, 0.0)


def calendar_days_held(entry_date_str: str, as_of: date) -> int:
    """Plain calendar-day gap -- Friday entry, Monday as_of is 3, no special
    weekend-skipping logic needed since the dates themselves already span it."""
    entry = date.fromisoformat(entry_date_str)
    return max((as_of - entry).days, 0)


_EXIT_ORDER_ID_FIELD = {
    "exited_925":   "exit_order_id_925",
    "exited_1159":  "exit_order_id_1159",
    "short_closed": "exit_order_id_239",
}
_EXIT_TIMESTAMP_FIELD = {
    "exited_925":   "exit_timestamp_925",
    "exited_1159":  "exit_timestamp_1159",
    "short_closed": "exit_timestamp_239",
}
_EXIT_PRICE_FIELD = {
    "exited_925":   "exit_price_925",
    "exited_1159":  "exit_price_1159",
    "short_closed": "exit_price_239",
}


def position_charge_summary(pos: dict) -> dict:
    """Full charge breakdown for one positions_dhan.json record -- entry leg,
    exit leg (once closed), fixed DP/pledge (delivery entries only, see
    is_delivery_buy's reasoning -- a short's entry/cover legs are always
    INTRADAY, never delivery, so both are always 0 for direction=="short"),
    and MTF interest accrued through either today (still open) or the exit
    date (closed -- interest stops accruing once covered/sold, so it's
    frozen there rather than keeping today's growing number).

    Entry/exit leg charges always come from estimate_trade_charges() (Dhan's
    published rate card), not the trade-book API -- deliberately, not as an
    outage fallback. Verified live 2026-08-20 against a real BAJAJHIND fill:
    the estimate matched the API's actual per-trade charges to within ₹0.05
    on a ₹378.78 leg (~0.01%), and the trade-book API itself is unreliable
    for this pipeline's purposes anyway (the historical range endpoint
    doesn't index same-day trades, and the same-day single-order endpoint
    doesn't carry charge fields at all) -- see git history for both. Given
    the estimate is already accurate and doesn't depend on either flaky
    endpoint, it's used unconditionally rather than treated as a fallback.
    "entry_source"/"exit_source" stay in the returned dict for
    forward-compatibility with anything that still branches on them; both
    are now always "estimated" (or "none" if the position doesn't even have
    enough data -- qty/price -- to estimate from)."""
    is_short = pos.get("direction") == "short"
    product  = pos.get("product", "") or ("INTRADAY" if is_short else "")
    status   = pos.get("status", "")

    entry_qty   = pos.get("quantity") if is_short else pos.get("actual_fill_quantity")
    entry_price = pos.get("entry_price") if is_short else pos.get("actual_fill_price")
    entry_side  = "SELL" if is_short else "BUY"

    if entry_qty and entry_price:
        entry_charges = estimate_trade_charges(entry_qty, entry_price, product, entry_side)
        entry_source = "estimated"
    else:
        entry_charges, entry_source = 0.0, "none"

    exit_field = _EXIT_ORDER_ID_FIELD.get(status)
    exit_price_field = _EXIT_PRICE_FIELD.get(status)
    exit_price = pos.get(exit_price_field) if exit_price_field else None
    exit_qty   = entry_qty  # exit leg estimate uses the same full position size as entry
    exit_side  = "BUY" if is_short else "SELL"
    if exit_field and exit_qty and exit_price:
        exit_charges = estimate_trade_charges(exit_qty, exit_price, product, exit_side)
        exit_source = "estimated"
    else:
        exit_charges, exit_source = 0.0, "none"

    dp = pledge = 0.0
    if not is_short and product in ("CNC", "MTF"):
        dp = _DP_CHARGE
        if product == "MTF":
            pledge = _MTF_PLEDGE_UNPLEDGE_CHARGE

    interest = 0.0
    if not is_short and product == "MTF":
        qty   = pos.get("actual_fill_quantity") or 0
        price = pos.get("actual_fill_price") or 0.0
        entry_date_str = pos.get("entry_date")
        if qty and price and entry_date_str:
            ts_field = _EXIT_TIMESTAMP_FIELD.get(status)
            ts = pos.get(ts_field) if ts_field else None
            as_of = datetime.fromisoformat(ts).date() if ts else datetime.now(_IST).date()
            days = calendar_days_held(entry_date_str, as_of)
            try:
                funded = funded_amount(pos["symbol"], qty, price)
                rate   = mtf_daily_rate_pct(funded)
                interest = funded * (rate / 100) * days
            except (RuntimeError, EnvironmentError):
                interest = 0.0  # non-fatal -- see docstring

    total = entry_charges + exit_charges + dp + pledge + interest
    return {
        "entry_charges": round(entry_charges, 2),
        "entry_source":  entry_source,
        "exit_charges":  round(exit_charges, 2),
        "exit_source":   exit_source,
        "dp_charge":     round(dp, 2),
        "pledge_charge": round(pledge, 2),
        "mtf_interest":  round(interest, 2),
        "total_charges": round(total, 2),
    }


def print_mtf_interest_report(as_of: date | None = None) -> float:
    """Prints MTF interest accrued so far on every currently-open MTF
    position in positions_dhan.json, and returns the total. as_of defaults
    to today (IST)."""
    if as_of is None:
        as_of = datetime.now(_IST).date()

    if not _POS_FILE.exists():
        print("[dhan] No positions_dhan.json found.")
        return 0.0
    positions = json.loads(_POS_FILE.read_text())
    mtf_positions = [p for p in positions
                     if p.get("product") == "MTF" and p.get("status") == "open"]

    if not mtf_positions:
        print("[dhan] No open MTF positions.")
        return 0.0

    header = f"{'SYMBOL':16}{'QTY':>7}{'FILL PRICE':>12}{'FUNDED AMT':>14}" \
             f"{'DAYS':>6}{'RATE/DAY':>10}{'INTEREST':>12}"
    print(header)
    print("-" * len(header))

    grand_total = 0.0
    for p in mtf_positions:
        sym   = p["symbol"]
        qty   = p["actual_fill_quantity"]
        price = p["actual_fill_price"]
        days  = calendar_days_held(p["entry_date"], as_of)

        funded = funded_amount(sym, qty, price)
        rate   = mtf_daily_rate_pct(funded)
        interest = funded * (rate / 100) * days
        grand_total += interest

        print(f"{sym:16}{qty:>7}{price:>12.2f}{funded:>14,.2f}"
              f"{days:>6}{rate:>9.4f}%{interest:>12.2f}")

    print("-" * len(header))
    print(f"\n[dhan] MTF interest as of {as_of.isoformat()}  ·  total: ₹{grand_total:,.2f}")
    return grand_total


def print_report(trades: list[dict]) -> None:
    if not trades:
        print("[dhan] No trades found for this range/filter.")
        return

    trades = sorted(trades, key=lambda t: (t.get("customSymbol", ""), t.get("exchangeTime", "")))
    products = product_map()

    # ── Part 1: trade-leg charges only (the 6 API fields, per BUY/SELL leg) ──
    col_w = {name: max(len(name), 7) for _, name in _CHARGE_FIELDS}
    header = f"{'SYMBOL':24}{'SIDE':6}{'QTY':>7}{'PRICE':>10}" \
             + "".join(f"{name:>{col_w[name]+2}}" for _, name in _CHARGE_FIELDS) \
             + f"{'LEG TOTAL':>11}"
    print("TRADE LEG CHARGES (buy leg and sell leg, each leg's own numbers)")
    print(header)
    print("-" * len(header))

    leg_totals = {name: 0.0 for _, name in _CHARGE_FIELDS}
    leg_grand_total = 0.0
    fixed_rows = []  # (symbol, dp, pledge) for delivery BUY legs only

    for t in trades:
        sym   = t.get("customSymbol", "?")
        side  = t.get("transactionType", "?")
        qty   = t.get("tradedQuantity", 0)
        price = t.get("tradedPrice", 0.0)
        api_total = trade_api_charges(t)
        leg_grand_total += api_total

        row = f"{sym[:23]:24}{side:6}{qty:>7}{price:>10.2f}"
        for field, name in _CHARGE_FIELDS:
            val = t.get(field, 0.0) or 0.0
            leg_totals[name] += val
            row += f"{val:>{col_w[name]+2}.2f}"
        row += f"{api_total:>11.2f}"
        print(row)

        dp, pledge = fixed_charges(t, products)
        if dp or pledge:
            fixed_rows.append((sym, dp, pledge))

    print("-" * len(header))
    footer = f"{'COLUMN TOTALS':24}{'':6}{'':7}{'':10}"
    for _, name in _CHARGE_FIELDS:
        footer += f"{leg_totals[name]:>{col_w[name]+2}.2f}"
    footer += f"{leg_grand_total:>11.2f}"
    print(footer)
    print(f"\n[dhan] {len(trades)} trade leg(s)  ·  trade-leg charges total: ₹{leg_grand_total:,.2f}")

    # ── Part 2: fixed, one-time-per-position charges (never a per-leg cost) ──
    print(f"\nFIXED POSITION-LEVEL CHARGES (one-time per position, not part of "
          f"either leg's total above)")
    fixed_header = f"{'SYMBOL':24}{'DP':>10}{'PLEDGE/UNPLEDGE':>18}{'TOTAL':>10}"
    print(fixed_header)
    print("-" * len(fixed_header))
    dp_total = pledge_total = 0.0
    for sym, dp, pledge in fixed_rows:
        dp_total     += dp
        pledge_total += pledge
        print(f"{sym[:23]:24}{dp:>10.2f}{pledge:>18.2f}{dp + pledge:>10.2f}")
    fixed_grand_total = dp_total + pledge_total
    print("-" * len(fixed_header))
    print(f"{'COLUMN TOTALS':24}{dp_total:>10.2f}{pledge_total:>18.2f}{fixed_grand_total:>10.2f}")
    print(f"\n[dhan] fixed charges total: ₹{fixed_grand_total:,.2f}  "
          f"(DP ₹{_DP_CHARGE:.2f}/delivery-position, pledge/unpledge ₹{_MTF_PLEDGE_UNPLEDGE_CHARGE:.2f}/MTF-position)")

    # ── Combined ──
    grand_total = leg_grand_total + fixed_grand_total
    print(f"\n[dhan] GRAND TOTAL (trade-leg charges + fixed charges): ₹{grand_total:,.2f}")


# ── CLI entry point ────────────────────────────────────────────────────────────

def _usage():
    print(__doc__)
    sys.exit(1)


if __name__ == "__main__":
    args = sys.argv[1:]

    if args and args[0] == "--mtf-interest":
        as_of = date.fromisoformat(args[1]) if len(args) > 1 else None
        try:
            print_mtf_interest_report(as_of)
        except (RuntimeError, EnvironmentError) as exc:
            print(f"\nError: {exc}", file=sys.stderr)
            sys.exit(1)
        sys.exit(0)

    if len(args) < 2:
        _usage()

    from_date, to_date = args[0], args[1]
    rest = args[2:]

    tracked_only = "--tracked-only" in rest
    symbol_filter = None
    if "--symbol" in rest:
        symbol_filter = rest[rest.index("--symbol") + 1]

    try:
        trades = get_trades(from_date, to_date)
    except (RuntimeError, EnvironmentError) as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        sys.exit(1)

    if tracked_only:
        ids = tracked_order_ids()
        trades = [t for t in trades if t.get("orderId") in ids]

    if symbol_filter:
        trades = [t for t in trades if t.get("customSymbol") == symbol_filter]

    print_report(trades)
