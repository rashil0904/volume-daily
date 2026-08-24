"""
dhan/uc_staged_entry.py — UC-based staged entry, Case A / Case B (Dhan only)
============================================================================
Runs ALONGSIDE the existing 3:21pm entry (dhan/run_trades.py's run_entry_321),
never replacing it. Watches live ticks (fed in by dhan/live_monitor.py's
MarketFeed websocket, gated behind --enable-uc-staged-entry, default off) for
an early, opportunistic entry whenever a symbol pops 19% above its previous
close before the normal 3:21pm entry would otherwise buy it:

    Case A qualification filter (checked continuously from market open,
    independent of the capital snapshot / windows below -- see
    update_case_a_qualification()):
        A symbol only becomes Case-A-eligible if it (1) hit upper circuit at
        some point BEFORE 14:30, AND (2) is subsequently seen off UC (LTP <
        upper_circuit) at some point during the 14:30-15:18 window. Both are
        one-way latches -- once case_a_qualified is True it never unlatches.
        A symbol that never hit UC before 14:30, or hit UC but stays locked
        through 15:18 without opening up, is never a Case A candidate --
        falls through untouched to the normal 3:21pm entry.
    Case A leg 1 (14:30-15:18 IST -- same window as leg 2, widened from an
                  earlier 14:30-15:00):
        For a case_a_qualified symbol: LTP crosses up through
        prev_close * 1.19 -> buy 50% of per_stock_capital.
    Case A leg 2 (14:30-15:18 IST):
        LTP retraces back down to prev_close * 1.17 -> buy the other 50%.
        If leg 2's window closes without firing, the remaining balance is
        completed with PRIORITY by the 3:21pm entry (Step 1 there), not
        treated as a fresh entry -- see run_entry_321.
    Case B (15:00-15:18 IST):
        For a symbol that is NOT case_a_qualified: LTP crosses up through
        prev_close * 1.19 -> buy 100% of per_stock_capital in one shot. No
        legs, no retrace, no UC-proximity gate (explicitly dropped, not an
        oversight -- confirmed three times now).

Case A/B tie-break during their overlapping 15:00-15:18 window: a symbol is
excluded from Case B as soon as case_a_qualified is True, even if leg 1
hasn't fired yet (confirmed -- "still being evaluated for leg 1" counts).
This is a deliberate check in evaluate_tick, not an accidental consequence of
entry_status alone (entry_status-only would let a qualified-but-not-yet-
triggered symbol slip into Case B, which is exactly what's excluded here).

per_stock_capital is NOT computed in this module -- it's a snapshot taken
once by dhan/live_monitor.py (the only place with access to the "qualified"
count driving it) and passed in as a plain parameter to every execute_*
call, then persisted onto the position row (capital_base) so the 3:21pm
entry -- a separate cron invocation, no shared memory with live_monitor.py's
long-running process -- can read back the exact original allocation for a
Step 1 completion.

State design
------------
Each watched symbol gets one UCState, held on dhan/live_monitor.py's
LiveMonitor instance (one dict, guarded by the same threading.Lock already
used for its existing per-symbol states). evaluate_tick() is a pure function
(no I/O) mirroring live_monitor.py's own evaluate_tick -- it decides whether
to fire and, critically, LATCHES entry_status to "order_placed" the instant
it decides to fire, synchronously, before the caller ever hands the slow
order-placement call to a thread pool. Since dhanhq's MarketFeed delivers one
message at a time to _on_message (never concurrently), this synchronous latch
is enough on its own to prevent a second tick from re-firing the same trigger
while the first order is still in flight -- no lock needed around the state
mutation itself, only around dict access shared with the heartbeat thread.

On confirmed success the executor resolves "order_placed" to "filled" or
"partially_filled" and writes the position row. On failure it reverts to the
prior *armable* state (not a terminal one) so a later tick can retry within
the same window -- leg 1 and Case B revert to "not_attempted" (a
permanently-missed trigger is fine, it just falls through to the 3:21pm
entry's Step 3 for the full amount, identical to "never triggered", since no
position row was ever written); leg 2 reverts to "partially_filled" (stays
armed rather than stranding the position -- either a later tick retries it,
or the 3:21pm entry's Step 1 completes it regardless of whether that retry
ever happened).

That final mutation (order_placed -> terminal/reverted) happens inside the
executor thread, NOT under live_monitor.py's lock -- deliberately not
locked, not an oversight. It's safe because (a) CPython's GIL makes a single
attribute assignment atomic, so a concurrent read never sees a torn value,
and (b) the only thing that could go wrong from a stale read -- evaluate_tick
observing "order_placed" a moment longer than strictly necessary -- just
means it correctly does nothing that cycle and picks up the real state on
the next one. Nothing ever re-fires an order from a stale read, since
"order_placed" isn't a recognized armable state in evaluate_tick.

Position-row fields this module adds are ADDITIVE, not a replacement for the
existing `status` field every other position row already uses -- `status`
still flips to the ordinary "open" once a symbol is fully filled (Case A leg
2, or Case B), so place_targets_915/check_exit_925/force_exit_1159 need ZERO
changes; they already only match status in ("open", "partial_exit_925_nodata").
The new fields (`case`, `case_a_qualified`, `entry_status`, `case_a_leg`,
`filled_amount`, `capital_base`) exist purely for this feature's own tracking
and for run_entry_321's Step 1/2/3 priority ordering.
"""

import sys
from dataclasses import dataclass
from datetime import date, datetime, time as dtime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_ROOT / "pipeline") not in sys.path:
    sys.path.insert(0, str(_ROOT / "pipeline"))

from dhan.run_trades import (
    _IST, _ts, _margin_check, _available_balance, _tick_round,
    _load_long_pos, _save_long_pos, _poll_fill_strict,
)
from dhan.trade import buy
from common.calc_utils import compute_shares
import notify

_CANDLES_DIR = _ROOT / "data" / "candles"

_CASE_A_START = dtime(14, 30)   # leg 1 and leg 2 share this window (widened -- both used to differ)
_CASE_A_END   = dtime(15, 18)
_CASE_B_START = dtime(15, 0)
_CASE_B_END   = dtime(15, 18)

_LEG_UP_MULT   = 1.19   # prev_close * 1.19 -- leg 1 (Case A) / the fill trigger (Case B)
_LEG_DOWN_MULT = 1.17   # prev_close * 1.17 -- leg 2 retrace (Case A only)


# ── prev_close ──────────────────────────────────────────────────────────────────

def load_prev_close(symbol: str) -> float | None:
    """Reads data/candles/<symbol>.csv (this pipeline's own 15-min history,
    kept fresh by the existing EOD/intraday jobs) and returns the close of the
    LAST candle strictly before today's date. Returns None if the file's
    missing or has no prior-day rows -- caller should just skip this symbol
    for Case A/B (it still gets the normal, untouched 3:21pm entry).

    NOT sourced from Dhan's own ohlc.close (REST /marketfeed/quote or the
    websocket Quote Data packet's "close" field) -- confirmed live 2026-08-24
    that field tracks TODAY's running/last price, not the previous trading
    day's close (THOMASCOOK: ohlc.close exactly equalled last_price mid-
    afternoon)."""
    path = _CANDLES_DIR / f"{symbol}.csv"
    if not path.exists():
        return None
    today_str = date.today().isoformat()
    last_close: float | None = None
    with open(path, newline="") as f:
        next(f, None)   # header
        for line in f:
            parts = line.rstrip("\n").split(",")
            if len(parts) < 5:
                continue
            ts_str = parts[0]
            if ts_str.startswith(today_str):
                break   # today's rows are appended last -- stop before them
            try:
                last_close = float(parts[4])
            except ValueError:
                continue
    return last_close


# ── Per-symbol state ────────────────────────────────────────────────────────────

@dataclass
class UCState:
    symbol:     str
    prev_close: float | None
    case:         str | None = None   # None | "A" | "B"
    entry_status: str = "not_attempted"
    # not_attempted | order_placed | partially_filled | filled
    case_a_leg:   str | None = None   # None | leg1_filled_watching_retrace | leg2_filled
    filled_amount: float = 0.0        # cumulative rupees filled today
    capital_base:  float = 0.0        # per_stock_capital snapshot this symbol used
    # Case A qualification filter (see update_case_a_qualification below) --
    # both latches are one-way, never unlatch once True.
    hit_uc_before_1430: bool = False
    off_uc_in_window:   bool = False
    case_a_qualified:   bool = False


def _in_case_a_window(t: dtime) -> bool:
    return _CASE_A_START <= t < _CASE_A_END


def _in_case_b_window(t: dtime) -> bool:
    return _CASE_B_START <= t < _CASE_B_END


def update_case_a_qualification(state: UCState, ltp: float, upper_circuit: float | None,
                                now: datetime | None = None) -> None:
    """Latches the Case A qualification filter from live ticks. Must be
    called on EVERY tick from market open onward (not gated on the
    per_stock_capital snapshot or the 14:30 window -- "hit UC before 14:30"
    has to be observed well before either exists). hit_uc_before_1430
    latches True on the first tick before 14:30 where LTP >= upper_circuit;
    off_uc_in_window latches True on the first tick during the 14:30-15:18
    window where LTP < upper_circuit, but only once hit_uc_before_1430 is
    already set. case_a_qualified is the AND of both -- once True it stays
    True for the rest of the day (checked first below as a cheap no-op)."""
    if state.case_a_qualified or not upper_circuit or upper_circuit <= 0 or ltp <= 0:
        return
    now_t = (now or datetime.now(_IST)).time()
    if now_t < _CASE_A_START:
        if ltp >= upper_circuit:
            state.hit_uc_before_1430 = True
    elif _in_case_a_window(now_t):
        if state.hit_uc_before_1430 and ltp < upper_circuit:
            state.off_uc_in_window = True
    if state.hit_uc_before_1430 and state.off_uc_in_window:
        state.case_a_qualified = True


def evaluate_tick(state: UCState, ltp: float, per_stock_capital: float,
                  now: datetime | None = None) -> str | None:
    """Pure, side-effecting-on-state-only (no I/O) -- mirrors live_monitor.py's
    own evaluate_tick. Returns a fired event name or None. Latches
    entry_status to "order_placed" the instant it fires -- see module
    docstring for why that alone is race-safe against a second tick
    re-firing the same trigger while the first order is still in flight.
    Assumes update_case_a_qualification() has already been called for this
    tick (live_monitor.py calls it unconditionally, first, on every tick)."""
    if state.prev_close is None or ltp <= 0 or state.prev_close <= 0:
        return None
    now_t = (now or datetime.now(_IST)).time()

    if state.entry_status == "not_attempted":
        if (state.case_a_qualified and _in_case_a_window(now_t)
                and ltp >= state.prev_close * _LEG_UP_MULT):
            state.entry_status = "order_placed"
            state.capital_base = per_stock_capital
            return "case_a_leg1"
        # Tie-break: a case_a_qualified symbol never fires Case B, even
        # before leg 1 itself has fired ("still being evaluated for leg 1").
        if (not state.case_a_qualified and _in_case_b_window(now_t)
                and ltp >= state.prev_close * _LEG_UP_MULT):
            state.entry_status = "order_placed"
            state.capital_base = per_stock_capital
            return "case_b_fill"
        return None

    if (state.entry_status == "partially_filled"
            and state.case_a_leg == "leg1_filled_watching_retrace"
            and _in_case_a_window(now_t)
            and ltp <= state.prev_close * _LEG_DOWN_MULT):
        state.entry_status = "order_placed"
        return "case_a_leg2"

    return None


# ── Order placement ─────────────────────────────────────────────────────────────

def _capped_limit_price(sym: str, trigger_price: float, upper_circuit: float | None) -> float:
    """0.5% above the trigger, capped 0.5% below the day's upper circuit --
    same fix already shipped for the sell-side 17% target (place_targets_915,
    GOPAL 2026-08-20). Buying right after a 19% pop is exactly the scenario
    where an uncapped limit can legally exceed a stock's circuit (e.g. a
    20%-band stock) and get rejected outright."""
    limit_price = _tick_round(sym, trigger_price * 1.005)
    if upper_circuit:
        limit_price = min(limit_price, _tick_round(sym, upper_circuit * 0.995))
    return limit_price


def _place_staged_buy(sym: str, qty: int, limit_price: float, dry_run: bool) -> dict | None:
    """One staged-entry BUY leg -- same MTF-then-CNC decision, and the same
    CNC-retry-on-genuine-MTF-ineligibility-rejection logic, as
    run_entry_321 (a Case A/B trigger can hit an MTF-ineligible scrip exactly
    like KLBRENG-B/WELSPLSOL did, 2026-08-21). Unlike run_entry_321's batch
    n-way path, this does NOT halve qty on a CNC fallback -- matches
    run_entry_321's own manual_mode behavior, which this feature's
    per-symbol trigger shape mirrors. Returns None (caller must not write a
    position row, and must revert its state latch) if nothing filled."""
    margin_info  = _margin_check(sym, qty, limit_price)
    has_leverage = margin_info is not None and margin_info["leverage"] >= 2
    product      = "MTF" if has_leverage else "CNC"

    margin_required = margin_info["margin_required"] if has_leverage else qty * limit_price
    available = _available_balance()
    if available is None or available < margin_required:
        print(f"[uc_staged] {sym}: SKIP -- insufficient/unverifiable balance.")
        return None

    try:
        order_id = buy(sym, "NSE_EQ", qty, order_type="LIMIT", price=limit_price,
                       product=product, dry_run=dry_run)
    except Exception as exc:
        print(f"[uc_staged] {sym}: ORDER FAILED -- {exc}")
        return None

    if dry_run:
        return {"order_id": order_id, "product": product,
                "fill_price": limit_price, "fill_qty": qty}

    fill_price, fill_qty, rejected, reason = _poll_fill_strict(order_id)
    if (fill_qty == 0 and product == "MTF" and rejected
            and "mtf product is not allow" in reason.lower()):
        print(f"[uc_staged] {sym}: MTF-INELIGIBLE -- retrying as CNC.")
        try:
            order_id = buy(sym, "NSE_EQ", qty, order_type="LIMIT", price=limit_price,
                           product="CNC", dry_run=dry_run)
            fill_price, fill_qty, _, _ = _poll_fill_strict(order_id)
            product = "CNC"
        except Exception as exc:
            print(f"[uc_staged] {sym}: CNC retry ORDER FAILED -- {exc}")
            return None

    if fill_qty == 0:
        print(f"[uc_staged] {sym}: NOT FILLED.")
        return None

    return {"order_id": order_id, "product": product,
            "fill_price": fill_price, "fill_qty": fill_qty}


def execute_case_a_leg1(sym: str, state: UCState, ltp: float,
                        upper_circuit: float | None, dry_run: bool = False) -> None:
    leg_capital = state.capital_base / 2
    leg_qty     = compute_shares(leg_capital, ltp)
    if leg_qty == 0:
        print(f"[uc_staged] {sym}: Case A leg1 SKIP -- 0 shares at LTP {ltp:.2f}.")
        state.entry_status = "not_attempted"
        return

    limit_price = _capped_limit_price(sym, ltp, upper_circuit)
    result = _place_staged_buy(sym, leg_qty, limit_price, dry_run)
    if result is None:
        state.entry_status = "not_attempted"   # retry-eligible on a later tick this window
        return

    fill_amount = result["fill_price"] * result["fill_qty"]
    positions = _load_long_pos()
    positions.append({
        "broker": "dhan", "symbol": sym, "entry_date": date.today().isoformat(),
        "case": "A", "case_a_qualified": state.case_a_qualified,
        "case_a_leg": "leg1_filled_watching_retrace",
        "entry_status": "partially_filled", "status": "leg1_filled",
        "capital_base": state.capital_base, "filled_amount": round(fill_amount, 2),
        "reference_price": ltp, "shares_intended": leg_qty,
        "actual_fill_price": result["fill_price"], "actual_fill_quantity": result["fill_qty"],
        "entry_order_id": result["order_id"], "entry_timestamp": _ts(),
        "product": result["product"],
    })
    if not dry_run:
        _save_long_pos(positions)
    state.case          = "A"
    state.entry_status  = "partially_filled"
    state.case_a_leg     = "leg1_filled_watching_retrace"
    state.filled_amount  = fill_amount
    try:
        notify.send_entry(broker="dhan", symbol=f"{sym} [Case A leg1]", ref_price=ltp,
                          shares=result["fill_qty"], order_id=result["order_id"], dry_run=dry_run)
    except Exception as exc:
        print(f"  [notify] Case A leg1 failed: {exc}", file=sys.stderr)


def execute_case_a_leg2(sym: str, state: UCState, ltp: float,
                        upper_circuit: float | None, dry_run: bool = False) -> None:
    positions = _load_long_pos()
    today = date.today().isoformat()
    pos = next((p for p in positions if p.get("symbol") == sym
               and p.get("entry_date") == today and p.get("case") == "A"
               and p.get("entry_status") == "partially_filled"), None)
    if pos is None:
        print(f"[uc_staged] {sym}: Case A leg2 fired but no leg1 row found -- skipping.")
        state.entry_status = "partially_filled"
        return

    remaining = pos["capital_base"] - pos["filled_amount"]
    leg_qty   = compute_shares(remaining, ltp)
    if leg_qty <= 0:
        state.entry_status = "filled"
        state.case_a_leg    = "leg2_filled"
        return

    limit_price = _capped_limit_price(sym, ltp, upper_circuit)
    result = _place_staged_buy(sym, leg_qty, limit_price, dry_run)
    if result is None:
        state.entry_status = "partially_filled"   # stay armed -- retry-eligible, or
        state.case_a_leg    = "leg1_filled_watching_retrace"  # Step 1 at 3:21pm completes it regardless
        return

    fill_amount = result["fill_price"] * result["fill_qty"]
    total_qty   = pos["actual_fill_quantity"] + result["fill_qty"]
    avg_price   = ((pos["actual_fill_price"] * pos["actual_fill_quantity"]
                   + result["fill_price"] * result["fill_qty"]) / total_qty)
    pos.update({
        "status": "open", "entry_status": "filled", "case_a_leg": "leg2_filled",
        "filled_amount": round(pos["filled_amount"] + fill_amount, 2),
        "actual_fill_price": round(avg_price, 4), "actual_fill_quantity": total_qty,
        "leg2_order_id": result["order_id"], "leg2_fill_price": result["fill_price"],
        "leg2_fill_quantity": result["fill_qty"], "leg2_timestamp": _ts(),
    })
    if not dry_run:
        _save_long_pos(positions)
    state.entry_status  = "filled"
    state.case_a_leg     = "leg2_filled"
    state.filled_amount += fill_amount
    try:
        notify.send_entry(broker="dhan", symbol=f"{sym} [Case A leg2]", ref_price=ltp,
                          shares=result["fill_qty"], order_id=result["order_id"], dry_run=dry_run)
    except Exception as exc:
        print(f"  [notify] Case A leg2 failed: {exc}", file=sys.stderr)


def execute_case_b(sym: str, state: UCState, ltp: float,
                   upper_circuit: float | None, dry_run: bool = False) -> None:
    qty = compute_shares(state.capital_base, ltp)
    if qty == 0:
        print(f"[uc_staged] {sym}: Case B SKIP -- 0 shares at LTP {ltp:.2f}.")
        state.entry_status = "not_attempted"
        return

    limit_price = _capped_limit_price(sym, ltp, upper_circuit)
    result = _place_staged_buy(sym, qty, limit_price, dry_run)
    if result is None:
        state.entry_status = "not_attempted"   # retry-eligible on a later tick this window
        return

    fill_amount = result["fill_price"] * result["fill_qty"]
    positions = _load_long_pos()
    positions.append({
        "broker": "dhan", "symbol": sym, "entry_date": date.today().isoformat(),
        "case": "B", "case_a_qualified": state.case_a_qualified,
        "entry_status": "filled", "status": "open",
        "capital_base": state.capital_base, "filled_amount": round(fill_amount, 2),
        "reference_price": ltp, "shares_intended": qty,
        "actual_fill_price": result["fill_price"], "actual_fill_quantity": result["fill_qty"],
        "entry_order_id": result["order_id"], "entry_timestamp": _ts(),
        "product": result["product"],
    })
    if not dry_run:
        _save_long_pos(positions)
    state.case          = "B"
    state.entry_status  = "filled"
    state.filled_amount = fill_amount
    try:
        notify.send_entry(broker="dhan", symbol=f"{sym} [Case B]", ref_price=ltp,
                          shares=result["fill_qty"], order_id=result["order_id"], dry_run=dry_run)
    except Exception as exc:
        print(f"  [notify] Case B failed: {exc}", file=sys.stderr)
