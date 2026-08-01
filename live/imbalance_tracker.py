#!/usr/bin/env python3
"""
imbalance_tracker.py — tick-rule order-flow imbalance tracker
=============================================================
Observation and CSV logging only. No order placement, no trading logic.

Plugs into live_monitor.py as a parallel logger. One ImbalanceState per
symbol; update_imbalance() called once per tick alongside evaluate_tick().
Writes to logs/imbalance_<date>.csv — separate from live_monitor's log.

Tick rule (standard market-microstructure heuristic):
  price up   → buyer lifted the offer  → "buy" volume
  price down → seller hit the bid      → "sell" volume
  unchanged  → ambiguous               → "unk" volume (tracked, not scored)

imbalance = (buy_vol - sell_vol) / (buy_vol + sell_vol)   range [-1, +1]

Two imbalances tracked per symbol:
  cumulative — all-day, since 9:15
  rolling    — last ROLLING_WINDOW_SECS only, via a time-pruned deque

The rolling figure is the operationally meaningful one: a stock that ran
hard at 9:25 AM and went flat for hours will still show a buy-skewed
cumulative value, even though the buying pressure is long gone.

Self-test (demonstrates the divergence this module exists to catch):
    python imbalance_tracker.py
"""

import csv
import sys
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_IST = ZoneInfo("Asia/Kolkata")

# ── Tunable constants ─────────────────────────────────────────────────────────
ROLLING_WINDOW_SECS = 15 * 60   # rolling window width in seconds
LABEL_BUY_THRESHOLD = 0.3       # rolling_imb > this           → "buy_full"
LABEL_NEUTRAL_FLOOR = 0.0       # rolling_imb in (this, 0.3]  → "wait_recheck"
                                 # rolling_imb <= this          → "skip"


# ── Per-symbol state ──────────────────────────────────────────────────────────

@dataclass
class ImbalanceState:
    symbol:       str

    # Previous-tick references (for delta and tick-rule direction)
    prev_ltp:     float = 0.0
    prev_cum_vol: float = 0.0

    # Cumulative all-day volume buckets
    buy_vol:      float = 0.0
    sell_vol:     float = 0.0
    unk_vol:      float = 0.0

    # Rolling window: deque of (unix_ts: float, cls: str, vol: float)
    # Pruned to ROLLING_WINDOW_SECS on every update_imbalance() call.
    rolling: deque = field(default_factory=deque)


# ── Core update — pure function (except state mutation) ───────────────────────

def update_imbalance(
    state: ImbalanceState,
    ltp: float,
    cum_vol: float,
    now: datetime | None = None,
) -> tuple[float | None, float | None, str]:
    """
    Process one tick for a symbol. Mutates state in place.

    Parameters
    ----------
    state   : per-symbol ImbalanceState
    ltp     : last traded price from this tick (tick.get("last_price"))
    cum_vol : cumulative day volume (tick.get("volume_traded") — NOT a delta)
    now     : current time; defaults to datetime.now(_IST); injectable for tests

    Returns
    -------
    (cum_imbalance, rolling_imbalance, label)
      - cum_imbalance    : None until at least one buy/sell classification exists
      - rolling_imbalance: None if no buy/sell in the rolling window
      - label            : one of "buy_full" | "wait_recheck" | "skip" | "no_data"
    """
    if now is None:
        now = datetime.now(_IST)
    now_ts = now.timestamp()

    # First tick — initialise reference values only, no classification yet
    if state.prev_ltp == 0.0:
        state.prev_ltp     = ltp
        state.prev_cum_vol = cum_vol
        return None, None, "no_data"

    delta_vol = cum_vol - state.prev_cum_vol

    if delta_vol > 0:
        if ltp > state.prev_ltp:
            cls = "buy"
            state.buy_vol  += delta_vol
        elif ltp < state.prev_ltp:
            cls = "sell"
            state.sell_vol += delta_vol
        else:
            cls = "unk"
            state.unk_vol  += delta_vol
        state.rolling.append((now_ts, cls, delta_vol))
    # delta_vol == 0: no new exchange volume (reconnect snapshot etc.) — skip

    state.prev_ltp     = ltp
    state.prev_cum_vol = cum_vol

    # Prune rolling window to the configured lookback
    cutoff = now_ts - ROLLING_WINDOW_SECS
    while state.rolling and state.rolling[0][0] < cutoff:
        state.rolling.popleft()

    # Cumulative imbalance (all day)
    cum_total = state.buy_vol + state.sell_vol
    cum_imb   = (state.buy_vol - state.sell_vol) / cum_total if cum_total > 0 else None

    # Rolling imbalance (last ROLLING_WINDOW_SECS)
    r_buy = r_sell = 0.0
    for (_, cls, vol) in state.rolling:
        if cls == "buy":
            r_buy  += vol
        elif cls == "sell":
            r_sell += vol
    r_total  = r_buy + r_sell
    roll_imb = (r_buy - r_sell) / r_total if r_total > 0 else None

    return cum_imb, roll_imb, make_label(roll_imb)


def make_label(rolling_imbalance: float | None) -> str:
    """Map rolling imbalance to a plain recommendation label (logging only)."""
    if rolling_imbalance is None:
        return "no_data"
    if rolling_imbalance > LABEL_BUY_THRESHOLD:
        return "buy_full"
    if rolling_imbalance > LABEL_NEUTRAL_FLOOR:
        return "wait_recheck"
    return "skip"


# ── CSV logger ────────────────────────────────────────────────────────────────

_LOG_FIELDS = [
    "timestamp", "symbol", "event",
    "cum_imbalance", "rolling_imbalance", "rolling_label",
    "buy_vol", "sell_vol", "unk_vol",
    "rolling_buy_vol", "rolling_sell_vol", "rolling_unk_vol",
]


class ImbalanceLogger:
    """Appends to logs/imbalance_<date>.csv. Thread-safe."""

    def __init__(self, log_dir: Path):
        log_dir.mkdir(exist_ok=True)
        self._path = log_dir / f"imbalance_{date.today().isoformat()}.csv"
        self._lock = threading.Lock()
        if not self._path.exists():
            with open(self._path, "w", newline="") as f:
                csv.DictWriter(f, fieldnames=_LOG_FIELDS).writeheader()

    def log_event(
        self,
        event: str,
        state: ImbalanceState,
        cum_imb: float | None,
        roll_imb: float | None,
        label: str,
    ) -> None:
        r_buy = r_sell = r_unk = 0.0
        for (_, cls, vol) in state.rolling:
            if cls == "buy":    r_buy  += vol
            elif cls == "sell": r_sell += vol
            else:               r_unk  += vol

        row = {
            "timestamp":         datetime.now(_IST).isoformat(timespec="seconds"),
            "symbol":            state.symbol,
            "event":             event,
            "cum_imbalance":     round(cum_imb,  4) if cum_imb  is not None else "",
            "rolling_imbalance": round(roll_imb, 4) if roll_imb is not None else "",
            "rolling_label":     label,
            "buy_vol":           int(state.buy_vol),
            "sell_vol":          int(state.sell_vol),
            "unk_vol":           int(state.unk_vol),
            "rolling_buy_vol":   int(r_buy),
            "rolling_sell_vol":  int(r_sell),
            "rolling_unk_vol":   int(r_unk),
        }
        with self._lock:
            with open(self._path, "a", newline="") as f:
                csv.DictWriter(f, fieldnames=_LOG_FIELDS).writerow(row)


# ── Self-test ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    PASS = "\033[32mPASS\033[0m"
    FAIL = "\033[31mFAIL\033[0m"
    failures = 0

    def check(label: str, cond: bool, detail: str = "") -> None:
        global failures
        if cond:
            print(f"  {PASS}  {label}")
        else:
            failures += 1
            print(f"  {FAIL}  {label}" + (f"  [{detail}]" if detail else ""))

    def ts(h: int, m: int, s: int = 0) -> datetime:
        return datetime(2026, 8, 4, h, m, s, tzinfo=_IST)

    # ── Scenario: strong buy morning, flat afternoon, sell-off in last 15 min ──
    #
    # Demonstrates the divergence this module exists to catch:
    #   cumulative imbalance stays positive (morning buying was dominant),
    #   rolling imbalance goes negative (selling wins the final 15 minutes),
    #   label → "skip" at the moment of interest.

    print("Scenario — strong buy morning / flat afternoon / late sell-off\n")

    s = ImbalanceState(symbol="TEST")

    # ── First tick: reference only, no classification ─────────────────────────
    c, r, lbl = update_imbalance(s, ltp=100.0, cum_vol=0, now=ts(9, 15, 0))
    check("First tick → no_data (reference tick)", lbl == "no_data" and c is None,
          f"c={c} r={r} lbl={lbl}")

    # ── Morning buying 9:16–9:25: large volumes, price consistently up ────────
    # Each tick: (ltp, cum_vol, expected direction)
    morning = [
        (102.0, 300_000,   ts(9, 16)),   # +300k buy
        (104.0, 800_000,   ts(9, 18)),   # +500k buy
        (107.0, 1_500_000, ts(9, 20)),   # +700k buy
        (106.0, 1_700_000, ts(9, 21)),   # +200k sell
        (108.0, 2_500_000, ts(9, 22)),   # +800k buy
        (107.0, 2_800_000, ts(9, 23)),   # +300k sell
        (109.0, 3_500_000, ts(9, 25)),   # +700k buy
    ]
    for ltp_, cv_, t_ in morning:
        c, r, lbl = update_imbalance(s, ltp=ltp_, cum_vol=cv_, now=t_)

    # buy=300+500+700+800+700=3000k  sell=200+300=500k
    # cum_imb = (3000-500)/(3000+500) ≈ 0.714  roll_imb > 0.3 (all within 15 min)
    check("After morning: cumulative positive",       c is not None and c > 0,   f"c={c:.4f}")
    check("After morning: rolling positive > 0.3",    r is not None and r > 0.3, f"r={r:.4f}")
    check("After morning: label is buy_full",          lbl == "buy_full",         f"lbl={lbl}")
    check("After morning: buy_vol > sell_vol",         s.buy_vol > s.sell_vol,
          f"buy={s.buy_vol:,.0f} sell={s.sell_vol:,.0f}")

    print()

    # ── Flat afternoon 11:00–14:15: price unchanged, small unk volumes ────────
    # All morning rolling entries expire by 11:00 (>15 min ago).
    flat = [
        (109.0, 4_000_000, ts(11,  0)),   # +500k unk
        (109.0, 4_200_000, ts(12, 30)),   # +200k unk
        (109.0, 4_400_000, ts(13, 45)),   # +200k unk
        (109.0, 4_500_000, ts(14, 15)),   # +100k unk  (16 min before sell-off)
    ]
    for ltp_, cv_, t_ in flat:
        c, r, lbl = update_imbalance(s, ltp=ltp_, cum_vol=cv_, now=t_)

    # Morning rolling entries are all >15 min old and pruned; only unk left.
    # r_buy=0, r_sell=0 → roll_imb=None → "no_data"
    check("After flat: cumulative still positive (morning buy dominant)",
          c is not None and c > 0, f"c={c:.4f}")
    check("After flat: rolling has no directional data (unk only / expired)",
          r is None, f"r={r}")
    check("After flat: label is no_data",   lbl == "no_data", f"lbl={lbl}")

    print()

    # ── Late sell-off 14:31–14:42: heavy selling, light buying ───────────────
    # All within last 15 min. Morning entries are long gone from rolling window.
    selloff = [
        (107.0, 4_600_000, ts(14, 31)),   # +100k sell
        (106.0, 4_750_000, ts(14, 33)),   # +150k sell
        (108.0, 4_800_000, ts(14, 35)),   # +50k  buy
        (105.0, 5_100_000, ts(14, 38)),   # +300k sell
        (104.0, 5_450_000, ts(14, 42)),   # +350k sell  ← final tick
    ]
    for ltp_, cv_, t_ in selloff:
        c, r, lbl = update_imbalance(s, ltp=ltp_, cum_vol=cv_, now=t_)

    # cum: buy=3050k sell=1400k  → cum_imb ≈ +0.37  (morning still dominant)
    # rolling: buy=50k sell=900k → roll_imb ≈ -0.89  (sell-off wins)
    check("Final tick: cumulative POSITIVE (morning buying still dominant)",
          c is not None and c > 0,
          f"cum_imb={c:.4f}  buy={s.buy_vol:,.0f}  sell={s.sell_vol:,.0f}")
    check("Final tick: rolling NEGATIVE (late sell-off wins)",
          r is not None and r < 0,
          f"roll_imb={r:.4f}")
    check("Final tick: label is 'skip'",   lbl == "skip", f"lbl={lbl}")

    # Core assertion: cumulative and rolling tell OPPOSITE stories
    check("DIVERGENCE: cum > 0 AND rolling < 0 simultaneously",
          c is not None and r is not None and c > 0 and r < 0,
          f"cum={c:.4f}  rolling={r:.4f}")

    print(f"\n  Values at final tick:")
    print(f"    buy_vol={s.buy_vol:>12,.0f}  sell_vol={s.sell_vol:>12,.0f}  unk_vol={s.unk_vol:>12,.0f}")
    print(f"    cum_imbalance   = {c:+.4f}")
    print(f"    rolling_imb     = {r:+.4f}")
    print(f"    rolling_label   = {lbl}")
    print(f"    rolling entries = {len(s.rolling)}")

    # ── make_label boundary tests ─────────────────────────────────────────────
    print("\nmake_label() boundary tests:\n")
    check("make_label(0.31)  → buy_full",     make_label(0.31)  == "buy_full")
    check("make_label(0.30)  → wait_recheck", make_label(0.30)  == "wait_recheck")
    check("make_label(0.15)  → wait_recheck", make_label(0.15)  == "wait_recheck")
    check("make_label(0.001) → wait_recheck", make_label(0.001) == "wait_recheck")
    check("make_label(0.0)   → skip",         make_label(0.0)   == "skip")
    check("make_label(-0.5)  → skip",         make_label(-0.5)  == "skip")
    check("make_label(None)  → no_data",      make_label(None)  == "no_data")

    # ── Edge: zero delta skipped, no spurious classification ─────────────────
    print("\nEdge case — zero delta tick (reconnect snapshot):\n")
    s2 = ImbalanceState(symbol="EDGE")
    update_imbalance(s2, ltp=100.0, cum_vol=1_000_000, now=ts(10, 0))   # reference
    c2, r2, lbl2 = update_imbalance(s2, ltp=101.0, cum_vol=1_000_000, now=ts(10, 1))  # delta=0
    check("Zero-delta tick: no classification added (buy_vol=0, sell_vol=0)",
          s2.buy_vol == 0 and s2.sell_vol == 0,
          f"buy={s2.buy_vol} sell={s2.sell_vol}")
    check("Zero-delta tick: label is no_data", lbl2 == "no_data", f"lbl={lbl2}")

    print(f"\n{'─' * 55}")
    if failures == 0:
        print("\033[32mAll assertions PASSED\033[0m")
        sys.exit(0)
    else:
        print(f"\033[31m{failures} assertion(s) FAILED\033[0m")
        sys.exit(1)
