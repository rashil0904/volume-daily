#!/usr/bin/env python3
"""
imbalance_tracker.py — order-book depth imbalance tracker
===========================================================
Observation and CSV logging only. No order placement, no trading logic.

Plugs into live_monitor.py as a parallel logger. One ImbalanceState per
symbol; update_imbalance() called once per tick alongside evaluate_tick().
Writes to logs/imbalance_<date>.csv — separate from live_monitor's log.

Requires KiteTicker MODE_FULL (not MODE_QUOTE) — depth is only present in
full-mode ticks. Each tick's 5-level bid/ask depth is summed on each side:

  imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty)   range [-1, +1]

Two figures tracked per symbol:
  instant — this tick's depth imbalance, a raw order-book snapshot
  rolling — mean of the instant imbalance over the last ROLLING_WINDOW_SECS

The rolling figure is the operationally meaningful one: a single large
resting order at one level can swing the instant imbalance hard in either
direction for one tick; averaging over the window smooths that out and
reflects sustained book pressure rather than a momentary snapshot.

NOTE: LABEL_BUY_THRESHOLD/LABEL_NEUTRAL_FLOOR below carry over the same
values used by the previous tick-rule version of this module, but that
version's imbalance was a ratio of *accumulated volume* over a window,
not an average of *instantaneous order-book* ratios -- the two metrics
don't necessarily have the same typical range or noise profile. Treat
these thresholds as an untuned starting point until validated against
real MODE_FULL data.

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
ROLLING_WINDOW_SECS = 20 * 60   # rolling window width in seconds
LABEL_BUY_THRESHOLD = 0.3       # rolling_imb > this           → "buy_full"
LABEL_NEUTRAL_FLOOR = 0.0       # rolling_imb in (this, 0.3]  → "wait_recheck"
                                 # rolling_imb <= this          → "skip"


# ── Per-symbol state ──────────────────────────────────────────────────────────

@dataclass
class ImbalanceState:
    symbol:          str

    # Most recent tick's raw depth totals + instant imbalance
    last_bid_qty:    int   = 0
    last_ask_qty:    int   = 0
    last_imbalance:  float | None = None

    # Rolling window: deque of (unix_ts: float, instant_imbalance: float)
    # Pruned to ROLLING_WINDOW_SECS on every update_imbalance() call.
    rolling: deque = field(default_factory=deque)


# ── Core update — pure function (except state mutation) ───────────────────────

def update_imbalance(
    state: ImbalanceState,
    bid_qty: int,
    ask_qty: int,
    now: datetime | None = None,
) -> tuple[float | None, float | None, str]:
    """
    Process one tick's depth for a symbol. Mutates state in place.

    Parameters
    ----------
    state   : per-symbol ImbalanceState
    bid_qty : total quantity across all 5 bid depth levels
              (sum of tick["depth"]["buy"][i]["quantity"] for i in 0..4)
    ask_qty : total quantity across all 5 ask depth levels
              (sum of tick["depth"]["sell"][i]["quantity"] for i in 0..4)
    now     : current time; defaults to datetime.now(_IST); injectable for tests

    Returns
    -------
    (instant_imbalance, rolling_imbalance, label)
      - instant_imbalance: None if bid_qty == ask_qty == 0 (no depth data)
      - rolling_imbalance: None if no samples in the rolling window
      - label            : one of "buy_full" | "wait_recheck" | "skip" | "no_data"
    """
    if now is None:
        now = datetime.now(_IST)
    now_ts = now.timestamp()

    state.last_bid_qty = bid_qty
    state.last_ask_qty = ask_qty

    total = bid_qty + ask_qty
    instant_imb = (bid_qty - ask_qty) / total if total > 0 else None
    state.last_imbalance = instant_imb

    if instant_imb is not None:
        state.rolling.append((now_ts, instant_imb))

    # Prune rolling window to the configured lookback
    cutoff = now_ts - ROLLING_WINDOW_SECS
    while state.rolling and state.rolling[0][0] < cutoff:
        state.rolling.popleft()

    rolling_imb = (
        sum(v for _, v in state.rolling) / len(state.rolling)
        if state.rolling else None
    )

    return instant_imb, rolling_imb, make_label(rolling_imb)


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
    "instant_imbalance", "rolling_imbalance", "rolling_label",
    "bid_qty", "ask_qty", "rolling_samples",
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
        instant_imb: float | None,
        roll_imb: float | None,
        label: str,
    ) -> None:
        row = {
            "timestamp":         datetime.now(_IST).isoformat(timespec="seconds"),
            "symbol":            state.symbol,
            "event":             event,
            "instant_imbalance": round(instant_imb, 4) if instant_imb is not None else "",
            "rolling_imbalance": round(roll_imb, 4) if roll_imb is not None else "",
            "rolling_label":     label,
            "bid_qty":           state.last_bid_qty,
            "ask_qty":           state.last_ask_qty,
            "rolling_samples":   len(state.rolling),
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

    # ── Scenario: one large resting bid spikes the instant reading, but the
    #    sustained rolling average reveals the book is actually ask-heavy ──
    #
    # Demonstrates the divergence this module exists to catch: a single
    # snapshot (instant_imbalance) can be misleading -- one big resting order
    # at one level -- while the rolling average over real sustained ticks
    # tells the true story.

    print("Scenario — one bid-heavy spike vs. sustained ask-heavy book\n")

    s = ImbalanceState(symbol="TEST")

    # ── Spike tick: one huge resting bid dominates the book momentarily ───────
    i, r, lbl = update_imbalance(s, bid_qty=90_000, ask_qty=10_000, now=ts(10, 0, 0))
    check("Spike tick: instant strongly positive (bid-heavy)",
          i is not None and i > 0.7, f"i={i}")
    check("Spike tick: rolling == instant (only one sample so far)",
          r is not None and abs(r - i) < 1e-9, f"r={r} i={i}")
    check("Spike tick: label is buy_full (single sample so far)",
          lbl == "buy_full", f"lbl={lbl}")

    print()

    # ── Sustained ask-heavy ticks over the following minutes ──────────────────
    sustained = [
        (20_000, 60_000, ts(10, 2)),
        (18_000, 62_000, ts(10, 4)),
        (22_000, 58_000, ts(10, 6)),
        (19_000, 61_000, ts(10, 8)),
        (21_000, 59_000, ts(10, 10)),
        (20_000, 60_000, ts(10, 12)),
    ]
    for bid_, ask_, t_ in sustained:
        i, r, lbl = update_imbalance(s, bid_qty=bid_, ask_qty=ask_, now=t_)

    # Latest tick alone is ask-heavy:
    check("Final tick: instant is ask-heavy (negative)",
          i is not None and i < 0, f"i={i:.4f}")
    # Rolling average blends the one bid spike with six sustained ask-heavy
    # ticks -- ask-heavy dominates by count, so the average should also be
    # negative even though the very first sample was strongly positive.
    check("Final tick: rolling average is ALSO negative (sustained pressure wins)",
          r is not None and r < 0, f"r={r:.4f}")
    check("Final tick: label is 'skip'", lbl == "skip", f"lbl={lbl}")
    check("DIVERGENCE: first-sample instant was positive, final rolling is negative",
          r is not None and r < 0, f"rolling={r:.4f}")

    print(f"\n  Values at final tick:")
    print(f"    bid_qty={s.last_bid_qty:>10,}  ask_qty={s.last_ask_qty:>10,}")
    print(f"    instant_imbalance = {i:+.4f}")
    print(f"    rolling_imbalance = {r:+.4f}")
    print(f"    rolling_label     = {lbl}")
    print(f"    rolling samples   = {len(s.rolling)}")

    # ── Rolling window pruning: samples older than 20 min drop off ───────────
    print("\nRolling window pruning — old samples expire\n")
    s2 = ImbalanceState(symbol="PRUNE")
    update_imbalance(s2, bid_qty=90_000, ask_qty=10_000, now=ts(9, 0, 0))   # will expire
    check("Old sample present before pruning", len(s2.rolling) == 1)
    i2, r2, lbl2 = update_imbalance(s2, bid_qty=10_000, ask_qty=90_000, now=ts(9, 21, 0))
    check("Old sample (21 min ago) pruned from rolling window",
          len(s2.rolling) == 1, f"len={len(s2.rolling)}")
    check("Rolling reflects only the fresh sample (strongly negative)",
          r2 is not None and r2 < -0.7, f"r2={r2}")

    # ── make_label() boundary tests ───────────────────────────────────────────
    print("\nmake_label() boundary tests:\n")
    check("make_label(0.31)  → buy_full",     make_label(0.31)  == "buy_full")
    check("make_label(0.30)  → wait_recheck", make_label(0.30)  == "wait_recheck")
    check("make_label(0.15)  → wait_recheck", make_label(0.15)  == "wait_recheck")
    check("make_label(0.001) → wait_recheck", make_label(0.001) == "wait_recheck")
    check("make_label(0.0)   → skip",         make_label(0.0)   == "skip")
    check("make_label(-0.5)  → skip",         make_label(-0.5)  == "skip")
    check("make_label(None)  → no_data",      make_label(None)  == "no_data")

    # ── Edge: zero depth on both sides → no classification ───────────────────
    print("\nEdge case — empty depth (no bid or ask quantity):\n")
    s3 = ImbalanceState(symbol="EDGE")
    i3, r3, lbl3 = update_imbalance(s3, bid_qty=0, ask_qty=0, now=ts(10, 0))
    check("Zero depth: instant_imbalance is None", i3 is None, f"i3={i3}")
    check("Zero depth: no sample added to rolling window", len(s3.rolling) == 0)
    check("Zero depth: label is no_data", lbl3 == "no_data", f"lbl={lbl3}")

    print(f"\n{'─' * 55}")
    if failures == 0:
        print("\033[32mAll assertions PASSED\033[0m")
        sys.exit(0)
    else:
        print(f"\033[31m{failures} assertion(s) FAILED\033[0m")
        sys.exit(1)
