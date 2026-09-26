#!/usr/bin/env python3
"""
main.py — NSE pipeline production orchestrator
===============================================
Replaces run_daily.py. Called by run_pipeline.sh via cron at 3:06 PM IST Mon–Fri.

Flow
----
  Step 1 : Fetch today's market cap via data_loader.load_market_cap()
  Step 2 : Detect new symbols vs universe_combined.csv; append new entrants
  Step 3 : For new symbols — resolve Dhan securityId, backfill 6-month candles
           For all symbols with candle files — fetch today's 15-min intraday candles
  Step 4 : Run STRICT signal check via signal_engine.get_signals()
  Step 5 : Write results/trades/trade_list_<date>.csv
  Step 6 : Send Telegram notification via notify.send_success() / send_failure()

Output: results/trades/trade_list_<date>.csv
"""

import socket

_orig_getaddrinfo = socket.getaddrinfo
def _ipv4_only_getaddrinfo(*args, **kwargs):
    return [r for r in _orig_getaddrinfo(*args, **kwargs) if r[0] == socket.AF_INET]
socket.getaddrinfo = _ipv4_only_getaddrinfo

import csv
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from dateutil.relativedelta import relativedelta

_PIPELINE_DIR = Path(__file__).resolve().parent
_ROOT         = _PIPELINE_DIR.parent
sys.path.insert(0, str(_PIPELINE_DIR))
sys.path.insert(0, str(_ROOT))

# ── Load .env (data_loader also does this, but load early so notify has creds) ─
_env_file = _PIPELINE_DIR / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            k, _, v = _line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

import data_loader
import signal_engine
import notify
from common.calc_utils import compute_allocation, compute_shares
from dhan.trade import security_id

# ── Paths ──────────────────────────────────────────────────────────────────────
UNIVERSE_FILE   = _ROOT / "data" / "universe_combined.csv"
INSTRUMENTS_DIR = _ROOT / "data" / "instruments"
CANDLES_DIR     = _ROOT / "data" / "candles"
TRADES_DIR      = _ROOT / "results" / "trades"
TRADES_DIR.mkdir(parents=True, exist_ok=True)

TODAY       = date.today()
BACKFILL_START = TODAY - relativedelta(months=6)
TOTAL_CAPITAL = 500_000

FIELDNAMES = ["symbol", "shares", "ref_price", "return_pct"]


# ── Universe / instrument helpers ──────────────────────────────────────────────

def _load_universe_symbols() -> set:
    with open(UNIVERSE_FILE, newline="") as f:
        return {r["symbol"].strip().upper() for r in csv.DictReader(f)}


def _append_new_to_universe(new_syms: dict) -> None:
    with open(UNIVERSE_FILE, "a", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["symbol", "snapshot_dates_appeared", "most_recent_mcap_cr"],
            extrasaction="ignore",
        )
        for sym, mcap in new_syms.items():
            w.writerow({
                "symbol":                    sym,
                "snapshot_dates_appeared":   TODAY.isoformat(),
                "most_recent_mcap_cr":       round(mcap, 2),
            })
    print(f"  Appended {len(new_syms)} new symbol(s) to {UNIVERSE_FILE.name}")


def _resolve_new_symbols(new_syms: dict) -> tuple:
    """Resolve each new symbol to a Dhan securityId via the shared scrip-master
    cache (dhan.trade.security_id) -- already the source of truth for every
    real order this system places, so no separate instrument-matching step is
    needed the way Upstox required. Returns (matched, unmatched); matched is a
    list of {"symbol": sym} dicts ready for data_loader.load_candles()."""
    matched, unmatched = [], []
    for sym in new_syms:
        try:
            security_id(sym)
        except ValueError:
            unmatched.append(sym)
            continue
        matched.append({"symbol": sym})
    return matched, unmatched


def _log_unmatched(unmatched: list) -> None:
    if not unmatched:
        return
    unmatched_file = INSTRUMENTS_DIR / "dhan_unmatched.csv"
    with open(unmatched_file, "a", newline="") as f:
        csv.writer(f).writerows([[s] for s in unmatched])
    print(f"  Appended {len(unmatched)} unmatched (no Dhan securityId) → {unmatched_file.name}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    t0          = time.time()
    mcap_status = "fresh"
    failed_step = ""
    new_syms    = {}

    print("=" * 60)
    print(f"main.py — {TODAY.isoformat()}")
    print("=" * 60)

    try:
        # ── Step 1: Fetch market cap ───────────────────────────────────────────
        failed_step = "Step 1: fetch market cap"
        print("\n── Step 1: Fetch market cap ────────────────────────────────")
        today_syms, mcap_status = data_loader.load_market_cap()
        if mcap_status == "failed":
            raise RuntimeError("Market cap fetch failed with no fallback.")
        print(f"  market_cap status: {mcap_status}  ({len(today_syms):,} symbols)")

        # ── Step 2: Update universe ────────────────────────────────────────────
        failed_step = "Step 2: update universe"
        print("\n── Step 2: Update universe ─────────────────────────────────")
        universe_syms = _load_universe_symbols()
        new_syms      = {s: m for s, m in today_syms.items() if s not in universe_syms}

        if not new_syms:
            print(f"  Universe unchanged — {len(universe_syms):,} symbols.")
        else:
            print(f"  {len(new_syms)} new symbol(s): "
                  + ", ".join(sorted(new_syms)[:10])
                  + ("…" if len(new_syms) > 10 else ""))
            _append_new_to_universe(new_syms)

        # ── Step 3: Candle data update ─────────────────────────────────────────
        failed_step = "Step 3: candle data update"
        print("\n── Step 3: Candle data update ──────────────────────────────")

        if new_syms:
            matched_new, unmatched_new = _resolve_new_symbols(new_syms)
            _log_unmatched(unmatched_new)
            print(f"  Resolved {len(matched_new)}/{len(new_syms)} new symbol(s) to a Dhan securityId.")
            if matched_new:
                print(f"  Backfilling {len(matched_new)} new symbol(s) from {BACKFILL_START} …")
                data_loader.load_candles(
                    matched_new, interval="15minute", mode="historical",
                    from_date=BACKFILL_START, to_date=TODAY,
                )
        else:
            print("  No new symbols — skipping instrument resolution and historical backfill.")

        candle_instruments = [{"symbol": p.stem} for p in sorted(CANDLES_DIR.glob("*.csv"))]
        print(f"  Fetching 15min intraday for {len(candle_instruments):,} symbols …")
        data_loader.load_candles(candle_instruments, interval="15minute", mode="intraday")

        # ── Step 4: Generate trade list ────────────────────────────────────────
        failed_step = "Step 4: generate trade list"
        print(f"\n── Step 4: Signal check (STRICT mode) ──────────────────────")
        print(f"  Universe: {len(today_syms):,} symbols")
        raw_signals = signal_engine.get_signals(today_syms, mode="strict")

        if not raw_signals:
            print(f"\n  No signals for {TODAY.isoformat()} — trade list not written.")
        else:
            n          = len(raw_signals)
            allocation = compute_allocation(TOTAL_CAPITAL, n)
            print(f"\n  Capital: ₹{TOTAL_CAPITAL:,.0f}  |  {n} signal(s)  →  ₹{allocation:,.0f}/stock")

            signals = []
            for s in raw_signals:
                shares = compute_shares(allocation, s["ref_price"])
                signals.append({
                    "symbol":     s["symbol"],
                    "shares":     shares,
                    "ref_price":  round(s["ref_price"], 2),
                    "return_pct": s["return_pct"],
                })

            trade_path = TRADES_DIR / f"trade_list_{TODAY.isoformat()}.csv"
            with open(trade_path, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=FIELDNAMES)
                w.writeheader()
                w.writerows(signals)

            print(f"\n  {'Symbol':<15}  {'Shares':>7}  {'Ref Price (3PM Open)':>20}")
            print(f"  {'-'*15}  {'-'*7}  {'-'*20}")
            for r in signals:
                print(f"  {r['symbol']:<15}  {r['shares']:>7}  ₹{r['ref_price']:>19,.2f}")
            print(f"\n  Trade list → {trade_path}")

        elapsed = time.time() - t0
        print(f"\n── Daily Run Complete ───────────────────────────────────────")
        print(f"  New symbols added : {len(new_syms):,}")
        print(f"  Total time        : {elapsed:.1f}s")
        print("=" * 60)

        # ── Step 5: Notify ─────────────────────────────────────────────────────
        print("\n── Sending notification ────────────────────────────────────")
        notify.send_success(TODAY.isoformat(), t0, mcap_status)

    except Exception as exc:
        elapsed = time.time() - t0
        print(f"\n── PIPELINE FAILED ─────────────────────────────────────────")
        print(f"  Step        : {failed_step}")
        print(f"  Error       : {exc}")
        print(f"  Time elapsed: {elapsed:.1f}s")
        print("=" * 60)
        try:
            notify.send_failure(TODAY.isoformat(), failed_step, str(exc), t0, mcap_status)
        except Exception as notify_exc:
            print(f"  WARNING: notification also failed: {notify_exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
