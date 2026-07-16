#!/usr/bin/env python3
"""
run_daily.py
============
Daily orchestrator — all candle logic lives in fetch_candles.py.

Flow:
  1. Run fetch_market_cap.py  → refreshes market_cap_daily/market_cap_<date>.csv
  2. Compare today's symbols against universe_combined.csv
  3a. NEW symbols found  → append to universe, match to Upstox, backfill 1 year of candles
  3b. No new symbols     → log and skip to step 4
  4. ALL symbols with candle files → append today's intraday 15-min candles
"""

import csv
import os
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

# ── Load .env ──────────────────────────────────────────────────────────────────
_env_file = Path(__file__).resolve().parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            k, _, v = _line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

if not os.environ.get("UPSTOX_ACCESS_TOKEN"):
    sys.exit("ERROR: Set UPSTOX_ACCESS_TOKEN in .env or environment.")

# Import after env is set (fetch_candles checks for the token at import time)
sys.path.insert(0, str(Path(__file__).resolve().parent))
import fetch_candles as fc
import notify

# ── Paths ──────────────────────────────────────────────────────────────────────
UNIVERSE_FILE   = Path("universe_combined.csv")
INSTRUMENTS_DIR = Path("instruments")
CANDLES_DIR     = Path("candles")
MCAP_DAILY_DIR  = Path("market_cap_daily")
TODAY           = date.today()
ONE_YEAR_AGO    = TODAY - timedelta(days=365)


# ── Helpers ────────────────────────────────────────────────────────────────────

def load_today_mcap_symbols():
    mcap_file = MCAP_DAILY_DIR / f"market_cap_{TODAY.isoformat()}.csv"
    if not mcap_file.exists():
        candidates = sorted(MCAP_DAILY_DIR.glob("market_cap_*.csv"))
        if not candidates:
            raise FileNotFoundError("No market_cap_*.csv found in market_cap_daily/")
        mcap_file = candidates[-1]
        print(f"  Using fallback: {mcap_file.name}")
    syms = {}
    with open(mcap_file, newline="") as f:
        for row in csv.DictReader(f):
            sym = row["symbol"].strip().upper()
            syms[sym] = float(row.get("mcap_cr", 0))
    print(f"  Today's market cap: {len(syms):,} symbols  ({mcap_file.name})")
    return syms


def load_universe_symbols():
    with open(UNIVERSE_FILE, newline="") as f:
        return {r["symbol"].strip().upper() for r in csv.DictReader(f)}


def append_new_to_universe(new_syms: dict):
    with open(UNIVERSE_FILE, "a", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["symbol", "snapshot_dates_appeared", "most_recent_mcap_cr"],
            extrasaction="ignore",
        )
        for sym, mcap in new_syms.items():
            w.writerow({"symbol": sym, "snapshot_dates_appeared": TODAY.isoformat(),
                        "most_recent_mcap_cr": round(mcap, 2)})
    print(f"  Appended {len(new_syms)} new symbols to {UNIVERSE_FILE}")


def load_instrument_lookup():
    """Returns {symbol: instrument_key} for all already-matched symbols."""
    f = INSTRUMENTS_DIR / "upstox_instruments.csv"
    if not f.exists():
        return {}
    with open(f, newline="") as fh:
        return {r["symbol"].strip().upper(): r for r in csv.DictReader(fh)}


def append_to_instruments(matched, unmatched):
    inst_file = INSTRUMENTS_DIR / "upstox_instruments.csv"
    with open(inst_file, "a", newline="") as f:
        w = csv.DictWriter(
            f, fieldnames=["symbol", "instrument_key", "trading_symbol", "series", "exchange"]
        )
        w.writerows(matched)
    print(f"  Appended {len(matched)} matched → {inst_file}")

    unmatched_file = INSTRUMENTS_DIR / "upstox_unmatched.csv"
    with open(unmatched_file, "a", newline="") as f:
        csv.writer(f).writerows([[s] for s in unmatched])
    if unmatched:
        print(f"  Appended {len(unmatched)} unmatched → {unmatched_file}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    t0           = time.time()
    mcap_status  = "fresh"
    failed_step  = ""
    new_syms     = {}

    print("=" * 60)
    print(f"run_daily.py — {TODAY.isoformat()}")
    print("=" * 60)

    try:
        # ── Step 1: Fetch market cap ───────────────────────────────────────────
        failed_step = "Step 1: fetch_market_cap"
        print("\n── Step 1: Fetch market cap ────────────────────────────────")
        result = subprocess.run([sys.executable, "fetch_market_cap.py"], env=os.environ)
        if result.returncode == 2:
            mcap_status = "stale"
        elif result.returncode != 0:
            raise RuntimeError(f"fetch_market_cap.py failed (exit {result.returncode})")

        # ── Step 2: Update universe ────────────────────────────────────────────
        failed_step = "Step 2: update universe"
        print("\n── Step 2: Update universe ─────────────────────────────────")
        today_syms    = load_today_mcap_symbols()
        universe_syms = load_universe_symbols()
        new_syms      = {s: m for s, m in today_syms.items() if s not in universe_syms}

        if not new_syms:
            print(f"  Universe unchanged — {len(universe_syms):,} symbols.")
        else:
            print(f"  {len(new_syms)} new symbol(s): " + ", ".join(sorted(new_syms)[:10])
                  + ("…" if len(new_syms) > 10 else ""))
            append_new_to_universe(new_syms)

        # ── Step 3: Candle data update ─────────────────────────────────────────
        failed_step = "Step 3: candle data update"
        print("\n── Step 3: Candle data update ──────────────────────────────")

        if new_syms:
            existing_matched = load_instrument_lookup()
            to_match = [s for s in new_syms if s not in existing_matched]
            if to_match:
                instruments = fc.download_nse_instruments()
                matched_new, unmatched_new = fc.match_instruments(to_match, instruments)
                append_to_instruments(matched_new, unmatched_new)
            else:
                matched_new = []
                print("  All new symbols already in instrument master.")
            if matched_new:
                print(f"  Backfilling {len(matched_new)} new symbol(s) from {ONE_YEAR_AGO} …")
                fc.fetch_all_candles(matched_new, from_date=ONE_YEAR_AGO)
        else:
            print("  No new symbols — skipping instrument match and historical backfill.")

        inst_lookup = load_instrument_lookup()
        candle_instruments = [
            inst_lookup[p.stem]
            for p in sorted(CANDLES_DIR.glob("*.csv"))
            if p.stem in inst_lookup
        ]
        no_key = [p.stem for p in CANDLES_DIR.glob("*.csv") if p.stem not in inst_lookup]
        if no_key:
            print(f"  Note: {len(no_key)} candle file(s) have no instrument key — skipping.")
        print(f"  Fetching intraday for {len(candle_instruments):,} symbols (whole universe) …")
        fc.fetch_all_intraday(candle_instruments)

        # ── Step 4: Generate today's trade list ───────────────────────────────
        failed_step = "Step 4: prepare_data"
        print("\n── Step 4: Generate trade list ─────────────────────────────")
        result = subprocess.run([sys.executable, "prepare_data.py"], env=os.environ)
        if result.returncode != 0:
            raise RuntimeError(f"prepare_data.py exited with code {result.returncode}")

        elapsed = time.time() - t0
        print(f"\n── Daily Run Complete ───────────────────────────────────────")
        print(f"  New symbols added : {len(new_syms):,}")
        print(f"  Total time        : {elapsed:.1f}s")
        print("=" * 60)

        # ── Notify ────────────────────────────────────────────────────────────
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
