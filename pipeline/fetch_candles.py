#!/usr/bin/env python3
"""
fetch_candles.py
================
Part 1: Resolve universe_combined.csv symbols → Upstox instrument_keys (NSE EQ/BE).
Part 2: Fetch 15-min historical candles (2022-01-01 → today) via Upstox V3 API.

Env vars:
  UPSTOX_ACCESS_TOKEN — Upstox API Bearer token (expires daily; regenerate before each run)

Outputs:
  instruments/upstox_instruments.csv   — matched: symbol, instrument_key, trading_symbol, series, exchange
  instruments/upstox_unmatched.csv     — symbols with no Upstox match (delisted / renamed)
  candles/<SYMBOL>.csv                 — per-symbol 15-min candles (timestamp,open,high,low,close,volume,oi)

Rate limits (confirmed from Upstox docs):
  50 req/sec | 500 req/min | 2,000 req/30min  →  binding limit: ~66/min
  With 4 workers × 4s delay each → ~60 req/min total (10 % headroom)

Estimated runtime: ~14 hours for 936 symbols × 55 monthly chunks. Run overnight.
"""

import calendar
import csv
import gzip
import json
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import quote

import requests

# ── Config ─────────────────────────────────────────────────────────────────────
ACCESS_TOKEN = (os.environ.get("UPSTOX_ACCESS_TOKEN") or "").strip()
if not ACCESS_TOKEN:
    sys.exit(
        "ERROR: Set UPSTOX_ACCESS_TOKEN environment variable.\n"
        "Upstox tokens expire daily — generate a fresh one at "
        "https://upstox.com/developer/api-documentation/authentication before each run."
    )

_ROOT           = Path(__file__).resolve().parent.parent
UNIVERSE_FILE   = _ROOT / "data" / "universe_combined.csv"
INSTRUMENTS_DIR = _ROOT / "data" / "instruments"
CANDLES_DIR     = _ROOT / "data" / "candles"
INSTRUMENTS_DIR.mkdir(parents=True, exist_ok=True)
CANDLES_DIR.mkdir(parents=True, exist_ok=True)

NSE_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
BASE_API            = "https://api.upstox.com/v3"

HISTORY_FROM = date.today() - timedelta(days=365)
HISTORY_TO   = date.today()

MAX_RETRIES = 3
WORKERS     = 5
CALL_DELAY  = 0.8  # seconds between API calls per worker → ~300 req/min total across 4 workers

API_HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/json",
}


# ── Date helpers ───────────────────────────────────────────────────────────────

def month_chunks(start: date, end: date):
    """1-month windows covering [start, end] — max range for 15-min candle requests."""
    chunks, cur = [], start
    while cur <= end:
        _, last = calendar.monthrange(cur.year, cur.month)
        chunk_end = min(date(cur.year, cur.month, last), end)
        chunks.append((cur, chunk_end))
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return chunks


# ── Part 1: Instrument matching ────────────────────────────────────────────────

def load_universe():
    if not UNIVERSE_FILE.exists():
        sys.exit(f"ERROR: {UNIVERSE_FILE} not found.")
    with open(UNIVERSE_FILE, newline="") as f:
        return [r["symbol"].strip().upper() for r in csv.DictReader(f)]


def download_nse_instruments():
    print("Downloading Upstox NSE instrument master …")
    resp = requests.get(NSE_INSTRUMENTS_URL, timeout=60)
    resp.raise_for_status()
    data = json.loads(gzip.decompress(resp.content))
    print(f"  {len(data):,} instruments loaded.")
    if data:
        print(f"  Field names: {list(data[0].keys())}")
    return data


def match_instruments(universe, instruments):
    # Filter to NSE EQ-segment equities (EQ and BE series)
    eq_instruments = [
        i for i in instruments
        if i.get("instrument_type") in ("EQ", "BE")
        and "NSE_EQ" in i.get("segment", "")
    ]
    print(f"  NSE EQ/BE instruments: {len(eq_instruments):,}")

    # Build lookup: base_symbol → list of candidates
    # Upstox trading_symbol can be "RELIANCE", "RELIANCE-EQ", or "RELIANCE-BE"
    lookup = {}
    for inst in eq_instruments:
        ts = inst.get("trading_symbol", "")
        base = ts.split("-")[0].upper().strip()
        lookup.setdefault(base, []).append(inst)

    matched, unmatched = [], []
    for sym in universe:
        candidates = lookup.get(sym, [])
        if not candidates:
            unmatched.append(sym)
            continue
        # Prefer EQ over BE when both exist
        eq_cands = [c for c in candidates if c.get("instrument_type") == "EQ"]
        chosen = eq_cands[0] if eq_cands else candidates[0]
        matched.append({
            "symbol":         sym,
            "instrument_key": chosen["instrument_key"],
            "trading_symbol": chosen.get("trading_symbol", ""),
            "series":         chosen.get("instrument_type", ""),
            "exchange":       chosen.get("exchange", "NSE"),
        })

    return matched, unmatched


def save_instruments(matched, unmatched):
    out = INSTRUMENTS_DIR / "upstox_instruments.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(
            f, fieldnames=["symbol", "instrument_key", "trading_symbol", "series", "exchange"]
        )
        w.writeheader()
        w.writerows(matched)
    print(f"  Matched   → {out}  ({len(matched):,} symbols)")

    out2 = INSTRUMENTS_DIR / "upstox_unmatched.csv"
    with open(out2, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol"])
        w.writerows([[s] for s in unmatched])
    print(f"  Unmatched → {out2}  ({len(unmatched):,} symbols)")


# ── Part 2: Candle fetch ───────────────────────────────────────────────────────

def fetch_chunk(session, instrument_key, from_d, to_d):
    """Fetch one 1-month chunk. Returns list of candle arrays, raises on permanent failure."""
    encoded_key = quote(instrument_key, safe="")
    url = f"{BASE_API}/historical-candle/{encoded_key}/minutes/15/{to_d}/{from_d}"

    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(CALL_DELAY)
        try:
            resp = session.get(url, headers=API_HEADERS, timeout=30)

            if resp.status_code == 429:
                backoff = 60 * attempt
                print(f"    429 rate-limited — backing off {backoff}s …")
                time.sleep(backoff)
                continue

            if resp.status_code == 200:
                return resp.json().get("data", {}).get("candles", [])

            resp.raise_for_status()

        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"chunk {from_d}–{to_d} failed after {MAX_RETRIES} attempts: {exc}")
            time.sleep(5 * attempt)

    return []


def fetch_symbol(symbol, instrument_key, total, counter, lock, from_date=None, to_date=None):
    """Historical fetch. from_date defaults to HISTORY_FROM, to_date defaults to today."""
    out_path = CANDLES_DIR / f"{symbol}.csv"

    # Idempotent: skip if already fetched
    if out_path.exists() and out_path.stat().st_size > 500:
        with lock:
            counter["done"] += 1
            counter["skipped"] += 1
            print(f"  [{counter['done']}/{total}] {symbol} — cached, skipping.")
        return

    session = requests.Session()
    chunks = month_chunks(from_date or HISTORY_FROM, to_date or HISTORY_TO)
    all_candles = []
    failed_chunks = 0

    for from_d, to_d in chunks:
        try:
            rows = fetch_chunk(session, instrument_key, from_d, to_d)
            all_candles.extend(rows)
        except Exception as exc:
            print(f"    WARN [{symbol}] chunk {from_d}–{to_d}: {exc}")
            failed_chunks += 1

    if failed_chunks == len(chunks):
        with lock:
            counter["done"] += 1
            counter["failed"] += 1
            print(f"  [{counter['done']}/{total}] {symbol} — FAILED (all {len(chunks)} chunks failed).")
        return

    seen, unique = set(), []
    for c in all_candles:
        if c[0] not in seen:
            seen.add(c[0])
            unique.append(c)
    unique.sort(key=lambda x: x[0])

    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "open", "high", "low", "close", "volume", "oi"])
        w.writerows(unique)

    with lock:
        counter["done"] += 1
        status = "partial" if failed_chunks else "ok"
        counter[status] += 1
        note = f" ({failed_chunks}/{len(chunks)} chunks failed)" if failed_chunks else ""
        print(f"  [{counter['done']}/{total}] {symbol} — {len(unique):,} candles{note}")


def fetch_all_candles(matched, from_date=None, to_date=None):
    """Historical fetch for a list of matched instruments."""
    total   = len(matched)
    counter = {"done": 0, "ok": 0, "partial": 0, "failed": 0, "skipped": 0}
    lock    = threading.Lock()
    start   = from_date or HISTORY_FROM
    end     = to_date or HISTORY_TO
    chunks_per_sym = len(month_chunks(start, end))
    est_h = (total * chunks_per_sym * CALL_DELAY / WORKERS) / 3600

    print(f"Fetching candles for {total} symbols ({chunks_per_sym} chunks each, from {start} to {end}) …")
    print(f"  Workers: {WORKERS}  |  Delay: {CALL_DELAY}s/call/worker  |  "
          f"Est. throughput: ~{int(WORKERS * 60 / CALL_DELAY)}/min")
    print(f"  Estimated runtime: ~{est_h:.1f} hours")

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {
            ex.submit(fetch_symbol, r["symbol"], r["instrument_key"], total, counter, lock, from_date, to_date): r["symbol"]
            for r in matched
        }
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as exc:
                sym = futures[fut]
                print(f"  UNHANDLED ERROR [{sym}]: {exc}")
                with lock:
                    counter["failed"] += 1

    elapsed = time.time() - t0
    print(f"\n── Candle Fetch Summary ─────────────────────────────────")
    print(f"  Fully fetched  : {counter['ok']:,}")
    print(f"  Partial        : {counter['partial']:,}  (some chunks failed — check logs)")
    print(f"  Failed entirely: {counter['failed']:,}")
    print(f"  Skipped/cached : {counter['skipped']:,}")
    print(f"  Time elapsed   : {elapsed / 3600:.2f} hours")


# ── Append-mode historical fetch (adds missing dates to existing files) ────────

def fetch_append_symbol(symbol, instrument_key, from_date, to_date, total, counter, lock):
    """Fetch candles for [from_date, to_date] and append only new rows to existing CSV."""
    out_path = CANDLES_DIR / f"{symbol}.csv"

    existing_ts = set()
    if out_path.exists():
        with open(out_path, newline="") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if row:
                    existing_ts.add(row[0])

    session = requests.Session()
    chunks = month_chunks(from_date, to_date)
    all_candles = []

    for from_d, to_d in chunks:
        try:
            rows = fetch_chunk(session, instrument_key, from_d, to_d)
            all_candles.extend(rows)
        except Exception as exc:
            print(f"    WARN [{symbol}] chunk {from_d}–{to_d}: {exc}")

    new_candles = [c for c in all_candles if c[0] not in existing_ts]
    if not new_candles:
        with lock:
            counter["done"] += 1
            counter["skipped"] += 1
        return

    new_candles.sort(key=lambda x: x[0])
    write_header = not out_path.exists()
    with open(out_path, "a", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(["timestamp", "open", "high", "low", "close", "volume", "oi"])
        w.writerows(new_candles)

    with lock:
        counter["done"] += 1
        counter["ok"] += 1
        print(f"  [{counter['done']}/{total}] {symbol} — +{len(new_candles)} candles appended")


def fetch_append_historical(matched, from_date, to_date):
    """Append historical candles for [from_date, to_date] to existing candle files."""
    total   = len(matched)
    counter = {"done": 0, "ok": 0, "skipped": 0, "failed": 0}
    lock    = threading.Lock()

    print(f"Appending historical candles for {total:,} symbols ({from_date} → {to_date}) …")
    print(f"  Workers: {WORKERS}  |  Delay: {CALL_DELAY}s/call/worker")

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {
            ex.submit(fetch_append_symbol, r["symbol"], r["instrument_key"],
                      from_date, to_date, total, counter, lock): r["symbol"]
            for r in matched
        }
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as exc:
                sym = futures[fut]
                print(f"  UNHANDLED ERROR [{sym}]: {exc}")
                with lock:
                    counter["failed"] += 1

    elapsed = time.time() - t0
    print(f"\n── Append Historical Summary ─────────────────────────────")
    print(f"  Appended       : {counter['ok']:,}")
    print(f"  Already current: {counter['skipped']:,}")
    print(f"  Failed         : {counter['failed']:,}")
    print(f"  Time elapsed   : {elapsed:.1f}s")


# ── Intraday fetch (today's data only) ────────────────────────────────────────

def fetch_intraday_symbol(symbol, instrument_key, total, counter, lock):
    """Fetch today's 15-min intraday candles and append new rows to candles/<symbol>.csv."""
    out_path = CANDLES_DIR / f"{symbol}.csv"

    existing_ts = set()
    if out_path.exists():
        with open(out_path, newline="") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if row:
                    existing_ts.add(row[0])

    session = requests.Session()
    encoded = quote(instrument_key, safe="")
    url = f"{BASE_API}/historical-candle/intraday/{encoded}/minutes/15"
    candles = []

    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(CALL_DELAY)
        try:
            resp = session.get(url, headers=API_HEADERS, timeout=30)
            if resp.status_code == 429:
                time.sleep(60 * attempt)
                continue
            if resp.status_code == 200:
                candles = resp.json().get("data", {}).get("candles", [])
                break
            resp.raise_for_status()
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                with lock:
                    counter["done"] += 1
                    counter["failed"] += 1
                    print(f"  [intraday {counter['done']}/{total}] {symbol} — FAILED: {exc}")
                return
            time.sleep(5 * attempt)

    new_candles = [c for c in candles if c[0] not in existing_ts]
    if not new_candles:
        with lock:
            counter["done"] += 1
            # If API returned nothing AND we have no candle for today, flag it
            today_str = date.today().isoformat()
            has_today = any(ts.startswith(today_str) for ts in existing_ts)
            if not candles and not has_today:
                counter["no_data"] += 1
                print(f"  [intraday {counter['done']}/{total}] {symbol} — no data from API (holiday/illiquid?)")
            else:
                counter["skipped"] += 1
        return

    write_header = not out_path.exists()
    with open(out_path, "a", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(["timestamp", "open", "high", "low", "close", "volume", "oi"])
        w.writerows(new_candles)

    with lock:
        counter["done"] += 1
        counter["ok"] += 1
        print(f"  [intraday {counter['done']}/{total}] {symbol} — +{len(new_candles)} candles")


def fetch_all_intraday(matched):
    """Append today's intraday candles for all matched instruments."""
    total   = len(matched)
    counter = {"done": 0, "ok": 0, "skipped": 0, "failed": 0, "no_data": 0}
    lock    = threading.Lock()

    print(f"Fetching intraday for {total:,} symbols with {WORKERS} workers …")
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {
            ex.submit(fetch_intraday_symbol, r["symbol"], r["instrument_key"], total, counter, lock): r["symbol"]
            for r in matched
        }
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as exc:
                sym = futures[fut]
                print(f"  UNHANDLED ERROR [intraday {sym}]: {exc}")
                with lock:
                    counter["failed"] += 1

    print(f"\n── Intraday Summary ──────────────────────────────────────")
    print(f"  Updated        : {counter['ok']:,}")
    print(f"  Already current: {counter['skipped']:,}")
    print(f"  No data from API: {counter['no_data']:,}  (outside market hours / holiday / illiquid)")
    print(f"  Failed         : {counter['failed']:,}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    # ── Part 1 ────────────────────────────────────────────────────────────────
    print("=" * 60)
    print("PART 1 — Instrument Matching")
    print("=" * 60)

    universe = load_universe()
    print(f"Universe: {len(universe):,} symbols from {UNIVERSE_FILE}")

    # Skip re-download if already done
    instruments_csv = INSTRUMENTS_DIR / "upstox_instruments.csv"
    if instruments_csv.exists():
        print(f"  {instruments_csv} already exists — loading cached match.")
        with open(instruments_csv, newline="") as f:
            matched = list(csv.DictReader(f))
        unmatched_csv = INSTRUMENTS_DIR / "upstox_unmatched.csv"
        unmatched_count = sum(1 for _ in open(unmatched_csv)) - 1 if unmatched_csv.exists() else "?"
        print(f"  Loaded {len(matched):,} matched, {unmatched_count} unmatched.")
    else:
        instruments = download_nse_instruments()
        matched, unmatched = match_instruments(universe, instruments)
        save_instruments(matched, unmatched)

    if not matched:
        sys.exit(
            "ERROR: No instruments matched.\n"
            "Check the 'Field names' line above — instrument_type / segment names may differ."
        )

    print(f"\nMatched: {len(matched):,} instruments ready for candle fetch.")

    # ── Part 2 ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("PART 2 — Historical 15-min Candle Fetch")
    print("=" * 60)

    fetch_all_candles(matched)

    print("=" * 60)
    print("Done.")


if __name__ == "__main__":
    main()
