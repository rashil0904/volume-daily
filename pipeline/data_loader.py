#!/usr/bin/env python3
"""
data_loader.py — single fetch module for the NSE pipeline
==========================================================
All candle and market-cap fetching lives here. No signal logic.

Public API
----------
  download_nse_instruments()                            -> list of instrument records
  match_instruments(universe_syms, instruments)         -> (matched, unmatched)
  load_market_cap()                                     -> (symbols_dict, status_str)
  load_candles(matched, interval, mode, from_date, to_date)
      15-min modes persist to data/candles/<symbol>.csv (returns None).
      1-min intraday returns {symbol: [candle_list]} without persisting.

interval: "15minute" (default) | "1minute"
mode:     "intraday" | "eod-fill" | "historical" | "append"
"""

import calendar
import csv
import gzip
import json
import os
import sys
import threading
import time
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import quote

import requests

# ── Load .env ─────────────────────────────────────────────────────────────────
_env_file = Path(__file__).resolve().parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

# ── Paths ──────────────────────────────────────────────────────────────────────
_ROOT           = Path(__file__).resolve().parent.parent
UNIVERSE_FILE   = _ROOT / "data" / "universe_combined.csv"
INSTRUMENTS_DIR = _ROOT / "data" / "instruments"
CANDLES_DIR     = _ROOT / "data" / "candles"
MCAP_DIR        = _ROOT / "data" / "market_cap_daily"

INSTRUMENTS_DIR.mkdir(parents=True, exist_ok=True)
CANDLES_DIR.mkdir(parents=True, exist_ok=True)
MCAP_DIR.mkdir(parents=True, exist_ok=True)

# ── Upstox API constants ───────────────────────────────────────────────────────
_BASE_V3            = "https://api.upstox.com/v3"
NSE_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

MAX_RETRIES = 3
WORKERS     = 5
CALL_DELAY  = 0.8   # seconds between calls per worker; ~300 req/min across 5 workers


# ── Helpers ────────────────────────────────────────────────────────────────────

def _api_headers() -> dict:
    token = (os.environ.get("UPSTOX_ACCESS_TOKEN") or "").strip()
    if not token:
        raise EnvironmentError(
            "UPSTOX_ACCESS_TOKEN not set. Add it to pipeline/.env before running."
        )
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def _interval_minutes(interval: str) -> str:
    """Convert interval string to Upstox URL segment ('15minute' → 'minutes/15')."""
    if interval == "1minute":
        return "minutes/1"
    return "minutes/15"


def month_chunks(start: date, end: date) -> list:
    """1-month windows covering [start, end] — max range for candle requests."""
    chunks, cur = [], start
    while cur <= end:
        _, last = calendar.monthrange(cur.year, cur.month)
        chunk_end = min(date(cur.year, cur.month, last), end)
        chunks.append((cur, chunk_end))
        cur = date(cur.year + 1, 1, 1) if cur.month == 12 else date(cur.year, cur.month + 1, 1)
    return chunks


# ── Instrument helpers ─────────────────────────────────────────────────────────

def download_nse_instruments() -> list:
    """Download Upstox NSE instrument master. Returns list of instrument dicts."""
    print("Downloading Upstox NSE instrument master …")
    resp = requests.get(NSE_INSTRUMENTS_URL, timeout=60)
    resp.raise_for_status()
    data = json.loads(gzip.decompress(resp.content))
    print(f"  {len(data):,} instruments loaded.")
    if data:
        print(f"  Field names: {list(data[0].keys())}")
    return data


def match_instruments(universe_syms: list, instruments: list) -> tuple:
    """Match NSE symbols to Upstox instrument keys. Returns (matched, unmatched)."""
    eq_instruments = [
        i for i in instruments
        if i.get("instrument_type") in ("EQ", "BE")
        and "NSE_EQ" in i.get("segment", "")
    ]
    print(f"  NSE EQ/BE instruments: {len(eq_instruments):,}")

    lookup: dict = {}
    for inst in eq_instruments:
        ts   = inst.get("trading_symbol", "")
        base = ts.split("-")[0].upper().strip()
        lookup.setdefault(base, []).append(inst)

    matched, unmatched = [], []
    for sym in universe_syms:
        candidates = lookup.get(sym, [])
        if not candidates:
            unmatched.append(sym)
            continue
        eq_cands = [c for c in candidates if c.get("instrument_type") == "EQ"]
        chosen   = eq_cands[0] if eq_cands else candidates[0]
        matched.append({
            "symbol":         sym,
            "instrument_key": chosen["instrument_key"],
            "trading_symbol": chosen.get("trading_symbol", ""),
            "series":         chosen.get("instrument_type", ""),
            "exchange":       chosen.get("exchange", "NSE"),
        })
    return matched, unmatched


def revalidate_instruments() -> None:
    """
    Re-check every already-tracked symbol's instrument_key against Upstox's current
    live instrument master, and correct any that have drifted.

    Confirmed to actually happen: POCL's cached key silently went stale after an
    ISIN change (corporate action) and started failing every candle fetch with
    "Invalid Instrument key" -- no error ever surfaced beyond a per-symbol log line.
    This closes that gap by re-validating the whole file daily, before the candle
    fetch step, instead of only matching a symbol once when it's first onboarded.

    Symbols no longer found at all in the current master (e.g. delisted/suspended --
    RELINFRA is a confirmed case) are flagged but left as-is; there's no valid key
    to replace them with, so they'll keep failing candle fetch until removed by hand.

    Rewrites data/instruments/upstox_instruments.csv in place only if something
    actually changed. Safe to re-run daily -- a no-op day just re-confirms.
    """
    inst_file = INSTRUMENTS_DIR / "upstox_instruments.csv"
    if not inst_file.exists():
        return

    with open(inst_file, newline="") as f:
        existing = list(csv.DictReader(f))
    if not existing:
        return

    print(f"Re-validating {len(existing):,} instrument keys against the live Upstox master …")
    tracked_symbols   = [r["symbol"].strip().upper() for r in existing]
    fresh_instruments = download_nse_instruments()
    matched, _unmatched = match_instruments(tracked_symbols, fresh_instruments)
    fresh_by_symbol = {m["symbol"]: m for m in matched}

    changed, missing, updated_rows = 0, [], []
    for row in existing:
        sym   = row["symbol"].strip().upper()
        fresh = fresh_by_symbol.get(sym)
        if fresh is None:
            missing.append(sym)
            updated_rows.append(row)  # nothing valid to replace it with -- leave as-is
        elif fresh["instrument_key"] != row["instrument_key"]:
            print(f"  {sym}: instrument_key changed {row['instrument_key']} -> {fresh['instrument_key']}")
            changed += 1
            updated_rows.append(fresh)
        else:
            updated_rows.append(row)

    if changed:
        with open(inst_file, "w", newline="") as f:
            w = csv.DictWriter(
                f, fieldnames=["symbol", "instrument_key", "trading_symbol", "series", "exchange"]
            )
            w.writeheader()
            w.writerows(updated_rows)
        print(f"  Updated {changed} instrument key(s).")
    else:
        print("  No instrument key changes.")

    if missing:
        print(f"  WARNING: {len(missing)} symbol(s) not found in the current Upstox master "
              f"(possibly delisted/suspended) -- left unchanged, will keep failing candle "
              f"fetch until removed by hand: {', '.join(missing)}")


# ── Historical (multi-month) candle fetch ─────────────────────────────────────

def _fetch_chunk(session, instrument_key: str, from_d: date, to_d: date,
                 interval: str) -> list:
    """Fetch one 1-month chunk of candles. Returns list of candle arrays."""
    encoded = quote(instrument_key, safe="")
    seg     = _interval_minutes(interval)
    url     = f"{_BASE_V3}/historical-candle/{encoded}/{seg}/{to_d}/{from_d}"
    headers = _api_headers()

    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(CALL_DELAY)
        try:
            resp = session.get(url, headers=headers, timeout=30)
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
                raise RuntimeError(
                    f"chunk {from_d}–{to_d} failed after {MAX_RETRIES} attempts: {exc}"
                )
            time.sleep(5 * attempt)
    return []


def _fetch_historical_symbol(symbol, instrument_key, total, counter, lock,
                              from_date, to_date, interval):
    out_path = CANDLES_DIR / f"{symbol}.csv"
    if out_path.exists() and out_path.stat().st_size > 500:
        with lock:
            counter["done"]    += 1
            counter["skipped"] += 1
            print(f"  [{counter['done']}/{total}] {symbol} — cached, skipping.")
        return

    session       = requests.Session()
    chunks        = month_chunks(from_date, to_date)
    all_candles   = []
    failed_chunks = 0

    for from_d, to_d in chunks:
        try:
            rows = _fetch_chunk(session, instrument_key, from_d, to_d, interval)
            all_candles.extend(rows)
        except Exception as exc:
            print(f"    WARN [{symbol}] chunk {from_d}–{to_d}: {exc}")
            failed_chunks += 1

    if failed_chunks == len(chunks):
        with lock:
            counter["done"]   += 1
            counter["failed"] += 1
            print(f"  [{counter['done']}/{total}] {symbol} — FAILED (all {len(chunks)} chunks).")
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


def _run_historical(matched: list, from_date: date, to_date: date, interval: str):
    total   = len(matched)
    counter = {"done": 0, "ok": 0, "partial": 0, "failed": 0, "skipped": 0}
    lock    = threading.Lock()
    est_h   = (total * len(month_chunks(from_date, to_date)) * CALL_DELAY / WORKERS) / 3600

    print(f"Fetching {interval} candles for {total} symbols ({from_date} → {to_date}) …")
    print(f"  Workers: {WORKERS}  |  Delay: {CALL_DELAY}s/call/worker  |  Est: ~{est_h:.1f}h")

    from concurrent.futures import ThreadPoolExecutor, as_completed
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {
            ex.submit(
                _fetch_historical_symbol,
                r["symbol"], r["instrument_key"],
                total, counter, lock,
                from_date, to_date, interval
            ): r["symbol"]
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
    print(f"\n── Historical Fetch Summary ─────────────────────────────────")
    print(f"  Fully fetched  : {counter['ok']:,}")
    print(f"  Partial        : {counter['partial']:,}")
    print(f"  Failed entirely: {counter['failed']:,}")
    print(f"  Skipped/cached : {counter['skipped']:,}")
    print(f"  Time elapsed   : {elapsed / 3600:.2f} hours")


# ── Append-historical fetch (adds missing date ranges to existing files) ───────

def _fetch_append_symbol(symbol, instrument_key, from_date, to_date,
                          total, counter, lock, interval):
    out_path = CANDLES_DIR / f"{symbol}.csv"

    existing_ts: set = set()
    if out_path.exists():
        with open(out_path, newline="") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if row:
                    existing_ts.add(row[0])

    session     = requests.Session()
    all_candles = []
    for from_d, to_d in month_chunks(from_date, to_date):
        try:
            rows = _fetch_chunk(session, instrument_key, from_d, to_d, interval)
            all_candles.extend(rows)
        except Exception as exc:
            print(f"    WARN [{symbol}] chunk {from_d}–{to_d}: {exc}")

    new_candles = [c for c in all_candles if c[0] not in existing_ts]
    if not new_candles:
        with lock:
            counter["done"]    += 1
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
        counter["ok"]   += 1
        print(f"  [{counter['done']}/{total}] {symbol} — +{len(new_candles)} candles appended")


def _run_append_historical(matched: list, from_date: date, to_date: date, interval: str):
    total   = len(matched)
    counter = {"done": 0, "ok": 0, "skipped": 0, "failed": 0}
    lock    = threading.Lock()

    print(f"Appending {interval} candles for {total:,} symbols ({from_date} → {to_date}) …")
    print(f"  Workers: {WORKERS}  |  Delay: {CALL_DELAY}s/call/worker")

    from concurrent.futures import ThreadPoolExecutor, as_completed
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {
            ex.submit(
                _fetch_append_symbol,
                r["symbol"], r["instrument_key"],
                from_date, to_date, total, counter, lock, interval
            ): r["symbol"]
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
    print(f"\n── Append Summary ────────────────────────────────────────────")
    print(f"  Appended       : {counter['ok']:,}")
    print(f"  Already current: {counter['skipped']:,}")
    print(f"  Failed         : {counter['failed']:,}")
    print(f"  Time elapsed   : {elapsed:.1f}s")


# ── 15-min intraday fetch (persists to CSV, merges in-progress candles) ───────

def _fetch_intraday_symbol_15min(symbol, instrument_key, total, counter, lock):
    """Fetch today's 15-min intraday candles; fresh rows always overwrite stale ones."""
    out_path = CANDLES_DIR / f"{symbol}.csv"

    existing_by_ts: dict = {}
    if out_path.exists():
        with open(out_path, newline="") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if row:
                    existing_by_ts[row[0]] = row

    session = requests.Session()
    encoded = quote(instrument_key, safe="")
    url     = f"{_BASE_V3}/historical-candle/intraday/{encoded}/minutes/15"
    candles = []

    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(CALL_DELAY)
        try:
            resp = session.get(url, headers=_api_headers(), timeout=30)
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
                    counter["done"]   += 1
                    counter["failed"] += 1
                    print(f"  [intraday {counter['done']}/{total}] {symbol} — FAILED: {exc}")
                return
            time.sleep(5 * attempt)

    if not candles:
        with lock:
            counter["done"] += 1
            today_str = date.today().isoformat()
            has_today = any(ts.startswith(today_str) for ts in existing_by_ts)
            if not has_today:
                counter["no_data"] += 1
                print(f"  [intraday {counter['done']}/{total}] {symbol} — no data (holiday/illiquid?)")
            else:
                counter["skipped"] += 1
        return

    new_count = sum(1 for c in candles if c[0] not in existing_by_ts)
    for c in candles:
        existing_by_ts[c[0]] = c

    merged = sorted(existing_by_ts.values(), key=lambda r: r[0])
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "open", "high", "low", "close", "volume", "oi"])
        w.writerows(merged)

    with lock:
        counter["done"] += 1
        counter["ok"]   += 1
        refreshed = len(candles) - new_count
        note = f"+{new_count} new" + (f", refreshed {refreshed}" if refreshed else "")
        print(f"  [intraday {counter['done']}/{total}] {symbol} — {note}")


def _run_intraday_15min(matched: list):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    total   = len(matched)
    counter = {"done": 0, "ok": 0, "skipped": 0, "failed": 0, "no_data": 0}
    lock    = threading.Lock()

    print(f"Fetching 15min intraday for {total:,} symbols with {WORKERS} workers …")
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {
            ex.submit(
                _fetch_intraday_symbol_15min,
                r["symbol"], r["instrument_key"], total, counter, lock
            ): r["symbol"]
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

    print(f"\n── Intraday 15min Summary ──────────────────────────────────────")
    print(f"  Updated         : {counter['ok']:,}")
    print(f"  Already current : {counter['skipped']:,}")
    print(f"  No data from API: {counter['no_data']:,}  (outside market hours / holiday / illiquid)")
    print(f"  Failed          : {counter['failed']:,}")


# ── 1-min intraday fetch (returns raw candle lists, does NOT persist) ─────────

def _run_intraday_1min(matched: list) -> dict:
    """Fetch today's 1-min candles for each symbol. Returns {symbol: [candle_list]}."""
    result: dict = {}
    for r in matched:
        sym     = r["symbol"]
        encoded = quote(r["instrument_key"], safe="")
        url     = f"{_BASE_V3}/historical-candle/intraday/{encoded}/minutes/1"
        session = requests.Session()
        candles = []

        for attempt in range(1, MAX_RETRIES + 1):
            time.sleep(CALL_DELAY)
            try:
                resp = session.get(url, headers=_api_headers(), timeout=15)
                if resp.status_code == 429:
                    time.sleep(30 * attempt)
                    continue
                resp.raise_for_status()
                candles = resp.json().get("data", {}).get("candles", [])
                break
            except requests.RequestException as exc:
                if attempt == MAX_RETRIES:
                    raise RuntimeError(f"1-min candle fetch failed for {sym}: {exc}") from exc
                time.sleep(5 * attempt)

        result[sym] = candles
    return result


# ── Public candle API ──────────────────────────────────────────────────────────

def load_candles(matched: list, interval: str = "15minute", mode: str = "intraday",
                 from_date: date | None = None, to_date: date | None = None):
    """
    Fetch candles for a list of matched instrument dicts.

    matched  : list of {"symbol": str, "instrument_key": str, ...}
    interval : "15minute" (default) | "1minute"
    mode     : "intraday" | "eod-fill" | "historical" | "append"
    from_date: start of date range (historical/append modes; defaults to 1 year ago)
    to_date  : end of date range   (historical/append modes; defaults to today)

    15-min modes persist to data/candles/<symbol>.csv; returns None.
    1-min intraday returns {symbol: [raw_candle_list]}; does not persist.
    """
    if interval == "1minute":
        return _run_intraday_1min(matched)

    # 15-minute modes
    if mode in ("intraday", "eod-fill"):
        _run_intraday_15min(matched)
        return None

    today = date.today()
    fd    = from_date or (today - timedelta(days=365))
    td    = to_date   or today

    if mode == "historical":
        _run_historical(matched, fd, td, interval)
    elif mode == "append":
        _run_append_historical(matched, fd, td, interval)
    else:
        raise ValueError(
            f"Unknown mode '{mode}'. Use 'intraday', 'eod-fill', 'historical', or 'append'."
        )
    return None


# ── Market-cap (Screener.in) ───────────────────────────────────────────────────

def load_market_cap() -> tuple:
    """
    Fetch today's market cap via fetch_market_cap.main() (lazy import so
    module-level sys.exit in that file only fires if credentials are absent).
    Returns (symbols_dict, status) where status is 'fresh' | 'stale' | 'failed'.
    symbols_dict is {symbol: mcap_cr}.
    """
    _pipeline_dir = Path(__file__).resolve().parent
    if str(_pipeline_dir) not in sys.path:
        sys.path.insert(0, str(_pipeline_dir))
    import fetch_market_cap as fmc

    rc = fmc.main()
    status = {0: "fresh", 2: "stale"}.get(rc, "failed")

    today     = date.today()
    mcap_file = MCAP_DIR / f"market_cap_{today.isoformat()}.csv"
    if not mcap_file.exists():
        candidates = sorted(MCAP_DIR.glob("market_cap_*.csv"))
        if not candidates:
            return {}, status
        mcap_file = candidates[-1]

    universe: dict = {}
    with open(mcap_file, newline="") as f:
        for row in csv.DictReader(f):
            sym      = row["symbol"].strip().upper()
            mcap_str = (row.get("mcap_cr") or "").strip()
            universe[sym] = float(mcap_str) if mcap_str else 0.0

    return universe, status


# ── CLI ────────────────────────────────────────────────────────────────────────
# load_candles(mode="eod-fill") is a library function with no standalone entry
# point elsewhere in this codebase, but the 3:45 PM cron job needs one directly
# invokable command. This restores that: refetches today's 15-min intraday
# candles for the whole known universe via the intraday endpoint (the historical
# endpoint has no same-day data — confirmed empirically), correcting whatever
# candle was mid-formation at the 3:01 PM run and adding the final 15:15 candle.
# Safe to re-run — load_candles's own merge logic overwrites stale rows and
# leaves everything else untouched.

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Candle data fetch CLI")
    parser.add_argument("--eod-fill", action="store_true",
                        help="Refresh today's 15-min intraday candles for the full known "
                             "universe (data/instruments/upstox_instruments.csv).")
    args = parser.parse_args()

    if args.eod_fill:
        inst_file = INSTRUMENTS_DIR / "upstox_instruments.csv"
        if not inst_file.exists():
            sys.exit(f"ERROR: {inst_file} not found — run the full pipeline at least once first.")
        with open(inst_file, newline="") as f:
            matched = list(csv.DictReader(f))
        print("=" * 60)
        print(f"EOD Fill — {date.today().isoformat()}")
        print(f"  Instruments : {len(matched):,}  ({inst_file.name})")
        print(f"  Endpoint    : intraday (historical endpoint has no same-day data)")
        print("=" * 60)
        load_candles(matched, interval="15minute", mode="eod-fill")
        print("\nEOD Fill complete.")
    else:
        parser.print_help()
