#!/usr/bin/env python3
"""
data_loader.py — single fetch module for the NSE pipeline
==========================================================
All candle and market-cap fetching lives here. No signal logic.

Migrated from Upstox to Dhan's own /charts/intraday Data API on 2026-09-24 --
see /root/.claude/plans/wondrous-inventing-frost.md for the full rationale.
Symbol -> Dhan securityId resolution is delegated to dhan.trade.security_id()
(the same cache already backing live order placement) rather than maintaining
a second, parallel instrument master the way the Upstox path required.

Public API
----------
  load_market_cap()                                     -> (symbols_dict, status_str)
  load_candles(matched, interval, mode, from_date, to_date)
      15-min modes persist to data/candles/<symbol>.csv (returns None).
      1-min intraday returns {symbol: [candle_list]} without persisting.

interval: "15minute" (default) | "1minute"
mode:     "intraday" | "eod-fill" | "historical" | "append"
matched:  list of {"symbol": str, ...} -- any extra legacy keys are ignored;
          securityId is resolved internally, on demand, per symbol.
"""

import csv
import os
import sys
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from dhan.auth import BASE_URL, get_session
from dhan.trade import security_id, RateLimiter

# ── Load .env ─────────────────────────────────────────────────────────────────
_env_file = Path(__file__).resolve().parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

# ── Paths ──────────────────────────────────────────────────────────────────────
UNIVERSE_FILE   = _ROOT / "data" / "universe_combined.csv"
INSTRUMENTS_DIR = _ROOT / "data" / "instruments"
CANDLES_DIR     = _ROOT / "data" / "candles"
MCAP_DIR        = _ROOT / "data" / "market_cap_daily"

INSTRUMENTS_DIR.mkdir(parents=True, exist_ok=True)
CANDLES_DIR.mkdir(parents=True, exist_ok=True)
MCAP_DIR.mkdir(parents=True, exist_ok=True)

_IST = ZoneInfo("Asia/Kolkata")

# ── Dhan Data API constants ─────────────────────────────────────────────────────
# Dhan's documented Data-API rate limit is 5 req/sec, 100,000/day (confirmed
# 2026-09-24 against https://docs.dhanhq.co -- see the plan doc above). Run at
# 4/sec (80% headroom) rather than the bare limit, matching this codebase's
# existing margin philosophy elsewhere (e.g. the order-API throttle at 7/10).
# A single shared RateLimiter blocks correctly across every ThreadPoolExecutor
# worker -- unlike the old Upstox-era per-worker CALL_DELAY sleep (which just
# assumed even spacing would average out under the limit), this actually
# enforces the ceiling, so there's no need to also keep a fixed per-call delay.
_candle_rate_limiter = RateLimiter(max_per_sec=4, window_seconds=1.2)
_MAX_RETRIES         = 3     # extra attempts after the first, only on a 429
_RETRY_BACKOFF       = 5.0   # seconds, doubles each retry
WORKERS              = 5

_INTERVAL_MAP = {"15minute": 15, "1minute": 1}

# /charts/intraday allows at most 90 days of range per request (confirmed via
# Dhan's own docs) -- chunk any wider backfill into windows just under that.
_MAX_CHUNK_DAYS = 85


# ── Helpers ────────────────────────────────────────────────────────────────────

def date_chunks(start: date, end: date, max_days: int = _MAX_CHUNK_DAYS) -> list:
    """[start, end] sliced into <=max_days windows -- Dhan's /charts/intraday
    hard cap is 90 days/request; max_days stays a little under that for
    safety margin against any off-by-one in Dhan's own boundary handling."""
    chunks, cur = [], start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=max_days - 1), end)
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return chunks


# ── Dhan candle fetch (all modes funnel through /charts/intraday) ─────────────

def _transpose_response(body: dict) -> list:
    """Dhan's /charts/intraday returns columnar arrays -- transpose into row
    tuples matching this module's [timestamp, open, high, low, close, volume,
    oi] CSV schema. Timestamps arrive as unix-epoch seconds; converted to
    Asia/Kolkata ISO8601 strings so common/calc_utils.py::load_clean_candles()
    (which already tz-converts to Asia/Kolkata whenever a tz is present) needs
    no changes downstream. oi is always written empty -- signal_engine.py
    never reads it, and this module always requests oi=False."""
    ts_list = body.get("timestamp") or []
    opens   = body.get("open") or []
    highs   = body.get("high") or []
    lows    = body.get("low") or []
    closes  = body.get("close") or []
    vols    = body.get("volume") or []
    return [
        [datetime.fromtimestamp(ts, tz=_IST).isoformat(),
         opens[i], highs[i], lows[i], closes[i], vols[i], ""]
        for i, ts in enumerate(ts_list)
    ]


def _fetch_dhan_chunk(session, sid: str, from_d: date, to_d: date,
                      interval_min: int) -> list:
    """Fetch one <=_MAX_CHUNK_DAYS-day chunk of candles for a Dhan securityId
    via POST /charts/intraday. Retried on 429 through the shared rate
    limiter, same backoff shape as dhan/charges.py::get_trades()'s already-
    proven fix. A 200 response can still carry a failure envelope (bad
    securityId/date-range) -- checked explicitly so that case is never
    mistaken for the distinct, legitimate "no candles today" case (holiday/
    illiquid symbol) that callers already handle separately."""
    # Full calendar-day bounds, not NSE-hours-only ("09:15:00"/"15:30:00") --
    # confirmed live 2026-09-24 against the comparison script that tighter
    # bounds silently clip the day's FIRST candle (Dhan's boundary handling
    # isn't simply inclusive at the exact requested instant). Verified with
    # full-day bounds every real trading-universe symbol (KSCL, FRONTSP,
    # INDIAGLYCO) returns the complete, correctly-timed 09:15-15:15 candle
    # set, exactly matching Upstox's own candle count for the same day.
    payload = {
        "securityId":      sid,
        "exchangeSegment": "NSE_EQ",
        "instrument":      "EQUITY",
        "interval":        interval_min,
        "oi":              False,
        "fromDate":        f"{from_d} 00:00:00",
        "toDate":          f"{to_d} 23:59:59",
    }
    backoff = _RETRY_BACKOFF
    for attempt in range(_MAX_RETRIES + 1):
        _candle_rate_limiter.acquire()
        resp = session.post(f"{BASE_URL}/charts/intraday", json=payload, timeout=30)
        if resp.status_code == 429 and attempt < _MAX_RETRIES:
            print(f"    429 rate-limited (attempt {attempt + 1}/{_MAX_RETRIES + 1}) "
                  f"-- backing off {backoff:.0f}s …")
            time.sleep(backoff)
            backoff *= 2
            continue
        break
    if not resp.ok:
        raise RuntimeError(f"chunk {from_d}–{to_d} failed: HTTP {resp.status_code} {resp.text[:300]}")
    body = resp.json()
    if isinstance(body, dict) and body.get("status") == "failure":
        raise RuntimeError(f"chunk {from_d}–{to_d} failed: {body.get('remarks')}")
    return _transpose_response(body)


# ── Historical (multi-chunk) candle fetch ──────────────────────────────────────

def _fetch_historical_symbol(symbol, total, counter, lock, from_date, to_date, interval):
    out_path = CANDLES_DIR / f"{symbol}.csv"
    if out_path.exists() and out_path.stat().st_size > 500:
        with lock:
            counter["done"]    += 1
            counter["skipped"] += 1
            print(f"  [{counter['done']}/{total}] {symbol} — cached, skipping.")
        return

    try:
        sid = security_id(symbol)
    except ValueError as exc:
        with lock:
            counter["done"]           += 1
            counter["no_security_id"] += 1
            print(f"  [{counter['done']}/{total}] {symbol} — SKIP: {exc}")
        return

    session, _    = get_session()
    interval_min  = _INTERVAL_MAP[interval]
    chunks        = date_chunks(from_date, to_date)
    all_candles   = []
    failed_chunks = 0

    for from_d, to_d in chunks:
        try:
            rows = _fetch_dhan_chunk(session, sid, from_d, to_d, interval_min)
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
    total      = len(matched)
    counter    = {"done": 0, "ok": 0, "partial": 0, "failed": 0, "skipped": 0, "no_security_id": 0}
    lock       = threading.Lock()
    n_chunks   = len(date_chunks(from_date, to_date))
    # A single shared rate limiter caps total throughput regardless of worker
    # count -- unlike the old Upstox-era per-worker CALL_DELAY estimate, this
    # divides by the limiter's own rate, not by WORKERS.
    est_h      = (total * n_chunks) / _candle_rate_limiter._max_per_sec / 3600

    print(f"Fetching {interval} candles for {total} symbols ({from_date} → {to_date}) …")
    print(f"  Workers: {WORKERS}  |  Rate limit: {_candle_rate_limiter._max_per_sec} req/sec (shared)  |  Est: ~{est_h:.1f}h")

    from concurrent.futures import ThreadPoolExecutor, as_completed
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {
            ex.submit(
                _fetch_historical_symbol,
                r["symbol"], total, counter, lock, from_date, to_date, interval
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
    print(f"  No securityId  : {counter['no_security_id']:,}")
    print(f"  Skipped/cached : {counter['skipped']:,}")
    print(f"  Time elapsed   : {elapsed / 3600:.2f} hours")


# ── Append-historical fetch (adds missing date ranges to existing files) ───────
# No current caller anywhere in this codebase (confirmed via grep) -- kept
# migrated for completeness/future one-off backfill-gap-fill use, per explicit
# user decision during the Upstox->Dhan migration rather than dropped as dead
# code.

def _fetch_append_symbol(symbol, from_date, to_date, total, counter, lock, interval):
    out_path = CANDLES_DIR / f"{symbol}.csv"

    existing_ts: set = set()
    if out_path.exists():
        with open(out_path, newline="") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if row:
                    existing_ts.add(row[0])

    try:
        sid = security_id(symbol)
    except ValueError as exc:
        with lock:
            counter["done"]           += 1
            counter["no_security_id"] += 1
            print(f"  [{counter['done']}/{total}] {symbol} — SKIP: {exc}")
        return

    session, _   = get_session()
    interval_min = _INTERVAL_MAP[interval]
    all_candles  = []
    for from_d, to_d in date_chunks(from_date, to_date):
        try:
            rows = _fetch_dhan_chunk(session, sid, from_d, to_d, interval_min)
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
    counter = {"done": 0, "ok": 0, "skipped": 0, "failed": 0, "no_security_id": 0}
    lock    = threading.Lock()

    print(f"Appending {interval} candles for {total:,} symbols ({from_date} → {to_date}) …")
    print(f"  Workers: {WORKERS}  |  Rate limit: {_candle_rate_limiter._max_per_sec} req/sec (shared)")

    from concurrent.futures import ThreadPoolExecutor, as_completed
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {
            ex.submit(
                _fetch_append_symbol,
                r["symbol"], from_date, to_date, total, counter, lock, interval
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
    print(f"  No securityId  : {counter['no_security_id']:,}")
    print(f"  Failed         : {counter['failed']:,}")
    print(f"  Time elapsed   : {elapsed:.1f}s")


# ── 15-min intraday fetch (persists to CSV, merges in-progress candles) ───────

def _fetch_intraday_symbol_15min(symbol, total, counter, lock):
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

    try:
        sid = security_id(symbol)
    except ValueError as exc:
        with lock:
            counter["done"]           += 1
            counter["no_security_id"] += 1
            print(f"  [intraday {counter['done']}/{total}] {symbol} — SKIP: {exc}")
        return

    session, _ = get_session()
    today      = date.today()
    try:
        candles = _fetch_dhan_chunk(session, sid, today, today, 15)
    except Exception as exc:
        with lock:
            counter["done"]   += 1
            counter["failed"] += 1
            print(f"  [intraday {counter['done']}/{total}] {symbol} — FAILED: {exc}")
        return

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
    counter = {"done": 0, "ok": 0, "skipped": 0, "failed": 0, "no_data": 0, "no_security_id": 0}
    lock    = threading.Lock()

    print(f"Fetching 15min intraday for {total:,} symbols with {WORKERS} workers …")
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {
            ex.submit(
                _fetch_intraday_symbol_15min,
                r["symbol"], total, counter, lock
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
    print(f"  No securityId   : {counter['no_security_id']:,}")
    print(f"  Failed          : {counter['failed']:,}")


# ── 1-min intraday fetch (returns raw candle lists, does NOT persist) ─────────

def _run_intraday_1min(matched: list) -> dict:
    """Fetch today's 1-min candles for each symbol. Returns {symbol: [candle_list]}.
    Sequential by design -- callers needing concurrency across symbols (e.g.
    dhan/run_trades.py::_prefetch_ref_prices()) call this once per single-symbol
    `matched` list from their own ThreadPoolExecutor; the shared
    _candle_rate_limiter is what keeps aggregate throughput correct either way."""
    result: dict = {}
    session, _ = get_session()
    today      = date.today()
    for r in matched:
        sym = r["symbol"]
        try:
            sid = security_id(sym)
        except ValueError as exc:
            print(f"  SKIP {sym}: {exc}")
            result[sym] = []
            continue
        try:
            result[sym] = _fetch_dhan_chunk(session, sid, today, today, 1)
        except Exception as exc:
            raise RuntimeError(f"1-min candle fetch failed for {sym}: {exc}") from exc
    return result


# ── Public candle API ──────────────────────────────────────────────────────────

def load_candles(matched: list, interval: str = "15minute", mode: str = "intraday",
                 from_date: date | None = None, to_date: date | None = None):
    """
    Fetch candles for a list of matched instrument dicts.

    matched  : list of {"symbol": str, ...} -- securityId is resolved
               internally via dhan.trade.security_id(); any extra legacy
               keys are ignored.
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
                        help="Refresh today's 15-min intraday candles for every symbol "
                             "with an existing candle file (data/candles/*.csv).")
    args = parser.parse_args()

    if args.eod_fill:
        matched = [{"symbol": p.stem} for p in sorted(CANDLES_DIR.glob("*.csv"))]
        if not matched:
            sys.exit(f"ERROR: no candle files under {CANDLES_DIR} — run the full pipeline at least once first.")
        print("=" * 60)
        print(f"EOD Fill — {date.today().isoformat()}")
        print(f"  Symbols  : {len(matched):,}  ({CANDLES_DIR})")
        print(f"  Endpoint : Dhan /charts/intraday (historical endpoint has no same-day data)")
        print("=" * 60)
        load_candles(matched, interval="15minute", mode="eod-fill")
        print("\nEOD Fill complete.")
    else:
        parser.print_help()
