#!/usr/bin/env python3
"""
fetch_market_cap.py — Daily live market cap from Screener.in Premium
=====================================================================
Uses your Premium account's official CSV Export feature (legitimate login).

Writes:
  market_cap_daily/market_cap_today_YYYY-MM-DD.csv  — {symbol, mcap_cr, ...}
  market_cap_daily/mcap_status.json                  — consumed by prepare_data.py

Exit codes:
  0 — Fresh data saved
  2 — Stale fallback used (previous export)  — pipeline continues with warning
  1 — No data at all (no prior file)          — pipeline falls back to snapshots
"""

import csv
import io
import json
import os
import re
import sys
import time
from datetime import date
from pathlib import Path

import requests

# ── CONFIG ───────────────────────────────────────────────────────────────────
# Load .env file if present (local dev); env vars take precedence (GitHub Actions)
_env_file = Path(__file__).resolve().parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

SCREENER_EMAIL    = os.environ.get("SCREENER_EMAIL")
SCREENER_PASSWORD = os.environ.get("SCREENER_PASSWORD")
if not SCREENER_EMAIL or not SCREENER_PASSWORD:
    sys.exit("ERROR: Set SCREENER_EMAIL and SCREENER_PASSWORD in a .env file or as environment variables.")

# Optional: if you have a saved Screener screen with the columns you want
# (NSE Code, Market Capitalization, P/E …), paste its numeric ID here.
# Example: for https://www.screener.in/screens/123456/, set SCREEN_ID = 123456
# Leave as 0 to use the raw query approach (no saved screen needed).
SCREEN_ID = 0

BASE_URL = "https://www.screener.in"

# Screener query — matches the strategy's 1,500–5,000 Cr band exactly.
SCREENER_QUERY = "Market Capitalization > 1500 AND Market Capitalization < 5000"

# Strategy band — used to write filtered_symbols_{TODAY}.csv for data_loading.py.
MCAP_MIN_CR = 1_500
MCAP_MAX_CR = 5_000

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE        = Path(__file__).resolve().parent
MCAP_DIR    = BASE / "market_cap_daily"
MCAP_DIR.mkdir(exist_ok=True)

TODAY      = date.today().isoformat()           # "2026-07-15"
OUT_PATH   = MCAP_DIR / f"market_cap_{TODAY}.csv"
STATUS_PATH = MCAP_DIR / "mcap_status.json"


# ── Status file ───────────────────────────────────────────────────────────────

def _write_status(status: str, file, message: str,
                  rows: int = 0, fallback_date=None) -> None:
    payload = {
        "date":          TODAY,
        "status":        status,        # "fresh" | "stale" | "failed"
        "file":          str(file) if file else None,
        "rows":          rows,
        "fallback_date": fallback_date,
        "message":       message,
    }
    STATUS_PATH.write_text(json.dumps(payload, indent=2))


# ── HTML/CSV helpers ──────────────────────────────────────────────────────────

def _extract_csrf(html: str) -> str:
    for pat in [
        r'csrfmiddlewaretoken["\s]+value=["\']([\w]+)["\']',
        r'name=["\']csrfmiddlewaretoken["\'][^>]+value=["\']([\w]+)["\']',
        r'value=["\']([\w]{40,})["\'][^>]+name=["\']csrfmiddlewaretoken["\']',
    ]:
        m = re.search(pat, html)
        if m:
            return m.group(1)
    raise ValueError("CSRF token not found in page — Screener.in layout may have changed")


def _find_col(headers: list, candidates: list):
    """Return the first header that contains any candidate string (case-insensitive)."""
    lower = [h.lower().strip() for h in headers]
    for cand in candidates:
        for i, h in enumerate(lower):
            if cand in h:
                return headers[i]
    return None


def _parse_csv(raw_text: str) -> list:
    reader  = csv.DictReader(io.StringIO(raw_text))
    headers = list(reader.fieldnames or [])
    print(f"  Screener export columns: {headers}")

    sym_col   = _find_col(headers, ["nse code", "nse symbol", "nse_code", "ticker", "symbol"])
    cap_col   = _find_col(headers, ["mar cap", "market cap", "mktcap", "marketcap", "mcap"])
    price_col = _find_col(headers, ["cmp", "current price", "ltp"])
    pe_col    = _find_col(headers, ["p/e", "pe ratio"])

    if not sym_col:
        raise ValueError(
            f"Cannot detect NSE symbol column.\n"
            f"Got columns: {headers}\n"
            f"Expected one of: 'NSE Code', 'NSE Symbol', 'Ticker'\n"
            f"→ In your Screener screen, add 'NSE Code' as a column, save, and retry."
        )
    if not cap_col:
        raise ValueError(
            f"Cannot detect market cap column.\n"
            f"Got columns: {headers}\n"
            f"Expected one of: 'Mar Cap', 'Market Capitalization'"
        )

    print(f"  Detected: symbol='{sym_col}'  mcap='{cap_col}'"
          + (f"  price='{price_col}'" if price_col else "")
          + (f"  pe='{pe_col}'" if pe_col else ""))

    rows = []
    for row in reader:
        sym = row.get(sym_col, "").strip().upper()
        if not sym:
            continue
        try:
            mcap = float(row.get(cap_col, "").replace(",", "").strip())
        except (ValueError, TypeError):
            continue
        if mcap <= 0:
            continue
        entry = {"symbol": sym, "mcap_cr": round(mcap, 2)}
        if price_col:
            try:
                entry["current_price"] = float(
                    row.get(price_col, "").replace(",", "").strip()
                )
            except (ValueError, TypeError):
                pass
        if pe_col:
            try:
                entry["pe_ratio"] = float(
                    row.get(pe_col, "").replace(",", "").strip()
                )
            except (ValueError, TypeError):
                pass
        rows.append(entry)
    return rows


def _save_csv(rows: list, path: Path) -> None:
    fieldnames = ["symbol", "mcap_cr"]
    if any("current_price" in r for r in rows):
        fieldnames.append("current_price")
    if any("pe_ratio" in r for r in rows):
        fieldnames.append("pe_ratio")
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ── Screener.in fetch ─────────────────────────────────────────────────────────

def _login(session: requests.Session) -> None:
    """Login and return (nothing); raises on failure."""
    print("  Connecting to Screener.in …")
    resp = session.get(f"{BASE_URL}/login/", timeout=20)
    resp.raise_for_status()
    csrf = _extract_csrf(resp.text)

    print("  Logging in …")
    resp = session.post(
        f"{BASE_URL}/login/",
        data={
            "username":            SCREENER_EMAIL,
            "password":            SCREENER_PASSWORD,
            "csrfmiddlewaretoken": csrf,
            "next":                "/",
        },
        headers={"Referer": f"{BASE_URL}/login/"},
        timeout=20,
        allow_redirects=True,
    )
    resp.raise_for_status()

    if "/login/" in resp.url or "Please enter a correct" in resp.text:
        raise ValueError(
            "Login FAILED — check SCREENER_EMAIL and SCREENER_PASSWORD environment variables"
        )
    print("  Logged in successfully.")


def _export_saved_screen(session: requests.Session) -> str:
    """Export a saved Screener screen by SCREEN_ID. Returns raw CSV text."""
    url = f"{BASE_URL}/screens/{SCREEN_ID}/export/"
    print(f"  Exporting saved screen {SCREEN_ID}: GET {url}")
    resp = session.get(url, timeout=120)
    resp.raise_for_status()
    return resp.text


def _export_raw_query(session: requests.Session) -> str:
    """
    Export via Screener's /api/export/screen/ endpoint.

    Flow:
      1. GET /screen/raw/?query=...  — runs the screen, returns page with export form + CSRF
      2. POST /api/export/screen/?query=...&sort=...&order=...&url_name=raw_query
         Body: just csrfmiddlewaretoken (query lives in the URL, not the body)

    The query sent to Screener (pre-filter, not the strategy's 1,500–5,000 Cr band):
        Market Capitalization > 500

    Encoded URL that is POSTed to:
        https://www.screener.in/api/export/screen/
            ?query=Market+Capitalization+%3E+500
            &sort=Market+Capitalization
            &order=asc
            &url_name=raw_query
    """
    # Step 1 — run the screen to get the export form (and its CSRF token)
    screen_resp = session.get(
        f"{BASE_URL}/screen/raw/",
        params={"query": SCREENER_QUERY, "sort": "Market Capitalization", "order": "asc"},
        timeout=30,
    )
    screen_resp.raise_for_status()
    csrf = _extract_csrf(screen_resp.text)

    # Step 2 — POST to export endpoint; query goes in URL, CSRF goes in body
    export_url = (
        f"{BASE_URL}/api/export/screen/"
        f"?query={requests.utils.quote(SCREENER_QUERY, safe='')}"
        f"&sort=Market+Capitalization"
        f"&order=asc"
        f"&url_name=raw_query"
    )
    print(f"  Screener query: \"{SCREENER_QUERY}\"")
    print(f"  POST {export_url}")

    resp = session.post(
        export_url,
        data={"csrfmiddlewaretoken": csrf},
        headers={"Referer": f"{BASE_URL}/screen/raw/"},
        timeout=120,
    )
    resp.raise_for_status()
    return resp.text



def _fetch_fresh() -> list:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    })

    _login(session)

    raw = _export_saved_screen(session) if SCREEN_ID else _export_raw_query(session)

    # Detect HTML error page instead of CSV
    content_preview = raw[:300].strip()
    if content_preview.startswith("<!") or "<html" in content_preview.lower():
        raise ValueError(
            f"Screener returned an HTML page instead of CSV.\n"
            f"Likely cause: not logged in as Premium, or export URL changed.\n"
            f"Preview: {content_preview[:200]}"
        )
    if len(raw) < 200:
        raise ValueError(
            f"Screener export unexpectedly short ({len(raw)} bytes) — possible error."
        )

    rows = _parse_csv(raw)
    if len(rows) < 50:
        raise ValueError(
            f"Parsed only {len(rows)} rows — expected 500+.\n"
            f"Check column detection output above."
        )
    return rows


# ── Fallback logic ────────────────────────────────────────────────────────────

def _find_latest_fallback():
    """Return the most recent market_cap_today_*.csv that is NOT today's file."""
    files = sorted(MCAP_DIR.glob("market_cap_*.csv"))
    return next((f for f in reversed(files) if f != OUT_PATH), None)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    t0 = time.time()
    print("=" * 60)
    print("fetch_market_cap.py — Screener.in daily market cap")
    print("=" * 60)

    # Idempotent — skip if today's file already looks complete
    if OUT_PATH.exists() and OUT_PATH.stat().st_size > 1_000:
        row_count = sum(1 for _ in open(OUT_PATH)) - 1
        msg = f"Already fetched today ({row_count:,} rows) — skipping."
        print(f"  {msg}")
        _write_status("fresh", OUT_PATH, msg, row_count)
        print("=" * 60)
        return 0

    try:
        rows = _fetch_fresh()
        _save_csv(rows, OUT_PATH)
        elapsed = time.time() - t0
        msg = f"Fetched {len(rows):,} stocks from Screener.in in {elapsed:.1f}s"
        print(f"  {msg}")
        print(f"  Saved {len(rows):,} rows → {OUT_PATH.name}")
        _write_status("fresh", OUT_PATH, msg, len(rows))
        print("=" * 60)
        return 0

    except Exception as exc:
        print(f"  FETCH FAILED: {exc}", file=sys.stderr)

        fallback = _find_latest_fallback()
        if fallback:
            fallback_date = fallback.stem.replace("market_cap_", "")
            msg = (
                f"Screener.in fetch failed ({type(exc).__name__}: {exc}). "
                f"Using stale data from {fallback_date}."
            )
            print(f"  STALE FALLBACK: {fallback.name}  (data from {fallback_date})")
            print(f"  WARNING: market cap data is from {fallback_date}.")
            _write_status("stale", fallback, msg, fallback_date=fallback_date)
            print("=" * 60)
            return 2

        msg = (
            f"Screener.in fetch failed ({type(exc).__name__}: {exc}) "
            f"and no previous market_cap_*.csv found."
        )
        print(f"  NO FALLBACK AVAILABLE.")
        _write_status("failed", None, msg)
        print("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
