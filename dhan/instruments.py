"""
dhan/instruments.py — symbol -> Dhan securityId resolution
============================================================
Kite accepts a plain tradingsymbol on every order; Dhan requires a numeric
securityId instead. There's no per-symbol lookup API for this -- Dhan
publishes a single instrument/scrip master CSV covering every exchange and
segment (~200k rows) and expects callers to filter it themselves.

Verified live (2026-08-14) against https://images.dhan.co/api-data/api-scrip-master.csv:
filtering SEM_EXM_EXCH_ID=='NSE', SEM_SEGMENT=='E', SEM_SERIES=='EQ' and
matching SEM_TRADING_SYMBOL gives exactly one row per plain NSE equity
symbol, with SEM_SMST_SECURITY_ID as the securityId Dhan's order/margin/quote
APIs expect. Spot-checked: RELIANCE->2885, CAPILLARY->759867, KKCL->13381,
EPACKPEB->759251, ARVSMART->10457.

Usage:
    from dhan.instruments import security_id
    sid = security_id("RELIANCE")   # "2885"
"""

import csv
from pathlib import Path
from datetime import datetime, timedelta

import requests

_ROOT       = Path(__file__).resolve().parent.parent
_CACHE_FILE = _ROOT / "data" / "instruments" / "dhan_scrip_master.csv"
_SOURCE_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
_MAX_AGE    = timedelta(days=7)

_cache: dict[str, str] = {}


def _refresh_if_stale() -> None:
    _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    if _CACHE_FILE.exists():
        age = datetime.now() - datetime.fromtimestamp(_CACHE_FILE.stat().st_mtime)
        if age < _MAX_AGE:
            return
    print(f"[dhan] Downloading instrument master → {_CACHE_FILE} ...")
    resp = requests.get(_SOURCE_URL, timeout=60)
    resp.raise_for_status()
    _CACHE_FILE.write_bytes(resp.content)
    print(f"[dhan] Instrument master saved ({len(resp.content):,} bytes).")


def _load_cache() -> None:
    global _cache
    if _cache:
        return
    _refresh_if_stale()
    with open(_CACHE_FILE, newline="") as f:
        for row in csv.DictReader(f):
            if (row.get("SEM_EXM_EXCH_ID") == "NSE"
                    and row.get("SEM_SEGMENT") == "E"
                    and row.get("SEM_SERIES") == "EQ"):
                sym = (row.get("SEM_TRADING_SYMBOL") or "").strip().upper()
                sid = (row.get("SEM_SMST_SECURITY_ID") or "").strip()
                if sym and sid:
                    _cache[sym] = sid


def security_id(symbol: str) -> str:
    """Returns the Dhan securityId for a plain NSE equity symbol.
    Raises ValueError if the symbol isn't found (never guesses for a real order)."""
    _load_cache()
    sid = _cache.get(symbol.strip().upper())
    if not sid:
        raise ValueError(
            f"[dhan] '{symbol}' not found in NSE equity instrument master "
            f"({_CACHE_FILE}). Symbol may be delisted/renamed, or the cache is stale "
            "-- delete the cache file to force a re-download."
        )
    return sid


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m dhan.instruments SYMBOL [SYMBOL ...]", file=sys.stderr)
        sys.exit(1)
    for s in sys.argv[1:]:
        try:
            print(f"{s.upper():15} -> {security_id(s)}")
        except ValueError as exc:
            print(exc, file=sys.stderr)
