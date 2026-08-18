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
_tick_cache: dict[str, float] = {}


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
    global _cache, _tick_cache
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
                tick_raw = (row.get("SEM_TICK_SIZE") or "").strip()
                if sym and tick_raw:
                    try:
                        # SEM_TICK_SIZE is in paise, not rupees -- confirmed via Dhan's
                        # own field description ("Minimum decimal point at which an
                        # instrument can be priced") plus the raw value distribution
                        # across the whole NSE-EQ master (1/5/10/50/100/500), which only
                        # makes sense as paise (0.01/0.05/0.10/0.50/1.00/5.00 rupees) --
                        # a literal "500 rupee tick" would be absurd for any equity.
                        # Confirmed live 2026-08-18: TVSSRICHAK's raw tick is 10.0000
                        # (-> ₹0.10), NOT the ₹0.05 every NSE equity was previously
                        # assumed to share -- that wrong assumption is what caused two
                        # real order rejections (EXCH:16283) earlier that day.
                        _tick_cache[sym] = float(tick_raw) / 100
                    except ValueError:
                        pass


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


def tick_size(symbol: str) -> float:
    """Returns the minimum valid LIMIT-price increment (in rupees) for a plain NSE
    equity symbol, per Dhan's own scrip master (SEM_TICK_SIZE, paise). Raises
    ValueError if the symbol/tick isn't found -- never guesses a tick for a real
    order, same philosophy as security_id() above."""
    _load_cache()
    tick = _tick_cache.get(symbol.strip().upper())
    if not tick:
        raise ValueError(
            f"[dhan] '{symbol}' has no tick size in the NSE equity instrument master "
            f"({_CACHE_FILE}). Symbol may be delisted/renamed, or the cache is stale "
            "-- delete the cache file to force a re-download."
        )
    return tick


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
