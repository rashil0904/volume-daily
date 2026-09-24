#!/usr/bin/env python3
"""
test_data_loader.py -- standalone verifier for the pure/mockable pieces of
the 2026-09-24 Upstox->Dhan candle-fetch migration in pipeline/data_loader.py
(see /root/.claude/plans/wondrous-inventing-frost.md):

  1. date_chunks() -- boundary/clipping correctness (no gaps, no overlaps,
     each chunk <= the configured max).
  2. _transpose_response() -- epoch-seconds -> Asia/Kolkata ISO8601 timestamp
     conversion against independently-computed fixed epoch values (NOT
     derived from this module's own conversion code -- a pure round-trip
     through the same function would let a consistent bug cancel itself
     out), plus columnar-array -> row-tuple transpose correctness.
  3. _fetch_dhan_chunk() -- retries on HTTP 429 through the shared rate
     limiter, and raises (rather than silently returning no data) on a 200
     response carrying Dhan's own failure envelope, which must never be
     mistaken for the distinct, legitimate "no candles today" case.

No real network calls, no real orders -- session.post is mocked throughout.

Usage:
    python3.11 pipeline/test/test_data_loader.py

Exit 0 on all-pass, exit 1 on any failure.
"""

import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

import data_loader as dl  # noqa: E402

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
failures = 0


def check(label: str, condition: bool, detail: str = "") -> None:
    global failures
    if condition:
        print(f"  {PASS}  {label}")
    else:
        failures += 1
        print(f"  {FAIL}  {label}" + (f"  [{detail}]" if detail else ""))


class FakeResponse:
    def __init__(self, status_code: int, body):
        self.status_code = status_code
        self.ok          = 200 <= status_code < 300
        self._body        = body
        self.text          = str(body)

    def json(self):
        return self._body


class FakeSession:
    """Returns each response in `responses` in order, one per .post() call."""
    def __init__(self, responses: list):
        self.responses = list(responses)
        self.calls     = []

    def post(self, url, json=None, timeout=None):
        self.calls.append((url, json))
        return self.responses.pop(0)


# ═════════════════════════════════════════════════════════════════════════════
print("Scenario 1 -- date_chunks(): boundary/clipping correctness\n")
# ═════════════════════════════════════════════════════════════════════════════

# Single-day range -> exactly one chunk, unchanged.
chunks = dl.date_chunks(date(2026, 1, 1), date(2026, 1, 1))
check("single-day range -> 1 chunk", chunks == [(date(2026, 1, 1), date(2026, 1, 1))], str(chunks))

# Range exactly max_days long -> exactly one chunk.
start, end = date(2026, 1, 1), date(2026, 1, 1) + timedelta(days=dl._MAX_CHUNK_DAYS - 1)
chunks = dl.date_chunks(start, end)
check(f"exactly {dl._MAX_CHUNK_DAYS}-day range -> 1 chunk", len(chunks) == 1, str(chunks))
check("that chunk spans the full range", chunks[0] == (start, end), str(chunks))

# Range one day over max_days -> two chunks, contiguous (no gap/overlap).
end2 = end + timedelta(days=1)
chunks2 = dl.date_chunks(start, end2)
check(f"{dl._MAX_CHUNK_DAYS + 1}-day range -> 2 chunks", len(chunks2) == 2, str(chunks2))
check("second chunk starts the day after the first ends (no gap, no overlap)",
      chunks2[1][0] == chunks2[0][1] + timedelta(days=1), str(chunks2))
check("last chunk's end == overall end", chunks2[-1][1] == end2, str(chunks2))

# 6-month backfill range (main.py's BACKFILL_START shape) -> fully contiguous, no overlaps.
bf_start, bf_end = date(2026, 3, 1), date(2026, 9, 1)
bf_chunks = dl.date_chunks(bf_start, bf_end)
contiguous = all(
    bf_chunks[i + 1][0] == bf_chunks[i][1] + timedelta(days=1)
    for i in range(len(bf_chunks) - 1)
)
check("6-month range: chunks are contiguous with no gaps/overlaps", contiguous, str(bf_chunks))
check("6-month range: first chunk starts at bf_start", bf_chunks[0][0] == bf_start)
check("6-month range: last chunk ends at bf_end", bf_chunks[-1][1] == bf_end)
check("6-month range: every chunk <= _MAX_CHUNK_DAYS long",
      all((c[1] - c[0]).days < dl._MAX_CHUNK_DAYS for c in bf_chunks), str(bf_chunks))


# ═════════════════════════════════════════════════════════════════════════════
print("\nScenario 2 -- _transpose_response(): epoch->IST conversion + column order\n")
# ═════════════════════════════════════════════════════════════════════════════

# Independently computed (NOT via data_loader's own code) via:
#   datetime(2026, 1, 5, 9, 15, 0, tzinfo=ZoneInfo("Asia/Kolkata")).timestamp()
#   datetime(2026, 1, 5, 15, 15, 0, tzinfo=ZoneInfo("Asia/Kolkata")).timestamp()
EPOCH_0915_IST = 1767584700
EPOCH_1515_IST = 1767606300

body = {
    "timestamp": [EPOCH_0915_IST, EPOCH_1515_IST],
    "open":      [100.0, 101.5],
    "high":      [102.0, 103.0],
    "low":       [99.5, 100.5],
    "close":     [101.0, 102.5],
    "volume":    [1000, 2000],
}
rows = dl._transpose_response(body)

check("_transpose_response returns 2 rows for 2 timestamps", len(rows) == 2, str(rows))
check("row 0 timestamp decodes to 09:15 IST", rows[0][0][11:16] == "09:15", rows[0][0])
check("row 1 timestamp decodes to 15:15 IST", rows[1][0][11:16] == "15:15", rows[1][0])
check("row 0 date portion is 2026-01-05", rows[0][0][:10] == "2026-01-05", rows[0][0])
check("row 0 column order is [ts, open, high, low, close, volume, oi]",
      rows[0][1:6] == [100.0, 102.0, 99.5, 101.0, 1000], str(rows[0]))
check("row 0 oi placeholder is empty (never read downstream)", rows[0][6] == "", str(rows[0]))
check("row 1 column order is [ts, open, high, low, close, volume, oi]",
      rows[1][1:6] == [101.5, 103.0, 100.5, 102.5, 2000], str(rows[1]))

empty_rows = dl._transpose_response({})
check("_transpose_response handles a missing/empty body without crashing", empty_rows == [], str(empty_rows))


# ═════════════════════════════════════════════════════════════════════════════
print("\nScenario 3 -- _fetch_dhan_chunk(): retries on 429, raises on failure envelope\n")
# ═════════════════════════════════════════════════════════════════════════════

with patch.object(dl.time, "sleep", lambda s: None):  # skip real backoff waits

    # -- 429 then success: retried, not raised, result returned correctly.
    success_body = {"timestamp": [EPOCH_0915_IST], "open": [1], "high": [1], "low": [1],
                     "close": [1], "volume": [1]}
    session = FakeSession([FakeResponse(429, {}), FakeResponse(200, success_body)])
    rows = dl._fetch_dhan_chunk(session, "12345", date(2026, 1, 5), date(2026, 1, 5), 15)
    check("429 then 200: retried exactly once (2 total calls)", len(session.calls) == 2, str(session.calls))
    check("429 then 200: returns the successful chunk's data", len(rows) == 1, str(rows))

    # -- persistent 429 beyond retry budget: raises, doesn't return [].
    session_429 = FakeSession([FakeResponse(429, {})] * (dl._MAX_RETRIES + 1))
    try:
        dl._fetch_dhan_chunk(session_429, "12345", date(2026, 1, 5), date(2026, 1, 5), 15)
        check("persistent 429 raises after retry budget exhausted", False, "no exception raised")
    except RuntimeError:
        check("persistent 429 raises after retry budget exhausted", True)

    # -- 200 response carrying Dhan's own failure envelope: must raise, not
    #    be silently treated as "no candles today" (that's a distinct,
    #    legitimate case callers handle separately for a genuine empty body).
    session_fail = FakeSession([FakeResponse(200, {"status": "failure", "remarks": "bad securityId"})])
    try:
        dl._fetch_dhan_chunk(session_fail, "bad-sid", date(2026, 1, 5), date(2026, 1, 5), 15)
        check("200 + failure envelope raises rather than returning silently", False, "no exception raised")
    except RuntimeError as exc:
        check("200 + failure envelope raises rather than returning silently", "bad securityId" in str(exc), str(exc))

    # -- genuine empty-but-successful body (e.g. holiday) -> empty list, no exception.
    session_empty = FakeSession([FakeResponse(200, {"timestamp": [], "open": [], "high": [],
                                                     "low": [], "close": [], "volume": []})])
    rows_empty = dl._fetch_dhan_chunk(session_empty, "12345", date(2026, 1, 5), date(2026, 1, 5), 15)
    check("genuine empty body (no failure envelope) returns [] without raising", rows_empty == [], str(rows_empty))


# ═════════════════════════════════════════════════════════════════════════════
print("\nScenario 4 -- _INTERVAL_MAP: string->int mapping Dhan's payload expects\n")
# ═════════════════════════════════════════════════════════════════════════════

check("'15minute' -> 15 (int, not str)", dl._INTERVAL_MAP["15minute"] == 15)
check("'1minute' -> 1 (int, not str)", dl._INTERVAL_MAP["1minute"] == 1)


print()
if failures:
    print(f"{failures} check(s) FAILED.")
    sys.exit(1)
print("All checks PASSED.")
sys.exit(0)
