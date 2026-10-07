#!/bin/bash
# Daily wrapper for run_entry_limit() --symbols mode -- runs every weekday,
# not a one-time trial (see git history for the 2026-09-23/24 one-time
# trial scripts this superseded, and for scripts/run_entry_limit_half.sh,
# which this replaced 2026-10-08: that version routed only the least-liquid
# HALF of each day's trade_list through this tranched mechanism, leaving
# the most-liquid half to the instant --entry safety net below. Now routes
# EVERY symbol in the day's trade_list through the gradual tranched-limit
# mechanism instead -- see module-level comment in dhan/run_trades.py for
# why tranching (rather than one instant order) reduces market impact.
# Installed as a recurring Mon-Fri crontab line fired at 15:16:00 IST --
# cron has no sub-minute granularity, so this can't fire at an exact second
# directly. Instead it fires ~60s ahead of the intended first-order instant
# (15:17:00) and Python holds TWICE internally (--prep-at / --fire-at
# below, both via dhan/run_trades.py's _hold_until, which only actually
# holds when 0 < seconds-remaining <=60):
#   1. --prep-at 151650 -- held until 15:16:50, THEN the balance check +
#      ref-price fetch + per-symbol sizing runs. Resolving `ref` (which
#      decides SHARE COUNT -- a one-time decision, never recalculated later)
#      as LATE as safely possible keeps sizing closer to the live price than
#      resolving it right after cron fires at 15:16:00 would. The actual
#      FILL price is unaffected either way -- the coordinator loop's first
#      tick fetches its own fresh quote for bidding regardless of when
#      sizing ran -- so this only improves sizing accuracy, not execution
#      price.
#   2. --fire-at 151700 -- held until exactly 15:17:00, then the coordinator
#      loop's first tick fires. 10s between prep and fire: enough for the
#      parallelized ref-price fetch (see _prefetch_ref_prices) plus
#      sequential per-symbol margin-checks to comfortably finish (confirmed
#      live 2026-09-23 the OLD fully-sequential design took ~8s for just 4
#      symbols) without cutting it dangerously close to the fire instant.
# At 15:17:00.000, EVERY active symbol's first tranche fires together (one
# thread-pool batch), not staggered one at a time.
#
# NOTE: cutoff (15:20) lands on the SAME clock minute as the existing --entry
# safety-net cron below -- this run is expected to genuinely overlap
# run_entry_321's dedup read every day, not just on a slow/edge-case sweep.
# That overlap is exactly what entry_limit_started/done markers +
# _wait_for_entry_limit_marker (dhan/run_trades.py) exist to handle safely.
# Now that this script claims the FULL trade_list rather than half of it,
# the 15:20 --entry cron is purely a safety net for any symbol that got
# zero fills here (both the limit phase AND the market sweep at cutoff
# failed -- e.g. circuit-locked) -- it should have nothing left to do on a
# normal day.
#
# Per-symbol capital is NOT computed here -- run_entry_limit() derives it
# internally via _full_signal_count(trade_date), dividing by the FULL day's
# signal count regardless of how many symbols this invocation was actually
# handed. Omit --capital entirely so it defaults to TOTAL_CAPITAL
# (the value that governs real order sizing in dhan/run_trades.py).
#
# Logs go to ~/entry_limit_all.log

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TRADE_DATE="$(date +%Y-%m-%d)"
PREP_AT="151650"
FIRE_AT="151700"
TRADE_LIST="$PROJECT_DIR/results/trades/trade_list_${TRADE_DATE}.csv"
CUTOFF_EPOCH=$(date -d "${TRADE_DATE} 15:20:00" +%s)
LOG_PREFIX="$(date '+%Y-%m-%d %H:%M:%S')"

cd "$PROJECT_DIR" || exit 1

echo ""
echo "=========================================="
echo "$LOG_PREFIX  run_entry_limit --symbols ALL-LIST daily run ($TRADE_DATE)"
echo "=========================================="

# Wait for trade_list, bounded so we never eat into/past the 15:20 cutoff.
ATTEMPTS=0
while [ ! -f "$TRADE_LIST" ]; do
    NOW_EPOCH=$(date +%s)
    if [ "$NOW_EPOCH" -ge "$CUTOFF_EPOCH" ]; then
        echo "$LOG_PREFIX  ABORT: cutoff (15:20) reached before $TRADE_LIST appeared."
        exit 1
    fi
    ATTEMPTS=$((ATTEMPTS + 1))
    if [ "$ATTEMPTS" -gt 20 ]; then
        echo "$LOG_PREFIX  ABORT: trade_list still missing after $ATTEMPTS checks (5 min) -- giving up."
        exit 1
    fi
    echo "$LOG_PREFIX  $TRADE_LIST not found yet -- waiting 15s (attempt $ATTEMPTS)..."
    sleep 15
done

echo "$LOG_PREFIX  Found $TRADE_LIST."

# Every symbol in today's trade_list, in file order -- no liquidity ranking
# or split anymore (see header comment: this script used to route only the
# least-liquid half here, 2026-09-23 through 2026-10-07).
ALL_SYMBOLS="$(python3.11 - "$TRADE_LIST" <<'PYEOF'
import csv, sys

with open(sys.argv[1], newline="") as f:
    rows = list(csv.DictReader(f))

symbols = [r["symbol"].strip().upper() for r in rows]
print(",".join(symbols))
PYEOF
)"

if [ -z "$ALL_SYMBOLS" ]; then
    echo "$LOG_PREFIX  No signals today -- exiting cleanly."
    exit 0
fi

echo "$LOG_PREFIX  Chosen symbols (full trade_list): $ALL_SYMBOLS"

NOW_EPOCH=$(date +%s)
if [ "$NOW_EPOCH" -ge "$CUTOFF_EPOCH" ]; then
    echo "$LOG_PREFIX  ABORT: cutoff (15:20) already reached -- not placing real orders this late."
    exit 1
fi

echo "$LOG_PREFIX  Running: python3.11 dhan/run_trades.py --entry-limit --symbols $ALL_SYMBOLS --prep-at $PREP_AT --fire-at $FIRE_AT"
python3.11 dhan/run_trades.py --entry-limit --symbols "$ALL_SYMBOLS" --prep-at "$PREP_AT" --fire-at "$FIRE_AT"
EXIT_CODE=$?

echo "$LOG_PREFIX  run_entry_limit --symbols exited with code $EXIT_CODE."
echo "=========================================="
exit $EXIT_CODE
