#!/bin/bash
# Daily wrapper for run_entry_limit() --symbols mode -- runs every weekday,
# not a one-time trial (see git history for the 2026-09-23/24 one-time
# trial scripts this superseded). Installed as a recurring Mon-Fri crontab
# line fired at 15:16:00 IST -- cron has no sub-minute granularity, so this
# can't fire at an exact second directly. Instead it fires ~60s ahead of
# the intended first-order instant (15:17:00) and Python holds TWICE
# internally (--prep-at / --fire-at below, both via dhan/run_trades.py's
# _hold_until, which only actually holds when 0 < seconds-remaining <=60):
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
#
# Splits each day's real trade_list in half by LIQUIDITY, not alphabet: the
# LEAST liquid half (floor(N/2) symbols, ranked by 36-day avg daily turnover
# -- see the symbol-selection block below) goes through this run_entry_limit
# --symbols call, since those are the symbols most likely to suffer real
# market impact from a single instant order. The remaining (most liquid)
# half is NOT touched by this script at all -- the existing recurring 15:20
# `--entry` cron already covers the full trade_list every weekday, and its
# dedup (positions_dhan_long.json,
# entry_date==today) plus the entry_limit_started/done marker wait will
# automatically skip whatever this script claims and only attempt the rest.
# No second cron entry needed for that half, and nothing here touches
# crontab, run_pipeline.sh, or the existing --entry cron.
#
# Per-symbol capital is NOT computed here -- run_entry_limit() derives it
# internally via _full_signal_count(trade_date), dividing by the FULL day's
# signal count regardless of how many symbols this invocation was actually
# handed. Omit --capital entirely so it defaults to TOTAL_CAPITAL
# (1,500,000, the value that governs real order sizing in dhan/run_trades.py).
#
# Logs go to ~/entry_limit_half.log

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
echo "$LOG_PREFIX  run_entry_limit --symbols HALF-LIST daily run ($TRADE_DATE)"
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

# LEAST-liquid half of the day's symbols (floor(N/2), ranked by 36-day avg
# daily TURNOVER = the same compute_36day_avg_volume() baseline
# signal_engine.py already used to qualify this symbol as a signal at all
# (so every trade_list symbol is guaranteed to have a valid value here --
# no separate "insufficient history" case to handle in practice), times
# trade_list's own ref_price -- turnover (rupees), not raw share volume,
# since capital allocation is rupee-based and a stock trading fewer, more
# expensive shares can move just as much real money per day as one trading
# many cheap shares. Routes the symbols MOST likely to suffer market impact
# from a single instant order through the gradual tranched-limit mechanism
# instead; the MOST liquid half (least impact-sensitive) goes to the
# existing instant --entry safety net, same as before.
HALF_SYMBOLS="$(python3.11 - "$TRADE_LIST" "$TRADE_DATE" <<'PYEOF'
import csv, sys
from datetime import date
from pathlib import Path

sys.path.insert(0, ".")
from common.calc_utils import load_clean_candles, compute_36day_avg_volume

today = date.fromisoformat(sys.argv[2])

with open(sys.argv[1], newline="") as f:
    rows = list(csv.DictReader(f))

ranked = []
for r in rows:
    sym       = r["symbol"].strip().upper()
    ref_price = float(r["ref_price"])
    candle_path = Path("data/candles") / f"{sym}.csv"
    avg_36 = None
    if candle_path.exists():
        df = load_clean_candles(candle_path)
        if not df.empty:
            avg_36 = compute_36day_avg_volume(df, today)
    # Missing/insufficient candle history -> treat as infinitely liquid
    # (never gets picked into the "least liquid" half) rather than crash or
    # guess -- defensive only, shouldn't happen for a real trade_list entry.
    turnover = (avg_36 * ref_price) if avg_36 else float("inf")
    ranked.append((turnover, sym))

ranked.sort()  # ascending turnover -- least liquid first
half = [sym for _, sym in ranked[: len(ranked) // 2]]
print(",".join(half))
PYEOF
)"

if [ -z "$HALF_SYMBOLS" ]; then
    echo "$LOG_PREFIX  No half to trade today (fewer than 2 signals) -- exiting cleanly."
    exit 0
fi

echo "$LOG_PREFIX  Chosen symbols (least liquid half by 36-day avg turnover): $HALF_SYMBOLS"

NOW_EPOCH=$(date +%s)
if [ "$NOW_EPOCH" -ge "$CUTOFF_EPOCH" ]; then
    echo "$LOG_PREFIX  ABORT: cutoff (15:20) already reached -- not placing real orders this late."
    exit 1
fi

echo "$LOG_PREFIX  Running: python3.11 dhan/run_trades.py --entry-limit --symbols $HALF_SYMBOLS --prep-at $PREP_AT --fire-at $FIRE_AT"
python3.11 dhan/run_trades.py --entry-limit --symbols "$HALF_SYMBOLS" --prep-at "$PREP_AT" --fire-at "$FIRE_AT"
EXIT_CODE=$?

echo "$LOG_PREFIX  run_entry_limit --symbols exited with code $EXIT_CODE."
echo "=========================================="
exit $EXIT_CODE
