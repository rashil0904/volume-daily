#!/bin/bash
# Daily pipeline runner — called by cron at 3:06 PM IST Mon–Fri (VM timezone: Asia/Kolkata).
# Logs go to ~/pipeline.log
#
# Sequence:
#   1. pipeline/main.py   — market cap → universe → candles → signals → trade_list CSV
#   2. dhan/run_trades.py --entry-limit
#                         — semi-aggressive limit entry (3:06–3:19 PM), then MARKET sweep
#                           at cutoff; run_trades.py --entry (3:20 cron) acts as safety net

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="python3.11"
LOG_PREFIX="$(date '+%Y-%m-%d %H:%M:%S')"

echo ""
echo "=========================================="
echo "$LOG_PREFIX  Starting NSE Daily Pipeline"
echo "=========================================="

cd "$PROJECT_DIR"

# Step 1: Run signal pipeline
echo "$LOG_PREFIX  Running pipeline..."
$PYTHON pipeline/main.py
PIPELINE_EXIT=$?

if [ $PIPELINE_EXIT -ne 0 ]; then
    echo "$LOG_PREFIX  Pipeline FAILED (exit $PIPELINE_EXIT) — skipping entry."
    echo "=========================================="
    exit $PIPELINE_EXIT
fi

echo "$LOG_PREFIX  Pipeline completed successfully."

# Step 2: Limit-order entry (starts immediately, runs until 3:19 PM cutoff)
echo "$LOG_PREFIX  Starting limit entry..."
$PYTHON dhan/run_trades.py --entry-limit
ENTRY_EXIT=$?

if [ $ENTRY_EXIT -eq 0 ]; then
    echo "$LOG_PREFIX  Limit entry completed successfully."
else
    echo "$LOG_PREFIX  Limit entry exited with code $ENTRY_EXIT (check logs)."
fi

echo "=========================================="
# Exit 0 even if entry had issues — the 3:20 cron safety-net (--entry) handles
# any symbols with 0 fills, and a non-zero entry exit should not mark the
# pipeline cron itself as failed in the OS job scheduler.
exit 0
