#!/bin/bash
# Daily auto-push of data/ and results/ changes — called by cron at 4:30 PM IST.
# Only stages data/ and results/ (candles, market cap, instruments, positions, trade
# book, trade lists) -- never touches code (zerodha/, pipeline/, scan/), so an
# in-progress code edit can never get swept into an unattended commit.
# Logs go to ~/push_data_updates.log

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_PREFIX="$(date '+%Y-%m-%d %H:%M:%S')"

cd "$PROJECT_DIR" || exit 1

echo ""
echo "=========================================="
echo "$LOG_PREFIX  Auto-push data + results"
echo "=========================================="

git add data/ results/

if git diff --cached --quiet; then
    echo "$LOG_PREFIX  No data/results changes — nothing to push."
    exit 0
fi

git commit -m "Data update — $(date '+%Y-%m-%d') candle + results refresh"

if git push; then
    echo "$LOG_PREFIX  Push succeeded."
else
    echo "$LOG_PREFIX  Push FAILED — changes committed locally, need manual push."
    exit 1
fi
