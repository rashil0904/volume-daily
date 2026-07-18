#!/bin/bash
# Daily pipeline runner — called by cron at 3:01 PM IST Mon–Fri (VM timezone: Asia/Kolkata).
# Logs go to ~/pipeline.log

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="python3.11"
LOG_PREFIX="$(date '+%Y-%m-%d %H:%M:%S')"

echo ""
echo "=========================================="
echo "$LOG_PREFIX  Starting NSE Daily Pipeline"
echo "=========================================="

cd "$PROJECT_DIR"

# Run pipeline
echo "$LOG_PREFIX  Running pipeline..."
$PYTHON pipeline/run_daily.py

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "$LOG_PREFIX  Pipeline completed successfully."
else
    echo "$LOG_PREFIX  Pipeline FAILED (exit $EXIT_CODE)."
fi

echo "=========================================="
exit $EXIT_CODE
