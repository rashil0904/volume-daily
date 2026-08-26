#!/bin/bash
# Daily launcher for dhan/live_monitor.py — called by cron at 9:13 AM IST Mon-Fri.
# Mirrors scripts/run_live_monitor.sh (which launches the Zerodha version) --
# live_monitor.py blocks forever, so this script kills any leftover instance
# from a prior day before starting a fresh one, to avoid stacking duplicate
# WebSocket connections/alerts over time.
# Logs go to ~/dhan_live_monitor.log

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_PREFIX="$(date '+%Y-%m-%d %H:%M:%S')"

cd "$PROJECT_DIR" || exit 1

echo ""
echo "=========================================="
echo "$LOG_PREFIX  Starting dhan/live_monitor.py"
echo "=========================================="

OLD_PID="$(pgrep -f 'dhan\.live_monitor')"
if [ -n "$OLD_PID" ]; then
    echo "$LOG_PREFIX  Killing leftover instance (PID $OLD_PID)"
    kill $OLD_PID
    sleep 2
fi

# One-day dry-run test of UC-based staged entry (2026-08-26 only) -- a cron
# job reverts this file back to its committed state that same afternoon, so
# this block should never persist past tomorrow. See git log if it's still
# here later than that.
UC_FLAGS=""
if [ "$(date '+%Y-%m-%d')" = "2026-08-26" ]; then
    UC_FLAGS="--enable-uc-staged-entry --dry-run"
    echo "$LOG_PREFIX  UC-based staged entry: DRY-RUN test enabled for today only (2026-08-26)"
fi

nohup python3.11 -u -m dhan.live_monitor $UC_FLAGS >> /root/dhan_live_monitor.log 2>&1 &
echo "$LOG_PREFIX  Started dhan/live_monitor.py (PID $!)"
