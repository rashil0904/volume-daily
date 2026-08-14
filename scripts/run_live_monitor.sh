#!/bin/bash
# Daily launcher for live_monitor.py — called by cron at 9:13 AM IST Mon-Fri.
# live_monitor.py blocks forever (ticker.connect(threaded=False), no self-exit),
# so this script kills any leftover instance from a prior day before starting a
# fresh one, to avoid stacking duplicate WebSocket connections/alerts over time.
# Logs go to ~/live_monitor.log

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_PREFIX="$(date '+%Y-%m-%d %H:%M:%S')"

cd "$PROJECT_DIR" || exit 1

echo ""
echo "=========================================="
echo "$LOG_PREFIX  Starting live_monitor.py"
echo "=========================================="

OLD_PID="$(pgrep -f 'zerodha/live_monitor.py')"
if [ -n "$OLD_PID" ]; then
    echo "$LOG_PREFIX  Killing leftover instance (PID $OLD_PID)"
    kill $OLD_PID
    sleep 2
fi

nohup python3.11 zerodha/live_monitor.py >> /root/live_monitor.log 2>&1 &
echo "$LOG_PREFIX  Started live_monitor.py (PID $!)"
