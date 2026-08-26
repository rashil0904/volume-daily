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

# UC-based staged entry -- DRY-RUN only, every day. No orders are actually
# placed (--dry-run); this just keeps generating real signal/fill logs so we
# can evaluate the feature before ever turning it live.
UC_FLAGS="--enable-uc-staged-entry --dry-run"
echo "$LOG_PREFIX  UC-based staged entry: DRY-RUN enabled"

nohup python3.11 -u -m dhan.live_monitor $UC_FLAGS >> /root/dhan_live_monitor.log 2>&1 &
echo "$LOG_PREFIX  Started dhan/live_monitor.py (PID $!)"
