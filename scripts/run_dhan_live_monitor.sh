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

# Order Update feed -- Phase 1 shadow-mode observation only (see
# dhan/order_update_feed.py). Opens a second, independent WebSocket
# connection to Dhan's Live Order Update feed and logs [WS_VALIDATION]
# comparisons against the existing polling path; places no orders and
# changes no decision. Safe to run alongside everything above.
ORDER_UPDATE_FLAGS="--enable-order-update-feed"
echo "$LOG_PREFIX  Order Update feed: shadow-mode ENABLED"

nohup python3.11 -u -m dhan.live_monitor $UC_FLAGS $ORDER_UPDATE_FLAGS >> /root/dhan_live_monitor.log 2>&1 &
echo "$LOG_PREFIX  Started dhan/live_monitor.py (PID $!)"
