#!/bin/bash
# Daily launcher for dhan/live_monitor.py — called by cron at 9:13 AM IST Mon-Fri.
# (Zerodha had an equivalent launcher/live_monitor.py; that side's code was
# removed 2026-09-10 pending a full rebuild -- see git history if reviving it.)
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

# UC-based staged entry -- LIVE, real orders (--dry-run removed 2026-09-09).
# Case A/B fires now place real MTF/CNC buys via _place_staged_buy, same
# margin-check/CNC-retry path as run_entry_321. Ran dry-run-only since
# 2026-08-27; went live before the day's first real fire could be verified
# against real price action (explicit call, not a default).
UC_FLAGS="--enable-uc-staged-entry"
echo "$LOG_PREFIX  UC-based staged entry: LIVE (real orders)"

# Order Update feed -- Phase 1 shadow-mode observation only (see
# dhan/order_update_feed.py). Opens a second, independent WebSocket
# connection to Dhan's Live Order Update feed and logs [WS_VALIDATION]
# comparisons against the existing polling path; places no orders and
# changes no decision. Safe to run alongside everything above.
ORDER_UPDATE_FLAGS="--enable-order-update-feed"
echo "$LOG_PREFIX  Order Update feed: shadow-mode ENABLED"

nohup python3.11 -u -m dhan.live_monitor $UC_FLAGS $ORDER_UPDATE_FLAGS >> /root/dhan_live_monitor.log 2>&1 &
echo "$LOG_PREFIX  Started dhan/live_monitor.py (PID $!)"
