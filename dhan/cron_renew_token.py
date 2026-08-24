#!/usr/bin/env python3
"""
dhan/cron_renew_token.py — daily automated Dhan access-token renewal
======================================================================
Thin cron wrapper around dhan.auth.renew_access_token(). Scheduled at
8:00am AND 8:00pm IST, every day (not just weekdays -- see crontab comment:
tokens are valid 24h, so a weekday-only schedule would leave Monday's token
expired since Friday's renewal, and RenewToken can't renew an
already-expired token).

Twice a day, not once: a fixed once-daily cron fires at the same wall-clock
time every day, so if renewal always happened almost exactly 24h apart, the
safety margin before the PREVIOUS token's real expiry would shrink to just
that one run's network/processing delay -- a second or two. Any single slow
cron tick, Dhan API hiccup, or slow script startup could then miss the
window entirely (RenewToken can't renew an already-expired token, same
failure mode as a fully manual token). Renewing every 12h instead keeps a
comfortable multi-hour buffer at all times, and a failed attempt is caught
by the next one hours later rather than a full day later.

Timing is deliberate, not incidental: both 8:00am and 8:00pm sit inside the
dead zone -- well after the previous live_monitor.py was killed (3:40pm
pkill) and well before the next one starts (9:10am launcher) or the first
trading call of the day (9:15am place-targets) -- see dhan/auth.py's module
docstring for why that matters (renewal invalidates the previous token
immediately, no overlap window).

On success: logs the new expiry time.
On failure: sends a Telegram alert via pipeline/notify.py's existing
"errors" topic (same channel already used for live_monitor start
failures/disconnects) and exits 1. renew_access_token() itself never
touches the saved token file on failure, so trading can still proceed on
the old token today if it hasn't actually expired yet.
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

from dhan.auth import renew_access_token   # noqa: E402
import notify                              # noqa: E402


def main() -> int:
    ok, message = renew_access_token()

    if ok:
        print(f"[dhan] {message}")
        return 0

    print(f"[dhan] ERROR: Token renewal failed -- {message}", file=sys.stderr)
    try:
        notify.send_token_renewal_failed(message)
    except Exception as exc:
        print(f"[dhan]   (also failed to send Telegram alert: {exc})", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
