#!/usr/bin/env python3
"""
notify.py — NSE pipeline Telegram notification
===============================================
Sends a success or failure message via Telegram Bot API after run_daily.py completes.

Importable API (used by run_daily.py):
  notify.send_success(date_str, start_ts, mcap_status="fresh")
  notify.send_failure(date_str, failed_step, error_msg, start_ts, mcap_status="fresh")

SETUP (one-time):
  1. Create a bot via Telegram @BotFather → copy the token.
  2. Start a chat with your bot (or add it to a group) → get the chat ID.
  3. Add TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to pipeline/.env
"""

import csv
import html as html_lib
import json
import os
import sys
import time
from pathlib import Path

import requests

# ── CONFIG ─────────────────────────────────────────────────────────────────────
_env_file = Path(__file__).resolve().parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise EnvironmentError(
        "Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in pipeline/.env before sending notifications."
    )

# Forum-topic routing -- message_thread_id per topic, read from pipeline/.env
# (same gitignored file as the token/chat_id above). A missing/unset topic
# falls back to no thread id (posts to General) rather than failing.
TOPIC_IDS = {
    "signals":       os.environ.get("TELEGRAM_TOPIC_SIGNALS"),
    "entries_exits": os.environ.get("TELEGRAM_TOPIC_ENTRIES_EXITS"),
    "live_tracking": os.environ.get("TELEGRAM_TOPIC_LIVE_TRACKING"),
    "errors":        os.environ.get("TELEGRAM_TOPIC_ERRORS"),
}
# ──────────────────────────────────────────────────────────────────────────────

PROJECT_DIR = Path(__file__).resolve().parent.parent


def _load_mcap_status() -> dict:
    path = PROJECT_DIR / "data" / "market_cap_daily" / "mcap_status.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _runtime_str(start_ts) -> str:
    if start_ts is None:
        return "n/a"
    total = int(time.time() - start_ts)
    m, s  = divmod(total, 60)
    return f"{m}m {s}s"


def _count_signals(trade_list_path: Path) -> int:
    if not trade_list_path.exists():
        return 0
    with open(trade_list_path, newline="") as f:
        return sum(1 for _ in csv.DictReader(f))


_TOTAL_CAPITAL = 500_000


def _text_trade_table(path: Path) -> str:
    if not path.exists():
        return "No trades triggered today."
    rows = list(csv.DictReader(open(path, newline="")))
    if not rows:
        return "No trades triggered today."

    has_shares = "shares" in rows[0]
    n = len(rows)

    header = f"{'Symbol':<12}  {'Shares':>6}  {'Entry Rs':>10}"
    sep    = "-" * len(header)
    lines  = [header, sep]
    for row in rows:
        sym = row.get("symbol", "")
        if has_shares:
            shares = int(row.get("shares") or 0)
            price  = float(row.get("ref_price") or 0)
        else:
            price      = float(row.get("entry_price_315pm") or row.get("ref_price") or 0)
            allocation = 125_000 if n <= 4 else _TOTAL_CAPITAL // n
            shares     = int(allocation / price) if price > 0 else 0
        lines.append(f"{sym:<12}  {shares:>6}  {price:>9,.2f}")
    return "\n".join(lines)


def _mcap_warning(mcap_status: str, mcap_st: dict) -> str:
    if mcap_status == "stale":
        d = mcap_st.get("fallback_date", "unknown date")
        return (
            f"WARNING: Today's market cap data could not be fetched live.\n"
            f"Using data from {d} instead.\n"
            f"Trade list below may be less accurate than usual."
        )
    if mcap_status == "failed":
        return "WARNING: Market cap fetch failed with no fallback. Treat signals with caution."
    return ""


# ── Telegram sender ───────────────────────────────────────────────────────────

def _send(text: str, topic: str | None = None) -> None:
    """topic: optional key into TOPIC_IDS. Omitted (or unset in .env) posts to
    General exactly as before -- adding topic routing never changes behavior
    for a caller that doesn't ask for it."""
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    if topic:
        thread_id = TOPIC_IDS.get(topic)
        if thread_id:
            payload["message_thread_id"] = int(thread_id)

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json=payload, timeout=12)
        resp.raise_for_status()
    except Exception as exc:
        print(f"  WARNING: Telegram notification failed: {exc}", file=sys.stderr)


# ── Public API (called by run_daily.py) ───────────────────────────────────────

def send_success(date_str: str, start_ts, mcap_status: str = "fresh") -> None:
    trade_list_path = PROJECT_DIR / "results" / "trades" / f"trade_list_{date_str}.csv"
    n_signals = _count_signals(trade_list_path)
    mcap_st   = _load_mcap_status()
    runtime   = _runtime_str(start_ts)
    warn      = _mcap_warning(mcap_status, mcap_st)
    table     = _text_trade_table(trade_list_path)

    parts = [
        "<b>NSE Pipeline — SUCCESS</b>",
        f"<b>Date:</b> {html_lib.escape(date_str)}",
        f"<b>Signals:</b> {n_signals}",
        f"<b>Runtime:</b> {runtime}",
    ]
    if warn:
        parts.append(f"\n<b>Warning:</b> {html_lib.escape(warn)}")
    parts.append("\n<b>Trade signals:</b>")
    parts.append(f"<pre>{html_lib.escape(table)}</pre>")

    _send("\n".join(parts), topic="signals")
    print(f"  Telegram sent ({n_signals} signal{'s' if n_signals != 1 else ''})")


def send_failure(date_str: str, failed_step: str, error_msg: str,
                 start_ts, mcap_status: str = "fresh") -> None:
    mcap_st = _load_mcap_status()
    runtime = _runtime_str(start_ts)
    warn    = _mcap_warning(mcap_status, mcap_st)

    parts = [
        "<b>NSE Pipeline — FAILED</b>",
        f"<b>Date:</b> {html_lib.escape(date_str)}",
        f"<b>Failed step:</b> {html_lib.escape(failed_step or 'unknown')}",
        f"<b>Runtime:</b> {runtime}",
    ]
    if warn:
        parts.append(f"\n<b>Warning:</b> {html_lib.escape(warn)}")
    parts.append("\n<b>Error:</b>")
    parts.append(f"<pre>{html_lib.escape(error_msg or '(no detail)')}</pre>")

    _send("\n".join(parts), topic="errors")
    print("  Telegram failure notification sent")


# ── Anytime scanner preview (scan_intraday.py) ────────────────────────────────

def send_scan_preview(date_str: str, as_of_hhmm: int, rows: list) -> None:
    """
    Sent by scan_intraday.py. Clearly labeled as a preview so it's never confused
    with the official 3:01 PM send_success() trade list.
    """
    parts = [
        "<b>NSE Intraday Scanner — PREVIEW</b>",
        f"<b>Date:</b> {html_lib.escape(date_str)}",
        f"<b>As of:</b> {as_of_hhmm:04d} IST",
        f"<b>Signals:</b> {len(rows)}",
    ]

    if rows:
        header = f"{'Symbol':<12}  {'Shares':>6}  {'Entry Rs':>9}  {'AsOf':>5}  {'VolR':>5}  {'Ret%':>6}"
        sep    = "-" * len(header)
        lines  = [header, sep]
        for r in rows:
            lines.append(
                f"{r['symbol']:<12}  {r['shares']:>6}  {r['ref_price']:>9,.2f}  "
                f"{r['as_of_hhmm']:>5}  {r['volume_ratio_vs_prorated']:>5.2f}  {r['return_pct']:>6.2f}"
            )
        table = "\n".join(lines)
        parts.append("\n<b>Preview signals (not the official trade list):</b>")
        parts.append(f"<pre>{html_lib.escape(table)}</pre>")
    else:
        parts.append("\nNo signals as of this check.")

    parts.append(
        "\n<i>Volume threshold is prorated to elapsed time in the 09:15-14:45 window — "
        "treat as a directional read, not a confirmed signal. Not read by any execution script.</i>"
    )

    _send("\n".join(parts), topic="live_tracking")
    print(f"  Telegram sent ({len(rows)} preview signal{'s' if len(rows) != 1 else ''})")


# ── Trade execution notifications (Stages 1 / 2 / 3) ─────────────────────────

def send_entry(broker: str, symbol: str, ref_price: float, shares: int,
               order_id: str, dry_run: bool = False) -> None:
    tag  = "  [DRY RUN]" if dry_run else ""
    text = "\n".join([
        f"<b>ENTRY — {html_lib.escape(symbol)}{tag}</b>",
        f"<b>Broker:</b> {html_lib.escape(broker)}",
        f"<b>Ref price:</b> &#8377;{ref_price:,.2f}",
        f"<b>Shares:</b> {shares}",
        f"<b>Order submitted:</b> {html_lib.escape(str(order_id))}",
    ])
    _send(text, topic="entries_exits")


def send_exit_925(broker: str, symbol: str, exit_price: float,
                  return_pct: float, pnl: float, dry_run: bool = False) -> None:
    tag   = "  [DRY RUN]" if dry_run else ""
    arrow = "▲" if return_pct >= 0 else "▼"
    text  = "\n".join([
        f"<b>{arrow} EXIT 9:25am — {html_lib.escape(symbol)}{tag}</b>",
        f"<b>Broker:</b> {html_lib.escape(broker)}",
        f"<b>Exit price:</b> &#8377;{exit_price:,.2f}",
        f"<b>Return:</b> {return_pct:+.2f}%",
        f"<b>Realised P&amp;L:</b> &#8377;{pnl:+,.2f}",
    ])
    _send(text, topic="entries_exits")


def send_exit_925_nodata(broker: str, symbol: str, shares_exited: int,
                         shares_remaining: int, exit_price: float,
                         dry_run: bool = False) -> None:
    tag  = "  [DRY RUN]" if dry_run else ""
    text = "\n".join([
        f"<b>&#9888; NO-DATA FALLBACK 9:25am — {html_lib.escape(symbol)}{tag}</b>",
        f"<b>Broker:</b> {html_lib.escape(broker)}",
        "Live price unavailable from broker — sold half position as precaution.",
        f"<b>Shares sold:</b> {shares_exited}  |  <b>Still open:</b> {shares_remaining}",
        f"<b>Partial fill price:</b> &#8377;{exit_price:,.2f}",
        "Remaining shares will be force-exited at 11:59am.",
    ])
    _send(text, topic="entries_exits")


def send_force_exit_1159(broker: str, symbol: str, exit_price: float,
                         return_pct: float, pnl: float,
                         dry_run: bool = False) -> None:
    tag   = "  [DRY RUN]" if dry_run else ""
    arrow = "▲" if return_pct >= 0 else "▼"
    text  = "\n".join([
        f"<b>{arrow} FORCE EXIT 11:59am — {html_lib.escape(symbol)}{tag}</b>",
        f"<b>Broker:</b> {html_lib.escape(broker)}",
        f"<b>Exit price:</b> &#8377;{exit_price:,.2f}",
        f"<b>Return:</b> {return_pct:+.2f}%",
        f"<b>Realised P&amp;L:</b> &#8377;{pnl:+,.2f}",
    ])
    _send(text, topic="entries_exits")


def send_short_open(broker: str, symbol: str, entry_price: float, shares: int,
                    source_exit_stage: str, order_id: str, dry_run: bool = False) -> None:
    tag  = "  [DRY RUN]" if dry_run else ""
    text = "\n".join([
        f"<b>&#128721; SHORT OPEN — {html_lib.escape(symbol)}{tag}</b>",
        f"<b>Broker:</b> {html_lib.escape(broker)}",
        f"<b>Triggered by:</b> {html_lib.escape(source_exit_stage)} long exit",
        f"<b>Short price:</b> &#8377;{entry_price:,.2f}",
        f"<b>Shares:</b> {shares}",
        "Squares off at 2:39pm.",
        f"<b>Order submitted:</b> {html_lib.escape(str(order_id))}",
    ])
    _send(text, topic="entries_exits")


def send_square_off_239(broker: str, symbol: str, entry_price: float, exit_price: float,
                        return_pct: float, pnl: float, dry_run: bool = False) -> None:
    tag   = "  [DRY RUN]" if dry_run else ""
    arrow = "▲" if return_pct >= 0 else "▼"
    text  = "\n".join([
        f"<b>{arrow} SHORT SQUARE-OFF 2:39pm — {html_lib.escape(symbol)}{tag}</b>",
        f"<b>Broker:</b> {html_lib.escape(broker)}",
        f"<b>Short price:</b> &#8377;{entry_price:,.2f}",
        f"<b>Cover price:</b> &#8377;{exit_price:,.2f}",
        f"<b>Return:</b> {return_pct:+.2f}%",
        f"<b>Realised P&amp;L:</b> &#8377;{pnl:+,.2f}",
    ])
    _send(text, topic="entries_exits")


def send_target_placed(broker: str, symbol: str, target_price: float,
                       order_id: str, dry_run: bool = False) -> None:
    tag  = "  [DRY RUN]" if dry_run else ""
    text = "\n".join([
        f"<b>&#127919; TARGET PLACED — {html_lib.escape(symbol)}{tag}</b>",
        f"<b>Broker:</b> {html_lib.escape(broker)}",
        f"<b>Target price:</b> &#8377;{target_price:,.2f}",
        f"<b>Order submitted:</b> {html_lib.escape(str(order_id))}",
    ])
    _send(text, topic="entries_exits")


def send_target_hit(broker: str, symbol: str, stage: str, exit_price: float,
                    return_pct: float, pnl: float, dry_run: bool = False) -> None:
    tag  = "  [DRY RUN]" if dry_run else ""
    text = "\n".join([
        f"<b>&#127919; TARGET HIT ({html_lib.escape(stage)}) — {html_lib.escape(symbol)}{tag}</b>",
        f"<b>Broker:</b> {html_lib.escape(broker)}",
        f"<b>Exit price:</b> &#8377;{exit_price:,.2f}",
        f"<b>Return:</b> {return_pct:+.2f}%",
        f"<b>Realised P&amp;L:</b> &#8377;{pnl:+,.2f}",
    ])
    _send(text, topic="entries_exits")


def send_cover_target_hit(broker: str, symbol: str, entry_price: float, exit_price: float,
                          return_pct: float, pnl: float, dry_run: bool = False) -> None:
    tag  = "  [DRY RUN]" if dry_run else ""
    text = "\n".join([
        f"<b>&#127919; COVER TARGET HIT — {html_lib.escape(symbol)}{tag}</b>",
        f"<b>Broker:</b> {html_lib.escape(broker)}",
        f"<b>Short price:</b> &#8377;{entry_price:,.2f}",
        f"<b>Cover price:</b> &#8377;{exit_price:,.2f}",
        f"<b>Return:</b> {return_pct:+.2f}%",
        f"<b>Realised P&amp;L:</b> &#8377;{pnl:+,.2f}",
    ])
    _send(text, topic="entries_exits")


def send_short_stoploss_hit(broker: str, symbol: str, entry_price: float, exit_price: float,
                            return_pct: float, pnl: float, dry_run: bool = False) -> None:
    tag  = "  [DRY RUN]" if dry_run else ""
    text = "\n".join([
        f"<b>&#128721; STOP-LOSS HIT — {html_lib.escape(symbol)}{tag}</b>",
        f"<b>Broker:</b> {html_lib.escape(broker)}",
        f"<b>Short price:</b> &#8377;{entry_price:,.2f}",
        f"<b>Cover price:</b> &#8377;{exit_price:,.2f}",
        f"<b>Return:</b> {return_pct:+.2f}%",
        f"<b>Realised P&amp;L:</b> &#8377;{pnl:+,.2f}",
    ])
    _send(text, topic="entries_exits")


def send_circuit_fetch_failed(broker: str, symbol: str, error_msg: str,
                              dry_run: bool = False) -> None:
    tag  = "  [DRY RUN]" if dry_run else ""
    text = "\n".join([
        f"<b>&#128308; CIRCUIT FETCH FAILED — {html_lib.escape(symbol)}{tag}</b>",
        f"<b>Broker:</b> {html_lib.escape(broker)}",
        "Could not fetch upper circuit for the stop-loss trigger — short and "
        "cover target remain live, unprotected by a stop-loss.",
        f"<b>Error:</b> {html_lib.escape(str(error_msg))}",
    ])
    _send(text, topic="errors")


def send_nothing_open_at_1159(broker: str) -> None:
    _send(
        f"<b>11:59am Exit — {html_lib.escape(broker)}</b>\n"
        "All positions already exited at 9:25am — nothing to force-close.",
        topic="entries_exits",
    )


def send_daily_summary(broker: str, n_opened: int, n_exited_925: int,
                       n_partial_nodata: int, n_force_1159: int,
                       total_pnl: float, dry_run: bool = False) -> None:
    pass  # PnL topic removed


# ── Live monitor notifications ────────────────────────────────────────────────

def send_monitor_qualified(symbol: str, ts_str: str, cum_vol: int, threshold: int,
                           vol_ratio: float, ltp: float, prev_vwap: float,
                           vwap_target: float) -> None:
    text = "\n".join([
        f"<b>&#9989; QUALIFIED — {html_lib.escape(symbol)}</b>",
        f"<b>Time:</b> {html_lib.escape(ts_str)} IST",
        f"<b>Volume:</b> {cum_vol:,} / {threshold:,} ({vol_ratio:.2f}&times; avg)",
        f"<b>LTP:</b> &#8377;{ltp:,.2f}",
        f"<b>VWAP target (prev+5%):</b> &#8377;{vwap_target:,.2f}",
        f"<b>Prev-day VWAP:</b> &#8377;{prev_vwap:,.2f}",
    ])
    _send(text, topic="live_tracking")


def send_monitor_near_circuit(symbol: str, ts_str: str, ltp: float,
                               pct_to_upper: float, upper_circuit: float) -> None:
    text = "\n".join([
        f"<b>&#9888; NEAR UPPER CIRCUIT — {html_lib.escape(symbol)}</b>",
        f"<b>Time:</b> {html_lib.escape(ts_str)} IST",
        f"<b>LTP:</b> &#8377;{ltp:,.2f}",
        f"<b>Upper circuit:</b> &#8377;{upper_circuit:,.2f}",
        f"<b>Gap to circuit:</b> {pct_to_upper:.2f}%",
    ])
    _send(text, topic="live_tracking")


def send_monitor_heartbeat(ts_str: str, tracking: int, qualified: int,
                           near_circuit: int) -> None:
    text = "\n".join([
        "<b>&#128994; Monitor Heartbeat</b>",
        f"<b>Time:</b> {html_lib.escape(ts_str)} IST",
        f"<b>Tracking:</b> {tracking} symbols",
        f"<b>Qualified today:</b> {qualified}",
        f"<b>Near circuit today:</b> {near_circuit}",
    ])
    _send(text, topic="errors")


def send_monitor_disconnect(code, reason: str) -> None:
    text = "\n".join([
        "<b>&#128308; Monitor WebSocket DISCONNECTED</b>",
        f"<b>Code:</b> {code}",
        f"<b>Reason:</b> {html_lib.escape(str(reason) or 'unknown')}",
        "Attempting to reconnect automatically…",
    ])
    _send(text, topic="errors")


def send_monitor_reconnect(attempt: int) -> None:
    _send(
        f"<b>&#128260; Monitor WebSocket RECONNECTING</b>\n"
        f"<b>Attempt:</b> {attempt}",
        topic="errors",
    )


def send_monitor_started(n_symbols: int) -> None:
    text = "\n".join([
        "<b>&#9989; Monitor Started</b>",
        f"<b>Tracking:</b> {n_symbols} symbols",
        "WebSocket connected — live monitoring is up.",
    ])
    _send(text, topic="errors")


def send_monitor_start_failure(error_msg: str) -> None:
    text = "\n".join([
        "<b>&#128308; Monitor FAILED TO START</b>",
        f"<b>Error:</b> {html_lib.escape(str(error_msg))}",
        "live_monitor.py did not reach the WebSocket connect stage — no monitoring is running.",
    ])
    _send(text, topic="errors")


def send_token_renewal_failed(error_msg: str) -> None:
    text = "\n".join([
        "<b>&#128308; Dhan Token Renewal FAILED (8:00am)</b>",
        f"<b>Error:</b> {html_lib.escape(str(error_msg))}",
        "The previously saved token was left untouched — trading will still work "
        "if it hasn't expired yet, but check it manually if this keeps failing.",
    ])
    _send(text, topic="errors")


# ── CLI (for manual testing) ──────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Send NSE pipeline result via Telegram")
    parser.add_argument("--date",        required=True)
    parser.add_argument("--status",      required=True, choices=["success", "failed"])
    parser.add_argument("--failed-step", default="")
    parser.add_argument("--error-msg",   default="")
    parser.add_argument("--mcap-status", default="fresh", choices=["fresh", "stale", "failed"])
    parser.add_argument("--start-ts",    type=float, default=None)
    args = parser.parse_args()

    try:
        if args.status == "success":
            send_success(args.date, args.start_ts, args.mcap_status)
        else:
            send_failure(args.date, args.failed_step, args.error_msg,
                         args.start_ts, args.mcap_status)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
