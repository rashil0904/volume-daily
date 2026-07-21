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
    if not trade_list_path.exists() or trade_list_path.stat().st_size < 50:
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

def _send(text: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=12,
        )
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

    _send("\n".join(parts))
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

    _send("\n".join(parts))
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

    _send("\n".join(parts))
    print(f"  Telegram sent ({len(rows)} preview signal{'s' if len(rows) != 1 else ''})")


# ── Trade execution notifications (Stages 1 / 2 / 3) ─────────────────────────

def send_entry(broker: str, symbol: str, ref_price: float, shares: int,
               order_id: str, dry_run: bool = False) -> None:
    tag  = "  [DRY RUN]" if dry_run else ""
    text = "\n".join([
        f"<b>ENTRY — {html_lib.escape(symbol)}{tag}</b>",
        f"<b>Broker:</b> {html_lib.escape(broker)}",
        f"<b>Ref price (15:15 close):</b> &#8377;{ref_price:,.2f}",
        f"<b>Shares:</b> {shares}",
        f"<b>Order submitted:</b> {html_lib.escape(str(order_id))}",
    ])
    _send(text)


def send_exit_945(broker: str, symbol: str, exit_price: float,
                  return_pct: float, pnl: float, dry_run: bool = False) -> None:
    tag   = "  [DRY RUN]" if dry_run else ""
    arrow = "▲" if return_pct >= 0 else "▼"
    text  = "\n".join([
        f"<b>{arrow} EXIT 9:45am — {html_lib.escape(symbol)}{tag}</b>",
        f"<b>Broker:</b> {html_lib.escape(broker)}",
        f"<b>Exit price:</b> &#8377;{exit_price:,.2f}",
        f"<b>Return:</b> {return_pct:+.2f}%",
        f"<b>Realised P&amp;L:</b> &#8377;{pnl:+,.2f}",
    ])
    _send(text)


def send_exit_945_nodata(broker: str, symbol: str, shares_exited: int,
                         shares_remaining: int, exit_price: float,
                         dry_run: bool = False) -> None:
    tag  = "  [DRY RUN]" if dry_run else ""
    text = "\n".join([
        f"<b>&#9888; NO-DATA FALLBACK 9:45am — {html_lib.escape(symbol)}{tag}</b>",
        f"<b>Broker:</b> {html_lib.escape(broker)}",
        "Live price unavailable from broker — sold half position as precaution.",
        f"<b>Shares sold:</b> {shares_exited}  |  <b>Still open:</b> {shares_remaining}",
        f"<b>Partial fill price:</b> &#8377;{exit_price:,.2f}",
        "Remaining shares will be force-exited at 12pm.",
    ])
    _send(text)


def send_force_exit_1200(broker: str, symbol: str, exit_price: float,
                         return_pct: float, pnl: float,
                         dry_run: bool = False) -> None:
    tag   = "  [DRY RUN]" if dry_run else ""
    arrow = "▲" if return_pct >= 0 else "▼"
    text  = "\n".join([
        f"<b>{arrow} FORCE EXIT 12pm — {html_lib.escape(symbol)}{tag}</b>",
        f"<b>Broker:</b> {html_lib.escape(broker)}",
        f"<b>Exit price:</b> &#8377;{exit_price:,.2f}",
        f"<b>Return:</b> {return_pct:+.2f}%",
        f"<b>Realised P&amp;L:</b> &#8377;{pnl:+,.2f}",
    ])
    _send(text)


def send_nothing_open_at_1200(broker: str) -> None:
    _send(
        f"<b>12pm Exit — {html_lib.escape(broker)}</b>\n"
        "All positions already exited at 9:45am — nothing to force-close."
    )


def send_daily_summary(broker: str, n_opened: int, n_exited_945: int,
                       n_partial_nodata: int, n_force_1200: int,
                       total_pnl: float, dry_run: bool = False) -> None:
    tag   = "  [DRY RUN]" if dry_run else ""
    arrow = "▲" if total_pnl >= 0 else "▼"
    text  = "\n".join([
        f"<b>{arrow} Daily Summary — {html_lib.escape(broker)}{tag}</b>",
        f"<b>Positions opened  :</b> {n_opened}",
        f"<b>Exited at 9:45am  :</b> {n_exited_945}",
        f"<b>Partial no-data   :</b> {n_partial_nodata}",
        f"<b>Force-closed 12pm :</b> {n_force_1200}",
        f"<b>Total P&amp;L     :</b> &#8377;{total_pnl:+,.2f}",
    ])
    _send(text)


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
