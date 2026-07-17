#!/usr/bin/env python3
"""
notify.py — NSE pipeline email notification
============================================
Sends a success or failure email after run_daily.py completes.

Importable API (used by run_daily.py):
  notify.send_success(date_str, start_ts, mcap_status="fresh")
  notify.send_failure(date_str, failed_step, error_msg, start_ts, mcap_status="fresh")

SETUP (one-time):
  1. Enable 2-Step Verification on your Gmail account.
  2. Go to: Google Account > Security > 2-Step Verification > App passwords
  3. Create an app password for "Mail" — you get a 16-character code.
  4. Add SENDER_APP_PASSWORD=<16-char-code> to your .env file.
"""

import csv
import html as html_lib
import json
import os
import smtplib
import sys
import time
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# ── CONFIG ─────────────────────────────────────────────────────────────────────
_env_file = Path(__file__).resolve().parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

SENDER_EMAIL        = os.environ.get("SENDER_EMAIL")
SENDER_APP_PASSWORD = os.environ.get("SENDER_APP_PASSWORD")
if not SENDER_EMAIL or not SENDER_APP_PASSWORD:
    raise EnvironmentError("Set SENDER_EMAIL and SENDER_APP_PASSWORD in .env before sending notifications.")
RECIPIENT_EMAILS    = [
    "paramshah1510@gmail.com",
    "khannakartik145@gmail.com",
    "kushalcchauhan88@gmail.com",
]

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
# ──────────────────────────────────────────────────────────────────────────────

PROJECT_DIR = Path(__file__).parent


def _load_mcap_status() -> dict:
    path = PROJECT_DIR / "market_cap_daily" / "mcap_status.json"
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
    m, s = divmod(total, 60)
    return f"{m}m {s}s"


def _count_signals(trade_list_path: Path) -> int:
    if not trade_list_path.exists() or trade_list_path.stat().st_size < 50:
        return 0
    with open(trade_list_path, newline="") as f:
        return sum(1 for _ in csv.DictReader(f))


# ── Trade table builders ──────────────────────────────────────────────────────

def _text_trade_table(path: Path) -> str:
    if not path.exists():
        return "No trades triggered today."
    rows = list(csv.DictReader(open(path, newline="")))
    if not rows:
        return "No trades triggered today."
    header = f"{'Symbol':<12} {'Entry ₹':>10}  {'MCap Cr':>8}  {'Vol×':>6}  {'Ret%':>6}"
    sep    = "─" * len(header)
    lines  = [header, sep]
    for row in rows:
        lines.append(
            f"{row.get('symbol',''):<12}"
            f" ₹{float(row.get('entry_price_315pm', 0)):>9,.2f}"
            f"  {float(row.get('market_cap_cr', 0)):>8,.0f}"
            f"  {float(row.get('volume_ratio', 0)):>5.2f}×"
            f"  +{float(row.get('return_pct_vs_prev_close', 0)):>4.2f}%"
        )
    return "\n".join(lines)


def _html_trade_table(path: Path) -> str:
    if not path.exists():
        return "<p><strong>No trades triggered today.</strong></p>"
    rows = list(csv.DictReader(open(path, newline="")))
    if not rows:
        return "<p><strong>No trades triggered today.</strong></p>"

    th_style = "background:#1F497D;color:white;padding:6px 10px;text-align:right;"
    th_l     = "background:#1F497D;color:white;padding:6px 10px;text-align:left;"
    body = (
        '<table border="0" cellpadding="6" cellspacing="0" '
        'style="border-collapse:collapse;font-family:Courier New,monospace;'
        'font-size:13px;margin:8px 0;">\n'
        f'<thead><tr>'
        f'<th style="{th_l}">Symbol</th>'
        f'<th style="{th_style}">Entry ₹ (3:15pm)</th>'
        f'<th style="{th_style}">MCap Cr</th>'
        f'<th style="{th_style}">Vol Ratio</th>'
        f'<th style="{th_style}">Return %</th>'
        '</tr></thead>\n<tbody>\n'
    )
    for i, row in enumerate(rows):
        bg   = "#EAF2FF" if i % 2 == 0 else "#FFFFFF"
        td   = f'style="padding:5px 10px;background:{bg};text-align:right;"'
        td_l = f'style="padding:5px 10px;background:{bg};text-align:left;"'
        body += (
            f'<tr>'
            f'<td {td_l}><strong>{row.get("symbol","")}</strong></td>'
            f'<td {td}>₹{float(row.get("entry_price_315pm", 0)):,.2f}</td>'
            f'<td {td}>{float(row.get("market_cap_cr", 0)):,.0f}</td>'
            f'<td {td}>{float(row.get("volume_ratio", 0)):.2f}×</td>'
            f'<td {td}>+{float(row.get("return_pct_vs_prev_close", 0)):.2f}%</td>'
            '</tr>\n'
        )
    body += "</tbody></table>"
    return body


# ── Market cap warning banners ────────────────────────────────────────────────

def _mcap_warning_text(mcap_status: str, mcap_st: dict) -> str:
    if mcap_status == "stale":
        d = mcap_st.get("fallback_date", "unknown date")
        return (
            f"\n⚠ WARNING: Today's market cap data could not be fetched live.\n"
            f"  Using data from {d} instead.\n"
            f"  Trade list below may be less accurate than usual.\n"
        )
    if mcap_status == "failed":
        return "\n⚠ WARNING: Market cap fetch failed with no fallback. Treat signals with caution.\n"
    return ""


def _mcap_warning_html(mcap_status: str, mcap_st: dict) -> str:
    if mcap_status == "stale":
        d = mcap_st.get("fallback_date", "unknown date")
        return (
            '<div style="background:#FFF3CD;border:1px solid #FFCC00;padding:12px 16px;'
            'margin-bottom:16px;border-radius:4px;">'
            '<strong style="color:#856404;">⚠ Stale Market Cap Data</strong><br>'
            f'Live Screener.in export failed. Using data from <strong>{d}</strong>. '
            'Verify signals before executing.'
            '</div>'
        )
    if mcap_status == "failed":
        return (
            '<div style="background:#FFF3CD;border:1px solid #FFCC00;padding:12px 16px;'
            'margin-bottom:16px;border-radius:4px;">'
            '<strong style="color:#856404;">⚠ Market Cap Data Unavailable</strong><br>'
            'Screener.in fetch failed with no fallback. Verify market caps manually.'
            '</div>'
        )
    return ""


# ── Email builders ────────────────────────────────────────────────────────────

def _build_success_email(date_str: str, n_signals: int, trade_list_path: Path,
                         start_ts, mcap_status: str) -> MIMEMultipart:
    mcap_st    = _load_mcap_status()
    runtime    = _runtime_str(start_ts)
    subject    = f"NSE Trades — {date_str} — {n_signals} signal{'s' if n_signals != 1 else ''}"
    if mcap_status == "stale":
        subject += " [STALE MCAP]"

    has_trades = trade_list_path.exists() and trade_list_path.stat().st_size > 50
    warn_text  = _mcap_warning_text(mcap_status, mcap_st)
    warn_html  = _mcap_warning_html(mcap_status, mcap_st)

    text = "\n".join([
        f"NSE Volume Breakout Pipeline — {date_str}",
        "=" * 52,
        "",
        f"Status        : SUCCESS",
        f"Runtime       : {runtime}",
        f"Signals today : {n_signals}",
        warn_text,
        "Trade signals:",
        _text_trade_table(trade_list_path),
        "",
        "CSV attached." if has_trades else "",
    ])

    trade_html  = _html_trade_table(trade_list_path)
    attach_note = "<p style='font-size:12px;color:#555;'>Trade list CSV attached.</p>" \
                  if has_trades else ""

    html = f"""<html><body style="font-family:Calibri,Arial,sans-serif;color:#222;max-width:720px;">
<h2 style="color:#1F497D;margin-bottom:4px;">NSE Pipeline — SUCCESS</h2>
<p style="color:#555;margin-top:0;">{date_str}</p>
{warn_html}
<table style="font-size:14px;margin-bottom:20px;border-spacing:0;">
  <tr><td style="padding:3px 20px 3px 0;color:#555;">Status</td>
      <td><strong style="color:green;">SUCCESS</strong></td></tr>
  <tr><td style="padding:3px 20px 3px 0;color:#555;">Runtime</td>
      <td>{runtime}</td></tr>
  <tr><td style="padding:3px 20px 3px 0;color:#555;">Signals today</td>
      <td><strong>{n_signals}</strong></td></tr>
</table>
<h3 style="color:#2E74B5;margin-bottom:6px;">Trade Signals</h3>
{trade_html}
{attach_note}
<p style="font-size:11px;color:#aaa;margin-top:32px;border-top:1px solid #eee;padding-top:8px;">
  NSE Volume Breakout · MCap ₹1,500–5,000 Cr · Volume ≥6× 36-day avg · Return ≥5% vs prev VWAP · Entry 3:15pm open
</p>
</body></html>"""

    outer = MIMEMultipart("mixed")
    outer["Subject"] = subject
    outer["From"]    = SENDER_EMAIL
    outer["To"]      = ", ".join(RECIPIENT_EMAILS)

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(text, "plain", "utf-8"))
    alt.attach(MIMEText(html, "html",  "utf-8"))
    outer.attach(alt)

    if has_trades:
        with open(trade_list_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition",
                        f"attachment; filename={trade_list_path.name}")
        outer.attach(part)

    return outer


def _build_failure_email(date_str: str, failed_step: str, error_msg: str,
                         start_ts, mcap_status: str) -> MIMEMultipart:
    mcap_st       = _load_mcap_status()
    runtime       = _runtime_str(start_ts)
    subject       = f"NSE Pipeline FAILED — {date_str}"
    warn_text     = _mcap_warning_text(mcap_status, mcap_st)
    warn_html     = _mcap_warning_html(mcap_status, mcap_st)
    escaped_error = html_lib.escape(error_msg or "(no error detail)")

    text = "\n".join([
        f"NSE Volume Breakout Pipeline — {date_str}",
        "=" * 52,
        "",
        f"Status      : FAILED",
        f"Failed step : {failed_step or 'unknown'}",
        f"Runtime     : {runtime}",
        warn_text,
        "Error detail:",
        "-" * 40,
        error_msg or "(none)",
        "-" * 40,
        "",
        "To retry:",
        f"  cd '{PROJECT_DIR}'",
        f"  python3 run_daily.py",
    ])

    html = f"""<html><body style="font-family:Calibri,Arial,sans-serif;color:#222;max-width:720px;">
<h2 style="color:#C00000;margin-bottom:4px;">NSE Pipeline — FAILED</h2>
<p style="color:#555;margin-top:0;">{date_str}</p>
{warn_html}
<table style="font-size:14px;margin-bottom:20px;border-spacing:0;">
  <tr><td style="padding:3px 20px 3px 0;color:#555;">Status</td>
      <td><strong style="color:red;">FAILED</strong></td></tr>
  <tr><td style="padding:3px 20px 3px 0;color:#555;">Failed step</td>
      <td><strong>{html_lib.escape(failed_step or 'unknown')}</strong></td></tr>
  <tr><td style="padding:3px 20px 3px 0;color:#555;">Runtime</td>
      <td>{runtime}</td></tr>
</table>
<h3 style="color:#C00000;margin-bottom:6px;">Error Detail</h3>
<pre style="background:#FFF0F0;border:1px solid #FFAAAA;padding:14px;
            font-size:12px;font-family:Courier New,monospace;
            white-space:pre-wrap;word-break:break-all;">{escaped_error}</pre>
<p style="font-size:12px;color:#555;margin-top:16px;">
  To retry:<br>
  <code>cd '{html_lib.escape(str(PROJECT_DIR))}'</code><br>
  <code>python3 run_daily.py</code>
</p>
<p style="font-size:11px;color:#aaa;margin-top:32px;border-top:1px solid #eee;padding-top:8px;">
  NSE Volume Breakout · MCap ₹1,500–5,000 Cr · Volume ≥6× 36-day avg · Return ≥5% vs prev VWAP
</p>
</body></html>"""

    outer = MIMEMultipart("mixed")
    outer["Subject"] = subject
    outer["From"]    = SENDER_EMAIL
    outer["To"]      = ", ".join(RECIPIENT_EMAILS)

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(text, "plain", "utf-8"))
    alt.attach(MIMEText(html, "html",  "utf-8"))
    outer.attach(alt)

    return outer


# ── SMTP sender ───────────────────────────────────────────────────────────────

def _send(msg: MIMEMultipart) -> None:
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(SENDER_EMAIL, SENDER_APP_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAILS, msg.as_string())


# ── Public API (called by run_daily.py) ───────────────────────────────────────

def send_success(date_str: str, start_ts, mcap_status: str = "fresh") -> None:
    trade_list_path = PROJECT_DIR / "results" / f"trade_list_{date_str}.csv"
    n_signals = _count_signals(trade_list_path)
    msg = _build_success_email(date_str, n_signals, trade_list_path, start_ts, mcap_status)
    _send(msg)
    print(f"  Email sent ({n_signals} signal{'s' if n_signals != 1 else ''}) "
          f"→ {', '.join(RECIPIENT_EMAILS)}")


def send_failure(date_str: str, failed_step: str, error_msg: str,
                 start_ts, mcap_status: str = "fresh") -> None:
    msg = _build_failure_email(date_str, failed_step, error_msg, start_ts, mcap_status)
    _send(msg)
    print(f"  Failure email sent → {', '.join(RECIPIENT_EMAILS)}")


# ── CLI (for manual use / testing) ───────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Send NSE pipeline result email")
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
        print(f"ERROR: email send failed: {e}", file=sys.stderr)
        sys.exit(1)
