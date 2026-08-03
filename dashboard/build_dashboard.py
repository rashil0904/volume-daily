#!/usr/bin/env python3
"""
dashboard/build_dashboard.py — Rebuild the "Trade Book" artifact HTML from results/trade_book.csv
====================================================================================================
Reads   : results/trade_book.csv        (written by zerodha/build_trade_book.py)
Reads   : dashboard/template.html       (HTML/CSS/JS shell with __PLACEHOLDER__ tokens)
Reads   : dashboard/fonts/*.b64         (embedded webfonts)
Writes  : dashboard/trade_book.html     (final, self-contained artifact HTML)

Only real capital positions count -- anything before _TRACKING_START_DATE is
excluded, mirroring the same cutoff zerodha/build_trade_book.py's trade book
already reflects (positions before that date were pre-live-capital test trades).

Usage:
    python3 dashboard/build_dashboard.py
"""

import csv
import json
from collections import defaultdict
from pathlib import Path

_ROOT       = Path(__file__).resolve().parent.parent
_DASH_DIR   = Path(__file__).resolve().parent
_CSV_FILE   = _ROOT / "results" / "trade_book.csv"
_TEMPLATE   = _DASH_DIR / "template.html"
_FONTS_DIR  = _DASH_DIR / "fonts"
_OUT_FILE   = _DASH_DIR / "trade_book.html"

_TRACKING_START_DATE = "2026-07-27"  # first day of real ₹1.5L capital


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_data() -> dict:
    rows = list(csv.DictReader(open(_CSV_FILE)))

    trades = []
    for r in rows:
        entry_date = r["Position entry date"]
        if entry_date < _TRACKING_START_DATE:
            continue
        trades.append({
            "symbol":         r["Stock Name"],
            "entry_date":     entry_date,
            "exit_date":      r["Position Exit date"] or None,
            "shares":         int(float(r["No of shares"])),
            "entry_price":    _f(r["Entry Price"]),
            "exit_price":     _f(r["Exit Price"]),
            "realised_pnl":   _f(r["Realised PnL"]),
            "realised_pct":   _f(r["Realised PnL Pct"]),
            "charges":        _f(r["Total Charges"]),
            "net_pnl":        _f(r["Net PnL"]),
            "net_pct":        _f(r["Net PnL Pct"]),
        })

    closed     = [t for t in trades if t["exit_date"]]
    open_t     = [t for t in trades if not t["exit_date"]]
    wins       = [t for t in closed if t["realised_pnl"] > 0]
    losses     = [t for t in closed if t["realised_pnl"] < 0]
    net_wins   = [t for t in closed if t["net_pnl"] is not None and t["net_pnl"] > 0]

    total_realised = sum(t["realised_pnl"] for t in closed)
    total_net      = sum(t["net_pnl"] for t in closed if t["net_pnl"] is not None)
    total_charges  = sum(t["charges"] for t in closed if t["charges"] is not None)

    by_exit = defaultdict(list)
    for t in closed:
        by_exit[t["exit_date"]].append(t)

    cum_gross = cum_net = 0.0
    curve = []
    for d in sorted(by_exit.keys()):
        day_trades = by_exit[d]
        day_gross = sum(t["realised_pnl"] for t in day_trades)
        day_net   = sum(t["net_pnl"] for t in day_trades if t["net_pnl"] is not None)
        cum_gross += day_gross
        cum_net   += day_net
        curve.append({
            "date": d, "n_trades": len(day_trades),
            "day_gross": round(day_gross, 2), "day_net": round(day_net, 2),
            "cum_gross": round(cum_gross, 2), "cum_net": round(cum_net, 2),
        })

    by_entry = defaultdict(list)
    for t in trades:
        by_entry[t["entry_date"]].append(t)
    day_bars = []
    for d in sorted(by_entry.keys()):
        day_trades = by_entry[d]
        closed_day = [t for t in day_trades if t["exit_date"]]
        gross = sum(t["realised_pnl"] for t in closed_day) if closed_day else None
        net   = sum(t["net_pnl"] for t in closed_day if t["net_pnl"] is not None) if closed_day else None
        day_bars.append({
            "date": d, "n_trades": len(day_trades), "n_closed": len(closed_day),
            "gross": round(gross, 2) if gross is not None else None,
            "net":   round(net, 2) if net is not None else None,
        })

    by_symbol = defaultdict(list)
    for t in closed:
        by_symbol[t["symbol"]].append(t)
    symbol_stats = [{
        "symbol": sym, "n": len(ts),
        "realised": round(sum(t["realised_pnl"] for t in ts), 2),
        "net":      round(sum(t["net_pnl"] for t in ts if t["net_pnl"] is not None), 2),
        "wins":     sum(1 for t in ts if t["realised_pnl"] > 0),
    } for sym, ts in by_symbol.items()]
    symbol_stats.sort(key=lambda x: -x["net"])

    summary = {
        "total_trades":  len(trades),
        "n_closed":      len(closed),
        "n_open":        len(open_t),
        "n_wins":        len(wins),
        "n_losses":      len(losses),
        "win_rate":      round(len(wins) / len(closed) * 100, 1) if closed else 0,
        "net_win_rate":  round(len(net_wins) / len(closed) * 100, 1) if closed else 0,
        "total_realised": round(total_realised, 2),
        "total_net":      round(total_net, 2),
        "total_charges":  round(total_charges, 2),
        "avg_win":  round(sum(t["realised_pnl"] for t in wins) / len(wins), 2) if wins else 0,
        "avg_loss": round(sum(t["realised_pnl"] for t in losses) / len(losses), 2) if losses else 0,
        "profit_factor": (round(sum(t["realised_pnl"] for t in wins) /
                                 abs(sum(t["realised_pnl"] for t in losses)), 2)
                          if losses else None),
        "best_trade":      max(closed, key=lambda t: t["realised_pnl"])["symbol"] if closed else None,
        "best_trade_pnl":  round(max(t["realised_pnl"] for t in closed), 2) if closed else None,
        "worst_trade":     min(closed, key=lambda t: t["realised_pnl"])["symbol"] if closed else None,
        "worst_trade_pnl": round(min(t["realised_pnl"] for t in closed), 2) if closed else None,
        "start_date": _TRACKING_START_DATE,
        "as_of": max((t["exit_date"] or t["entry_date"]) for t in trades) if trades else _TRACKING_START_DATE,
    }

    return {"trades": trades, "curve": curve, "day_bars": day_bars,
            "symbol_stats": symbol_stats, "summary": summary}


def main() -> None:
    data = build_data()
    template = _TEMPLATE.read_text()

    fonts = {
        "__FRAUNCES_NORMAL_B64__": "fraunces-normal.b64",
        "__FRAUNCES_ITALIC_B64__": "fraunces-italic.b64",
        "__PLEXSANS_B64__":        "plexsans.b64",
        "__PLEXMONO_400_B64__":    "plexmono-400.b64",
        "__PLEXMONO_500_B64__":    "plexmono-500.b64",
        "__PLEXMONO_600_B64__":    "plexmono-600.b64",
    }
    for placeholder, filename in fonts.items():
        b64 = (_FONTS_DIR / filename).read_text().strip()
        if placeholder not in template:
            raise RuntimeError(f"template missing placeholder {placeholder}")
        template = template.replace(placeholder, b64)

    if "__DATA_JSON__" not in template:
        raise RuntimeError("template missing __DATA_JSON__ placeholder")
    template = template.replace("__DATA_JSON__", json.dumps(data, separators=(",", ":")))

    _OUT_FILE.write_text(template)
    print(f"[dashboard] Wrote {_OUT_FILE}  ({len(template):,} bytes)")
    print(f"[dashboard] {data['summary']['total_trades']} positions "
          f"({data['summary']['n_closed']} closed, {data['summary']['n_open']} open) "
          f"as of {data['summary']['as_of']}")


if __name__ == "__main__":
    main()
