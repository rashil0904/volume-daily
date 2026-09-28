#!/usr/bin/env python3
"""
build_pnl_simple.py -- generates strategy_pnl_simple.xlsx, an 8-sheet P&L
tracker for a live NSE intraday strategy that trades both long and short
positions.

Sheet order: Total PnL, Trade Log, Day Wise PnL, Position Type Stats,
Bucket Stats, Company Stats, Charges, Monthly & Weekly PnL.
Every computed cell is a formula string -- openpyxl never pre-computes a
value in Python. Formulas avoid XLOOKUP/XMATCH/SORT/FILTER/UNIQUE/SEQUENCE
for LibreOffice/Google Sheets compatibility; MAXIFS/MINIFS are written as
_xlfn.MAXIFS/_xlfn.MINIFS so they don't show #NAME? in older Excel/LO
formula-function tables.

Usage:
    python3 build_pnl_simple.py
"""

import json
import re
import sys
from datetime import date, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
import dhan.charges as dhan_charges  # noqa: E402  (import after sys.path setup)

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.formatting.rule import CellIsRule, FormulaRule, DataBarRule

RESULTS_DIR = Path(__file__).resolve().parent
OUT_PATH    = RESULTS_DIR / "strategy_pnl_simple.xlsx"
# Long and short positions live in separate files (see dhan/run_trades.py's
# "Positions JSON" section for why) -- merged back together here since the
# Trade Log sheet shows both long and short trades in one list.
DHAN_POSITIONS_LONG_PATH  = RESULTS_DIR / "positions_dhan_long.json"
DHAN_POSITIONS_SHORT_PATH = RESULTS_DIR / "positions_dhan_short.json"

# Trade Log is auto-synced from Dhan's real fills starting this date -- entries
# before it (from before this workbook existed) are out of scope, not missing data.
PNL_START_DATE = "2026-08-19"

# ── Number formats ──────────────────────────────────────────────────────────
# Every non-integer figure in the workbook shows exactly 2 decimal places --
# INR and PCT both round to 2 digits; NUM (trade/win/loss counts) stays a
# plain integer since a count of trades has no fractional meaning.
INR   = '₹#,##0.00;(₹#,##0.00);"-"'
PCT   = '0.00%;(0.00%);"-"'
NUM   = '#,##0;(#,##0);"-"'
PRICE = '#,##0.00'
DATE  = 'yyyy-mm-dd'
XMULT = '0.00"x"'

# ── Colors / fonts / fills ──────────────────────────────────────────────────
FONT_NAME = "Arial"

HEADER_FONT = Font(name=FONT_NAME, bold=True, color="FFFFFFFF")
HEADER_FILL = PatternFill(start_color="FF1F3864", end_color="FF1F3864", fill_type="solid")

INPUT_FONT = Font(name=FONT_NAME, color="FF0000FF")
INPUT_FILL = PatternFill(start_color="FFFFF2CC", end_color="FFFFF2CC", fill_type="solid")

FORMULA_FONT = Font(name=FONT_NAME, color="FF000000")
LABEL_FONT   = Font(name=FONT_NAME, color="FF000000", bold=True)
TOTAL_FONT   = Font(name=FONT_NAME, color="FF000000", bold=True)

EXAMPLE_FONT = Font(name=FONT_NAME, italic=True, color="FF0000FF")
EXAMPLE_FILL = PatternFill(start_color="FFF2F2F2", end_color="FFF2F2F2", fill_type="solid")

TITLE_FONT    = Font(name=FONT_NAME, bold=True, size=14, color="FF1F3864")
SUBTITLE_FONT = Font(name=FONT_NAME, italic=True, size=10, color="FF808080")

RED_FONT      = Font(name=FONT_NAME, color="FFFF0000")

CENTER = Alignment(horizontal="center", vertical="center")
LEFT   = Alignment(horizontal="left", vertical="center")


# ── Styling helpers ──────────────────────────────────────────────────────────

def set_title_subtitle(ws, title: str, subtitle: str, ncols: int) -> None:
    last_col = get_column_letter(ncols)
    ws.merge_cells(f"A1:{last_col}1")
    c = ws["A1"]
    c.value = title
    c.font = TITLE_FONT
    c.alignment = LEFT
    ws.row_dimensions[1].height = 22

    ws.merge_cells(f"A2:{last_col}2")
    c = ws["A2"]
    c.value = subtitle
    c.font = SUBTITLE_FONT
    c.alignment = LEFT
    ws.row_dimensions[2].height = 16


def style_header_row(ws, row: int, ncols: int) -> None:
    for col in range(1, ncols + 1):
        cell = ws.cell(row=row, column=col)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = CENTER


def style_input(cell, number_format: str | None = None) -> None:
    cell.font = INPUT_FONT
    cell.fill = INPUT_FILL
    if number_format:
        cell.number_format = number_format


def style_formula(cell, number_format: str | None = None) -> None:
    cell.font = FORMULA_FONT
    if number_format:
        cell.number_format = number_format


def style_example(cell, number_format: str | None = None) -> None:
    cell.font = EXAMPLE_FONT
    cell.fill = EXAMPLE_FILL
    if number_format:
        cell.number_format = number_format


# ── Sheet 2: Trade Log ───────────────────────────────────────────────────────

TL_FIRST_ROW = 4
TL_LAST_ROW  = 503
TL_HEADERS   = ["Trade ID", "Symbol", "Position", "Entry Date", "Entry Price",
                "Qty", "Exit Date", "Exit Price", "Gross P&L", "Gross Return %",
                "Costs", "Net P&L", "Net Return %", "Status", "Return Bucket"]


def _extract_exit(position: dict) -> tuple[str | None, float | None]:
    """Finds whichever exit_price_<stage>/exit_timestamp_<stage> pair is
    present on a position record -- stage suffix varies by exit path (916,
    1159 for a long; 239 for a mirrored short's square-off). Returns
    (iso_date_str, price), or (None, None) if the position is still open
    (no exit fields yet)."""
    for key, value in position.items():
        m = re.fullmatch(r"exit_price_(\w+)", key)
        if m and value is not None:
            ts = position.get(f"exit_timestamp_{m.group(1)}")
            if ts:
                return ts[:10], float(value)
    return None, None


def _position_to_trade_row(position: dict) -> dict | None:
    """Maps one positions_dhan.json record (long or mirrored short) to a
    Trade Log row dict. Costs isn't set here -- see _live_cost, attached by
    the caller (load_trades_from_positions)."""
    symbol = position.get("symbol")
    if not symbol:
        return None

    is_short = position.get("direction") == "short"
    entry_price = position.get("entry_price") if is_short else position.get("actual_fill_price")
    qty         = position.get("quantity")     if is_short else position.get("actual_fill_quantity")
    if entry_price is None or qty is None:
        return None

    entry_date = position.get("entry_date") or (position.get("entry_timestamp") or "")[:10]
    if not entry_date or entry_date < PNL_START_DATE:
        return None

    exit_date, exit_price = _extract_exit(position)
    entry_order_id = position.get("entry_order_id") or ""
    # A tranche entry (run_entry_limit) stores entry_order_id as a LIST --
    # every real LIMIT/MARKET order that funded it, see dhan/charges.py's
    # _oid_list -- joined into one readable id for display here.
    if isinstance(entry_order_id, list):
        entry_order_id = "+".join(entry_order_id)
    trade_id = f"DHAN-{entry_order_id}" if entry_order_id else f"DHAN-{symbol}-{entry_date}"

    return {
        "id": trade_id, "symbol": symbol, "position": "SHORT" if is_short else "LONG",
        "entry_date": date.fromisoformat(entry_date), "entry_price": float(entry_price), "qty": int(qty),
        "exit_date": date.fromisoformat(exit_date) if exit_date else None,
        "exit_price": exit_price,
        # return_bucket only exists on positions entered after the
        # return-bucketed exit schedule shipped (2026-09-26) -- older trades
        # get the literal "Untagged" here (NOT ""/blank -- COUNTIFS/SUMIFS
        # matching a blank/"" criteria against another FORMULA-computed
        # range at Trade Log's 500-row scale is unreliable in LibreOffice,
        # confirmed via a direct repro: it silently undercounts to 0 even
        # when real matches exist. An explicit literal sidesteps that
        # engine quirk entirely -- see Bucket Stats' matching note).
        "bucket": position.get("return_bucket") or "Untagged",
    }


def _live_cost(position: dict, trade_index: dict[str, dict],
               interest_index: dict[str, float]) -> float | None:
    """Real per-position charges -- brokerage/STT/exchange/SEBI/stamp/GST on
    the entry (and exit, once closed) leg, plus DP/pledge and MTF interest
    where applicable -- via dhan.charges.position_charge_summary(). Entry/
    exit leg charges are Dhan's real trade-book figures ONLY (trade_index) --
    no rate-card estimate fallback (removed 2026-09-15); a leg not yet in
    Dhan's trade-book (same-day fills, mainly) reads as 0 until a later run
    picks it up. MTF interest still prefers a real-ledger allocation
    (interest_index, see mtf_interest_allocation_index) over its own funded
    x rate x days formula fallback -- that's a separate mechanism, not
    covered by the leg-charge estimate removal. trade_index/interest_index
    are both built ONCE for the whole run, see load_trades_from_positions.
    DP/pledge stay the fixed estimate always -- Dhan's ledger only exposes
    those as one combined daily total with no way to attribute it back to
    a single position (see dhan.charges.dp_ledger_total's docstring).
    Returns None on ANY failure (expired token, Dhan API outage) rather
    than raising -- a None here means build_trade_log writes 0, not a
    guessed/stale number, so a blank Costs cell always means "no charge
    data yet," never "possibly out of date"."""
    try:
        return dhan_charges.position_charge_summary(
            position, trade_index, interest_index)["total_charges"]
    except Exception:
        return None


def _live_charge_breakdown(position: dict, trade_index: dict[str, dict],
                           interest_index: dict[str, float]) -> dict | None:
    """Per-category charge split (Brokerage/STT/Exchange/SEBI/Stamp/GST/DP/
    Pledge/MTF Interest) for the Charges sheet, via
    dhan.charges.position_charge_breakdown() -- same real-API-only,
    no-guessed-numbers rules as _live_cost (see its own docstring), just
    exposing the categories individually instead of one total. Returns None
    on ANY failure, same reasoning as _live_cost: a blank row means "no
    charge data yet," never a guessed number."""
    try:
        return dhan_charges.position_charge_breakdown(position, trade_index, interest_index)
    except Exception:
        return None


def _load_json_list(path: Path) -> list:
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return []


def load_trades_from_positions() -> list[dict]:
    """Reads positions_dhan_long.json + positions_dhan_short.json and
    returns one Trade Log row dict per position dated PNL_START_DATE or
    later (long or short) -- open positions come back with
    exit_date/exit_price=None, matching Trade Log's own Open/Closed status
    formula. Each row also carries a live "cost" (see _live_cost) so Costs
    auto-updates every sync instead of staying purely manual. A missing/
    unreadable/empty file on either side just contributes no rows from that
    side, rather than failing the whole sync."""
    positions = _load_json_list(DHAN_POSITIONS_LONG_PATH) + _load_json_list(DHAN_POSITIONS_SHORT_PATH)

    # One trade-book fetch and one MTF-interest-allocation pass for the
    # WHOLE run, not one per position -- see dhan.charges.charges_index and
    # mtf_interest_allocation_index. Both fall back to {} (every leg reads
    # 0/"pending", every position's interest reads its own formula fallback)
    # on an outright API/auth failure rather than raising -- a workbook sync
    # shouldn't fail outright just because live charge data isn't reachable.
    try:
        trade_index = dhan_charges.charges_index(PNL_START_DATE)
    except Exception:
        trade_index = {}
    try:
        interest_index = dhan_charges.mtf_interest_allocation_index(positions, PNL_START_DATE)
    except Exception:
        interest_index = {}

    trades = []
    for pos in positions:
        row = _position_to_trade_row(pos)
        if row is None:
            continue
        row["cost"] = _live_cost(pos, trade_index, interest_index)
        row["charges"] = _live_charge_breakdown(pos, trade_index, interest_index)
        trades.append(row)

    # Sorted by exit date -- still-open positions (no exit yet) have none,
    # so they sort after every closed trade rather than colliding with a
    # placeholder date; symbol is just the tiebreak within either group.
    trades.sort(key=lambda t: (t["exit_date"] is None, t["exit_date"] or date.max, t["symbol"]))
    return trades


def build_trade_log(ws, trades: list[dict] | None = None) -> None:
    """trades=None (or empty) falls back to the two worked examples -- same
    as the original standalone builder. trades=[...] (from
    load_trades_from_positions) auto-syncs real Dhan fills into rows 4+
    instead, Costs included -- see _live_cost for what happens when a
    live charge fetch fails (0, not a stale carried-over number)."""
    set_title_subtitle(
        ws, "Trade Log",
        "One row per position -- long and short trades are logged separately. "
        "Fill columns A-H, K and O; formulas compute I, J, L, M, N.",
        len(TL_HEADERS),
    )

    for col, header in enumerate(TL_HEADERS, start=1):
        ws.cell(row=3, column=col, value=header)
    style_header_row(ws, 3, len(TL_HEADERS))

    widths = [26, 14, 10, 12, 12, 9, 12, 12, 12, 13, 10, 12, 12, 10, 14]
    for col, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(col)].width = w

    manual_fmts = {"D": DATE, "E": PRICE, "F": NUM, "G": DATE, "H": PRICE, "K": INR}
    synced_rows: dict[int, dict] = {}

    if trades:
        # Auto-synced from real Dhan fills -- still input-styled (blue/yellow)
        # since Costs (K) stays user-editable and everything regenerates from
        # position JSON on every run anyway.
        for offset, trade in enumerate(trades[: TL_LAST_ROW - TL_FIRST_ROW + 1]):
            r = TL_FIRST_ROW + offset
            synced_rows[r] = trade
            row_vals = {
                "A": trade["id"], "B": trade["symbol"], "C": trade["position"],
                "D": trade["entry_date"], "E": trade["entry_price"], "F": trade["qty"],
                "G": trade["exit_date"] or "", "H": trade["exit_price"] if trade["exit_price"] is not None else "",
                "K": trade["cost"] if trade.get("cost") is not None else 0,
                "O": trade.get("bucket") or "",
            }
            for col_letter, val in row_vals.items():
                cell = ws[f"{col_letter}{r}"]
                cell.value = val
                style_input(cell, manual_fmts.get(col_letter))
    else:
        # No real trades yet -- two worked examples (rows 4-5), delete before real use.
        examples = [
            {
                "id": "EX-LONG-1 (EXAMPLE — delete before real use)",
                "symbol": "RELIANCE", "position": "LONG",
                "entry_date": date(2026, 7, 31), "entry_price": 2500.00, "qty": 100,
                "exit_date": date(2026, 8, 1), "exit_price": 2550.00, "costs": 150,
                "bucket": "10-15",
            },
            {
                "id": "EX-SHORT-1 (EXAMPLE — delete before real use)",
                "symbol": "TCS", "position": "SHORT",
                "entry_date": date(2026, 7, 31), "entry_price": 3600.00, "qty": 50,
                "exit_date": date(2026, 8, 1), "exit_price": 3550.00, "costs": 100,
                "bucket": "5-10",
            },
        ]
        for offset, ex in enumerate(examples):
            r = TL_FIRST_ROW + offset
            row_vals = {
                "A": ex["id"], "B": ex["symbol"], "C": ex["position"],
                "D": ex["entry_date"], "E": ex["entry_price"], "F": ex["qty"],
                "G": ex["exit_date"], "H": ex["exit_price"], "K": ex["costs"],
                "O": ex["bucket"],
            }
            for col_letter, val in row_vals.items():
                cell = ws[f"{col_letter}{r}"]
                cell.value = val
                style_example(cell, manual_fmts.get(col_letter))

    # Manual input styling (for every row not already synced/example above) + formulas, rows 4-503.
    for r in range(TL_FIRST_ROW, TL_LAST_ROW + 1):
        if r not in synced_rows and not (not trades and r in (4, 5)):
            for col_letter in ("A", "B", "C", "D", "E", "F", "G", "H", "K", "O"):
                cell = ws[f"{col_letter}{r}"]
                style_input(cell, manual_fmts.get(col_letter))

        i_formula = (f'=IF(OR($B{r}="",$H{r}=""),"",'
                     f'IF($C{r}="LONG",($H{r}-$E{r})*$F{r},($E{r}-$H{r})*$F{r}))')
        j_formula = f'=IF(OR($B{r}="",$I{r}="",$E{r}=0,$F{r}=0),"",$I{r}/($E{r}*$F{r}))'
        l_formula = f'=IF(OR($B{r}="",$I{r}=""),"",$I{r}-IF($K{r}="",0,$K{r}))'
        m_formula = f'=IF(OR($B{r}="",$L{r}="",$E{r}=0,$F{r}=0),"",$L{r}/($E{r}*$F{r}))'
        n_formula = f'=IF($B{r}="","",IF($H{r}="","Open","Closed"))'

        cell_i = ws[f"I{r}"]; cell_i.value = i_formula; style_formula(cell_i, INR)
        cell_j = ws[f"J{r}"]; cell_j.value = j_formula; style_formula(cell_j, PCT)
        cell_l = ws[f"L{r}"]; cell_l.value = l_formula; style_formula(cell_l, INR)
        cell_m = ws[f"M{r}"]; cell_m.value = m_formula; style_formula(cell_m, PCT)
        cell_n = ws[f"N{r}"]; cell_n.value = n_formula; style_formula(cell_n)

    # Data validation: Position dropdown.
    dv = DataValidation(type="list", formula1='"LONG,SHORT"', allow_blank=True)
    ws.add_data_validation(dv)
    dv.add(f"C{TL_FIRST_ROW}:C{TL_LAST_ROW}")

    # Data validation: Return Bucket dropdown.
    dv_bucket = DataValidation(type="list", formula1='"5-10,10-15,15-20,Untagged"', allow_blank=True)
    ws.add_data_validation(dv_bucket)
    dv_bucket.add(f"O{TL_FIRST_ROW}:O{TL_LAST_ROW}")

    # Conditional formatting: Net P&L < 0 -> red font.
    ws.conditional_formatting.add(
        f"L{TL_FIRST_ROW}:L{TL_LAST_ROW}",
        CellIsRule(operator="lessThan", formula=["0"], font=RED_FONT),
    )

    ws.freeze_panes = "C4"
    ws.auto_filter.ref = f"A3:O{TL_LAST_ROW}"


# ── Sheet 3: Day Wise PnL ────────────────────────────────────────────────────

DW_FIRST_ROW = 4
DW_LAST_ROW  = 403
DW_HEADERS   = ["Date", "Long Gross P&L", "Short Gross P&L", "Total Gross P&L",
                "Long Net P&L", "Short Net P&L", "Total Net P&L"]


def build_day_wise(ws) -> None:
    set_title_subtitle(
        ws, "Day Wise PnL",
        "One row per calendar date, attributed by Exit Date -- continues forward "
        "automatically from the first date in A4.",
        len(DW_HEADERS),
    )

    for col, header in enumerate(DW_HEADERS, start=1):
        ws.cell(row=3, column=col, value=header)
    style_header_row(ws, 3, len(DW_HEADERS))

    widths = [14, 16, 16, 16, 16, 16, 16]
    for col, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(col)].width = w

    # First date is a manual anchor -- matches PNL_START_DATE (Trade Log's own
    # sync cutoff), so the calendar doesn't run rows before real data exists.
    a4 = ws["A4"]
    a4.value = date.fromisoformat(PNL_START_DATE)
    style_input(a4, DATE)

    tl = "'Trade Log'"
    for r in range(DW_FIRST_ROW, DW_LAST_ROW + 1):
        if r > DW_FIRST_ROW:
            a_formula = f'=IF(A{r-1}="","",A{r-1}+1)'
            cell_a = ws[f"A{r}"]; cell_a.value = a_formula; style_formula(cell_a, DATE)

        g = f'IF($A{r}="","",'

        b_formula = (f'={g}SUMIFS({tl}!$I$4:$I$503,{tl}!$G$4:$G$503,$A{r},'
                     f'{tl}!$C$4:$C$503,"LONG"))')
        c_formula = (f'={g}SUMIFS({tl}!$I$4:$I$503,{tl}!$G$4:$G$503,$A{r},'
                     f'{tl}!$C$4:$C$503,"SHORT"))')
        d_formula = f'=IF($A{r}="","",$B{r}+$C{r})'
        e_formula = (f'={g}SUMIFS({tl}!$L$4:$L$503,{tl}!$G$4:$G$503,$A{r},'
                     f'{tl}!$C$4:$C$503,"LONG"))')
        f_formula = (f'={g}SUMIFS({tl}!$L$4:$L$503,{tl}!$G$4:$G$503,$A{r},'
                     f'{tl}!$C$4:$C$503,"SHORT"))')
        g_formula = f'=IF($A{r}="","",$E{r}+$F{r})'

        for col_letter, formula in (
            ("B", b_formula), ("C", c_formula), ("D", d_formula),
            ("E", e_formula), ("F", f_formula), ("G", g_formula),
        ):
            cell = ws[f"{col_letter}{r}"]
            cell.value = formula
            style_formula(cell, INR)

    ws.conditional_formatting.add(
        f"G{DW_FIRST_ROW}:G{DW_LAST_ROW}",
        CellIsRule(operator="lessThan", formula=["0"], font=RED_FONT),
    )

    ws.freeze_panes = "B4"


# ── Sheet 4: Position Type Stats ─────────────────────────────────────────────

PS_HEADERS = ["Position", "Trades", "Wins", "Losses", "Win Rate", "Gross P&L",
              "Net P&L", "Avg Net P&L / Trade", "Best Trade", "Worst Trade",
              "Profit Factor"]


def build_position_stats(ws) -> None:
    set_title_subtitle(
        ws, "Position Type Stats",
        "Long vs. short performance, computed straight from Trade Log.",
        len(PS_HEADERS),
    )

    for col, header in enumerate(PS_HEADERS, start=1):
        ws.cell(row=3, column=col, value=header)
    style_header_row(ws, 3, len(PS_HEADERS))

    widths = [12, 10, 9, 10, 11, 14, 14, 16, 14, 14, 14]
    for col, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(col)].width = w

    tl = "'Trade Log'"
    col_fmt = {"B": NUM, "C": NUM, "D": NUM, "E": PCT, "F": INR, "G": INR,
               "H": INR, "I": INR, "J": INR, "K": XMULT}

    for r, position in ((4, "LONG"), (5, "SHORT")):
        cell_a = ws[f"A{r}"]; cell_a.value = position; style_formula(cell_a)

        formulas = {
            "B": f"=COUNTIF({tl}!$C$4:$C$503,$A{r})",
            "C": f'=COUNTIFS({tl}!$C$4:$C$503,$A{r},{tl}!$L$4:$L$503,">0")',
            "D": f'=COUNTIFS({tl}!$C$4:$C$503,$A{r},{tl}!$L$4:$L$503,"<0")',
            "E": f'=IFERROR($C{r}/$B{r},"")',
            "F": f"=SUMIF({tl}!$C$4:$C$503,$A{r},{tl}!$I$4:$I$503)",
            "G": f"=SUMIF({tl}!$C$4:$C$503,$A{r},{tl}!$L$4:$L$503)",
            "H": f'=IFERROR($G{r}/$B{r},"")',
            "I": f'=IFERROR(_xlfn.MAXIFS({tl}!$L$4:$L$503,{tl}!$C$4:$C$503,$A{r}),"")',
            "J": f'=IFERROR(_xlfn.MINIFS({tl}!$L$4:$L$503,{tl}!$C$4:$C$503,$A{r}),"")',
            "K": (f'=IFERROR(SUMIFS({tl}!$L$4:$L$503,{tl}!$C$4:$C$503,$A{r},{tl}!$L$4:$L$503,">0")/'
                  f'ABS(SUMIFS({tl}!$L$4:$L$503,{tl}!$C$4:$C$503,$A{r},{tl}!$L$4:$L$503,"<0")),"")'),
        }
        for col_letter, formula in formulas.items():
            cell = ws[f"{col_letter}{r}"]
            cell.value = formula
            style_formula(cell, col_fmt[col_letter])

    # TOTAL row.
    total_row = 6
    ws[f"A{total_row}"] = "TOTAL"
    ws[f"A{total_row}"].font = TOTAL_FONT

    total_formulas = {
        "B": "=SUM(B4:B5)",
        "C": "=SUM(C4:C5)",
        "D": "=SUM(D4:D5)",
        "E": f'=IFERROR(C{total_row}/B{total_row},"")',
        "F": "=SUM(F4:F5)",
        "G": "=SUM(G4:G5)",
        "H": f'=IFERROR(G{total_row}/B{total_row},"")',
        "K": (f'=IFERROR(SUMIF({tl}!$L$4:$L$503,">0")/'
              f'ABS(SUMIF({tl}!$L$4:$L$503,"<0")),"")'),
    }
    for col_letter, formula in total_formulas.items():
        cell = ws[f"{col_letter}{total_row}"]
        cell.value = formula
        cell.font = TOTAL_FONT
        cell.number_format = col_fmt[col_letter]


# ── Sheet: Bucket Stats ──────────────────────────────────────────────────────
# Same shape as Position Type Stats, grouped by Trade Log's Return Bucket (O)
# column instead of Position (C). "Untagged" catches trades with no
# return_bucket at all -- every trade entered before the return-bucketed
# exit schedule shipped (2026-09-26); real bucket data (and therefore
# meaningful bucket-wise stats) only exists for trades entered from then on.

BK_HEADERS = ["Bucket", "Trades", "Wins", "Losses", "Win Rate", "Gross P&L",
              "Net P&L", "Avg Net P&L / Trade", "Best Trade", "Worst Trade",
              "Profit Factor"]
BK_BUCKETS = ["5-10", "10-15", "15-20", "Untagged"]


def build_bucket_stats(ws) -> None:
    set_title_subtitle(
        ws, "Bucket Stats",
        "Performance by entry-signal return bucket (5-10% / 10-15% / 15-20%), "
        "computed straight from Trade Log. Untagged = entered before the "
        "return-bucketed exit schedule shipped (2026-09-26).",
        len(BK_HEADERS),
    )

    for col, header in enumerate(BK_HEADERS, start=1):
        ws.cell(row=3, column=col, value=header)
    style_header_row(ws, 3, len(BK_HEADERS))

    widths = [12, 10, 9, 10, 11, 14, 14, 16, 14, 14, 14]
    for col, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(col)].width = w

    tl = "'Trade Log'"
    col_fmt = {"B": NUM, "C": NUM, "D": NUM, "E": PCT, "F": INR, "G": INR,
               "H": INR, "I": INR, "J": INR, "K": XMULT}

    def _row_formulas(r: int) -> dict:
        # $A{r} -- every row (including "Untagged") matches a real, always-
        # populated literal in Trade Log's O column. Deliberately NOT a
        # blank/"" criteria: COUNTIFS/SUMIFS matching blank against another
        # FORMULA-computed range at Trade Log's 500-row scale is unreliable
        # in LibreOffice (confirmed via direct repro -- silently undercounts
        # to 0 even when real matches exist), which is why untagged trades
        # are tagged with the literal "Untagged" in Trade Log rather than
        # left blank -- see _position_to_trade_row's own note.
        criteria = f"$A{r}"
        return {
            "B": f"=COUNTIF({tl}!$O$4:$O$503,{criteria})",
            "C": f'=COUNTIFS({tl}!$O$4:$O$503,{criteria},{tl}!$L$4:$L$503,">0")',
            "D": f'=COUNTIFS({tl}!$O$4:$O$503,{criteria},{tl}!$L$4:$L$503,"<0")',
            "E": f'=IFERROR($C{r}/$B{r},"")',
            "F": f"=SUMIF({tl}!$O$4:$O$503,{criteria},{tl}!$I$4:$I$503)",
            "G": f"=SUMIF({tl}!$O$4:$O$503,{criteria},{tl}!$L$4:$L$503)",
            "H": f'=IFERROR($G{r}/$B{r},"")',
            "I": f'=IFERROR(_xlfn.MAXIFS({tl}!$L$4:$L$503,{tl}!$O$4:$O$503,{criteria}),"")',
            "J": f'=IFERROR(_xlfn.MINIFS({tl}!$L$4:$L$503,{tl}!$O$4:$O$503,{criteria}),"")',
            "K": (f'=IFERROR(SUMIFS({tl}!$L$4:$L$503,{tl}!$O$4:$O$503,{criteria},{tl}!$L$4:$L$503,">0")/'
                  f'ABS(SUMIFS({tl}!$L$4:$L$503,{tl}!$O$4:$O$503,{criteria},{tl}!$L$4:$L$503,"<0")),"")'),
        }

    for offset, bucket in enumerate(BK_BUCKETS):
        r = 4 + offset
        cell_a = ws[f"A{r}"]; cell_a.value = bucket; style_formula(cell_a)
        for col_letter, formula in _row_formulas(r).items():
            cell = ws[f"{col_letter}{r}"]
            cell.value = formula
            style_formula(cell, col_fmt[col_letter])

    # TOTAL row.
    total_row = 4 + len(BK_BUCKETS)
    first_data_row = 4
    last_data_row = total_row - 1
    ws[f"A{total_row}"] = "TOTAL"
    ws[f"A{total_row}"].font = TOTAL_FONT

    total_formulas = {
        "B": f"=SUM(B{first_data_row}:B{last_data_row})",
        "C": f"=SUM(C{first_data_row}:C{last_data_row})",
        "D": f"=SUM(D{first_data_row}:D{last_data_row})",
        "E": f'=IFERROR(C{total_row}/B{total_row},"")',
        "F": f"=SUM(F{first_data_row}:F{last_data_row})",
        "G": f"=SUM(G{first_data_row}:G{last_data_row})",
        "H": f'=IFERROR(G{total_row}/B{total_row},"")',
        "K": (f'=IFERROR(SUMIF({tl}!$L$4:$L$503,">0")/'
              f'ABS(SUMIF({tl}!$L$4:$L$503,"<0")),"")'),
    }
    for col_letter, formula in total_formulas.items():
        cell = ws[f"{col_letter}{total_row}"]
        cell.value = formula
        cell.font = TOTAL_FONT
        cell.number_format = col_fmt[col_letter]


# ── Sheet 5: Company Stats ───────────────────────────────────────────────────
# Unlike Position Type Stats' fixed LONG/SHORT rows, the set of symbols isn't
# knowable in advance -- and UNIQUE()/FILTER() aren't allowed (LibreOffice/
# Google Sheets compatibility). So the distinct symbol list is computed here
# in Python from the same `trades` already loaded for Trade Log, and written
# as literal row labels -- same pattern as Position Type Stats' "LONG"/
# "SHORT", just a dynamically-sized list instead of a fixed pair. Purely a
# derived report (no manual-entry rows below the real data, unlike Trade
# Log): a symbol only belongs here once it's actually appeared in a trade.

CS_HEADERS = ["Symbol", "Trades", "Wins", "Losses", "Win Rate", "Gross P&L",
              "Net P&L", "Avg Net P&L / Trade", "Best Trade", "Worst Trade"]


def build_company_stats(ws, trades: list[dict]) -> None:
    set_title_subtitle(
        ws, "Company Stats",
        "Per-symbol performance across every logged trade, computed straight from Trade Log.",
        len(CS_HEADERS),
    )

    for col, header in enumerate(CS_HEADERS, start=1):
        ws.cell(row=3, column=col, value=header)
    style_header_row(ws, 3, len(CS_HEADERS))

    widths = [14, 10, 9, 10, 11, 14, 14, 16, 14, 14]
    for col, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(col)].width = w

    tl = "'Trade Log'"
    col_fmt = {"B": NUM, "C": NUM, "D": NUM, "E": PCT, "F": INR, "G": INR,
               "H": INR, "I": INR, "J": INR}

    symbols = sorted({t["symbol"] for t in (trades or [])})
    first_row = 4

    for offset, symbol in enumerate(symbols):
        r = first_row + offset
        cell_a = ws[f"A{r}"]; cell_a.value = symbol; style_formula(cell_a)

        formulas = {
            "B": f"=COUNTIF({tl}!$B$4:$B$503,$A{r})",
            "C": f'=COUNTIFS({tl}!$B$4:$B$503,$A{r},{tl}!$L$4:$L$503,">0")',
            "D": f'=COUNTIFS({tl}!$B$4:$B$503,$A{r},{tl}!$L$4:$L$503,"<0")',
            "E": f'=IFERROR($C{r}/$B{r},"")',
            "F": f"=SUMIF({tl}!$B$4:$B$503,$A{r},{tl}!$I$4:$I$503)",
            "G": f"=SUMIF({tl}!$B$4:$B$503,$A{r},{tl}!$L$4:$L$503)",
            "H": f'=IFERROR($G{r}/$B{r},"")',
            "I": f'=IFERROR(_xlfn.MAXIFS({tl}!$L$4:$L$503,{tl}!$B$4:$B$503,$A{r}),"")',
            "J": f'=IFERROR(_xlfn.MINIFS({tl}!$L$4:$L$503,{tl}!$B$4:$B$503,$A{r}),"")',
        }
        for col_letter, formula in formulas.items():
            cell = ws[f"{col_letter}{r}"]
            cell.value = formula
            style_formula(cell, col_fmt[col_letter])

    if symbols:
        total_row = first_row + len(symbols)
        last_data_row = total_row - 1
        ws[f"A{total_row}"] = "TOTAL"
        ws[f"A{total_row}"].font = TOTAL_FONT

        total_formulas = {
            "B": f"=SUM(B{first_row}:B{last_data_row})",
            "C": f"=SUM(C{first_row}:C{last_data_row})",
            "D": f"=SUM(D{first_row}:D{last_data_row})",
            "E": f'=IFERROR(C{total_row}/B{total_row},"")',
            "F": f"=SUM(F{first_row}:F{last_data_row})",
            "G": f"=SUM(G{first_row}:G{last_data_row})",
            "H": f'=IFERROR(G{total_row}/B{total_row},"")',
        }
        for col_letter, formula in total_formulas.items():
            cell = ws[f"{col_letter}{total_row}"]
            cell.value = formula
            cell.font = TOTAL_FONT
            cell.number_format = col_fmt[col_letter]

    ws.freeze_panes = "A4"


# ── Sheet 6: Charges ─────────────────────────────────────────────────────────
# Per-trade breakdown of the real API charge categories (Brokerage/STT/
# Exchange/SEBI/Stamp/GST, summed across entry+exit legs) plus DP/Pledge/MTF
# Interest -- see dhan.charges.position_charge_breakdown(). Same "no
# manual-entry padding" convention as Company Stats: purely a derived report,
# one row per trade actually in Trade Log, in the same exit-date order.
# Category values are synced Python numbers (real API data, like Trade Log's
# own synced columns), NOT formulas -- there's nothing else in the workbook
# to derive them FROM. Only Total Charges is a formula, so a manual tweak to
# one category still recalculates the row's total live.

CH_HEADERS = ["Trade ID", "Symbol", "Position", "Exit Date", "Brokerage", "STT",
              "Exchange", "SEBI", "Stamp", "GST", "DP", "Pledge/Unpledge",
              "MTF Interest", "Total Charges"]
_CH_CATEGORY_KEYS = ["Brokerage", "STT", "Exchange", "SEBI", "Stamp", "GST"]


def build_charges(ws, trades: list[dict] | None = None) -> None:
    set_title_subtitle(
        ws, "Charges",
        "Real per-trade charge breakdown from Dhan's trade-book, by category -- synced from Trade Log.",
        len(CH_HEADERS),
    )

    for col, header in enumerate(CH_HEADERS, start=1):
        ws.cell(row=3, column=col, value=header)
    style_header_row(ws, 3, len(CH_HEADERS))

    widths = [26, 14, 10, 12, 12, 10, 12, 10, 10, 10, 10, 15, 13, 14]
    for col, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(col)].width = w

    manual_fmts = {"D": DATE}
    charge_fmts = {col_letter: INR for col_letter in "EFGHIJKLM"}

    first_row = 4
    trades = trades or []

    for offset, trade in enumerate(trades):
        r = first_row + offset
        charges = trade.get("charges") or {}

        row_vals = {
            "A": trade["id"], "B": trade["symbol"], "C": trade["position"],
            "D": trade["exit_date"] or "",
            "E": charges.get("Brokerage", 0), "F": charges.get("STT", 0),
            "G": charges.get("Exchange", 0), "H": charges.get("SEBI", 0),
            "I": charges.get("Stamp", 0), "J": charges.get("GST", 0),
            "K": charges.get("dp_charge", 0), "L": charges.get("pledge_charge", 0),
            "M": charges.get("mtf_interest", 0),
        }
        for col_letter, val in row_vals.items():
            cell = ws[f"{col_letter}{r}"]
            cell.value = val
            style_input(cell, manual_fmts.get(col_letter) or charge_fmts.get(col_letter))

        cell_n = ws[f"N{r}"]
        cell_n.value = f"=SUM(E{r}:M{r})"
        style_formula(cell_n, INR)

    if trades:
        total_row = first_row + len(trades)
        last_data_row = total_row - 1
        ws[f"A{total_row}"] = "TOTAL"
        ws[f"A{total_row}"].font = TOTAL_FONT

        for col_letter in "EFGHIJKLMN":
            cell = ws[f"{col_letter}{total_row}"]
            cell.value = f"=SUM({col_letter}{first_row}:{col_letter}{last_data_row})"
            cell.font = TOTAL_FONT
            cell.number_format = INR

    ws.freeze_panes = "C4"
    if trades:
        ws.auto_filter.ref = f"A3:N{first_row + len(trades) - 1}"


# ── Sheet: Monthly & Weekly PnL ───────────────────────────────────────────────
# Two tables, one sheet: month-wise then week-wise P&L, both attributed by
# Exit Date like Day Wise PnL. The distinct month/week list isn't knowable in
# advance (same reasoning as Company Stats' symbol list -- no UNIQUE()/
# FILTER() for LibreOffice/Google Sheets compatibility), so it's computed
# here in Python from the same `trades` list and written as literal date
# bounds baked into each row's own SUMIFS/COUNTIFS -- there's no dependency
# on the label cell itself, unlike Position/Bucket Stats' $A{r}-driven
# formulas, since a month/week is a date RANGE, not a single matchable value.

MW_HEADERS = ["Period", "Trades", "Wins", "Losses", "Win Rate", "Gross P&L",
              "Net P&L", "Avg Net P&L / Trade", "Best Trade", "Worst Trade",
              "Profit Factor"]


def _month_bounds(y: int, m: int) -> tuple[str, str]:
    start = f"DATE({y},{m},1)"
    end = f"DATE({y+1},1,1)" if m == 12 else f"DATE({y},{m+1},1)"
    return start, end


def _week_bounds(monday: date) -> tuple[str, str]:
    end = monday + timedelta(days=7)
    start = f"DATE({monday.year},{monday.month},{monday.day})"
    return start, f"DATE({end.year},{end.month},{end.day})"


def _period_row_formulas(r: int, tl: str, start_expr: str, end_expr: str) -> dict:
    date_cond = f'{tl}!$G$4:$G$503,">="&{start_expr},{tl}!$G$4:$G$503,"<"&{end_expr}'
    return {
        "B": f"=COUNTIFS({date_cond})",
        "C": f'=COUNTIFS({date_cond},{tl}!$L$4:$L$503,">0")',
        "D": f'=COUNTIFS({date_cond},{tl}!$L$4:$L$503,"<0")',
        "E": f'=IFERROR($C{r}/$B{r},"")',
        "F": f"=SUMIFS({tl}!$I$4:$I$503,{date_cond})",
        "G": f"=SUMIFS({tl}!$L$4:$L$503,{date_cond})",
        "H": f'=IFERROR($G{r}/$B{r},"")',
        "I": f'=IFERROR(_xlfn.MAXIFS({tl}!$L$4:$L$503,{date_cond}),"")',
        "J": f'=IFERROR(_xlfn.MINIFS({tl}!$L$4:$L$503,{date_cond}),"")',
        "K": (f'=IFERROR(SUMIFS({tl}!$L$4:$L$503,{date_cond},{tl}!$L$4:$L$503,">0")/'
              f'ABS(SUMIFS({tl}!$L$4:$L$503,{date_cond},{tl}!$L$4:$L$503,"<0")),"")'),
    }


def _write_period_table(ws, trades: list[dict], first_row: int, periods: list,
                        label_fn, bounds_fn, col_fmt: dict, tl: str) -> int:
    """Writes one table (month or week rows) starting at first_row. periods
    is the sorted list of distinct period keys (month (y,m) tuples, or week
    Monday dates) already computed by the caller. Returns the row right
    after the TOTAL row, so the caller can place whatever comes next."""
    for offset, period in enumerate(periods):
        r = first_row + offset
        label = label_fn(period)
        cell_a = ws[f"A{r}"]; cell_a.value = label
        style_formula(cell_a, DATE if isinstance(label, date) else None)
        start_expr, end_expr = bounds_fn(period)
        for col_letter, formula in _period_row_formulas(r, tl, start_expr, end_expr).items():
            cell = ws[f"{col_letter}{r}"]
            cell.value = formula
            style_formula(cell, col_fmt[col_letter])

    if not periods:
        return first_row

    total_row = first_row + len(periods)
    last_data_row = total_row - 1
    ws[f"A{total_row}"] = "TOTAL"
    ws[f"A{total_row}"].font = TOTAL_FONT
    total_formulas = {
        "B": f"=SUM(B{first_row}:B{last_data_row})",
        "C": f"=SUM(C{first_row}:C{last_data_row})",
        "D": f"=SUM(D{first_row}:D{last_data_row})",
        "E": f'=IFERROR(C{total_row}/B{total_row},"")',
        "F": f"=SUM(F{first_row}:F{last_data_row})",
        "G": f"=SUM(G{first_row}:G{last_data_row})",
        "H": f'=IFERROR(G{total_row}/B{total_row},"")',
        "K": (f'=IFERROR(SUMIF({tl}!$L$4:$L$503,">0")/'
              f'ABS(SUMIF({tl}!$L$4:$L$503,"<0")),"")'),
    }
    for col_letter, formula in total_formulas.items():
        cell = ws[f"{col_letter}{total_row}"]
        cell.value = formula
        cell.font = TOTAL_FONT
        cell.number_format = col_fmt[col_letter]

    return total_row + 1


def build_monthly_weekly(ws, trades: list[dict] | None = None) -> None:
    trades = trades or []
    set_title_subtitle(
        ws, "Monthly & Weekly PnL",
        "Month-wise and week-wise performance, attributed by Exit Date -- computed straight from Trade Log.",
        len(MW_HEADERS),
    )

    widths = [18, 10, 9, 10, 11, 14, 14, 16, 14, 14, 14]
    for col, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(col)].width = w

    tl = "'Trade Log'"
    col_fmt = {"B": NUM, "C": NUM, "D": NUM, "E": PCT, "F": INR, "G": INR,
               "H": INR, "I": INR, "J": INR, "K": XMULT}

    closed = [t for t in trades if t.get("exit_date")]
    months = sorted({(t["exit_date"].year, t["exit_date"].month) for t in closed})
    weeks  = sorted({t["exit_date"] - timedelta(days=t["exit_date"].weekday()) for t in closed})

    row = 3
    ws[f"A{row}"] = "MONTHLY PnL"
    ws[f"A{row}"].font = LABEL_FONT
    row += 1
    for col, header in enumerate(MW_HEADERS, start=1):
        ws.cell(row=row, column=col, value=header)
    style_header_row(ws, row, len(MW_HEADERS))
    row += 1

    row = _write_period_table(
        ws, trades, row, months,
        label_fn=lambda ym: f"{ym[0]:04d}-{ym[1]:02d}",
        bounds_fn=lambda ym: _month_bounds(*ym),
        col_fmt=col_fmt, tl=tl,
    )

    row += 1  # spacer
    ws[f"A{row}"] = "WEEKLY PnL"
    ws[f"A{row}"].font = LABEL_FONT
    row += 1
    for col, header in enumerate(MW_HEADERS, start=1):
        ws.cell(row=row, column=col, value=header)
    style_header_row(ws, row, len(MW_HEADERS))
    row += 1

    _write_period_table(
        ws, trades, row, weeks,
        label_fn=lambda monday: monday,
        bounds_fn=_week_bounds,
        col_fmt=col_fmt, tl=tl,
    )

    ws.freeze_panes = "A4"


# ── Sheet 1: Total PnL (dashboard) ──────────────────────────────────────────
# Layout map (all formulas identical in substance to the original plain-list
# version -- only cell *addresses* moved, to fit the card/table grid):
#   Row 1     Title (A1:P1)
#   Row 2     Subtitle (A2:P2)
#   Row 3     Base Capital -- manual input (A3 label, B3 value)
#   Row 4     Band: STRATEGY SNAPSHOT
#   Row 5     KPI card labels
#   Rows 6-8  KPI card numbers (merged 3 rows tall) -- Net P&L ₹ / Net P&L % /
#             Win Rate / Profit Factor, in that column order
#   Row 9     spacer
#   Row 10    Band: DETAILED STATS
#   Row 11    secondary table header (Metric / Value)
#   Rows12-20 secondary stats (Total Trades ... Gross P&L %)
#
# The 3 charts (equity curve, win/loss pie, gross-vs-net bar) that used to
# sit below this were removed 2026-09-28 at the user's request -- see git
# history if they're ever wanted back.

BC_ROW          = 3
BAND_SNAPSHOT   = 4
CARD_LABEL_ROW  = 5
CARD_NUM_TOP    = 6
CARD_NUM_BOTTOM = 8
BAND_STATS      = 10
STATS_HDR_ROW   = 11
STATS_FIRST_ROW = 12
STATS_LAST_ROW  = 20
DASHBOARD_COLS  = 16  # A..P

BAND_FONT = Font(name=FONT_NAME, bold=True, size=11, color="FF1F3864")
BAND_FILL = PatternFill(start_color="FFD9E2F3", end_color="FFD9E2F3", fill_type="solid")

CARD_LABEL_FONT = Font(name=FONT_NAME, size=9, color="FFFFFFFF")
CARD_NUM_FONT   = Font(name=FONT_NAME, size=26, bold=True, color="FFFFFFFF")

GREEN_FILL = PatternFill(start_color="FF2E7D32", end_color="FF2E7D32", fill_type="solid")
RED_FILL   = PatternFill(start_color="FFC62828", end_color="FFC62828", fill_type="solid")

CARD_NAVY   = "FF1F3864"   # Net P&L (₹) base, before conditional flip
CARD_TEAL   = "FF1B7A75"   # Net P&L (%) base, before conditional flip
CARD_AMBER  = "FFC77D02"   # Win Rate, static
CARD_PURPLE = "FF5B2C83"   # Profit Factor, static

# (col_start, col_end, label, formula, number_format, base_fill_hex, conditional)
CARD_DEFS = [
    (2,  4,  "NET P&L (₹)",  "=SUM('Trade Log'!$L$4:$L$503)", INR,   CARD_NAVY,  True),
    (6,  8,  "NET P&L (%)",  '=IF($B$3=0,"",$B$6/$B$3)',      PCT,   CARD_TEAL,  True),
    (10, 12, "WIN RATE",     '=IFERROR($C$13/$C$12,"")',      PCT,  CARD_AMBER, False),
    (14, 16, "PROFIT FACTOR",
     '=IFERROR(SUMIF(\'Trade Log\'!$L$4:$L$503,">0")/'
     'ABS(SUMIF(\'Trade Log\'!$L$4:$L$503,"<0")),"")', XMULT, CARD_PURPLE, False),
]

# (row, label, formula, number_format, databar)
STATS_ROWS = [
    (12, "Total Trades",      "=COUNT('Trade Log'!$L$4:$L$503)", NUM,  False),
    (13, "Winning Trades",    '=COUNTIF(\'Trade Log\'!$L$4:$L$503,">0")', NUM, False),
    (14, "Losing Trades",     '=COUNTIF(\'Trade Log\'!$L$4:$L$503,"<0")', NUM, False),
    (15, "Average Win (₹)",  '=IFERROR(AVERAGEIF(\'Trade Log\'!$L$4:$L$503,">0"),"")', INR, True),
    (16, "Average Loss (₹)", '=IFERROR(AVERAGEIF(\'Trade Log\'!$L$4:$L$503,"<0"),"")', INR, True),
    (17, "Best Trade (₹)",   "=IFERROR(MAX('Trade Log'!$L$4:$L$503),\"\")", INR, True),
    (18, "Worst Trade (₹)",  "=IFERROR(MIN('Trade Log'!$L$4:$L$503),\"\")", INR, True),
    (19, "Gross P&L (₹)",    "=SUM('Trade Log'!$I$4:$I$503)", INR, False),
    (20, "Gross P&L (%)",    '=IF($B$3=0,"",$C$19/$B$3)', PCT, False),
]


def _band(ws, row: int, text: str) -> None:
    last_col = get_column_letter(DASHBOARD_COLS)
    ws.merge_cells(f"A{row}:{last_col}{row}")
    cell = ws[f"A{row}"]
    cell.value = text
    cell.font = BAND_FONT
    cell.fill = BAND_FILL
    cell.alignment = Alignment(horizontal="left", vertical="center", indent=1)
    ws.row_dimensions[row].height = 18
    for col in range(1, DASHBOARD_COLS + 1):
        ws.cell(row=row, column=col).fill = BAND_FILL


def _kpi_card(ws, col_start: int, col_end: int, label: str, formula: str,
              number_format: str, fill_hex: str, conditional: bool) -> str:
    """Builds one KPI card (label strip + big merged number block). Returns
    the number cell's address (e.g. 'B6') so callers can cross-reference it
    and, for conditional cards, anchor the green/red flip rule on it."""
    first_col_letter = get_column_letter(col_start)
    last_col_letter  = get_column_letter(col_end)
    label_range = f"{first_col_letter}{CARD_LABEL_ROW}:{last_col_letter}{CARD_LABEL_ROW}"
    num_range   = f"{first_col_letter}{CARD_NUM_TOP}:{last_col_letter}{CARD_NUM_BOTTOM}"

    ws.merge_cells(label_range)
    ws.merge_cells(num_range)

    fill = PatternFill(start_color=fill_hex, end_color=fill_hex, fill_type="solid")
    for row in (CARD_LABEL_ROW, *range(CARD_NUM_TOP, CARD_NUM_BOTTOM + 1)):
        for col in range(col_start, col_end + 1):
            ws.cell(row=row, column=col).fill = fill

    label_cell = ws[f"{first_col_letter}{CARD_LABEL_ROW}"]
    label_cell.value = label
    label_cell.font = CARD_LABEL_FONT
    label_cell.alignment = CENTER

    num_cell = ws[f"{first_col_letter}{CARD_NUM_TOP}"]
    num_cell.value = formula
    num_cell.font = CARD_NUM_FONT
    num_cell.alignment = CENTER
    num_cell.number_format = number_format

    if conditional:
        anchor = f"${first_col_letter}${CARD_NUM_TOP}"
        for rng in (label_range, num_range):
            ws.conditional_formatting.add(rng, FormulaRule(formula=[f"{anchor}>=0"], fill=GREEN_FILL))
            ws.conditional_formatting.add(rng, FormulaRule(formula=[f"{anchor}<0"], fill=RED_FILL))

    return num_cell.coordinate


def build_total_pnl_dashboard(ws) -> None:
    set_title_subtitle(
        ws, "Total PnL",
        "Overall strategy performance -- set Base Capital below; everything "
        "else calculates automatically from Trade Log.",
        DASHBOARD_COLS,
    )

    # Column widths -- wide "card area" columns, narrow gap/margin columns.
    ws.column_dimensions["A"].width = 4
    for group_start in (2, 6, 10, 14):  # B, F, J, N
        for offset in range(3):
            ws.column_dimensions[get_column_letter(group_start + offset)].width = 15
    for gap_col in (5, 9, 13):  # E, I, M
        ws.column_dimensions[get_column_letter(gap_col)].width = 8

    # Base Capital -- manual input, unchanged from the original.
    ws["A3"].value = "Base Capital (₹)"
    ws["A3"].font = LABEL_FONT
    style_input(ws["B3"], INR)
    ws["B3"].value = 1_500_000

    # Section 1: KPI cards.
    _band(ws, BAND_SNAPSHOT, "STRATEGY SNAPSHOT")
    for col_start, col_end, label, formula, fmt, fill_hex, conditional in CARD_DEFS:
        _kpi_card(ws, col_start, col_end, label, formula, fmt, fill_hex, conditional)

    # Section 2: secondary stats table (plain, compact, 2 columns).
    _band(ws, BAND_STATS, "DETAILED STATS")
    ws["B11"].value = "Metric"
    ws["C11"].value = "Value"
    for addr in ("B11", "C11"):
        ws[addr].font = HEADER_FONT
        ws[addr].fill = HEADER_FILL
        ws[addr].alignment = CENTER

    for row, label, formula, fmt, databar in STATS_ROWS:
        cell_b = ws[f"B{row}"]
        cell_b.value = label
        cell_b.font = LABEL_FONT

        cell_c = ws[f"C{row}"]
        cell_c.value = formula
        style_formula(cell_c, fmt)

    # Data bars on the four currency stats (Average Win/Loss, Best/Worst Trade),
    # scaled together so relative magnitude reads at a glance.
    ws.conditional_formatting.add(
        "C15:C18",
        DataBarRule(start_type="min", start_value=None, end_type="max", end_value=None,
                    color="638EC6"),
    )

    # Keep title/subtitle/Base Capital/band/cards visible while scrolling.
    ws.freeze_panes = "A9"


# ── Build ─────────────────────────────────────────────────────────────────────

def main() -> None:
    trades = load_trades_from_positions()

    wb = Workbook()

    ws_total = wb.active
    ws_total.title = "Total PnL"
    ws_log      = wb.create_sheet("Trade Log")
    ws_day      = wb.create_sheet("Day Wise PnL")
    ws_stats    = wb.create_sheet("Position Type Stats")
    ws_bucket   = wb.create_sheet("Bucket Stats")
    ws_company  = wb.create_sheet("Company Stats")
    ws_charges  = wb.create_sheet("Charges")
    ws_mw       = wb.create_sheet("Monthly & Weekly PnL")

    build_trade_log(ws_log, trades)
    build_day_wise(ws_day)
    build_total_pnl_dashboard(ws_total)
    build_position_stats(ws_stats)
    build_bucket_stats(ws_bucket)
    build_company_stats(ws_company, trades)
    build_charges(ws_charges, trades)
    build_monthly_weekly(ws_mw, trades)

    for ws in wb.worksheets:
        ws.sheet_view.showGridLines = False

    wb.save(OUT_PATH)
    print(f"Wrote {OUT_PATH} ({len(trades)} synced trade(s) from Dhan positions, "
          f"from {PNL_START_DATE} onward)" if trades else f"Wrote {OUT_PATH} (no synced trades yet -- examples shown)")


if __name__ == "__main__":
    main()
