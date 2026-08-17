// ============================================================================
// Dhan trading dashboard — Google Apps Script, bound to the target Sheet.
// Paste this WHOLE file into: Sheet -> Extensions -> Apps Script -> Code.gs
//
// Two entry points you actually run:
//   1. doPost(e)         -- the webhook. Deploy -> New deployment -> Web app.
//                           Called by sheets/push_to_sheets.py, never by hand.
//   2. setupDashboard()  -- ONE-TIME setup. Run manually from this editor
//                           (Run menu, or the play button) AFTER doPost is
//                           deployed and at least one row has been pushed to
//                           Trade Log (so formatting has something to apply
//                           to). Safe to re-run -- every builder function is
//                           idempotent (clears + rebuilds its own tab/ranges).
//
// A third function, rebuildTodayCalendar(), regenerates the Today tab's
// calendar heatmap for the CURRENT month. setupDashboard() calls it once;
// call installMonthlyCalendarTrigger() (also manual, one-time) to have it
// re-run automatically on the 1st of every month -- see that function's
// comment for why this can't just be a plain formula.
// ============================================================================

var SHARED_SECRET = "REPLACE_ME_WITH_A_RANDOM_STRING";

var SHEET_NAMES = {
  TRADE_LOG:   "Trade Log",
  DAILY:       "Daily Summary",
  EXIT_REASON: "Exit-Reason Breakdown",
  SYMBOL:      "Symbol Stats",
  TODAY:       "Today",
};

// Bound on how many trade rows every formula below plans for. 3000 closed
// trades is years of headroom at this strategy's pace -- raise it later by
// re-running setupDashboard() if you ever actually approach it.
var MAX_ROW = 3000;

var TRADE_LOG_HEADERS = [
  "Date", "Symbol", "Direction", "Entry Price", "Qty",
  "Exit Price", "Exit Reason", "Product", "Return %", "P&L", "Capital Deployed",
  "Win (1/0)", "Rolling 20 Win Rate",
];

// Order matters here -- it's the fixed row order used on the Exit-Reason
// Breakdown tab and the fixed slice/bar order for its charts.
var REASON_ORDER = [
  "Target hit", "9:25 profit exit", "9:25 no-data partial",
  "11:59 force exit", "2:39 square-off", "Stop-loss hit", "Unrecognized — review",
];

var REASON_EMOJI = {
  "Target hit":            "🎯",
  "Stop-loss hit":         "🛑",
  "11:59 force exit":      "⏰",
  "9:25 profit exit":      "💰",
  "2:39 square-off":       "🔁",
  "9:25 no-data partial":  "⚠️",
  "Unrecognized — review": "❓",
};

var REASON_COLORS = {
  "Target hit":            "#34a853",
  "9:25 profit exit":      "#4285f4",
  "9:25 no-data partial":  "#fbbc04",
  "11:59 force exit":      "#f9a825",
  "2:39 square-off":       "#9c27b0",
  "Stop-loss hit":         "#ea4335",
  "Unrecognized — review": "#757575",
};

var COLOR = {
  darkBg:    "#0d0d0d",
  panelBg:   "#1a1a1a",
  white:     "#ffffff",
  green:     "#34a853",
  red:       "#ea4335",
  amber:     "#fbbc04",
  gold:      "#c9a227",
  gray:      "#9aa0a6",
};

function reasonLabel_(reason) {
  return (REASON_EMOJI[reason] || "") + " " + reason;
}


// ── Webhook (unchanged from the first pass -- this is what's already live) ───

function doPost(e) {
  var result;
  try {
    var body = JSON.parse(e.postData.contents);

    if (body.secret !== SHARED_SECRET) {
      return _jsonResponse({ status: "error", message: "invalid secret" });
    }

    if (body.action === "append_trades") {
      result = _appendTrades(body.rows);
    } else {
      result = { status: "error", message: "unknown action: " + body.action };
    }
  } catch (err) {
    result = { status: "error", message: String(err) };
  }
  return _jsonResponse(result);
}

function doGet(e) {
  return _jsonResponse({ status: "ok", message: "Trade Log webhook is live" });
}

function _jsonResponse(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj))
      .setMimeType(ContentService.MimeType.JSON);
}

function _appendTrades(rows) {
  if (!rows || !rows.length) {
    return { status: "ok", appended: 0 };
  }
  // rows arrive as the 11 Trade Log data columns (Date..Capital Deployed) --
  // pad with the 2 helper columns (Win flag, Rolling 20 Win Rate) so every
  // appended row keeps the sheet's real column count (13) intact; the helper
  // formulas below fill themselves in on top of these placeholder blanks.
  var sheet    = _getOrCreateTradeLogSheet();
  var startRow = sheet.getLastRow() + 1;
  var padded   = rows.map(function (r) { return r.concat(["", ""]); });
  sheet.getRange(startRow, 1, padded.length, TRADE_LOG_HEADERS.length).setValues(padded);
  _fillTradeLogHelperFormulas_(sheet, startRow, padded.length);
  return { status: "ok", appended: rows.length };
}

function _getOrCreateTradeLogSheet() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEET_NAMES.TRADE_LOG);

  if (!sheet) {
    sheet = ss.insertSheet(SHEET_NAMES.TRADE_LOG);
    var headerRange = sheet.getRange(1, 1, 1, TRADE_LOG_HEADERS.length);
    headerRange.setValues([TRADE_LOG_HEADERS]);
    headerRange.setBackground("#1a1a1a").setFontColor("#ffffff").setFontWeight("bold");
    sheet.setFrozenRows(1);
  }
  return sheet;
}

// L (Win 1/0) and M (Rolling 20 Win Rate) for the newly-appended row range --
// set once per push rather than pre-filled across all 3000 rows, so a fresh
// Trade Log sheet doesn't show thousands of "" placeholder formula results
// before setupDashboard() has run. setupDashboard() re-fills the FULL bound
// range afterward (see buildTradeLog_), which is harmless/idempotent here.
function _fillTradeLogHelperFormulas_(sheet, startRow, numRows) {
  var winRange = sheet.getRange(startRow, 12, numRows, 1); // L
  winRange.setFormulaR1C1('=IF(RC[-3]="","",IF(RC[-3]>0,1,0))');
  var rollRange = sheet.getRange(startRow, 13, numRows, 1); // M
  rollRange.setFormulaR1C1(
    '=IF(RC[-12]="","",IFERROR(AVERAGE(OFFSET(RC[-1],-MIN(19,ROW()-2),0,MIN(20,ROW()-1),1)),""))'
  );
}


// ============================================================================
// ONE-TIME SETUP
// ============================================================================

function setupDashboard() {
  buildTradeLog_();
  buildDailySummary_();
  buildExitReasonBreakdown_();
  buildSymbolStats_();
  buildToday_();
  SpreadsheetApp.getActiveSpreadsheet().toast("Dashboard setup complete.", "Dhan Dashboard", 8);
}


// ── Trade Log: helper columns + conditional formatting ───────────────────────

function buildTradeLog_() {
  var sheet = _getOrCreateTradeLogSheet();

  // Re-assert headers (covers a Trade Log created before the helper columns existed).
  var headerRange = sheet.getRange(1, 1, 1, TRADE_LOG_HEADERS.length);
  headerRange.setValues([TRADE_LOG_HEADERS]);
  headerRange.setBackground("#1a1a1a").setFontColor("#ffffff").setFontWeight("bold");
  sheet.setFrozenRows(1);

  _fillTradeLogHelperFormulas_(sheet, 2, MAX_ROW - 1);

  var rules = [];

  // 3-color scale on Return % (I) and P&L (J).
  [9, 10].forEach(function (col) {
    var range = sheet.getRange(2, col, MAX_ROW - 1, 1);
    rules.push(
      SpreadsheetApp.newConditionalFormatRule()
        .setGradientMinpointWithValue(COLOR.red, SpreadsheetApp.InterpolationType.MIN, "")
        .setGradientMidpointWithValue("#ffffff", SpreadsheetApp.InterpolationType.PERCENTILE, "50")
        .setGradientMaxpointWithValue(COLOR.green, SpreadsheetApp.InterpolationType.MAX, "")
        .setRanges([range])
        .build()
    );
  });

  // Per-Exit-Reason background tag color on the Exit Reason column (G).
  var reasonRange = sheet.getRange(2, 7, MAX_ROW - 1, 1);
  REASON_ORDER.forEach(function (reason) {
    rules.push(
      SpreadsheetApp.newConditionalFormatRule()
        .whenTextContains(reason)
        .setBackground(REASON_COLORS[reason])
        .setFontColor("#ffffff")
        .setRanges([reasonRange])
        .build()
    );
  });

  sheet.setConditionalFormatRules(rules);

  // Helper columns visually distinguished as "computed, don't touch."
  sheet.getRange(1, 12, 1, 2).setBackground("#3c3c3c").setFontColor("#bbbbbb");
  sheet.setColumnWidths(1, 11, 110);
  sheet.setColumnWidths(12, 2, 130);
}


// ── Daily Summary: formulas off Trade Log + 3 native charts ──────────────────

function buildDailySummary_() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEET_NAMES.DAILY) || ss.insertSheet(SHEET_NAMES.DAILY);
  sheet.clear();
  sheet.getCharts().forEach(function (c) { sheet.removeChart(c); });

  var headers = [
    "Date", "# Trades", "# Wins", "# Losses", "Win Rate", "# Target Hits",
    "# Stop-Loss Hits", "Gross P&L", "Capital Deployed", "Return on Capital %",
    "Cumulative P&L", "Positive P&L (chart helper)", "Negative P&L (chart helper)",
  ];
  var headerRange = sheet.getRange(1, 1, 1, headers.length);
  headerRange.setValues([headers]);
  headerRange.setBackground("#1a1a1a").setFontColor("#ffffff").setFontWeight("bold");
  sheet.setFrozenRows(1);

  var tl = "'" + SHEET_NAMES.TRADE_LOG + "'";
  var last = MAX_ROW;

  sheet.getRange("A2").setFormula(
    "=SORT(UNIQUE(FILTER(" + tl + "!A2:A" + last + "," + tl + "!A2:A" + last + "<>\"\")))"
  );

  var body = "A2:M" + last;
  sheet.getRange("B2:B" + last).setFormulaR1C1(
    '=IF(RC[-1]="","",COUNTIF(' + tl + '!R2C1:R' + last + 'C1,RC[-1]))'
  );
  sheet.getRange("C2:C" + last).setFormulaR1C1(
    '=IF(RC[-2]="","",COUNTIFS(' + tl + '!R2C1:R' + last + 'C1,RC[-2],' + tl + '!R2C10:R' + last + 'C10,">0"))'
  );
  sheet.getRange("D2:D" + last).setFormulaR1C1(
    '=IF(RC[-3]="","",COUNTIFS(' + tl + '!R2C1:R' + last + 'C1,RC[-3],' + tl + '!R2C10:R' + last + 'C10,"<=0"))'
  );
  sheet.getRange("E2:E" + last).setFormulaR1C1(
    '=IF(RC[-4]="","",IF(RC[-3]=0,0,RC[-2]/RC[-3]))'
  );
  sheet.getRange("F2:F" + last).setFormulaR1C1(
    '=IF(RC[-5]="","",COUNTIFS(' + tl + '!R2C1:R' + last + 'C1,RC[-5],' + tl + '!R2C7:R' + last + 'C7,"*Target hit*"))'
  );
  sheet.getRange("G2:G" + last).setFormulaR1C1(
    '=IF(RC[-6]="","",COUNTIFS(' + tl + '!R2C1:R' + last + 'C1,RC[-6],' + tl + '!R2C7:R' + last + 'C7,"*Stop-loss hit*"))'
  );
  sheet.getRange("H2:H" + last).setFormulaR1C1(
    '=IF(RC[-7]="","",SUMIF(' + tl + '!R2C1:R' + last + 'C1,RC[-7],' + tl + '!R2C10:R' + last + 'C10))'
  );
  sheet.getRange("I2:I" + last).setFormulaR1C1(
    '=IF(RC[-8]="","",SUMIF(' + tl + '!R2C1:R' + last + 'C1,RC[-8],' + tl + '!R2C11:R' + last + 'C11))'
  );
  sheet.getRange("J2:J" + last).setFormulaR1C1(
    '=IF(RC[-9]="","",IF(RC[-1]=0,0,RC[-2]/RC[-1]))'
  );
  sheet.getRange("K2:K" + last).setFormulaR1C1(
    '=IF(RC[-10]="","",SUM(R2C8:RC8))'
  );
  sheet.getRange("L2:L" + last).setFormulaR1C1(
    '=IF(RC[-11]="","",IF(RC[-4]>=0,RC[-4],0))'
  );
  sheet.getRange("M2:M" + last).setFormulaR1C1(
    '=IF(RC[-12]="","",IF(RC[-5]<0,RC[-5],0))'
  );

  sheet.getRange("E2:E" + last).setNumberFormat("0.0%");
  sheet.getRange("J2:J" + last).setNumberFormat("0.0%");
  sheet.getRangeList(["H2:H" + last, "K2:K" + last, "L2:M" + last]).setNumberFormat("#,##0.00");
  sheet.setColumnWidths(1, 11, 115);
  sheet.hideColumns(12, 2); // chart-helper columns, not meant to be read directly

  // Chart 1: Equity Curve (Date vs Cumulative P&L).
  var equityChart = sheet.newChart()
    .setChartType(Charts.ChartType.LINE)
    .addRange(sheet.getRange("A1:A" + last))
    .addRange(sheet.getRange("K1:K" + last))
    .setPosition(2, 15, 0, 0)
    .setOption("title", "Equity Curve — Cumulative P&L")
    .setOption("legend", { position: "none" })
    .setOption("colors", [COLOR.green])
    .setOption("width", 760)
    .setOption("height", 300)
    .build();
  sheet.insertChart(equityChart);

  // Chart 2: Daily P&L, green/red by sign (two stacked series via helper cols).
  var dailyPnlChart = sheet.newChart()
    .setChartType(Charts.ChartType.COLUMN)
    .addRange(sheet.getRange("A1:A" + last))
    .addRange(sheet.getRange("L1:M" + last))
    .setPosition(20, 15, 0, 0)
    .setOption("title", "Daily P&L")
    .setOption("legend", { position: "none" })
    .setOption("series", {
      0: { color: COLOR.green },
      1: { color: COLOR.red },
    })
    .setOption("isStacked", true)
    .setOption("width", 760)
    .setOption("height", 300)
    .build();
  sheet.insertChart(dailyPnlChart);

  // Chart 3: rolling 20-trade win rate -- this is a per-TRADE metric (Trade
  // Log column M), not a per-day one, so it's sourced from Trade Log directly
  // even though the chart itself lives here.
  var tlSheet = ss.getSheetByName(SHEET_NAMES.TRADE_LOG);
  var rollingChart = sheet.newChart()
    .setChartType(Charts.ChartType.LINE)
    .addRange(tlSheet.getRange("A1:A" + last))
    .addRange(tlSheet.getRange("M1:M" + last))
    .setPosition(38, 15, 0, 0)
    .setOption("title", "Rolling 20-Trade Win Rate")
    .setOption("legend", { position: "none" })
    .setOption("colors", ["#4285f4"])
    .setOption("vAxis", { format: "percent", viewWindow: { min: 0, max: 1 } })
    .setOption("width", 760)
    .setOption("height", 300)
    .build();
  sheet.insertChart(rollingChart);
}


// ── Exit-Reason Breakdown: fixed 7-category table + donut/bar charts ─────────

function buildExitReasonBreakdown_() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEET_NAMES.EXIT_REASON) || ss.insertSheet(SHEET_NAMES.EXIT_REASON);
  sheet.clear();
  sheet.getCharts().forEach(function (c) { sheet.removeChart(c); });

  var headers = ["Exit Reason", "# Trades", "Win Rate", "Avg Return %", "Total P&L", "% of Total P&L"];
  var headerRange = sheet.getRange(1, 1, 1, headers.length);
  headerRange.setValues([headers]);
  headerRange.setBackground("#1a1a1a").setFontColor("#ffffff").setFontWeight("bold");
  sheet.setFrozenRows(1);

  var lastRow = 1 + REASON_ORDER.length;
  var labels  = REASON_ORDER.map(function (r) { return [reasonLabel_(r)]; });
  sheet.getRange(2, 1, labels.length, 1).setValues(labels);

  var tl   = "'" + SHEET_NAMES.TRADE_LOG + "'";
  var last = MAX_ROW;

  sheet.getRange(2, 2, REASON_ORDER.length, 1).setFormulaR1C1(
    '=IF(RC[-1]="","",COUNTIF(' + tl + '!R2C7:R' + last + 'C7,RC[-1]))'
  );
  sheet.getRange(2, 3, REASON_ORDER.length, 1).setFormulaR1C1(
    '=IF(RC[-2]="","",IF(RC[-1]=0,0,COUNTIFS(' + tl + '!R2C7:R' + last + 'C7,RC[-2],'
      + tl + '!R2C10:R' + last + 'C10,">0")/RC[-1]))'
  );
  sheet.getRange(2, 4, REASON_ORDER.length, 1).setFormulaR1C1(
    '=IF(RC[-3]="","",IFERROR(AVERAGEIF(' + tl + '!R2C7:R' + last + 'C7,RC[-3],'
      + tl + '!R2C9:R' + last + 'C9),0))'
  );
  sheet.getRange(2, 5, REASON_ORDER.length, 1).setFormulaR1C1(
    '=IF(RC[-4]="","",SUMIF(' + tl + '!R2C7:R' + last + 'C7,RC[-4],' + tl + '!R2C10:R' + last + 'C10))'
  );
  sheet.getRange(2, 6, REASON_ORDER.length, 1).setFormulaR1C1(
    '=IF(RC[-5]="","",IFERROR(RC[-1]/SUMIF(' + tl + '!R2C7:R' + last + 'C7,"<>",'
      + tl + '!R2C10:R' + last + 'C10),0))'
  );

  sheet.getRange(2, 3, REASON_ORDER.length, 1).setNumberFormat("0.0%");
  sheet.getRange(2, 6, REASON_ORDER.length, 1).setNumberFormat("0.0%");
  sheet.getRange(2, 4, REASON_ORDER.length, 2).setNumberFormat("#,##0.00");
  sheet.setColumnWidths(1, 6, 140);

  var reasonColorsInOrder = REASON_ORDER.map(function (r) { return REASON_COLORS[r]; });

  var donutChart = sheet.newChart()
    .setChartType(Charts.ChartType.PIE)
    .addRange(sheet.getRange(1, 1, lastRow, 1))
    .addRange(sheet.getRange(1, 5, lastRow, 1))
    .setPosition(2, 8, 0, 0)
    .setOption("title", "P&L Contribution by Exit Reason")
    .setOption("pieHole", 0.4)
    .setOption("colors", reasonColorsInOrder)
    .setOption("width", 480)
    .setOption("height", 320)
    .build();
  sheet.insertChart(donutChart);

  var winRateChart = sheet.newChart()
    .setChartType(Charts.ChartType.BAR)
    .addRange(sheet.getRange(1, 1, lastRow, 1))
    .addRange(sheet.getRange(1, 3, lastRow, 1))
    .setPosition(2, 15, 0, 0)
    .setOption("title", "Win Rate by Exit Reason")
    .setOption("legend", { position: "none" })
    .setOption("colors", reasonColorsInOrder)
    .setOption("hAxis", { format: "percent", viewWindow: { min: 0, max: 1 } })
    .setOption("width", 480)
    .setOption("height", 320)
    .build();
  sheet.insertChart(winRateChart);
}


// ── Symbol Stats: dynamic per-symbol table, sortable, no chart ───────────────

function buildSymbolStats_() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEET_NAMES.SYMBOL) || ss.insertSheet(SHEET_NAMES.SYMBOL);
  sheet.clear();

  var headers = ["Symbol", "# Trades", "Win Rate", "Avg Return %", "Total P&L", "Best Trade", "Worst Trade"];
  var headerRange = sheet.getRange(1, 1, 1, headers.length);
  headerRange.setValues([headers]);
  headerRange.setBackground("#1a1a1a").setFontColor("#ffffff").setFontWeight("bold");
  sheet.setFrozenRows(1);

  var tl   = "'" + SHEET_NAMES.TRADE_LOG + "'";
  var last = MAX_ROW;

  sheet.getRange("A2").setFormula(
    "=SORT(UNIQUE(FILTER(" + tl + "!B2:B" + last + "," + tl + "!B2:B" + last + "<>\"\")))"
  );

  sheet.getRange("B2:B" + last).setFormulaR1C1(
    '=IF(RC[-1]="","",COUNTIF(' + tl + '!R2C2:R' + last + 'C2,RC[-1]))'
  );
  sheet.getRange("C2:C" + last).setFormulaR1C1(
    '=IF(RC[-2]="","",IF(RC[-1]=0,0,COUNTIFS(' + tl + '!R2C2:R' + last + 'C2,RC[-2],'
      + tl + '!R2C10:R' + last + 'C10,">0")/RC[-1]))'
  );
  sheet.getRange("D2:D" + last).setFormulaR1C1(
    '=IF(RC[-3]="","",IFERROR(AVERAGEIF(' + tl + '!R2C2:R' + last + 'C2,RC[-3],'
      + tl + '!R2C9:R' + last + 'C9),0))'
  );
  sheet.getRange("E2:E" + last).setFormulaR1C1(
    '=IF(RC[-4]="","",SUMIF(' + tl + '!R2C2:R' + last + 'C2,RC[-4],' + tl + '!R2C10:R' + last + 'C10))'
  );
  sheet.getRange("F2:F" + last).setFormulaR1C1(
    '=IF(RC[-5]="","",IFERROR(MAXIFS(' + tl + '!R2C9:R' + last + 'C9,'
      + tl + '!R2C2:R' + last + 'C2,RC[-5]),0))'
  );
  sheet.getRange("G2:G" + last).setFormulaR1C1(
    '=IF(RC[-6]="","",IFERROR(MINIFS(' + tl + '!R2C9:R' + last + 'C9,'
      + tl + '!R2C2:R' + last + 'C2,RC[-6]),0))'
  );

  sheet.getRange("C2:C" + last).setNumberFormat("0.0%");
  sheet.getRangeList(["D2:D" + last, "E2:E" + last, "F2:G" + last]).setNumberFormat("#,##0.00");
  sheet.setColumnWidths(1, 7, 120);

  var filterRange = sheet.getRange(1, 1, last, headers.length);
  var existing = sheet.getFilter();
  if (existing) existing.remove();
  filterRange.createFilter();
}


// ── Today: dark command-center tab ────────────────────────────────────────────

function buildToday_() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEET_NAMES.TODAY) || ss.insertSheet(SHEET_NAMES.TODAY);
  sheet.clear();
  sheet.clearConditionalFormatRules();
  sheet.getCharts().forEach(function (c) { sheet.removeChart(c); });

  var maxCols = 16, maxRows = 60;
  var full = sheet.getRange(1, 1, maxRows, maxCols);
  full.setBackground(COLOR.darkBg);
  sheet.setHiddenGridlines(true);
  for (var c = 1; c <= maxCols; c++) sheet.setColumnWidth(c, 95);

  var tl   = "'" + SHEET_NAMES.TRADE_LOG + "'";
  var ds   = "'" + SHEET_NAMES.DAILY + "'";
  var last = MAX_ROW;

  // Title
  sheet.getRange("A1:P1").merge().setValue("🎯 DHAN LIVE — TODAY")
    .setFontSize(22).setFontWeight("bold").setFontColor(COLOR.white)
    .setHorizontalAlignment("left");
  sheet.setRowHeight(1, 44);

  // ── KPI tiles (row 3-6) ────────────────────────────────────────────────────
  var tiles = [
    { col: 1,  label: "TODAY'S P&L",
      formula: '=IFERROR(SUMIF(' + tl + '!A2:A' + last + ',TODAY(),' + tl + '!J2:J' + last + '),0)',
      fmt: "+#,##0.00;-#,##0.00", signColor: true },
    { col: 4,  label: "WIN RATE (ALL-TIME)",
      formula: '=IFERROR(COUNTIFS(' + tl + '!J2:J' + last + ',">0")/COUNTA(' + tl + '!A2:A' + last + '),0)',
      fmt: "0.0%", signColor: false },
    { col: 7,  label: "WIN RATE (LAST 30D)",
      formula: '=IFERROR(COUNTIFS(' + tl + '!A2:A' + last + ',">="&(TODAY()-30),'
        + tl + '!J2:J' + last + ',">0")/COUNTIFS(' + tl + '!A2:A' + last + ',">="&(TODAY()-30)),0)',
      fmt: "0.0%", signColor: false },
    { col: 10, label: "TARGET-HIT RATE",
      formula: '=IFERROR(COUNTIF(' + tl + '!G2:G' + last + ',"*Target hit*")/COUNTA(' + tl + '!A2:A' + last + '),0)',
      fmt: "0.0%", signColor: false },
    { col: 13, label: "STOP-LOSS-HIT RATE",
      formula: '=IFERROR(COUNTIF(' + tl + '!G2:G' + last + ',"*Stop-loss hit*")/COUNTA(' + tl + '!A2:A' + last + '),0)',
      fmt: "0.0%", signColor: false },
  ];
  tiles.forEach(function (t) {
    var labelRange = sheet.getRange(3, t.col, 1, 2).merge();
    labelRange.setValue(t.label).setFontColor(COLOR.gray).setFontSize(10).setFontWeight("bold");
    var valueRange = sheet.getRange(4, t.col, 2, 2).merge();
    valueRange.setFormula(t.formula).setNumberFormat(t.fmt)
      .setFontSize(26).setFontWeight("bold").setHorizontalAlignment("left")
      .setVerticalAlignment("middle");
    if (t.signColor) {
      sheet.getRange(4, t.col, 2, 2)
        .setFontColor(COLOR.white); // default; conditional rules below override by sign
    } else {
      valueRange.setFontColor(COLOR.white);
    }
  });

  // Streak tile (uses the custom function below).
  sheet.getRange(3, 16, 1, 1).setValue("STREAK").setFontColor(COLOR.gray)
    .setFontSize(10).setFontWeight("bold");
  sheet.getRange(4, 16, 2, 1).setFormula("=CURRENT_STREAK()")
    .setFontSize(20).setFontWeight("bold").setFontColor(COLOR.white)
    .setVerticalAlignment("middle");

  // Sign-based coloring for Today's P&L tile.
  var pnlTile = sheet.getRange(4, 1, 2, 2);
  sheet.setConditionalFormatRules(sheet.getConditionalFormatRules().concat([
    SpreadsheetApp.newConditionalFormatRule().whenNumberGreaterThan(0)
      .setFontColor(COLOR.green).setRanges([pnlTile]).build(),
    SpreadsheetApp.newConditionalFormatRule().whenNumberLessThan(0)
      .setFontColor(COLOR.red).setRanges([pnlTile]).build(),
  ]));

  // ── Sparkline: last 20 trading days' daily P&L (row 8-9) ───────────────────
  sheet.getRange("A8:C8").merge().setValue("LAST 20 DAYS P&L")
    .setFontColor(COLOR.gray).setFontSize(10).setFontWeight("bold");
  sheet.getRange("A9:F10").merge().setFormula(
    '=IFERROR(SPARKLINE(' +
      'SORT(QUERY(' + ds + '!A2:H' + last + ',"select H where A is not null order by A desc limit 20",0),1,TRUE),' +
      '{"charttype","column";"color",' + JSON.stringify(COLOR.green) + ';"negcolor",' + JSON.stringify(COLOR.red) + '}' +
    '),"—")'
  ).setVerticalAlignment("middle");

  // ── Progress bars (rows 12-16) ──────────────────────────────────────────────
  sheet.getRange("A12:P12").merge().setValue("PROGRESS")
    .setFontColor(COLOR.gray).setFontSize(10).setFontWeight("bold");

  var bars = [
    { row: 13, label: "Win Rate",
      rateFormula: '=IFERROR(COUNTIFS(' + tl + '!J2:J' + last + ',">0")/COUNTA(' + tl + '!A2:A' + last + '),0)',
      color: COLOR.green },
    { row: 14, label: "Target-Hit Rate",
      rateFormula: '=IFERROR(COUNTIF(' + tl + '!G2:G' + last + ',"*Target hit*")/COUNTA(' + tl + '!A2:A' + last + '),0)',
      color: COLOR.amber },
    { row: 15, label: "Capital Deployed vs Base",
      // No live open-position/available-balance feed reaches this sheet
      // (push_to_sheets.py only pushes CLOSED trades) -- this approximates
      // "deployed" as today's closed-trade capital against a manually-set
      // total capital base cell (F17 below), not a live broker balance.
      rateFormula: '=IFERROR(SUMIF(' + tl + '!A2:A' + last + ',TODAY(),' + tl + '!K2:K' + last + ')/$F$17,0)',
      color: "#4285f4" },
  ];
  sheet.getRange("F17").setValue(1500000).setFontColor(COLOR.white).setFontWeight("bold")
    .setNumberFormat("#,##0");
  bars.forEach(function (b) {
    sheet.getRange(b.row, 1, 1, 2).merge().setValue(b.label)
      .setFontColor(COLOR.white).setFontWeight("bold");
    var rateCell = sheet.getRange(b.row, 3, 1, 1);
    rateCell.setFormula(b.rateFormula).setNumberFormat("0.0%").setFontColor(b.color).setFontWeight("bold");
    var barCell = sheet.getRange(b.row, 4, 1, 9).merge();
    barCell.setFormula('=REPT("█",ROUND(' + rateCell.getA1Notation() + '*20,0))')
      .setFontColor(b.color).setHorizontalAlignment("left");
  });
  sheet.getRange("A17").setValue("Total capital base (edit to match TOTAL_CAPITAL in dhan/run_trades.py) ->")
    .setFontColor(COLOR.gray).setFontSize(9);
  sheet.getRange("A17:D17").merge();

  // ── Trophy card: best trade this week (rows 19-23) ──────────────────────────
  // Background/border applied to the whole block WITHOUT merging it (merging
  // a multi-row block collapses it to one cell, which would make rows 20-22
  // below un-addressable) -- each row inside the card is merged individually
  // instead, purely for the text to span the card's width.
  var trophy = sheet.getRange(19, 1, 5, 6);
  trophy.setBackground("#2b2410")
    .setBorder(true, true, true, true, false, false, COLOR.gold, SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
  sheet.getRange(19, 1, 1, 6).merge().setValue("🏆 BEST TRADE THIS WEEK")
    .setFontColor(COLOR.gold).setFontWeight("bold").setFontSize(13)
    .setVerticalAlignment("top").setBackground("#2b2410");
  sheet.getRange(20, 1, 1, 6).merge().setBackground("#2b2410").setFormula(
    '="Symbol: "&IFERROR(INDEX(' + tl + '!B2:B' + last + ',MATCH(1,(' + tl + '!A2:A' + last + '>=TODAY()-7)*('
      + tl + '!I2:I' + last + '=MAXIFS(' + tl + '!I2:I' + last + ',' + tl + '!A2:A' + last + ',">="&(TODAY()-7))),0)),"—")'
  ).setFontColor(COLOR.white);
  sheet.getRange(21, 1, 1, 6).merge().setBackground("#2b2410").setFormula(
    '="Return: "&IFERROR(TEXT(MAXIFS(' + tl + '!I2:I' + last + ',' + tl + '!A2:A' + last + ',">="&(TODAY()-7)),"0.00")&"%","—")'
  ).setFontColor(COLOR.white);
  sheet.getRange(22, 1, 1, 6).merge().setBackground("#2b2410").setFormula(
    '="Exit reason: "&IFERROR(INDEX(' + tl + '!G2:G' + last + ',MATCH(1,(' + tl + '!A2:A' + last + '>=TODAY()-7)*('
      + tl + '!I2:I' + last + '=MAXIFS(' + tl + '!I2:I' + last + ',' + tl + '!A2:A' + last + ',">="&(TODAY()-7))),0)),"—")'
  ).setFontColor(COLOR.white);

  // ── Regime badge (row 25-26) ────────────────────────────────────────────────
  sheet.getRange(25, 1, 1, 4).merge().setValue("REGIME")
    .setFontColor(COLOR.gray).setFontSize(10).setFontWeight("bold");
  var regimeCell = sheet.getRange(26, 1, 2, 4).merge();
  regimeCell.setFormula(
    '=IFERROR(' +
      'IF(INDEX(' + tl + '!M2:M' + last + ',COUNTA(' + tl + '!A2:A' + last + '))' +
        '>COUNTIFS(' + tl + '!J2:J' + last + ',">0")/COUNTA(' + tl + '!A2:A' + last + ')+0.1,"🔥 Hot streak",' +
      'IF(INDEX(' + tl + '!M2:M' + last + ',COUNTA(' + tl + '!A2:A' + last + '))' +
        '<COUNTIFS(' + tl + '!J2:J' + last + ',">0")/COUNTA(' + tl + '!A2:A' + last + ')-0.1,"🧊 Cold streak",' +
      '"😐 Normal")),"😐 Normal")'
  ).setFontSize(16).setFontWeight("bold").setVerticalAlignment("middle");

  sheet.setConditionalFormatRules(sheet.getConditionalFormatRules().concat([
    SpreadsheetApp.newConditionalFormatRule().whenTextContains("Hot")
      .setBackground(COLOR.green).setFontColor(COLOR.white).setRanges([regimeCell]).build(),
    SpreadsheetApp.newConditionalFormatRule().whenTextContains("Cold")
      .setBackground("#4285f4").setFontColor(COLOR.white).setRanges([regimeCell]).build(),
    SpreadsheetApp.newConditionalFormatRule().whenTextContains("Normal")
      .setBackground("#5f6368").setFontColor(COLOR.white).setRanges([regimeCell]).build(),
  ]));

  // ── Calendar heatmap ─────────────────────────────────────────────────────────
  sheet.getRange(29, 1, 1, 7).merge().setValue("THIS MONTH")
    .setFontColor(COLOR.gray).setFontSize(10).setFontWeight("bold");

  rebuildTodayCalendar();
}

// Rebuilds the calendar heatmap grid for the CURRENT month. Split out from
// buildToday_() so it can be re-run on its own (see
// installMonthlyCalendarTrigger below) without rebuilding the whole tab --
// month-to-month, which weekday a given date falls on shifts, so a purely
// static formula grid built once would misalign; this regenerates the grid's
// shape each time it runs, while each cell's color is still a live formula
// against Daily Summary, so today's/this week's colors stay current between
// rebuilds.
function rebuildTodayCalendar() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEET_NAMES.TODAY);
  if (!sheet) return;

  var startRow = 31, weekdayRow = startRow;
  var dowLabels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
  sheet.getRange(weekdayRow, 1, 1, 7).setValues([dowLabels])
    .setFontColor(COLOR.gray).setFontWeight("bold").setHorizontalAlignment("center");

  // Clear any previously-built grid (up to 6 weeks) before rebuilding.
  var gridTop = weekdayRow + 1;
  sheet.getRange(gridTop, 1, 6 * 2, 7).clearContent().setBackground(COLOR.darkBg);

  var today     = new Date();
  var year      = today.getFullYear();
  var month     = today.getMonth(); // 0-indexed
  var firstDay  = new Date(year, month, 1);
  var daysInMo  = new Date(year, month + 1, 0).getDate();
  // ISO-ish Mon=0..Sun=6 offset for the 1st of the month:
  var firstDow  = (firstDay.getDay() + 6) % 7;

  var ds = "'" + SHEET_NAMES.DAILY + "'";
  var rules = sheet.getConditionalFormatRules().filter(function (r) {
    // keep the P&L-tile / regime-badge rules already set by buildToday_,
    // drop any previous calendar-cell rules (they'd otherwise accumulate on re-run)
    return r.getRanges().every(function (rg) { return rg.getRow() < gridTop; });
  });

  var day = 1;
  for (var week = 0; week < 6 && day <= daysInMo; week++) {
    var dateRow  = gridTop + week * 2;
    var pnlRow   = dateRow + 1;
    for (var dow = 0; dow < 7; dow++) {
      if (week === 0 && dow < firstDow) continue;
      if (day > daysInMo) break;
      var dateCell = sheet.getRange(dateRow, dow + 1);
      dateCell.setValue(day).setFontColor(COLOR.white).setHorizontalAlignment("center")
        .setFontWeight("bold");
      var pnlCell = sheet.getRange(pnlRow, dow + 1);
      pnlCell.setFormula(
        '=IFERROR(SUMIF(' + ds + '!A2:A' + MAX_ROW + ',DATE(' + year + ',' + (month + 1) + ',' + day + '),'
          + ds + '!H2:H' + MAX_ROW + '),"")'
      ).setNumberFormat("+#,##0;-#,##0").setFontColor(COLOR.white).setHorizontalAlignment("center");

      // Absolute ($-anchored) reference -- this rule applies to a 2-row block
      // (date cell + P&L cell), and whenFormulaSatisfied rules re-adjust a
      // non-anchored reference per row within their range. Without the $, the
      // date-row cell of the block would end up testing the WRONG (next) row's
      // P&L value instead of this same block's own pnlCell.
      var absRef = '$' + String.fromCharCode(64 + dow + 1) + '$' + pnlRow;
      var block  = sheet.getRange(dateRow, dow + 1, 2, 1);
      rules.push(
        SpreadsheetApp.newConditionalFormatRule()
          .whenFormulaSatisfied('=' + absRef + '=""')
          .setBackground("#2a2a2a").setRanges([block]).build()
      );
      rules.push(
        SpreadsheetApp.newConditionalFormatRule()
          .whenFormulaSatisfied('=' + absRef + '>0')
          .setBackground(COLOR.green).setRanges([block]).build()
      );
      rules.push(
        SpreadsheetApp.newConditionalFormatRule()
          .whenFormulaSatisfied('=' + absRef + '<0')
          .setBackground(COLOR.red).setRanges([block]).build()
      );
      rules.push(
        SpreadsheetApp.newConditionalFormatRule()
          .whenFormulaSatisfied('=' + absRef + '=0')
          .setBackground("#5f6368").setRanges([block]).build()
      );
      day++;
    }
  }
  sheet.setConditionalFormatRules(rules);
}

// Apps Script triggers need an authorized manual run to install (can't be
// created from doPost for security reasons) -- run this ONCE yourself from
// the editor (select it in the function dropdown, click Run) to make the
// calendar heatmap reshape itself automatically on the 1st of every month.
// Without this, re-run rebuildTodayCalendar() by hand after a month rolls over.
function installMonthlyCalendarTrigger() {
  ScriptApp.getProjectTriggers().forEach(function (t) {
    if (t.getHandlerFunction() === "rebuildTodayCalendar") ScriptApp.deleteTrigger(t);
  });
  ScriptApp.newTrigger("rebuildTodayCalendar")
    .timeBased()
    .onMonthDay(1)
    .atHour(1)
    .create();
}

/**
 * Current win/loss streak as of the most recent closed trade, e.g. "🔥 5W" or
 * "🧊 3L". A custom function (recalculates like any formula) because a
 * trailing-run-length calculation is impractical as a plain native formula.
 * @customfunction
 */
function CURRENT_STREAK() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var tl = ss.getSheetByName(SHEET_NAMES.TRADE_LOG);
  if (!tl) return "—";

  var dates = tl.getRange(2, 1, MAX_ROW - 1, 1).getValues();
  var wins  = tl.getRange(2, 12, MAX_ROW - 1, 1).getValues(); // column L
  var n = 0;
  for (var i = 0; i < dates.length; i++) {
    if (dates[i][0] === "" || dates[i][0] === null) break;
    n = i + 1;
  }
  if (n === 0) return "—";

  var lastVal = wins[n - 1][0];
  if (lastVal === "" || lastVal === null) return "—";
  var streak = 1;
  for (var j = n - 2; j >= 0; j--) {
    if (wins[j][0] === lastVal) {
      streak++;
    } else {
      break;
    }
  }
  return (lastVal === 1 ? "🔥 " : "🧊 ") + streak + (lastVal === 1 ? "W" : "L");
}
