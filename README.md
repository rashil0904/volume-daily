# NSE Volume Pipeline

Automated NSE mid-cap momentum scanner running daily at **3:01 PM IST** (Mon–Fri) on a DigitalOcean Ubuntu VM. Scans ~499 symbols in the ₹1,500–5,000 Cr market-cap band, fires on volume + return conditions, generates a trade list, and executes 3-stage live trades via Zerodha. Upstox is used for market data only — no trading happens through Upstox.

---

## Table of Contents

- [Strategy Overview](#strategy-overview)
- [Signal Logic](#signal-logic)
- [Capital Allocation](#capital-allocation)
- [Complete Daily Flow](#complete-daily-flow)
  - [Part 1 — Signal Pipeline (3:01 PM)](#part-1--signal-pipeline-301-pm)
  - [Part 2 — EOD Candle Fill (3:45 PM)](#part-2--eod-candle-fill-345-pm)
  - [Part 3 — Live Trading (3-Stage, Zerodha)](#part-3--live-trading-3-stage-zerodha)
- [Repository Structure](#repository-structure)
- [First-Time VM Setup](#first-time-vm-setup)
- [Configuration](#configuration)
- [Daily Operations](#daily-operations)
- [Manual Commands](#manual-commands)
- [Telegram Notifications](#telegram-notifications)
- [Cron Schedule Summary](#cron-schedule-summary)

---

## Strategy Overview

The pipeline targets **NSE equities with market cap ₹1,500–5,000 Cr**. It screens for stocks where unusual volume has built throughout the day *and* the price has broken above the prior day's close. Entries happen at 3:15 PM IST (market close). Positions are held overnight; an early-morning exit is taken if the stock is up, otherwise a forced exit at noon.

| Parameter | Value |
|---|---|
| Universe | NSE EQ/BE segment, ₹1,500–5,000 Cr |
| Candle interval | 15-minute OHLCV (pipeline), 1-minute (live trading) |
| Data source | Upstox V3 API (data/analytics only — no Upstox trading) |
| Market cap source | Screener.in Premium (live daily export) |
| Entry | Market buy at 3:15 PM IST (open of the 15:00 candle) |
| Broker / product type | Zerodha, Delivery (CNC) |
| Capital | ₹5,00,000 total |
| Pipeline schedule | Mon–Fri at 3:01 PM IST via cron |

---

## Signal Logic

**All three conditions must pass** for a symbol to appear in the trade list.

### 1. Market Cap Filter

Symbol must be present in today's Screener.in export (₹1,500–5,000 Cr band). This is the first gate — only stocks currently in the universe are evaluated.

### 2. Volume Condition

```
Cumulative volume (09:15–14:45) ≥ 6 × 36-day rolling average full-day volume
```

- Rolling window: 36 prior trading days, non-zero volume days only
- Symbols with fewer than 36 days of history are skipped
- Cumulative volume is measured up to and including the 14:45 candle
- The 15:00 candle is intentionally excluded from volume (only available after 3:15 PM)

### 3. Return Condition

```
Open of 15:00 candle ≥ 5% above previous trading day's VWAP
```

Previous day VWAP is calculated from the 15:00 and 15:15 candles of the prior trading day:

```
VWAP = Σ((H + L + C) / 3 × Volume) / Σ(Volume)
```

The 15:00 and 15:15 candles represent the closing auction of each trading day. The pipeline writes these two candles via the 3:45 PM EOD fill cron (see below), since they are not available at 3:01 PM when the main scan runs.

---

## Capital Allocation

Total capital is capped at **₹5,00,000** regardless of how many signals fire.

| Signals | Allocation per stock | Total deployed |
|---|---|---|
| 1 | ₹1,25,000 | ₹1,25,000 |
| 2 | ₹1,25,000 | ₹2,50,000 |
| 3 | ₹1,25,000 | ₹3,75,000 |
| 4 | ₹1,25,000 | ₹5,00,000 |
| 5 | ₹1,00,000 | ₹5,00,000 |
| 6 | ₹83,333 | ₹5,00,000 |
| n ≥ 5 | ₹5,00,000 ÷ n | ₹5,00,000 |

Entry sizing: `shares = floor(allocation / close_of_15:14_candle)` (falls back to 15:13 close if 15:14 isn't published yet)

---

## Complete Daily Flow

### Timeline Overview

```
 9:15 AM  — Market opens
 3:00 PM  — Last auction period begins
 3:01 PM  — [CRON] Pipeline runs: scans volume/return, writes trade list
 3:15 PM  — Stage 1: fetch 15:14 candle (fallback 15:13), size positions, place buys (manual — not yet cron-scheduled)
 3:45 PM  — [CRON] EOD fill: corrects/backfills 15:00 + 15:15 candles via intraday API
─────────── overnight hold ───────────────────────────────────────────────────
 9:45 AM  — Stage 2: check live P&L from Kite positions/holdings; exit if positive (manual — not yet cron-scheduled)
12:00 PM  — Stage 3: force-exit any positions still open (manual — not yet cron-scheduled)
```

> Only the 3:01 PM signal pipeline and the 3:45 PM EOD fill are currently wired into cron. `run_trades.py`'s three trading stages exist and work, but must be run manually (or scheduled separately if you want full automation — see [Add Live Trading Crons](#add-live-trading-crons-optional)).

---

### Part 1 — Signal Pipeline (3:01 PM)

`run_pipeline.sh` is called by cron at 3:01 PM IST. It calls `pipeline/run_daily.py`, which runs four steps in sequence:

#### Step 1 — Fetch Market Cap

`fetch_market_cap.py` logs into Screener.in (Premium), runs the query `Market Capitalization > 1500 AND Market Capitalization < 5000`, and exports the result to `data/market_cap_daily/market_cap_<date>.csv`.

Exit codes:
- `0` — fresh data saved
- `2` — Screener.in unavailable; using previous export as stale fallback (pipeline continues with warning)
- `1` — no data at all (pipeline fails)

#### Step 2 — Update Universe

Compares today's market-cap symbols against `data/universe_combined.csv`. New symbols not previously seen are appended automatically. This keeps the tracked universe expanding as stocks enter the cap band.

#### Step 3 — Candle Data Update

Two sub-operations run:

1. **New symbols** — downloads the Upstox NSE instrument master, matches new symbols to their `instrument_key`, then backfills 1 year of 15-min candles into `data/candles/<SYMBOL>.csv`
2. **All symbols** — fetches today's intraday 15-min candles and merges new/updated rows into every existing candle file (file is kept fully sorted; a fresh fetch overwrites any existing row for the same timestamp — see Part 2 for why that matters)

At this point (3:01 PM), the candle files will have data through approximately 14:45–15:00. The candle "in progress" at fetch time is necessarily incomplete (collapsed OHLC, minimal volume) until the 3:45 PM EOD fill corrects it, and the final 15:15 candle of the session doesn't exist yet at all.

Rate limiting: Upstox allows ~66 req/min. The fetcher uses 5 parallel workers with a 0.8s per-call delay.

#### Step 4 — Generate Trade List

`prepare_data.py` applies all three signal conditions to every symbol that has both a candle file and a current market-cap record.

**Important**: The return condition checks whether the open of the **15:00 candle** is ≥ 5% above prior day's VWAP. Prior day's VWAP is derived from the prior day's 15:00 and 15:15 candles, which are correct and settled in the candle files because they were fixed by the *previous afternoon's* EOD fill cron.

Passing symbols are written to `results/trade_list_<date>.csv` (columns: `symbol`, `shares`, `ref_price`). If no signals fire, no file is written and the pipeline exits cleanly.

#### Notification

`pipeline/notify.py` sends a message to the **"NSE Volume Alerts"** Telegram group. A success message includes the full trade table; a failure message includes the failed step and error detail. Notification failures are logged as warnings and do not crash the pipeline.

---

### Part 2 — EOD Candle Fill (3:45 PM)

Runs via cron at 3:45 PM IST, 15 minutes after NSE closes (3:30 PM). This step exists to fix a real, verified data-completeness problem:

1. **The candle "in progress" when the 3:01 PM run fetches intraday data is incomplete.** Whatever 15-minute period is currently forming at fetch time gets written with collapsed OHLC (open = high = low = close, near-zero volume) — a single-tick snapshot, not the settled candle.
2. **The very last candle of the session (15:15) doesn't exist yet at 3:01 PM at all** — it hasn't happened yet.

`run_eod_fill()` inside `fetch_candles.py` calls `fetch_all_intraday()` — the **intraday** endpoint (`/v3/historical-candle/intraday/`), not the historical one. This was empirically verified: the historical endpoint (`/v3/historical-candle/`) returns **zero rows for same-day dates**, so an earlier version of this job that used it silently did nothing every single day. The intraday endpoint does return the full, settled session once the market has closed.

The merge logic in `fetch_intraday_symbol()` lets freshly fetched data **overwrite** any existing row with the same timestamp (not just append missing ones), so this run both adds the missing 15:15 candle and corrects the stale in-progress candle from the 3:01 PM run. The file is re-sorted on every write.

```bash
python pipeline/fetch_candles.py --eod-fill
```

---

### Part 3 — Live Trading (3-Stage, Zerodha)

Trading execution is fully independent of the signal pipeline and runs entirely through **Zerodha** (`zerodha/run_trades.py`). Upstox is not used for order placement — only for the 1-minute reference-price candles Zerodha's own price data doesn't provide as conveniently.

#### Stage 1 — Entry at 3:15 PM

**Script**: `python zerodha/run_trades.py --entry`

1. Reads `results/trade_list_<today>.csv` (produced by the 3:01 PM pipeline run)
2. For each symbol, fetches today's 1-minute intraday candles via Upstox V3 API
3. Reads the **close of the 15:14 candle** as the reference price for sizing — falls back to the **15:13 close** if 15:14 isn't published yet
4. Calculates `shares = floor(allocation / ref_price)`
5. Places a MARKET BUY order via Zerodha (Kite Connect), with `market_protection` set so the order isn't rejected by the API
6. Polls for fill confirmation (up to 36 seconds, 12 retries × 3s)
7. Writes entry details to `results/positions_zerodha.json`
8. Sends a Telegram entry notification per position

#### Stage 2 — Exit Check at 9:45 AM (Next Day)

**Script**: `python zerodha/run_trades.py --exit-945`

1. Loads all open positions from `results/positions_zerodha.json`
2. For each position, pulls **Kite's own computed P&L** for that symbol from `/portfolio/positions` (same-day) or `/portfolio/holdings` (settled) — not candle-based at all, so it isn't affected by whether a specific candle has been published
3. **If P&L > 0**: places MARKET SELL for the full quantity → marks `exited_945`
4. **If P&L ≤ 0**: holds the position for Stage 3 forced exit at noon
5. **If the symbol isn't found in either endpoint** (lookup failure, or already exited): sells half the position as a precaution → marks `partial_exit_945_nodata` → remaining shares flow to Stage 3

#### Stage 3 — Forced Exit at 12:00 PM

**Script**: `python zerodha/run_trades.py --exit-1200`

1. Loads all positions with status `open` or `partial_exit_945_nodata`
2. If nothing is open, sends a Telegram confirmation and exits
3. For each remaining position, cross-checks Zerodha-held quantity via the portfolio positions/holdings API before selling (catches manual interventions or prior partial fills) — mismatches are skipped for manual review, not force-sold
4. Places MARKET SELL for the confirmed quantity
5. Calculates blended P&L across any partial 9:45 AM exit and the 12 PM remainder
6. Marks positions `exited_1200`, sends Telegram notifications, and prints a daily summary

#### Positions JSON Schema

Each entry in `results/positions_zerodha.json`:

```json
{
  "broker": "zerodha",
  "symbol": "CYIENTDLM",
  "entry_date": "2026-07-20",
  "reference_price": 580.5,
  "shares_intended": 215,
  "actual_fill_price": 581.2,
  "actual_fill_quantity": 215,
  "entry_order_id": "...",
  "status": "open",
  "entry_timestamp": "2026-07-20T15:15:42+05:30"
}
```

Status progression:
- `open` — entered, no exit yet
- `exited_945` — fully exited at 9:45 AM (return was positive)
- `partial_exit_945_nodata` — half exited at 9:45 AM (P&L lookup unavailable), remaining open
- `exited_1200` — force-exited at noon (or completed after partial)

#### Alternative: execute_trades.py (Simpler, No Live Candle)

If you want to execute from the pre-calculated CSV without any live candle API or the 3-stage lifecycle:

```bash
python zerodha/execute_trades.py
```

This reads `results/trade_list_<date>.csv` directly (which has pre-calculated `shares` and `ref_price` from the 3 PM open). No positions JSON, no fill polling, no stages, no automatic exit — just a batch buy you manage yourself.

---

## Repository Structure

```
volume-daily/
├── pipeline/
│   ├── run_daily.py          # Daily orchestrator — called by cron at 3:01 PM
│   ├── fetch_market_cap.py   # Logs into Screener.in, exports market cap CSV
│   ├── fetch_candles.py      # Upstox V3 — historical + intraday + EOD fill mode
│   ├── prepare_data.py       # Signal scanner — produces trade_list_<date>.csv (anchored to 15:00 candle)
│   ├── scan_intraday.py      # Anytime preview scanner — same logic, usable mid-day (not the official run)
│   ├── notify.py             # Telegram notifications (pipeline + trade alerts)
│   ├── requirements.txt      # pandas, requests, python-dotenv
│   └── .env                  # ← NOT in git — credentials live here
│
├── zerodha/
│   ├── auth.py               # Kite session management (daily login, expires 6 AM IST)
│   ├── trade.py               # buy(), sell(), order_status() via Kite — CLI too
│   ├── execute_trades.py     # Batch buyer from trade list CSV via Kite
│   └── run_trades.py         # 3-stage live trading via Kite (Stage 1/2/3 with positions JSON)
│
├── data/
│   ├── candles/               # ~485 per-symbol 15-min OHLCV CSV files (data only — no Upstox trading)
│   ├── instruments/
│   │   ├── upstox_instruments.csv   # symbol → instrument_key mapping
│   │   └── upstox_unmatched.csv     # symbols with no Upstox match
│   ├── market_cap_daily/      # Daily Screener.in exports + mcap_status.json
│   └── universe_combined.csv  # All symbols ever seen in the 1,500–5,000 Cr band
│
├── results/
│   ├── trade_list_YYYY-MM-DD.csv     # Official signal output (one per trading day)
│   ├── scan_YYYY-MM-DD_HHMM.csv      # scan_intraday.py preview output (not read by any execution script)
│   └── positions_zerodha.json        # Zerodha trade book (all-time, all statuses)
│
├── run_pipeline.sh           # Cron entry point — calls pipeline/run_daily.py
├── setup_vm.sh               # One-time VM provisioning script
└── .env.example               # Credential template — copy to pipeline/.env and fill in
```

---

## First-Time VM Setup

**Prerequisites**: DigitalOcean Ubuntu 22.04+ droplet. Your SSH public key must be added to the droplet.

```bash
# 1. SSH into the droplet
ssh root@<DROPLET_IP>

# 2. Clone the repo
git clone git@github.com:rashil0904/volume-daily.git
cd volume-daily

# 3. Run the setup script
chmod +x setup_vm.sh
./setup_vm.sh

# 4. Fill in credentials
nano pipeline/.env
```

`setup_vm.sh` does the following automatically:
- Installs `python3.11`, `python3.11-venv`, `git`
- Sets the system timezone to `Asia/Kolkata` (`timedatectl set-timezone Asia/Kolkata`)
- Installs Python dependencies from `pipeline/requirements.txt`
- Creates required data directories (`data/candles`, `data/instruments`, `data/market_cap_daily`, `results`)
- Writes a blank `pipeline/.env` (if not already present)
- Registers the main pipeline cron: `1 15 * * 1-5 /bin/bash ~/volume-daily/run_pipeline.sh >> ~/pipeline.log 2>&1`

After filling in `.env`, run a manual test:

```bash
python3.11 pipeline/run_daily.py
```

### Add the EOD Fill Cron (Manual Step)

The setup script does not add the EOD fill cron. Add it manually:

```bash
crontab -e
```

Add this line:

```
45 15 * * 1-5 python3.11 /root/volume-daily/pipeline/fetch_candles.py --eod-fill >> /root/pipeline.log 2>&1
```

This runs at 3:45 PM IST Mon–Fri, 15 minutes after market close, using the **intraday** endpoint to backfill the 15:15 candle and correct the in-progress 15:00 candle from the 3:01 PM run.

### Add Live Trading Crons (Optional)

`run_trades.py`'s three stages are not currently scheduled — they must be run manually unless you add crons for them:

```
# Stage 1 — entry at 3:15 PM
15 15 * * 1-5 cd /root/volume-daily && /usr/bin/python3.11 zerodha/run_trades.py --entry >> /root/trades.log 2>&1

# Stage 2 — exit check at 9:45 AM next morning
45 9 * * 2-6 cd /root/volume-daily && /usr/bin/python3.11 zerodha/run_trades.py --exit-945 >> /root/trades.log 2>&1

# Stage 3 — forced exit at noon
0 12 * * 2-6 cd /root/volume-daily && /usr/bin/python3.11 zerodha/run_trades.py --exit-1200 >> /root/trades.log 2>&1
```

Stage 2 and 3 run Tuesday–Saturday (the morning after Monday–Friday entries). Also remember: `python -m zerodha.auth` must be re-run every day before 3 PM — the Kite Connect access token expires at 6:00 AM IST daily, and none of these crons refresh it for you.

---

## Configuration

Copy `.env.example` to `pipeline/.env` and fill in all values. This file is gitignored and must never be committed.

| Key | Description |
|---|---|
| `SCREENER_EMAIL` | Screener.in login email (Premium account required for export) |
| `SCREENER_PASSWORD` | Screener.in password |
| `UPSTOX_ACCESS_TOKEN` | Upstox data token for candle fetches (from developer portal). Analytics only — no Upstox trading. |
| `TELEGRAM_BOT_TOKEN` | Bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | Group chat ID (negative integer for supergroups, e.g. `-1004409929427`) |
| `ZERODHA_API_KEY` | Zerodha Kite API key — used for order execution |
| `ZERODHA_API_SECRET` | Zerodha Kite API secret |
| `ZERODHA_REDIRECT_URI` | `https://kite.trade/` |

> **Security**: `pipeline/.env` is in `.gitignore`. Never commit credentials. Use `.env.example` as the key-name reference only.

---

## Daily Operations

### Morning Token Refresh (Required for Live Trading)

The **Zerodha (Kite Connect) trading token** expires at 6:00 AM IST every day. Refresh it each morning before trading:

```bash
python -m zerodha.auth
# Opens browser → log in → copy the redirect URL → paste it back
```

The token is saved to `zerodha/.token.json` and reused automatically until the next 6 AM IST expiry.

The **candle data token** (`UPSTOX_ACCESS_TOKEN`) is long-lived — update it in `.env` only when it eventually expires. `run_trades.py` uses this token for price data, not for trading.

### Monitoring

```bash
# Live pipeline log stream
tail -f ~/pipeline.log

# Live trades log (if you added the trading crons)
tail -f ~/trades.log

# Last run summary
grep -E "(Starting|completed|FAILED|Stage)" ~/pipeline.log | tail -20

# Verify all crons are registered
crontab -l

# Verify system time is IST
date

# Check today's open positions
cat results/positions_zerodha.json | python3 -m json.tool
```

### Pulling Updates

```bash
cd ~/volume-daily
git pull
```

Note: `run_pipeline.sh` does not `git pull` automatically — code updates pushed to GitHub do not reach the VM until you pull manually.

---

## Manual Commands

### Pipeline

```bash
# Run the full pipeline manually
python3.11 pipeline/run_daily.py

# Run only the EOD candle fill
python3.11 pipeline/fetch_candles.py --eod-fill

# Preview signals at any time of day (not the official 3:01 PM run — see scan_intraday.py)
python3.11 pipeline/scan_intraday.py

# Test Telegram — success notification
python pipeline/notify.py --date 2026-07-20 --status success

# Test Telegram — failure notification
python pipeline/notify.py --date 2026-07-20 --status failed \
  --failed-step "Step 3" --error-msg "API timeout"
```

### 3-Stage Trading (Zerodha)

```bash
# Stage 1 — entry dry run (preview without orders)
python zerodha/run_trades.py --entry --dry-run

# Stage 1 — live entry
python zerodha/run_trades.py --entry

# Stage 2 — exit check at 9:45 AM
python zerodha/run_trades.py --exit-945

# Stage 3 — forced exit at 12 PM
python zerodha/run_trades.py --exit-1200
```

### Batch Execution from CSV

```bash
# Preview today's trades without placing orders
python zerodha/execute_trades.py --dry-run

# Execute today's trades (live)
python zerodha/execute_trades.py

# Execute trades for a specific date
python zerodha/execute_trades.py --date 2026-07-17
```

### Single-Stock Manual Orders

Use these to test connectivity, verify tokens, or place/exit positions manually without any pipeline dependency:

```bash
python -m zerodha.trade buy RELIANCE NSE 1 MARKET
python -m zerodha.trade sell RELIANCE NSE 1 MARKET
python -m zerodha.trade orders               # all today's orders
python -m zerodha.trade status <order_id>    # single order status
python -m zerodha.trade cancel <order_id>    # cancel an order
```

### Auth

```bash
# Refresh Zerodha trading token (daily, before market open)
python -m zerodha.auth
```

---

## Telegram Notifications

Notifications go to the **"NSE Volume Alerts"** group via `@nse_volume_alerts_bot` (chat ID `-1004409929427`).

### Pipeline Notifications

**Success** — sent after `run_daily.py` completes:
- Date, number of signals, pipeline runtime
- Trade table: symbol | shares | entry price (3 PM open)
- Warning banner if market cap data was stale

**Failure** — sent if any step crashes:
- Date, failed step name, runtime, full error message

### Trade Notifications

| Event | When sent |
|---|---|
| `ENTRY` | After each Stage 1 buy order is submitted |
| `EXIT 9:45am` | After a Stage 2 profitable sell fills |
| `NO-DATA FALLBACK 9:45am` | When live P&L is unavailable from Kite; half position sold |
| `FORCE EXIT 12pm` | After each Stage 3 sell fills |
| `12pm Exit — nothing to close` | If all positions already exited at 9:45 AM |
| `Daily Summary` | After Stage 3 completes; shows P&L for the day |

### Setting Up on a New Bot or Group

1. Create a bot via `@BotFather` → copy the token → set `TELEGRAM_BOT_TOKEN` in `pipeline/.env`
2. Add the bot to your Telegram group
3. Send a message in the group, then run:
   ```bash
   curl "https://api.telegram.org/bot<TOKEN>/getUpdates"
   ```
4. Find `chat.id` in the response (negative integer for supergroups) → set `TELEGRAM_CHAT_ID` in `pipeline/.env`

---

## Cron Schedule Summary

All times IST (UTC+5:30). Cron expressions below are in **server local time (IST)**, since the VM's timezone is set to `Asia/Kolkata` — not UTC.

| Time (IST) | Cron (IST) | Command | Status |
|---|---|---|---|
| 3:01 PM Mon–Fri | `1 15 * * 1-5` | `run_pipeline.sh` | ✅ Scheduled |
| 3:45 PM Mon–Fri | `45 15 * * 1-5` | `fetch_candles.py --eod-fill` | ✅ Scheduled |
| 3:15 PM Mon–Fri | `15 15 * * 1-5` | `zerodha/run_trades.py --entry` | ⚠️ Not scheduled — manual only, unless added |
| 9:45 AM Tue–Sat | `45 9 * * 2-6` | `zerodha/run_trades.py --exit-945` | ⚠️ Not scheduled — manual only, unless added |
| 12:00 PM Tue–Sat | `0 12 * * 2-6` | `zerodha/run_trades.py --exit-1200` | ⚠️ Not scheduled — manual only, unless added |

> The 44-minute gap between the pipeline (3:01 PM) and EOD fill (3:45 PM) leaves plenty of margin — the pipeline typically finishes in under 3 minutes, and the EOD fill deliberately waits until 15 minutes after market close (3:30 PM) so the final candle has settled.

---

*Pipeline runs Mon–Fri · DigitalOcean Ubuntu 22.04 · Python 3.11 · Upstox V3 API (data) · Zerodha Kite Connect (execution)*
