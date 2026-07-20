# NSE Volume Pipeline

Automated NSE mid-cap momentum scanner running daily at **3:01 PM IST** (Mon–Fri) on a DigitalOcean Ubuntu VM. Scans ~940 symbols in the ₹1,500–5,000 Cr market-cap band, fires on volume + return conditions, generates a trade list, and executes 3-stage live trades via Upstox or Zerodha.

---

## Table of Contents

- [Strategy Overview](#strategy-overview)
- [Signal Logic](#signal-logic)
- [Capital Allocation](#capital-allocation)
- [Complete Daily Flow](#complete-daily-flow)
  - [Part 1 — Signal Pipeline (3:01 PM)](#part-1--signal-pipeline-301-pm)
  - [Part 2 — EOD Candle Fill (7:00 PM)](#part-2--eod-candle-fill-700-pm)
  - [Part 3 — Live Trading (3-Stage)](#part-3--live-trading-3-stage)
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
| Data source | Upstox V3 API |
| Market cap source | Screener.in Premium (live daily export) |
| Entry | Market buy at 3:15 PM IST (open of the 15:00 candle) |
| Product type | Delivery (CNC for Zerodha, D for Upstox) |
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

The 15:00 and 15:15 candles represent the closing auction of each trading day. The pipeline pipeline writes these two candles via the 7 PM EOD fill cron (see below), since they are not available at 3:01 PM when the main scan runs.

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

Entry sizing: `shares = floor(allocation / close_of_15:13_candle)`

---

## Complete Daily Flow

### Timeline Overview

```
 9:15 AM  — Market opens
 3:00 PM  — Last auction period begins
 3:01 PM  — [CRON] Pipeline runs: scans volume/return, writes trade list
 3:15 PM  — [CRON] Stage 1: fetch 15:13 candle, size positions, place buys
~7:00 PM  — [CRON] EOD fill: backfills 15:00 + 15:15 candles via historical API
─────────── overnight hold ───────────────────────────────────────────────────
 9:45 AM  — [CRON] Stage 2: check 09:43 candle; exit if return > 0
12:00 PM  — [CRON] Stage 3: force-exit any positions still open
```

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
2. **All symbols** — fetches today's intraday 15-min candles and appends new rows to every existing candle file

At this point (3:01 PM), the candle files will have data through approximately 14:45. The 15:00 and 15:15 candles are not yet available — they are added by the 7 PM EOD fill cron.

Rate limiting: Upstox allows ~66 req/min. The fetcher uses 5 parallel workers with a 0.8s per-call delay.

#### Step 4 — Generate Trade List

`prepare_data.py` applies all three signal conditions to every symbol that has both a candle file and a current market-cap record.

**Important**: The return condition checks whether the open of the **15:00 candle** is ≥ 5% above prior day's VWAP. Prior day's VWAP is derived from the prior day's 15:00 and 15:15 candles, which exist in the candle files because they were written by the *previous evening's* EOD fill cron.

Passing symbols are written to `results/trade_list_<date>.csv` (columns: `symbol`, `shares`, `ref_price`). If no signals fire, no file is written and the pipeline exits cleanly.

#### Notification

`pipeline/notify.py` sends a message to the **"NSE Volume Alerts"** Telegram group. A success message includes the full trade table; a failure message includes the failed step and error detail. Notification failures are logged as warnings and do not crash the pipeline.

---

### Part 2 — EOD Candle Fill (7:00 PM)

Runs via cron at 7 PM IST. This step is critical: the 15:00 and 15:15 candles are published by Upstox to the historical endpoint only after the market fully settles (typically 4–6 PM). Without this step, the prior day's VWAP would be missing for all symbols, and the return condition would never pass the following day.

```bash
python pipeline/fetch_candles.py --eod-fill
```

This runs `run_eod_fill()` inside `fetch_candles.py`, which calls `fetch_append_historical()` with `from_date=today, to_date=today`. It uses the `/v3/historical-candle/` endpoint (not intraday), deduplicates by timestamp, and appends only new rows to each symbol's candle file. The 3:01 PM intraday run and the 7 PM historical run do not conflict.

---

### Part 3 — Live Trading (3-Stage)

The trading execution is fully independent of the signal pipeline. Two broker scripts are available — one for Upstox, one for Zerodha — with identical decision logic. Both use the Upstox V3 intraday candle API for all price data, regardless of which broker places the orders.

#### Stage 1 — Entry at 3:15 PM

**Script**: `python upstox/run_trades.py --entry` or `python zerodha/run_trades.py --entry`

1. Reads `results/trade_list_<today>.csv` (produced by the 3:01 PM pipeline run)
2. For each symbol, fetches today's 1-minute intraday candles via Upstox V3 API
3. Reads the **close of the 15:13 candle** as the reference price for sizing
4. Calculates `shares = floor(allocation / ref_price)`
5. Places a MARKET BUY order via the broker
6. Polls for fill confirmation (up to 36 seconds, 12 retries × 3s)
7. Writes entry details to `results/positions_upstox.json` or `results/positions_zerodha.json`
8. Sends a Telegram entry notification per position

Why 15:13 candle? The 15:00–15:14 period is the closing auction. The 15:13 candle close gives a settled reference price before the 15:15 continuous session opens. The buy order is placed into the 15:15 open.

#### Stage 2 — Exit Check at 9:45 AM (Next Day)

**Script**: `python upstox/run_trades.py --exit-945` or `python zerodha/run_trades.py --exit-945`

1. Loads all open positions from the positions JSON
2. For each position, fetches today's 1-minute candles and reads the **close of the 09:43 candle**
3. Calculates return: `(09:43_close - entry_fill_price) / entry_fill_price * 100`
4. **If return > 0**: places MARKET SELL for the full quantity → marks `exited_945`
5. **If return ≤ 0**: holds the position for Stage 3 forced exit at noon
6. **If 09:43 candle is unavailable** (data not yet published): sells half the position as a precaution → marks `partial_exit_945_nodata` → remaining shares flow to Stage 3

#### Stage 3 — Forced Exit at 12:00 PM

**Script**: `python upstox/run_trades.py --exit-1200` or `python zerodha/run_trades.py --exit-1200`

1. Loads all positions with status `open` or `partial_exit_945_nodata`
2. If nothing is open, sends a Telegram confirmation and exits
3. For each remaining position, cross-checks broker-held quantity via positions/holdings API endpoints before selling (catches manual interventions or prior partial fills)
4. Places MARKET SELL for the confirmed quantity
5. Calculates blended P&L across any partial 9:45 AM exits and the 12 PM remainder
6. Marks positions `exited_1200`, sends Telegram notifications, and prints a daily summary

#### Positions JSON Schema

Each entry in `positions_upstox.json` or `positions_zerodha.json`:

```json
{
  "broker": "upstox",
  "symbol": "CYIENTDLM",
  "entry_date": "2026-07-19",
  "reference_price": 580.5,
  "shares_intended": 215,
  "actual_fill_price": 581.2,
  "actual_fill_quantity": 215,
  "entry_order_id": "...",
  "status": "open",
  "entry_timestamp": "2026-07-19T15:15:42+05:30"
}
```

Status progression:
- `open` — entered, no exit yet
- `exited_945` — fully exited at 9:45 AM (return was positive)
- `partial_exit_945_nodata` — half exited at 9:45 AM (candle unavailable), remaining open
- `exited_1200` — force-exited at noon (or completed after partial)

#### Broker Differences

| Aspect | Upstox (`run_trades.py`) | Zerodha (`run_trades.py`) |
|---|---|---|
| Order API | Upstox V2 (`/v2/order/place`) | Kite API (`/orders`) |
| Product code | `"D"` (delivery) | `"CNC"` (delivery) |
| Fill status check | `"complete"` (lowercase) | `"COMPLETE"` (uppercase) |
| Sandbox mode | `--mode sandbox` supported | No sandbox flag |
| Broker qty check | `/portfolio/short-term-positions` + `/portfolio/long-term-holdings` | `/portfolio/positions` (net) + `/portfolio/holdings` |
| Positions file | `results/positions_upstox.json` | `results/positions_zerodha.json` |

#### Alternative: execute_trades.py (Simpler, No Live Candle)

If you want to execute from the pre-calculated CSV without any live candle API:

```bash
python upstox/execute_trades.py       # reads trade_list CSV, uses ref_price column for sizing
python zerodha/execute_trades.py
```

This reads `results/trade_list_<date>.csv` directly (which has pre-calculated `shares` and `ref_price` from the 3 PM open). No positions JSON, no fill polling, no stages — just a batch buy. Use this as a simpler fallback when you don't need the full 3-stage lifecycle.

---

## Repository Structure

```
volume-daily/
├── pipeline/
│   ├── run_daily.py          # Daily orchestrator — called by cron at 3:01 PM
│   ├── fetch_market_cap.py   # Logs into Screener.in, exports market cap CSV
│   ├── fetch_candles.py      # Upstox V3 — historical + intraday + EOD fill mode
│   ├── prepare_data.py       # Signal scanner — produces trade_list_<date>.csv
│   ├── notify.py             # Telegram notifications (pipeline + trade alerts)
│   ├── requirements.txt      # pandas, requests, python-dotenv
│   └── .env                  # ← NOT in git — credentials live here
│
├── upstox/
│   ├── auth.py               # OAuth login + token persistence (live / sandbox)
│   ├── trade.py              # buy(), sell(), cancel_order(), order_status() — CLI too
│   ├── execute_trades.py     # Batch buyer from trade list CSV (no live candle)
│   └── run_trades.py         # 3-stage live trading (Stage 1/2/3 with positions JSON)
│
├── zerodha/
│   ├── auth.py               # Kite session management
│   ├── trade.py              # buy(), sell(), order_status() via Kite — CLI too
│   ├── execute_trades.py     # Batch buyer from trade list CSV via Kite
│   └── run_trades.py         # 3-stage live trading via Kite (same logic as upstox/)
│
├── data/
│   ├── candles/              # ~940 per-symbol 15-min OHLCV CSV files
│   ├── instruments/
│   │   ├── upstox_instruments.csv   # symbol → instrument_key mapping
│   │   └── upstox_unmatched.csv     # symbols with no Upstox match
│   ├── market_cap_daily/     # Daily Screener.in exports + mcap_status.json
│   └── universe_combined.csv # All symbols ever seen in the 1,500–5,000 Cr band
│
├── results/
│   ├── trade_list_YYYY-MM-DD.csv    # Signal output (one per trading day)
│   ├── positions_upstox.json        # Upstox trade book (all-time, all statuses)
│   └── positions_zerodha.json       # Zerodha trade book
│
├── run_pipeline.sh           # Cron entry point — calls pipeline/run_daily.py
├── setup_vm.sh               # One-time VM provisioning script
└── .env.example              # Credential template — copy to pipeline/.env and fill in
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

### Add the 7 PM EOD Fill Cron (Manual Step)

The setup script does not add the EOD fill cron. Add it manually:

```bash
crontab -e
```

Add this line:

```
0 19 * * 1-5 cd /root/volume-daily && /usr/bin/python3.11 pipeline/fetch_candles.py --eod-fill >> /root/eod_fill.log 2>&1
```

This runs at 7:00 PM IST Mon–Fri, backfilling the 15:00 and 15:15 candles once the historical endpoint has settled data.

### Add Live Trading Crons (Optional)

If using `run_trades.py` for automated execution, add these three crons:

```
# Stage 1 — entry at 3:15 PM
15 15 * * 1-5 cd /root/volume-daily && /usr/bin/python3.11 upstox/run_trades.py --entry >> /root/trades.log 2>&1

# Stage 2 — exit check at 9:45 AM next morning
45 9 * * 2-6 cd /root/volume-daily && /usr/bin/python3.11 upstox/run_trades.py --exit-945 >> /root/trades.log 2>&1

# Stage 3 — forced exit at noon
0 12 * * 2-6 cd /root/volume-daily && /usr/bin/python3.11 upstox/run_trades.py --exit-1200 >> /root/trades.log 2>&1
```

Adjust `upstox` → `zerodha` if using Zerodha. Note Stage 2 and 3 run Tuesday–Saturday (the morning after Monday–Friday entries).

---

## Configuration

Copy `.env.example` to `pipeline/.env` and fill in all values. This file is gitignored and must never be committed.

| Key | Description |
|---|---|
| `SCREENER_EMAIL` | Screener.in login email (Premium account required for export) |
| `SCREENER_PASSWORD` | Screener.in password |
| `UPSTOX_ACCESS_TOKEN` | Upstox data token for candle fetches (from developer portal) |
| `UPSTOX_LIVE_API_KEY` | Upstox live trading API key |
| `UPSTOX_LIVE_API_SECRET` | Upstox live trading API secret |
| `UPSTOX_LIVE_REDIRECT_URI` | `http://127.0.0.1/` |
| `UPSTOX_SANDBOX_ACCESS_TOKEN` | Upstox sandbox static token (~30 days) |
| `UPSTOX_MODE` | `live` or `sandbox` — controls which mode `execute_trades.py` uses |
| `TELEGRAM_BOT_TOKEN` | Bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | Group chat ID (negative integer for supergroups, e.g. `-1004409929427`) |
| `ZERODHA_API_KEY` | Zerodha Kite API key |
| `ZERODHA_API_SECRET` | Zerodha Kite API secret |
| `ZERODHA_REDIRECT_URI` | `https://kite.trade/` |

> **Security**: `pipeline/.env` is in `.gitignore`. Never commit credentials. Use `.env.example` as the key-name reference only.

---

## Daily Operations

### Morning Token Refresh (Required for Live Trading)

The **Upstox live trading token** expires at midnight IST every day. Refresh it each morning before the 3 PM pipeline run:

```bash
python -m upstox.auth live
# Opens browser → log in → copy the redirect URL → paste it back
```

The token is saved to `upstox/.token_live.json` and reused automatically until midnight.

The **candle data token** (`UPSTOX_ACCESS_TOKEN`) is long-lived — update it in `.env` only when it eventually expires. Both `run_trades.py` scripts use this token for price data.

### Monitoring

```bash
# Live pipeline log stream
tail -f ~/pipeline.log

# Live trades log
tail -f ~/trades.log

# Last run summary
grep -E "(Starting|completed|FAILED|Stage)" ~/pipeline.log | tail -20

# Verify all crons are registered
crontab -l

# Verify system time is IST
date

# Check today's open positions
cat results/positions_upstox.json | python3 -m json.tool
```

### Pulling Updates

```bash
cd ~/volume-daily
git pull
```

---

## Manual Commands

### Pipeline

```bash
# Run the full pipeline manually
python3.11 pipeline/run_daily.py

# Run only the EOD candle fill
python3.11 pipeline/fetch_candles.py --eod-fill

# Test Telegram — success notification
python pipeline/notify.py --date 2026-07-19 --status success

# Test Telegram — failure notification
python pipeline/notify.py --date 2026-07-19 --status failed \
  --failed-step "Step 3" --error-msg "API timeout"
```

### 3-Stage Trading (Upstox)

```bash
# Stage 1 — entry dry run (preview without orders)
python upstox/run_trades.py --entry --dry-run

# Stage 1 — live entry
python upstox/run_trades.py --entry

# Stage 2 — exit check at 9:45 AM
python upstox/run_trades.py --exit-945

# Stage 3 — forced exit at 12 PM
python upstox/run_trades.py --exit-1200

# Run in sandbox mode
python upstox/run_trades.py --entry --mode sandbox
```

### 3-Stage Trading (Zerodha)

```bash
# Stage 1 — entry dry run
python zerodha/run_trades.py --entry --dry-run

# Stage 1 — live entry
python zerodha/run_trades.py --entry

# Stage 2 — exit check
python zerodha/run_trades.py --exit-945

# Stage 3 — forced exit
python zerodha/run_trades.py --exit-1200
```

### Batch Execution from CSV (Upstox)

```bash
# Preview today's trades without placing orders
python upstox/execute_trades.py --dry-run

# Execute today's trades (live)
python upstox/execute_trades.py

# Execute trades for a specific date
python upstox/execute_trades.py --date 2026-07-17

# Execute in sandbox mode
python upstox/execute_trades.py --mode sandbox --dry-run
```

### Single-Stock Manual Orders

Use these to test connectivity, verify tokens, or place/exit positions manually without any pipeline dependency:

```bash
# Upstox — place single buy
python upstox/trade.py buy RELIANCE NSE 1 MARKET

# Upstox — place single sell
python upstox/trade.py sell RELIANCE NSE 1 MARKET

# Upstox — check all today's orders
python upstox/trade.py orders

# Upstox — check a specific order
python upstox/trade.py status <order_id>

# Upstox — cancel an order
python upstox/trade.py cancel <order_id>

# Zerodha — same pattern
python zerodha/trade.py buy RELIANCE NSE 1 MARKET
python zerodha/trade.py sell RELIANCE NSE 1 MARKET
python zerodha/trade.py orders
```

### Auth

```bash
# Refresh Upstox live trading token
python -m upstox.auth live

# Verify current Upstox auth status
python -m upstox.auth
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
| `NO-DATA FALLBACK 9:45am` | When 09:43 candle is unavailable; half position sold |
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

All times IST (UTC+5:30). Cron expressions are in UTC.

| Time (IST) | UTC cron | Command | Purpose |
|---|---|---|---|
| 3:01 PM Mon–Fri | `1 9 * * 1-5` | `run_pipeline.sh` | Signal scan, candle update, trade list |
| 3:15 PM Mon–Fri | `15 9 * * 1-5` | `run_trades.py --entry` | Stage 1: buy entries |
| 7:00 PM Mon–Fri | `30 13 * * 1-5` | `fetch_candles.py --eod-fill` | Backfill 15:00 + 15:15 candles |
| 9:45 AM Tue–Sat | `15 4 * * 2-6` | `run_trades.py --exit-945` | Stage 2: exit if up |
| 12:00 PM Tue–Sat | `30 6 * * 2-6` | `run_trades.py --exit-1200` | Stage 3: forced exit |

> Note: `run_daily.py` exits in under 5 minutes typically; `run_trades.py --entry` runs immediately after at 3:15 PM. The 14-minute gap between crons (3:01 PM → 3:15 PM) is intentional — the pipeline writes the trade list, then the entry script picks it up.

---

*Pipeline runs Mon–Fri · DigitalOcean Ubuntu 22.04 · Python 3.11 · Upstox V3 API · Zerodha Kite API*
