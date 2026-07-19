# NSE Volume Pipeline

Automated NSE mid-cap momentum scanner running daily at **3:01 PM IST** (Mon–Fri) on a DigitalOcean Ubuntu VM. Scans ~940 symbols in the ₹1,500–5,000 Cr market-cap band, fires on volume + return conditions, and sends results to Telegram.

---

## Table of Contents

- [Strategy Overview](#strategy-overview)
- [Signal Logic](#signal-logic)
- [Capital Allocation](#capital-allocation)
- [How the Pipeline Works](#how-the-pipeline-works)
- [Repository Structure](#repository-structure)
- [First-Time VM Setup](#first-time-vm-setup)
- [Configuration](#configuration)
- [Daily Operations](#daily-operations)
- [Manual Commands](#manual-commands)
- [Telegram Notifications](#telegram-notifications)

---

## Strategy Overview

The pipeline targets **NSE equities with market cap ₹1,500–5,000 Cr**. It screens for stocks where unusual volume has built throughout the day *and* the price has broken above the prior day's close, entering at the 3:15 PM market open with a defined capital budget.

| Parameter | Value |
|---|---|
| Universe | NSE EQ/BE segment, ₹1,500–5,000 Cr |
| Candle interval | 15-minute OHLCV |
| Data source | Upstox V3 API |
| Market cap source | Screener.in Premium (live daily export) |
| Entry | Market buy at 3:15 PM IST (open of the 15:00 candle) |
| Product | Delivery (CNC) |
| Capital | ₹5,00,000 total |
| Schedule | Mon–Fri at 3:01 PM IST via cron |

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

### 3. Return Condition

```
Open of 15:00 candle ≥ 5% above previous trading day's VWAP
```

Previous day VWAP is calculated from the 15:00 and 15:15 candles of the prior trading day:

```
VWAP = Σ((H + L + C) / 3 × Volume) / Σ(Volume)
```

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

Shares = `floor(allocation / open_of_3pm_candle)`

---

## How the Pipeline Works

`run_pipeline.sh` is called by cron at 3:01 PM IST. It calls `pipeline/run_daily.py`, which runs four steps in sequence:

### Step 1 — Fetch Market Cap

`fetch_market_cap.py` logs into Screener.in (Premium), runs the query `Market Capitalization > 1500 AND Market Capitalization < 5000`, and exports the result to `data/market_cap_daily/market_cap_<date>.csv`.

Exit codes:
- `0` — fresh data saved
- `2` — Screener.in unavailable; using previous export as stale fallback (pipeline continues with warning)
- `1` — no data at all (pipeline fails)

### Step 2 — Update Universe

Compares today's market-cap symbols against `data/universe_combined.csv`. New symbols not previously seen are appended automatically. This keeps the tracked universe expanding as stocks enter the cap band.

### Step 3 — Candle Data Update

Two sub-operations run:

1. **New symbols** — downloads the Upstox NSE instrument master, matches new symbols to their `instrument_key`, then backfills 1 year of 15-min candles into `data/candles/<SYMBOL>.csv`
2. **All symbols** — fetches today's intraday 15-min candles and appends new rows to every existing candle file

Rate limiting: Upstox allows ~66 req/min. The fetcher uses 5 parallel workers with a 0.8s per-call delay.

### Step 4 — Generate Trade List

`prepare_data.py` applies all three signal conditions to every symbol that has both a candle file and a current market-cap record. Passing symbols are written to `results/trade_list_<date>.csv` (columns: `symbol`, `shares`, `ref_price`). If no signals fire, no file is written and the pipeline exits cleanly.

### Notification

`notify.py` sends a message to the **"NSE Volume Alerts"** Telegram group via `@nse_volume_alerts_bot`. A success message includes the full trade table; a failure message includes the failed step and error detail. Notification failures are logged as warnings and do not crash the pipeline.

---

## Repository Structure

```
volume-daily/
├── pipeline/
│   ├── run_daily.py          # Daily orchestrator — called by cron
│   ├── fetch_market_cap.py   # Logs into Screener.in, exports market cap CSV
│   ├── fetch_candles.py      # Upstox V3 — historical + intraday 15-min candles
│   ├── prepare_data.py       # Signal scanner — produces trade_list_<date>.csv
│   ├── notify.py             # Telegram notifications (success / failure)
│   ├── requirements.txt      # pandas, requests, python-dotenv
│   └── .env                  # ← NOT in git — credentials live here
│
├── upstox/
│   ├── auth.py               # OAuth login + token persistence (live / sandbox)
│   ├── trade.py              # buy(), sell(), cancel_order(), order_status()
│   └── execute_trades.py     # Reads trade list CSV, places MARKET BUY orders
│
├── zerodha/                  # Zerodha auth scaffold (not used in pipeline)
│
├── data/
│   ├── candles/              # ~940 per-symbol 15-min OHLCV CSV files
│   ├── instruments/
│   │   ├── upstox_instruments.csv   # symbol → instrument_key mapping
│   │   └── upstox_unmatched.csv     # symbols with no Upstox match
│   ├── market_cap_daily/     # Daily Screener.in exports + mcap_status.json
│   └── universe_combined.csv # All symbols ever seen in the 1,500–5,000 Cr band
│
├── results/                  # trade_list_YYYY-MM-DD.csv (one file per trading day)
├── run_pipeline.sh           # Cron entry point — calls pipeline/run_daily.py
├── setup_vm.sh               # One-time VM provisioning script
└── .env.example              # Credential template — copy to pipeline/.env and fill in
```

---

## First-Time VM Setup

**Prerequisites**: DigitalOcean Ubuntu 22.04+ droplet. Your SSH public key must be added to the droplet. The GitHub repo must be accessible from the VM (deploy key or HTTPS).

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
- Registers the cron job: `1 15 * * 1-5 /bin/bash ~/volume-daily/run_pipeline.sh >> ~/pipeline.log 2>&1`

After filling in `.env`, verify everything works:

```bash
python3.11 pipeline/run_daily.py
```

---

## Configuration

Copy `.env.example` to `pipeline/.env` and fill in all values. This file is gitignored and must never be committed.

| Key | Description |
|---|---|
| `SCREENER_EMAIL` | Screener.in login email (Premium account required for export) |
| `SCREENER_PASSWORD` | Screener.in password |
| `UPSTOX_ACCESS_TOKEN` | Upstox data token for candle fetches (long-lived; from developer portal) |
| `UPSTOX_LIVE_API_KEY` | Upstox live trading API key |
| `UPSTOX_LIVE_API_SECRET` | Upstox live trading API secret |
| `UPSTOX_LIVE_REDIRECT_URI` | `http://127.0.0.1/` |
| `UPSTOX_SANDBOX_ACCESS_TOKEN` | Upstox sandbox static token (~30 days) |
| `UPSTOX_MODE` | `live` or `sandbox` — controls which mode `execute_trades.py` uses |
| `TELEGRAM_BOT_TOKEN` | Bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | Group chat ID (negative integer for supergroups, e.g. `-1004409929427`) |
| `ZERODHA_API_KEY` | Zerodha Kite API key (not used by pipeline; for manual use) |
| `ZERODHA_API_SECRET` | Zerodha Kite API secret |
| `ZERODHA_REDIRECT_URI` | `https://kite.trade/` |

> **Security**: `pipeline/.env` is in `.gitignore`. Never commit credentials. Use `.env.example` as the reference for key names only.

---

## Daily Operations

### Token Refresh

The pipeline uses two Upstox tokens with different lifecycles:

**Candle data token** (`UPSTOX_ACCESS_TOKEN`): Used by `fetch_candles.py` to pull historical and intraday OHLCV data. This is a long-lived token obtained from the Upstox developer portal — update it in `.env` when it eventually expires.

**Live trading token**: Used by `execute_trades.py` to place orders. This token **expires at midnight IST every day**. Refresh it each morning before the 3 PM run:

```bash
python -m upstox.auth live
# Opens browser → log in → copy the redirect URL → paste it back
```

The token is saved to `upstox/.token_live.json` and reused automatically until midnight.

### Monitoring

```bash
# Live log stream
tail -f ~/pipeline.log

# Last run summary
grep -E "(Starting|completed|FAILED)" ~/pipeline.log | tail -20

# Verify cron is registered
crontab -l

# Verify system time is IST
date
```

### Pulling Updates

```bash
cd ~/volume-daily
git pull
```

---

## Manual Commands

```bash
# Run the full pipeline manually
python3.11 pipeline/run_daily.py

# Preview today's trades without placing orders
python upstox/execute_trades.py --dry-run

# Execute today's trades (live)
python upstox/execute_trades.py

# Execute trades for a specific date
python upstox/execute_trades.py --date 2026-07-17

# Execute in sandbox mode (dry run)
python upstox/execute_trades.py --mode sandbox --dry-run

# Test Telegram — success notification
python pipeline/notify.py --date 2026-07-17 --status success

# Test Telegram — failure notification
python pipeline/notify.py --date 2026-07-17 --status failed \
  --failed-step "Step 3" --error-msg "API timeout"

# Place a single manual buy order
python upstox/trade.py buy RELIANCE NSE 10 MARKET

# Place a manual sell order
python upstox/trade.py sell RELIANCE NSE 10 MARKET

# Check today's all orders
python upstox/trade.py orders

# Check a specific order's status
python upstox/trade.py status <order_id>

# Cancel an order
python upstox/trade.py cancel <order_id>

# Refresh live trading token
python -m upstox.auth live

# Verify current auth status
python -m upstox.auth
```

---

## Telegram Notifications

Notifications go to the **"NSE Volume Alerts"** Telegram group via `@nse_volume_alerts_bot`.

**Success message** includes:
- Date, number of signals, pipeline runtime
- Trade table: symbol | shares | entry price (3 PM open)
- Warning banner if market cap data was stale

**Failure message** includes:
- Date, failed step name, runtime
- Full error message from the exception

### Setting Up on a New Bot or Group

1. Create a bot via `@BotFather` → copy the token → set `TELEGRAM_BOT_TOKEN` in `pipeline/.env`
2. Add the bot to your Telegram group
3. Send a message in the group, then run:
   ```bash
   curl "https://api.telegram.org/bot<TOKEN>/getUpdates"
   ```
4. Find `chat.id` in the response (negative integer for supergroups) → set `TELEGRAM_CHAT_ID` in `pipeline/.env`

---

*Pipeline runs Mon–Fri at 3:01 PM IST · DigitalOcean Ubuntu 22.04 · Python 3.11 · Upstox V3 API*
