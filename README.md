# NSE Volume Pipeline

Automated NSE mid-cap momentum scanner running daily at **3:01 PM IST** (Mon–Fri) on a DigitalOcean Ubuntu VM. Scans the ₹1,500–5,000 Cr market-cap band, fires on volume + return conditions, generates a trade list, and executes a full 3-stage live trading cycle via Zerodha (CNC delivery) — entry, next-day exit check, and forced noon exit. A separate, standalone script supports MTF (leveraged) entries. Upstox is used for market data only — no trading happens through Upstox.

---

## Table of Contents

- [Strategy Overview](#strategy-overview)
- [Signal Logic](#signal-logic)
- [Capital Allocation](#capital-allocation)
- [Complete Daily Flow](#complete-daily-flow)
  - [Part 1 — Signal Pipeline (3:01 PM)](#part-1--signal-pipeline-301-pm)
  - [Part 2 — EOD Candle Fill (3:45 PM)](#part-2--eod-candle-fill-345-pm)
  - [Part 3 — Live Trading (3-Stage, Zerodha CNC)](#part-3--live-trading-3-stage-zerodha-cnc)
  - [Part 4 — Trade Book (4:00 PM)](#part-4--trade-book-400-pm)
  - [Part 5 — Auto-Push Data & Results (4:30 PM)](#part-5--auto-push-data--results-430-pm)
- [MTF (Leveraged) Entries — Standalone Script](#mtf-leveraged-entries--standalone-script)
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
| Candle interval | 15-minute OHLCV (pipeline), 1-minute (live trading reference price) |
| Data source | Upstox V3 API (data/analytics only — no Upstox trading) |
| Market cap source | Screener.in Premium (live daily export) |
| Entry reference price | Close of the 15:14 candle (falls back to 15:13, then 14:45) |
| Broker / product type | Zerodha, Delivery (CNC) — MTF available via a separate standalone script |
| Live trading capital | ₹1,50,000 total (set via `--capital` in the entry cron) |
| Pipeline schedule | Mon–Fri, fully automated end-to-end via cron |

---

## Signal Logic

**All three conditions must pass** for a symbol to appear in the trade list. Implemented in `pipeline/signal_engine.py`.

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
Reference candle open ≥ 5% above previous trading day's VWAP
```

- **STRICT mode** (the real 3:01 PM run): reference candle is fixed at 15:00 (falls back to the 14:45 close if 15:00 hasn't posted yet)
- **PRORATED mode** (`scan/scan_intraday.py`, anytime preview): reference candle is whatever's latest as of the check time — this is a directional preview only, never read by any execution script

Previous day VWAP is calculated from the 15:00 and 15:15 candles of the prior trading day:

```
VWAP = Σ((H + L + C) / 3 × Volume) / Σ(Volume)
```

---

## Capital Allocation

**Two separate, independent allocations exist** — don't confuse them:

1. **`pipeline/main.py`'s own trade list** uses a hardcoded ₹5,00,000 reference to compute the `shares`/`ref_price` columns written to `results/trades/trade_list_<date>.csv` — this is display/notification sizing only, shown in the Telegram signal message.
2. **The actual live execution** (`zerodha/run_trades.py --entry --capital 150000`) ignores those CSV columns entirely — it only reads the `symbol` column, then recomputes quantity itself from the **real** capital passed via `--capital` and its own freshly-fetched 15:14 reference price.

Both use the same tiered rule — max ₹ per position is `capital/4` (idle capital allowed when signals ≤ 4); once signals hit 5+, the full capital splits equally instead of over-allocating:

```python
allocation = capital / 4 if n <= 4 else capital / n
shares     = floor(allocation / ref_price)
```

At the live ₹1,50,000 capital currently configured:

| Signals | Allocation per stock | Total deployed |
|---|---|---|
| 1–4 | ₹37,500 | up to ₹1,50,000 |
| 5 | ₹30,000 | ₹1,50,000 |
| 6 | ₹25,000 | ₹1,50,000 |
| n ≥ 5 | ₹1,50,000 ÷ n | ₹1,50,000 |

To change the live capital, edit the `--capital` value in the entry cron line (see [Cron Schedule Summary](#cron-schedule-summary)) — the `capital/4`-or-`n` cap is derived automatically, nothing else needs to change.

---

## Complete Daily Flow

### Timeline Overview

```
 9:15 AM  — Market opens
 9:45 AM  — [CRON] Stage 2: exit check — sell if live P&L > 0, else hold for noon
12:00 PM  — [CRON] Stage 3: forced exit — sell whatever's still open
 3:00 PM  — Last auction period begins
 3:01 PM  — [CRON] Signal pipeline: scans volume/return, writes trade list, notifies Telegram
 3:15 PM  — [CRON] Stage 1: fetch 15:14 reference candle, size positions, place buys
 3:45 PM  — [CRON] EOD fill: corrects/backfills 15:00 + 15:15 candles via intraday API
 4:00 PM  — [CRON] Regenerate results/trade_book.csv
 4:30 PM  — [CRON] Auto-commit + push data/results changes to GitHub
─────────── overnight hold, cycle repeats ─────────────────────────────────────
```

> All of the above are live cron jobs, Mon–Fri only (`1-5`) — see [Cron Schedule Summary](#cron-schedule-summary) for exact times and commands. The **Zerodha access token still requires a manual daily login before 9:45 AM** — see [Daily Operations](#daily-operations); nothing in this pipeline can automate that step (Kite Connect's OAuth login is a regulatory requirement, not a limitation of this code).

---

### Part 1 — Signal Pipeline (3:01 PM)

`run_pipeline.sh` is called by cron at 3:01 PM IST. It calls `pipeline/main.py`, which runs the following steps in sequence:

1. **Fetch market cap** — `data_loader.load_market_cap()` logs into Screener.in (Premium), runs the query `Market Capitalization > 1500 AND Market Capitalization < 5000`, and exports to `data/market_cap_daily/market_cap_<date>.csv`. Falls back to the most recent prior export (with a warning) if the live fetch fails, or fails the whole run if there's no fallback at all.
2. **Update universe** — compares today's market-cap symbols against `data/universe_combined.csv`; new symbols not previously seen are appended automatically.
3. **Candle data update** — for brand-new symbols: matches Upstox `instrument_key` and backfills 1 year of 15-min candles. For every symbol with a candle file: fetches today's intraday 15-min candles (in-progress candle at fetch time is necessarily incomplete until the 3:45 PM EOD fill corrects it).
4. **Signal check (STRICT mode)** — `signal_engine.get_signals()` applies all three conditions above.
5. **Write trade list** — `results/trades/trade_list_<date>.csv` (columns: `symbol`, `shares`, `ref_price` — see the capital-allocation caveat above; these sizing columns are for the notification only, not what actually gets bought).
6. **Notify** — `pipeline/notify.py` sends the signal count + trade table to Telegram (or a failure message with the failed step + error, if any step raised).

---

### Part 2 — EOD Candle Fill (3:45 PM)

Runs via cron at 3:45 PM IST, 15 minutes after NSE closes (3:30 PM). Fixes two real data-completeness gaps:

1. **The candle "in progress" when the 3:01 PM run fetches intraday data is incomplete** — collapsed OHLC, near-zero volume, a single-tick snapshot, not the settled candle.
2. **The final candle of the session (15:15) doesn't exist yet at 3:01 PM at all.**

```bash
python3.11 pipeline/data_loader.py --eod-fill
```

This uses the **intraday** endpoint (not the historical one — the historical endpoint returns zero rows for same-day dates), and lets freshly fetched data **overwrite** any existing row for the same timestamp, correcting the in-progress candle and adding the missing 15:15 bar. The file is re-sorted on every write.

---

### Part 3 — Live Trading (3-Stage, Zerodha CNC)

Trading execution is fully independent of the signal pipeline, runs entirely through **Zerodha** (`zerodha/run_trades.py`), and — as of the current cron config — runs **fully automatically every weekday**. Upstox is used only for the 1-minute reference-price candles.

#### Stage 1 — Entry at 3:15 PM

```bash
python zerodha/run_trades.py --entry --capital 150000
```

1. Reads `results/trades/trade_list_<today>.csv` (symbol column only — see [Capital Allocation](#capital-allocation))
2. Fetches today's 1-minute intraday candles via Upstox V3 for each symbol; reference price is the close of the **15:14 candle** (falls back to 15:13, then 14:45)
3. `shares = floor(allocation / ref_price)`, where `allocation` follows the `capital/4`-or-`n` rule
4. Places a MARKET BUY via Zerodha (`zerodha/trade.py`), with `market_protection` fixed at **0.75%** (see note below)
5. Polls for fill confirmation (up to 36 seconds, 12 retries × 3s); a genuinely rejected order (e.g. outside circuit limits) is logged and skipped, not recorded as a position
6. Writes entry details to `results/positions_zerodha.json`

> **`market_protection` note**: Kite's MARKET-order protection collar defaults to a *dynamic*, per-order percentage (observed live: 0.50%–2.00% on different orders the same day) — not a fixed rate. When that default happens to land at 1–2% on a stock already close to its circuit band, the protected price can exceed the exchange's real circuit limit and the order gets rejected outright, even though a real fill would have landed comfortably inside the band. Fixing it at **0.75%** (explicit, in `zerodha/trade.py`) keeps the collar predictable and tighter than what previously caused false "outside circuit limits" rejections.

#### Stage 2 — Exit Check at 9:45 AM (Next Day)

```bash
python zerodha/run_trades.py --exit-945
```

1. Loads all open positions from `results/positions_zerodha.json`
2. Pulls **Kite's own computed P&L** for each symbol from `/portfolio/positions` (same-day) or `/portfolio/holdings` (settled)
3. **If P&L > 0**: confirms broker-held quantity first (see broker-qty note below), then places a MARKET SELL for the full quantity → marks `exited_945`
4. **If P&L ≤ 0**: holds the position for the Stage 3 forced exit at noon
5. **If the symbol isn't found in either endpoint**: sells half the position as a precaution → marks `partial_exit_945_nodata` → remaining shares flow to Stage 3

#### Stage 3 — Forced Exit at 12:00 PM

```bash
python zerodha/run_trades.py --exit-1200
```

1. Loads all positions with status `open` or `partial_exit_945_nodata`
2. For each, cross-checks Zerodha-held quantity via `/portfolio/positions` and `/portfolio/holdings` before selling — mismatches are skipped for manual review, not force-sold
3. Places a MARKET SELL for the confirmed quantity
4. Calculates blended P&L across any partial 9:45 AM exit and the 12 PM remainder
5. Marks positions `exited_1200` and prints a daily summary

> **Broker-qty / T1 settlement note**: Kite splits holding quantity into `quantity` (fully settled) and `t1_quantity` (bought the previous trading day, pending T+1 settlement) — both are sellable. The broker-qty check in Stages 2 and 3 sums `quantity + t1_quantity`; earlier it only read `quantity`, which saw `0` for every position bought the day before and falsely skipped real exits as "mismatches."

> **`--dry-run` note**: all three stages accept `--dry-run` (simulates without placing real orders) — and `_save_pos()` is correctly gated behind `if not dry_run`, so a dry run never overwrites `results/positions_zerodha.json` with fabricated data.

#### Positions JSON Schema

```json
{
  "broker": "zerodha",
  "symbol": "HUHTAMAKI",
  "entry_date": "2026-07-22",
  "reference_price": 293.01,
  "shares_intended": 8,
  "actual_fill_price": 293.57,
  "actual_fill_quantity": 8,
  "entry_order_id": "260722221221771",
  "status": "exited_945",
  "entry_timestamp": "2026-07-22T15:15:07+05:30",
  "exit_price_945": 310.33,
  "exit_order_id_945": "260723220291770",
  "exit_timestamp_945": "2026-07-23T09:45:14+05:30",
  "realized_return_pct": 5.709,
  "realized_pnl": 134.08
}
```

Status progression: `open` → `exited_945` **or** `partial_exit_945_nodata` → `exited_1200`.

#### Alternative: execute_trades.py (Simpler, No Live Candle)

```bash
python zerodha/execute_trades.py
```

Reads `results/trades/trade_list_<date>.csv` directly (pre-calculated `shares`/`ref_price` from the ₹5L reference sizing). No positions JSON, no fill polling, no stages, no automatic exit — a batch buy you manage yourself.

---

### Part 4 — Trade Book (4:00 PM)

```bash
python3.11 zerodha/build_trade_book.py
```

Flattens `results/positions_zerodha.json` into `results/trade_book.csv` — one row per position with: **Stock Name, Position entry date, Position Exit date, No of shares, Entry Price, Exit Price, Realised PnL, Realised PnL Pct**. Open positions show entry-side fields only; exit fields stay blank until they close. Runs daily right after the 3:45 PM EOD fill, so it always reflects that day's Stage 2/3 exits.

> **Charges caveat**: figures in this file are **gross** P&L — brokerage (₹0 for Zerodha equity delivery), STT, exchange/SEBI charges, and GST can be estimated live via `POST /margins/orders`, but that endpoint is a *pre-trade margin estimator*, not a settled contract note, and consistently undershoots the real number (confirmed against Console: it omitted stamp duty entirely in every response tested). **DP (depository participant) charges — ₹13 + 18% GST = ₹15.34 per scrip per sell, confirmed exact against Console — are not available via any Kite Connect API at all** and are not included in this file. On small position sizes, DP charges alone can turn a "profitable" gross trade net-negative.

---

### Part 5 — Auto-Push Data & Results (4:30 PM)

```bash
./push_data_updates.sh
```

Stages **only** `data/` and `results/` (candles, market cap, instruments, positions, trade book, trade lists) — never code (`zerodha/`, `pipeline/`, `scan/`), so an in-progress code edit can never get swept into an unattended commit. Commits as `Data update — <date> candle + results refresh` and pushes to `main`. If nothing changed that day, it exits cleanly with no empty commit.

---

## MTF (Leveraged) Entries — Standalone Script

```bash
python zerodha/run_trades_mtf.py --entry --capital 150000 [--dry-run]
python zerodha/run_trades_mtf.py --entry --symbol CHEMPLASTS --shares 3 [--dry-run]
python zerodha/run_trades_mtf.py --entry --symbol "NSE_EQ|INE002A01018" --shares 5   # for symbols outside the tracked universe
```

**Completely separate from the CNC flow** — `run_trades.py` and `trade.py` are untouched by this script; `trade.py`'s `buy()` already accepted `product` as a parameter, so no new order-placement code was needed there. Key differences from CNC entry:

- Same trade list, same reference-price logic, same `capital/4`-or-`n` allocation rule — but places orders with `product="MTF"` instead of `"CNC"`.
- **Before every order**: checks live per-symbol `leverage` and required margin via `POST /margins/orders` (product=MTF) — leverage is per-stock and time-varying, never assumed fixed — then confirms `GET /user/margins` covers it. Insufficient margin or a failed lookup **skips that symbol** (logged) without halting the rest of the run.
- Logs to `results/trades/mtf_entries_<date>.csv` (symbol, quantity, ref/fill price, leverage, margin required, order ID, status) — kept separate from `positions_zerodha.json` so it can never be picked up by the CNC Stage 2/3 exit logic, which has no MTF-exit awareness at all.
- `--symbol`/`--shares`: buy one specific stock manually instead of reading the day's trade list — `--capital` in this mode is the amount for *that one stock* (not divided by 4, since the /4 rule is about proportioning a multi-signal batch, not a deliberate single pick).
- Prints a reminder at the end of every run: **MTF buys trigger a same-day email pledge approval (by ~7 PM) that this script cannot complete** — check email/Kite manually.

**Not wired into any cron job** — run manually until tested live in real market hours.

---

## Repository Structure

```
volume-daily/
├── pipeline/
│   ├── main.py                # Daily orchestrator — called by cron at 3:01 PM
│   ├── fetch_market_cap.py    # Logs into Screener.in, exports market cap CSV
│   ├── data_loader.py         # Upstox V3 — historical + intraday + EOD fill + market cap loader
│   ├── signal_engine.py       # Signal conditions (market cap / volume / return) — STRICT + PRORATED modes
│   ├── notify.py              # Telegram notifications (pipeline success/failure, scan preview)
│   ├── requirements.txt       # pandas, requests, python-dotenv
│   └── .env                   # ← NOT in git — credentials live here
│
├── scan/
│   └── scan_intraday.py       # Anytime preview scanner (PRORATED mode) — not read by any execution script
│
├── zerodha/
│   ├── auth.py                 # Kite session management (daily login, expires 6 AM IST)
│   ├── trade.py                 # buy(), sell(), order_status() via Kite — CLI too; market_protection fixed at 0.75%
│   ├── execute_trades.py       # Batch buyer from trade list CSV via Kite
│   ├── run_trades.py           # 3-stage live trading (CNC) via Kite — Stage 1/2/3 with positions JSON
│   ├── run_trades_mtf.py       # Standalone MTF entry script — separate from the CNC flow entirely
│   └── build_trade_book.py     # Flattens positions_zerodha.json → results/trade_book.csv
│
├── data/
│   ├── candles/                 # Per-symbol 15-min OHLCV CSV files (data only — no Upstox trading)
│   ├── instruments/
│   │   ├── upstox_instruments.csv   # symbol → instrument_key mapping
│   │   └── upstox_unmatched.csv     # symbols with no Upstox match
│   ├── market_cap_daily/        # Daily Screener.in exports + mcap_status.json
│   └── universe_combined.csv    # All symbols ever seen in the 1,500–5,000 Cr band
│
├── results/
│   ├── trades/
│   │   ├── trade_list_YYYY-MM-DD.csv     # Official signal output (one per trading day)
│   │   └── mtf_entries_YYYY-MM-DD.csv    # MTF script's own log (see above)
│   ├── scans/                            # scan_intraday.py preview output (not read by any execution script)
│   ├── positions_zerodha.json            # Zerodha CNC trade book (all-time, all statuses)
│   └── trade_book.csv                    # Flattened per-position P&L view (regenerated daily at 4 PM)
│
├── run_pipeline.sh            # Cron entry point — calls pipeline/main.py
├── push_data_updates.sh       # Auto-commit + push data/results — cron'd 4:30 PM
├── setup_vm.sh                # One-time VM provisioning script
└── .env.example                # Credential template — copy to pipeline/.env and fill in
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

After filling in `.env`, run a manual test:

```bash
python3.11 pipeline/main.py
```

Then register all 7 cron jobs listed in [Cron Schedule Summary](#cron-schedule-summary) via `crontab -e`.

---

## Configuration

Copy `.env.example` to `pipeline/.env` and fill in all values. This file is gitignored and must never be committed.

| Key | Description |
|---|---|
| `SCREENER_EMAIL` | Screener.in login email (Premium account required for export) |
| `SCREENER_PASSWORD` | Screener.in password |
| `UPSTOX_ACCESS_TOKEN` | Upstox data token for candle fetches (from developer portal). Analytics only — no Upstox trading. |
| `TELEGRAM_BOT_TOKEN` | Bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | Group chat ID (negative integer for supergroups) |
| `ZERODHA_API_KEY` | Zerodha Kite API key — used for order execution |
| `ZERODHA_API_SECRET` | Zerodha Kite API secret |
| `ZERODHA_REDIRECT_URI` | `https://kite.trade/` |

> **Security**: `pipeline/.env` is in `.gitignore`. Never commit credentials. Use `.env.example` as the key-name reference only.

---

## Daily Operations

### Morning Token Refresh (Required — Cannot Be Automated)

The **Zerodha (Kite Connect) trading token** expires at 6:00 AM IST every day — a regulatory requirement, not something this codebase can work around. Refresh it each morning **before the 9:45 AM exit-check cron fires**:

```bash
python -m zerodha.auth
# Opens browser → log in → copy the redirect URL → paste it back
```

If the token is missing/expired, `zerodha.auth._login()` falls through to an interactive prompt that blocks waiting for stdin — in a non-interactive/cron context this just hangs/times out, so **the exit-945 and exit-1200 jobs will silently fail to close positions if you forget this step.** The token is saved to `zerodha/.token.json` and reused automatically until the next 6 AM IST expiry.

The **candle data token** (`UPSTOX_ACCESS_TOKEN`) is long-lived — update it in `.env` only when it eventually expires.

### Monitoring

```bash
# Live pipeline log stream
tail -f ~/pipeline.log

# Live trades log
tail -f ~/trades_test.log

# Trade book / auto-push logs
tail -f ~/trade_book.log
tail -f ~/push_data_updates.log

# Verify all crons are registered
crontab -l

# Verify system time is IST
date

# Check today's open positions
python3 -m json.tool results/positions_zerodha.json

# Check the flattened P&L view
column -s, -t results/trade_book.csv
```

### Pulling Updates

```bash
cd ~/volume-daily
git pull
```

Note: `run_pipeline.sh` does not `git pull` automatically — code updates pushed to GitHub do not reach the VM until you pull manually. (Data/results changes, in the other direction, *do* push automatically at 4:30 PM via `push_data_updates.sh`.)

---

## Manual Commands

### Pipeline

```bash
# Run the full pipeline manually
python3.11 pipeline/main.py

# Run only the EOD candle fill
python3.11 pipeline/data_loader.py --eod-fill

# Preview signals at any time of day (PRORATED mode — not the official 3:01 PM run)
python3.11 scan/scan_intraday.py
```

### 3-Stage Trading (Zerodha CNC)

```bash
# Stage 1 — entry dry run (preview without orders)
python zerodha/run_trades.py --entry --capital 150000 --dry-run

# Stage 1 — live entry
python zerodha/run_trades.py --entry --capital 150000

# Stage 2 — exit check at 9:45 AM
python zerodha/run_trades.py --exit-945

# Stage 3 — forced exit at 12 PM
python zerodha/run_trades.py --exit-1200
```

### MTF (Leveraged) Entries

```bash
# Full daily trade list, MTF instead of CNC
python zerodha/run_trades_mtf.py --entry --capital 150000 --dry-run

# One specific stock, exact share count
python zerodha/run_trades_mtf.py --entry --symbol CHEMPLASTS --shares 3 --dry-run
```

### Trade Book

```bash
python3.11 zerodha/build_trade_book.py
```

### Batch Execution from CSV

```bash
python zerodha/execute_trades.py --dry-run
python zerodha/execute_trades.py
python zerodha/execute_trades.py --date 2026-07-17
```

### Single-Stock Manual Orders

```bash
python -m zerodha.trade buy RELIANCE NSE 1 MARKET
python -m zerodha.trade sell RELIANCE NSE 1 MARKET
python -m zerodha.trade orders               # all today's orders
python -m zerodha.trade status <order_id>    # single order status
python -m zerodha.trade cancel <order_id>    # cancel an order
```

### Auth

```bash
# Refresh Zerodha trading token (daily, before 9:45 AM)
python -m zerodha.auth
```

---

## Telegram Notifications

Only two notification types are currently active — all per-trade notifications (entry/exit/summary) were **removed** to reduce noise; the functions still exist in `pipeline/notify.py` but `zerodha/run_trades.py` no longer calls them.

| Event | Sent by | When |
|---|---|---|
| Pipeline **success** | `pipeline/main.py` | After the 3:01 PM run completes — date, signal count, runtime, full trade table |
| Pipeline **failure** | `pipeline/main.py` | If any step crashes — date, failed step, runtime, error detail |
| Scan **preview** | `scan/scan_intraday.py` | Whenever run manually — clearly labeled as a preview, not the official signal |

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

All times IST (UTC+5:30), server timezone set to `Asia/Kolkata`. All 7 jobs are Mon–Fri only (`1-5`).

| Time (IST) | Cron | Command |
|---|---|---|
| 9:45 AM | `45 9 * * 1-5` | `zerodha/run_trades.py --exit-945` |
| 12:00 PM | `0 12 * * 1-5` | `zerodha/run_trades.py --exit-1200` |
| 3:01 PM | `1 15 * * 1-5` | `run_pipeline.sh` |
| 3:15 PM | `15 15 * * 1-5` | `zerodha/run_trades.py --entry --capital 150000` |
| 3:45 PM | `45 15 * * 1-5` | `pipeline/data_loader.py --eod-fill` |
| 4:00 PM | `0 16 * * 1-5` | `zerodha/build_trade_book.py` |
| 4:30 PM | `30 16 * * 1-5` | `push_data_updates.sh` |

`zerodha/run_trades_mtf.py` is **not** in this list — it's a standalone script, run manually.

---

*Pipeline runs Mon–Fri · DigitalOcean Ubuntu 22.04 · Python 3.11 · Upstox V3 API (data) · Zerodha Kite Connect (execution, CNC + standalone MTF)*
