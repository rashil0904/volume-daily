# NSE Volume Pipeline ("volume-daily-btst")

Automated NSE mid-cap momentum/gap-up scanner and live trading system, running unattended
on a DigitalOcean Ubuntu VM via cron. Scans the ₹1,500–5,000 Cr market-cap band for unusual
volume + a strong intraday gap, buys near the close, holds overnight (**BTST** — Buy Today,
Sell Tomorrow), and exits the next trading morning on a schedule keyed to how strong the
original signal was. Candle data, live reference prices, and live order execution all go
through **Dhan**.

**For the full strategy in detail** (signal logic, entry/exit mechanics, capital, charges
model, reconciliation, notifications) **see [`STRATEGY.md`](STRATEGY.md)**. **For the
history of every real bug fixed and design decision made along the way — essential context
for understanding *why* things are built the way they are — see
[`ISSUES_RESOLVED.md`](ISSUES_RESOLVED.md)**. This README covers setup, configuration, and
day-to-day operations only.

**Zerodha's execution code was removed 2026-09-10, pending a full rebuild** that never
happened — it previously ran a parallel live trading pipeline against the same signals. Its
historical trade data (`results/positions_zerodha_long.json`, `positions_zerodha_short.json`,
`trade_book.xlsx`) was kept, not deleted, but none of it is live.

---

## Table of Contents

- [Quick Facts](#quick-facts)
- [Signal Logic (summary)](#signal-logic-summary)
- [Complete Daily Flow](#complete-daily-flow)
- [Dhan Live Trading (summary)](#dhan-live-trading-summary)
- [Repository Structure](#repository-structure)
- [First-Time VM Setup](#first-time-vm-setup)
- [Configuration](#configuration)
- [Daily Operations](#daily-operations)
- [Manual Commands](#manual-commands)
- [Testing](#testing)
- [Telegram Notifications](#telegram-notifications)
- [Cron Schedule Summary](#cron-schedule-summary)

---

## Quick Facts

| | |
|---|---|
| Universe | NSE EQ/BE, ₹1,500–5,000 Cr market cap |
| Signal conditions | 6× 36-day avg volume **and** ≥5% gap over prior-day VWAP |
| Live trading capital | **₹11,00,000** (`TOTAL_CAPITAL`, effective 2026-11-01 — see [`STRATEGY.md §3`](STRATEGY.md#3-capital-allocation)) |
| Entry | Tranched LIMIT (~3:06–3:20 PM) + a MARKET safety-net fallback (3:21 PM) |
| Exit | Return-bucketed 3-stage schedule — see [`STRATEGY.md §5`](STRATEGY.md#5-exit--return-bucketed-3-stage-schedule) |
| Broker (live) | Dhan — candle data, reference prices, and order execution all go through it |
| P&L tracking | One workbook, `results/volume-daily-btst-pnl.xlsx`, synced to Google Drive daily |
| Data source | Dhan Data API (`/charts/intraday`) — migrated off Upstox 2026-09-24 |

---

## Signal Logic (summary)

**All three conditions must pass** (`pipeline/signal_engine.py`):
1. **Market cap** ₹1,500–5,000 Cr (Screener.in Premium export)
2. **Volume**: cumulative volume (09:15–14:45) ≥ 6× the 36-trading-day rolling average
   full-day volume
3. **Return**: the 15:00 reference candle close (falls back to 14:45) ≥ 5% above the prior
   trading day's VWAP

Full detail, including the exact VWAP formula and the PRORATED preview mode, is in
[`STRATEGY.md §2`](STRATEGY.md#2-signal-logic-pipelinesignal_enginepy).

---

## Complete Daily Flow

```
 9:10 AM  — [CRON] Live monitor starts (tick-by-tick qualifying-condition watch)
 9:12 AM  — [CRON] --reconcile: catches any position the broker already closed/filled
            that our own confirmation poll missed
 9:15 AM  — [CRON] --place-targets: 17% profit-target LIMIT sells for every open long
 9:15–9:44 AM — [CRON] Exit checks (--exit-916), one per return bucket (see below)
10:17–11:59 AM — [CRON] Forced exits (--exit-1159), one per return bucket
 2:28–2:57 PM  — [CRON] Short square-offs (--square-off-239), one per return bucket
 3:06 PM  — [CRON] Signal pipeline: scans volume/return, writes trade list, notifies Telegram
 3:06–3:20 PM — Tranched LIMIT entry fires immediately after the pipeline (run_entry_limit)
 3:20 PM  — [CRON] MARKET-entry safety net for anything still unfilled (--entry)
 3:40 PM  — [CRON] Live monitor stopped
 4:00 PM  — [CRON] EOD candle fill: corrects the in-progress 15:00 candle, adds the 15:15 one
 4:15 PM  — [CRON] Auto-push data/results changes to GitHub
 4:45 PM  — [CRON] --sync-pnl: regenerates the P&L workbook, pushes it to Google Drive
─────────── overnight hold, cycle repeats ─────────────────────────────────────
```

See [Cron Schedule Summary](#cron-schedule-summary) for the exact times (every exit-stage
line above is really 3 separate cron lines, one per return bucket) and
[`STRATEGY.md`](STRATEGY.md) for what each stage actually decides.

### Signal Pipeline (3:06 PM) — `pipeline/main.py`

1. **Fetch market cap** — logs into Screener.in (Premium), exports the ₹1,500–5,000 Cr band.
   Falls back to the most recent prior export (with a warning) if the live fetch fails.
2. **Update universe** — new symbols get appended to `data/universe_combined.csv`.
3. **Candle data update** — backfills 6 months of 15-min candles for brand-new symbols,
   fetches today's intraday candles for every tracked symbol.
4. **Signal check (STRICT mode)** — `signal_engine.get_signals()`.
5. **Write trade list** — `results/trades/trade_list_<date>.csv`.
6. **Notify Telegram** — signal count + trade table, or a failure message naming the failed
   step.

### EOD Candle Fill (4:00 PM) — `pipeline/data_loader.py --eod-fill`

Fixes two real data-completeness gaps: the candle "in progress" when the 3:06 PM run
fetched intraday data is incomplete (collapsed OHLC), and the session's final 15:15 candle
doesn't exist yet at 3:06 PM at all. Uses the intraday endpoint (the historical one returns
zero rows for same-day dates) and overwrites any existing row for the same timestamp.

### Auto-Push Data & Results (4:15 PM) — `scripts/push_data_updates.sh`

Stages **only** `data/` and `results/` — never code, so an in-progress code edit can never
get swept into an unattended commit. Commits as `Data update — <date> candle + results
refresh` and pushes to `main`. Exits cleanly with no empty commit if nothing changed.

---

## Dhan Live Trading (summary)

Full detail (entry mechanics, exit schedule, mirrored shorts, charges model,
reconciliation) is in [`STRATEGY.md`](STRATEGY.md). Quick reference:

| Parameter | Value |
|---|---|
| Live trading capital | **₹11,00,000** (`TOTAL_CAPITAL` in `dhan/run_trades.py`) |
| Product type | MTF (leverage ≥2x) or CNC (fallback, half capital), per-symbol live check |
| Entry | Tranched LIMIT at best_bid+1 tick (`run_entry_limit`, primary) + LIMIT 0.75% above LTP MARKET-safety-net (`run_entry_321`, backup) |
| Profit target | 17% LIMIT sell, placed 9:15 AM for every open long (capped at 0.5% below the day's upper circuit if needed) |
| Exit schedule | 3 independent return-bucketed schedules (5-10% / 10-15% / 15-20%) — see [`STRATEGY.md §5`](STRATEGY.md#5-exit--return-bucketed-3-stage-schedule) |
| Mirrored shorts | Opened on any long exit with realized return ≥3%; 5% cover target + UC-based stop-loss |
| Reconciliation | `--reconcile` (9:12 AM) + `_reconcile_pending_shorts` (every square-off run) — catches anything the broker did that a confirmation poll missed; see [`ISSUES_RESOLVED.md §3`](ISSUES_RESOLVED.md#3--phantom-fills-and-the-reconciliation-philosophy) |
| Auth | 24h access token, auto-renews itself 8 AM + 8 PM daily — no OAuth handshake |

> **Why LIMIT, not MARKET, for the safety-net entry**: Dhan/NSE silently apply their own
> price-protection band to a `MARKET` order (confirmed live 2026-08-17) — ordinary
> same-second price movement produced 0-fills. An explicit LIMIT 0.75% above live LTP gives
> wider, predictable headroom instead.

### Positions JSON Schema

Longs (`results/positions_dhan_long.json`) and shorts (`results/positions_dhan_short.json`)
live in **separate files** (not one combined file with a `direction` field — a same-day
mirrored short used to silently block a fresh same-day long re-entry on the same symbol
when they shared a file; see [`ISSUES_RESOLVED.md §12`](ISSUES_RESOLVED.md#12--position-file-structure)).

```json
{
  "broker": "dhan", "symbol": "TVSSRICHAK", "entry_date": "2026-08-18",
  "actual_fill_price": 4836.00, "actual_fill_quantity": 3,
  "entry_order_id": "...", "status": "exited_916", "product": "CNC",
  "return_bucket": "5-10", "target_order_id": "...", "target_price": 5658.12,
  "exit_price_916": 5658.12, "exit_order_id_916": "...",
  "realized_return_pct": 17.0, "realized_pnl": 2466.36
}
```

```json
{
  "broker": "dhan", "symbol": "TVSSRICHAK", "direction": "short",
  "entry_price": 5658.12, "quantity": 3, "product": "INTRADAY",
  "source_exit_stage": "916", "return_bucket": "5-10", "status": "short_closed",
  "cover_target_order_id": "...", "cover_target_price": 5375.21,
  "stop_order_id": "...", "stop_trigger_price": 5820.00,
  "exit_price_239": 5375.21, "exit_order_id_239": "...",
  "realized_return_pct": 5.0, "realized_pnl": 848.73
}
```

### Auth (Automated Daily Renewal, Manual Fallback)

Unlike Kite Connect, Dhan has no OAuth handshake for a personal account — the **very first**
token still has to be generated by hand at **web.dhan.co → Profile → Access DhanHQ Trading
APIs** (valid 24h):

```bash
python -m dhan.auth <ACCESS_TOKEN>
```

From then on, `python -m dhan.auth --renew` renews it automatically, twice a day (8:00 AM
and 8:00 PM IST) via `GET /v2/RenewToken`, verifying the new token with a real `/fundlimit`
call before saving — a failed renewal (or failed verification) leaves the previous saved
token completely untouched. Saved to `dhan/.token.json` (gitignored). A renewal failure
sends a Telegram alert (`errors` topic) rather than failing silently.

### Removed Features

- **UC-based staged entry (Case A/B)**: live 2026-09-09 → removed 2026-09-21 after a real
  bug (one branch re-fired on the same symbol every tick instead of latching once). See
  [`ISSUES_RESOLVED.md §2`](ISSUES_RESOLVED.md#2--entry-mechanism-evolution). Design writeup
  kept at `results/UC_Staged_Entry_Explained.docx`.
- **Zerodha execution** — removed 2026-09-10, pending a rebuild that never happened.

---

## Repository Structure

```
volume-daily/
├── STRATEGY.md                  # Full strategy detail — the real source of truth
├── ISSUES_RESOLVED.md           # History of every real bug fixed + why things are built this way
│
├── pipeline/
│   ├── main.py                  # Daily orchestrator — called by cron at 3:06 PM
│   ├── fetch_market_cap.py      # Logs into Screener.in, exports market cap CSV
│   ├── data_loader.py           # Dhan /charts/intraday — historical + intraday + EOD fill + market cap loader
│   ├── signal_engine.py         # Signal conditions (market cap / volume / return) — STRICT + PRORATED modes
│   ├── notify.py                # All Telegram sends — pipeline, scan preview, trading-stage, live-monitor alerts
│   ├── test/test_data_loader.py # Standalone test — date-chunking, epoch->IST, 429/failure handling
│   ├── requirements.txt
│   └── .env                     # ← NOT in git — credentials live here
│
├── scan/
│   └── scan_intraday.py         # Anytime preview scanner (PRORATED mode) — not read by any execution script
│
├── zerodha/                     # REMOVED 2026-09-10, pending a rebuild — see git history
│
├── dhan/
│   ├── auth.py                  # Access-token save/reuse + automated renew_access_token()
│   ├── trade.py                 # buy(), sell(), place_order(), order_status(), cancel_order() — CLI too
│   ├── charges.py                # Real trade-book charges + MTF-interest allocation (no estimates — see STRATEGY.md §6)
│   ├── run_trades.py            # Entry (321 + limit-tranched) / targets / bucketed exits / mirrored shorts /
│   │                             #   square-off / --reconcile / --sync-pnl — the main live-trading script
│   ├── live_monitor.py          # MarketFeed WebSocket monitor, 9:10 AM–3:40 PM — Telegram-only
│   ├── order_update_feed.py     # Dhan Order Update WebSocket -- powers the WS-first fill-confirmation path
│   └── test/                    # All Dhan tests — standalone, fully mocked, no pytest (see Testing below)
│
├── scripts/
│   ├── run_pipeline.sh          # Cron entry point — calls pipeline/main.py
│   ├── run_dhan_live_monitor.sh # Cron entry point — same, for dhan/live_monitor.py
│   ├── run_entry_limit_all.sh   # Cron entry point — tranched limit-entry for the full trade list
│   ├── push_data_updates.sh     # Auto-commit + push data/results — cron'd 4:15 PM
│   └── setup_vm.sh              # One-time VM provisioning script
│
├── data/
│   ├── candles/                 # Per-symbol 15-min OHLCV CSV files, sourced from Dhan
│   ├── instruments/
│   │   └── dhan_scrip_master.csv    # Dhan's own NSE scrip master — symbol → securityId/tick size
│   ├── market_cap_daily/        # Daily Screener.in exports + mcap_status.json
│   └── universe_combined.csv    # All symbols ever seen in the 1,500–5,000 Cr band
│
└── results/
    ├── build_pnl_simple.py               # Builds volume-daily-btst-pnl.xlsx
    ├── trades/trade_list_YYYY-MM-DD.csv  # Official signal output (one per trading day)
    ├── positions_dhan_long.json          # Dhan longs, all-time, all statuses
    ├── positions_dhan_short.json         # Dhan mirrored shorts, all-time, all statuses
    ├── pending_short_verifications.json  # Short-open orders awaiting re-check (see ISSUES_RESOLVED.md §3)
    ├── positions_zerodha_long.json       # Zerodha longs — historical only, code removed
    ├── positions_zerodha_short.json      # Zerodha mirrored shorts — historical only, code removed
    └── volume-daily-btst-pnl.xlsx        # The one consolidated P&L workbook (see STRATEGY.md §9)
```

---

## First-Time VM Setup

**Prerequisites**: DigitalOcean Ubuntu 22.04+ droplet. Your SSH public key must be added to
the droplet.

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
- Creates required data directories (`data/candles`, `data/instruments`,
  `data/market_cap_daily`, `results`)
- Writes a blank `pipeline/.env` (if not already present)

After filling in `.env`, run a manual test (`python3.11 pipeline/main.py`), then register
every cron job listed in [Cron Schedule Summary](#cron-schedule-summary) via `crontab -e`.

---

## Configuration

Copy `.env.example` to `pipeline/.env` and fill in all values. This file is gitignored and
must never be committed.

| Key | Description |
|---|---|
| `SCREENER_EMAIL` | Screener.in login email (Premium account required for export) |
| `SCREENER_PASSWORD` | Screener.in password |
| `TELEGRAM_BOT_TOKEN` | Bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | Group chat ID (negative integer for supergroups) |
| `TELEGRAM_TOPIC_SIGNALS` | *(optional)* Forum topic ID for pipeline signal notifications |
| `TELEGRAM_TOPIC_ENTRIES_EXITS` | *(optional)* Forum topic ID for entry/exit/summary notifications |
| `TELEGRAM_TOPIC_LIVE_TRACKING` | *(optional)* Forum topic ID for qualified/near-circuit live-monitor alerts |
| `TELEGRAM_TOPIC_ERRORS` | *(optional)* Forum topic ID for pipeline failures + monitor start/stop/heartbeat |
| `TELEGRAM_TOPIC_PNL` | *(optional)* Forum topic ID for the daily P&L summary |
| `DHAN_CLIENT_ID` | Dhan client ID (from web.dhan.co) — the access token itself is *not* stored here, see [Auth](#auth-automated-daily-renewal-manual-fallback) |

(`ZERODHA_API_KEY`/`ZERODHA_API_SECRET`/`ZERODHA_REDIRECT_URI` are unused for now —
Zerodha's execution code was removed 2026-09-10, pending a rebuild.)

> **Security**: `pipeline/.env` is in `.gitignore`. Never commit credentials. Use
> `.env.example` as the key-name reference only.

---

## Daily Operations

### Morning Token Refresh (Normally Nothing to Do)

The Dhan access token expires every 24h but **renews itself automatically** at 8:00 AM and
8:00 PM daily. A hand-generated token (**web.dhan.co → Profile → Access DhanHQ Trading
APIs**) is only needed once, ever, or as a fallback if automated renewal has been failing:

```bash
python -m dhan.auth <ACCESS_TOKEN>
```

### Monitoring

```bash
# Signal pipeline + EOD fill log
tail -f ~/pipeline.log

# Dhan trading log (entries, exits, targets, shorts, square-offs, reconcile, sync-pnl)
tail -f ~/dhan_trades.log

# Live monitor log, token renewal log, data-push log, tranched-entry log
tail -f ~/dhan_live_monitor.log
tail -f ~/dhan_token_renew.log
tail -f ~/push_data_updates.log
tail -f ~/entry_limit_all.log

# Verify all crons are registered
crontab -l

# Verify system time is IST
date

# Check today's open Dhan positions
python3 -m json.tool results/positions_dhan_long.json
python3 -m json.tool results/positions_dhan_short.json
```

### Pulling Updates

```bash
cd ~/volume-daily
git pull
```

`run_pipeline.sh` does not `git pull` automatically — code updates pushed to GitHub do not
reach the VM until you pull manually. (Data/results changes, in the other direction, *do*
push automatically at 4:15 PM via `scripts/push_data_updates.sh`.)

---

## Manual Commands

### Pipeline

```bash
python3.11 pipeline/main.py                      # Run the full signal pipeline manually
python3.11 pipeline/data_loader.py --eod-fill     # Run only the EOD candle fill
python3.11 scan/scan_intraday.py                  # Preview signals anytime (PRORATED mode)
```

### Dhan Live Trading

```bash
# Entry
python dhan/run_trades.py --entry --dry-run                                  # safety-net entry, dry run
python dhan/run_trades.py --entry
python dhan/run_trades.py --entry --symbol RELIANCE --capital 50000 --bucket 5-10 --dry-run  # manual single-stock

# Profit targets, 9:15 AM
python dhan/run_trades.py --place-targets

# Exit / forced-exit / square-off — ALL require --bucket (5-10 / 10-15 / 15-20), no legacy fallback
python dhan/run_trades.py --exit-916       --bucket 5-10
python dhan/run_trades.py --exit-1159      --bucket 10-15
python dhan/run_trades.py --square-off-239 --bucket 15-20

# Reconciliation (also runs automatically — see Cron Schedule Summary)
python dhan/run_trades.py --reconcile

# P&L workbook regen + Google Drive push
python dhan/run_trades.py --sync-pnl

# Tranched limit entry for the day's full trade list
bash scripts/run_entry_limit_all.sh
```

### Auth

```bash
python -m dhan.auth <ACCESS_TOKEN>    # Save today's Dhan access token (first time, or if auto-renewal has lapsed)
python -m dhan.auth --renew           # Manually trigger a renewal (normally cron-only, 8 AM/8 PM)
```

---

## Testing

All Dhan tests live in `dhan/test/` — standalone, fully mocked scripts (no pytest, no real
network/file I/O, no real token file ever touched).

```bash
python dhan/test/test_all.py           # Runs every file below, one consolidated pass/fail report
python dhan/test/test_all.py -v        #   ...same, but streams every file's full output live

python dhan/test/test_targets.py                  # Profit targets, OCO short stop-loss, every exit-reason branch
python dhan/test/test_auth_renew.py                # Token renewal: success, renew failure, verify failure, GET-not-POST
python dhan/test/test_batch_concurrency.py         # Wave-based batching, OCO status resolution from one Order Book snapshot
python dhan/test/test_parallel_orders.py           # RateLimiter sliding window, entry/exit parallel-phase timing
python dhan/test/test_exit_stage_timing.py         # 916/1159/239's :50-prep / :00-fire staging holds
python dhan/test/test_order_update_feed.py         # Order Update WebSocket shadow-mode validation layer
python dhan/test/test_entry_limit_subset.py        # run_entry_limit --symbols subset sizing + hand-off marker
python dhan/test/test_return_buckets.py            # Return-bucket classification + per-bucket schedule routing
python dhan/test/test_charges.py                   # Real trade-book charges + MTF-interest allocation
python dhan/test/test_reconcile_stale_exits.py     # --reconcile: broker-already-closed long recovery
python dhan/test/test_reconcile_pending_shorts.py  # Short-open orders recovered after a late fill
```

`test_all.py` runs each file as its own fresh subprocess, not imported together — several
files apply process-wide mocks at import time that are only safe in isolation (see
`test_all.py`'s own docstring, and [`ISSUES_RESOLVED.md §11`](ISSUES_RESOLVED.md#11--testing-infrastructure)
for a real cross-test-isolation bug this caused).

`pipeline/data_loader.py` has its own standalone test, same style, outside `dhan/test/`:

```bash
python pipeline/test/test_data_loader.py
```

---

## Telegram Notifications

Every notification function in `pipeline/notify.py` is routed across Telegram forum topics
(`signals`, `entries_exits`, `live_tracking`, `errors`, `pnl`) via `TELEGRAM_TOPIC_*` env
vars, falling back to the group's General topic if a topic ID isn't set.

| Event | Sent by | Notes |
|---|---|---|
| Pipeline **success** / **failure** | `pipeline/main.py` | Date, signal count, runtime, full trade table (or failed step + error) |
| Scan **preview** | `scan/scan_intraday.py` | Clearly labeled as a preview, not the official signal |
| **Entry** | `dhan/run_trades.py` | Tagged with the position's return bucket; **no order ID** in any message |
| **Exit** (916/1159/239) | `dhan/run_trades.py` | Tagged with the return bucket; no "target placed" alert exists |
| **Short open** / **square-off** | `dhan/run_trades.py` | — |
| **Daily summary** | `dhan/run_trades.py` | — |
| Monitor **started**/**failed**/**qualified**/**near circuit**/**disconnect**/**reconnect**/**heartbeat** | `dhan/live_monitor.py` | `qualified` shows the real volume ratio (`cum_vol / avg_36`, not the already-6×'d threshold) |

Every trading-stage and live-monitor notification call is wrapped in its own `try/except` —
a Telegram outage can never break order placement, position tracking, or the monitor loop
itself.

### Setting Up on a New Bot or Group

1. Create a bot via `@BotFather` → copy the token → set `TELEGRAM_BOT_TOKEN`
2. Add the bot to your Telegram group
3. Send a message in the group, then run `curl "https://api.telegram.org/bot<TOKEN>/getUpdates"`
4. Find `chat.id` in the response (negative integer for supergroups) → set `TELEGRAM_CHAT_ID`
5. *(Optional)* For forum topics, get each topic's `message_thread_id` the same way (send a
   message in that topic, check the same `getUpdates` response) and set the matching
   `TELEGRAM_TOPIC_*` variable. Any topic left unset just falls back to General.

---

## Cron Schedule Summary

All times IST (UTC+5:30), server timezone `Asia/Kolkata`. All jobs below are Mon–Fri
(`1-5`) unless noted. Every weekday line also carries a self-expiring market-holiday guard
(`[ "$(date +%Y-%m-%d)" = "<holiday-date>" ] || <command>`, chained with `||` per holiday as
they come up — see [`ISSUES_RESOLVED.md §14`](ISSUES_RESOLVED.md#14--holiday-handling));
omitted from the table below for readability.

### Signal Pipeline

| Time | Command |
|---|---|
| 3:10 PM | `scripts/run_pipeline.sh` |
| 4:00 PM | `pipeline/data_loader.py --eod-fill` |
| 4:15 PM | `scripts/push_data_updates.sh` |

### Dhan Live Trading

| Time | Command |
|---|---|
| 8:00 AM & 8:00 PM (**every day**) | `python -m dhan.auth --renew` |
| 9:10 AM | `scripts/run_dhan_live_monitor.sh` (start) |
| 9:12 AM | `dhan/run_trades.py --reconcile` |
| 9:15 AM | `dhan/run_trades.py --place-targets` |
| 9:15 AM | `dhan/run_trades.py --exit-916 --bucket 5-10` |
| 9:17 AM | `dhan/run_trades.py --exit-916 --bucket 10-15` |
| 9:43 AM | `dhan/run_trades.py --exit-916 --bucket 15-20` |
| 10:17 AM | `dhan/run_trades.py --exit-1159 --bucket 5-10` |
| 11:40 AM | `dhan/run_trades.py --exit-1159 --bucket 15-20` |
| 11:58 AM | `dhan/run_trades.py --exit-1159 --bucket 10-15` |
| 2:28 PM | `dhan/run_trades.py --square-off-239 --bucket 5-10` |
| 2:39 PM | `dhan/run_trades.py --square-off-239 --bucket 15-20` |
| 2:56 PM | `dhan/run_trades.py --square-off-239 --bucket 10-15` |
| 3:16 PM | `scripts/run_entry_limit_all.sh` (tranched LIMIT entry, full trade list) |
| 3:20 PM | `dhan/run_trades.py --entry` (MARKET safety net) |
| 3:40 PM | `pkill -f 'dhan\.live_monitor'` (stop) |
| 3:45 PM | `dhan/run_trades.py --sync-pnl` (P&L workbook + Google Drive push) |

> Every exit-stage line fires **one minute before** its real decision instant — the script
> holds internally at a `:50` prep point and the `:00` fire instant, same mechanism as
> `run_entry_321`'s own 15:20:57/15:21:00 staging hold. See
> [`STRATEGY.md §5`](STRATEGY.md#5-exit--return-bucketed-3-stage-schedule) for why each
> bucket has its own 3 times, and
> [`ISSUES_RESOLVED.md §5`](ISSUES_RESOLVED.md#5--exit-schedule-evolution) for how this
> schedule evolved from one fixed set of times to three bucket-specific ones.
>
> The token-renewal job runs **every day**, not just Mon–Fri — a token still needs renewing
> on a holiday so Monday's first real run doesn't start from an expired one.

---

*Pipeline runs Mon–Fri · DigitalOcean Ubuntu 22.04 · Python 3.11 · Dhan (data, execution,
live monitoring) · Telegram alerts throughout · See [`STRATEGY.md`](STRATEGY.md) and
[`ISSUES_RESOLVED.md`](ISSUES_RESOLVED.md) for full detail.*
