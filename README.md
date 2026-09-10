# NSE Volume Pipeline

Automated NSE mid-cap momentum scanner running daily at **3:06 PM IST** (Mon–Fri) on a DigitalOcean Ubuntu VM. Scans the ₹1,500–5,000 Cr market-cap band, fires on volume + return conditions, and generates a trade list. Upstox is used for market data only — no trading happens through Upstox.

Live execution currently runs entirely through **Dhan** (`dhan/`) — own capital pool, own positions file, own broker-specific quirks (LIMIT entries, 17% profit targets, mirrored intraday shorts with a UC-based stop-loss). See [Dhan Pipeline](#dhan-pipeline-independent-parallel-broker) below.

**Zerodha's execution/live-monitor code was removed 2026-09-10, pending a full rebuild** — it previously ran a parallel live trading pipeline against the same signals. Its historical trade data (`results/positions_zerodha_long.json`, `positions_zerodha_short.json`, `trade_book.csv`/`.xlsx`) was kept, not deleted. This document no longer describes any Zerodha execution flow.

---

## Table of Contents

- [Strategy Overview](#strategy-overview)
- [Signal Logic](#signal-logic)
- [Capital Allocation](#capital-allocation)
- [Complete Daily Flow](#complete-daily-flow)
  - [Part 1 — Signal Pipeline (3:06 PM)](#part-1--signal-pipeline-306-pm)
  - [Part 2 — EOD Candle Fill (4:00 PM)](#part-2--eod-candle-fill-400-pm)
  - [Part 3 — Auto-Push Data & Results (5:00 PM)](#part-3--auto-push-data--results-500-pm)
- [Dhan Pipeline (Independent, Parallel Broker)](#dhan-pipeline-independent-parallel-broker)
  - [UC-Based Staged Entry (Case A/B) — Off By Default](#uc-based-staged-entry-case-ab--off-by-default)
- [Repository Structure](#repository-structure)
- [First-Time VM Setup](#first-time-vm-setup)
- [Configuration](#configuration)
- [Daily Operations](#daily-operations)
- [Manual Commands](#manual-commands)
- [Telegram Notifications](#telegram-notifications)
- [Cron Schedule Summary](#cron-schedule-summary)

---

## Strategy Overview

The pipeline targets **NSE equities with market cap ₹1,500–5,000 Cr**. It screens for stocks where unusual volume has built throughout the day *and* the price has broken above the prior day's close. Entries happen at 3:21 PM IST (near market close). Positions are held overnight; an early-morning exit is taken if the stock is up, otherwise a forced exit late morning.

| Parameter | Value |
|---|---|
| Universe | NSE EQ/BE segment, ₹1,500–5,000 Cr |
| Candle interval | 15-minute OHLCV (pipeline), 1-minute (live trading reference price) |
| Data source | Upstox V3 API (data/analytics only — no Upstox trading) |
| Market cap source | Screener.in Premium (live daily export) |
| Entry reference price | Close of the 15:20 candle (falls back to 15:19) |
| Pipeline schedule | Mon–Fri, fully automated end-to-end via cron |

Live execution/product-type details are broker-specific — see [Dhan Pipeline](#dhan-pipeline-independent-parallel-broker).

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

- **STRICT mode** (the real 3:06 PM run): reference candle is fixed at 15:00 (falls back to the 14:45 close if 15:00 hasn't posted yet)
- **PRORATED mode** (`scan/scan_intraday.py`, anytime preview): reference candle is whatever's latest as of the check time — this is a directional preview only, never read by any execution script

Previous day VWAP is calculated from the 15:00 and 15:15 candles of the prior trading day:

```
VWAP = Σ((H + L + C) / 3 × Volume) / Σ(Volume)
```

---

## Capital Allocation

**Two separate, independent allocations exist** — don't confuse them:

1. **`pipeline/main.py`'s own trade list** uses a hardcoded ₹5,00,000 reference to compute the `shares`/`ref_price` columns written to `results/trades/trade_list_<date>.csv` — this is display/notification sizing only, shown in the Telegram signal message.
2. **The actual live execution** (`dhan/run_trades.py --entry`) ignores those CSV columns entirely — it only reads the `symbol` column, then recomputes quantity itself from the **real** capital pool and its own freshly-fetched 15:20 reference price.

Both use the same tiered rule — max ₹ per position is `capital/4` (idle capital allowed when signals ≤ 4); once signals hit 5+, the full capital splits equally instead of over-allocating:

```python
allocation = capital / 4 if n <= 4 else capital / n
shares     = floor(allocation / ref_price)
```

See [Dhan Pipeline](#dhan-pipeline-independent-parallel-broker) for the live capital figure currently configured and how to change it.

---

## Complete Daily Flow

### Timeline Overview

```
 3:00 PM  — Last auction period begins
 3:06 PM  — [CRON] Signal pipeline: scans volume/return, writes trade list, notifies Telegram
 4:00 PM  — [CRON] EOD fill: corrects/backfills 15:00 + 15:15 candles via intraday API
 5:00 PM  — [CRON] Auto-commit + push data/results changes to GitHub
─────────── overnight hold, cycle repeats ─────────────────────────────────────
```

> This is the shared signal-pipeline timeline only. Live trading execution currently runs entirely through Dhan, on its own separate schedule — see [Dhan Pipeline](#dhan-pipeline-independent-parallel-broker) and [Cron Schedule Summary](#cron-schedule-summary).

---

### Part 1 — Signal Pipeline (3:06 PM)

`run_pipeline.sh` is called by cron at 3:06 PM IST. It calls `pipeline/main.py`, which runs the following steps in sequence:

1. **Fetch market cap** — `data_loader.load_market_cap()` logs into Screener.in (Premium), runs the query `Market Capitalization > 1500 AND Market Capitalization < 5000`, and exports to `data/market_cap_daily/market_cap_<date>.csv`. Falls back to the most recent prior export (with a warning) if the live fetch fails, or fails the whole run if there's no fallback at all.
2. **Update universe** — compares today's market-cap symbols against `data/universe_combined.csv`; new symbols not previously seen are appended automatically.
3. **Candle data update** — for brand-new symbols: matches Upstox `instrument_key` and backfills 1 year of 15-min candles. For every symbol with a candle file: fetches today's intraday 15-min candles (in-progress candle at fetch time is necessarily incomplete until the 4:00 PM EOD fill corrects it).
4. **Signal check (STRICT mode)** — `signal_engine.get_signals()` applies all three conditions above.
5. **Write trade list** — `results/trades/trade_list_<date>.csv` (columns: `symbol`, `shares`, `ref_price` — see the capital-allocation caveat above; these sizing columns are for the notification only, not what actually gets bought).
6. **Notify** — `pipeline/notify.py` sends the signal count + trade table to Telegram (or a failure message with the failed step + error, if any step raised).

---

### Part 2 — EOD Candle Fill (4:00 PM)

Runs via cron at 4:00 PM IST, after NSE closes (3:30 PM). Fixes two real data-completeness gaps:

1. **The candle "in progress" when the 3:06 PM run fetches intraday data is incomplete** — collapsed OHLC, near-zero volume, a single-tick snapshot, not the settled candle.
2. **The final candle of the session (15:15) doesn't exist yet at 3:06 PM at all.**

```bash
python3.11 pipeline/data_loader.py --eod-fill
```

This uses the **intraday** endpoint (not the historical one — the historical endpoint returns zero rows for same-day dates), and lets freshly fetched data **overwrite** any existing row for the same timestamp, correcting the in-progress candle and adding the missing 15:15 bar. The file is re-sorted on every write.

---

### Part 3 — Auto-Push Data & Results (5:00 PM)

```bash
./scripts/push_data_updates.sh
```

Stages **only** `data/` and `results/` (candles, market cap, instruments, positions, trade book, trade lists) — never code, so an in-progress code edit can never get swept into an unattended commit. Commits as `Data update — <date> candle + results refresh` and pushes to `main`. If nothing changed that day, it exits cleanly with no empty commit.

---

## Dhan Pipeline (Independent, Parallel Broker)

Live execution — own broker (Dhan, via `dhan/`), own capital pool, own positions file (`results/positions_dhan.json`), own live monitor, own auth model. It reads the **same** `results/trades/trade_list_<date>.csv` the signal pipeline writes.

| Parameter | Value |
|---|---|
| Broker / product type | Dhan, CNC or MTF (per-symbol leverage check) |
| Live trading capital | ₹14,50,000 total (`TOTAL_CAPITAL` in `dhan/run_trades.py`) |
| Entry order type | **LIMIT**, 0.75% above live LTP — not MARKET (see note below) |
| Profit target | 17% LIMIT sell, placed at 9:15 AM for every open long |
| Mirrored shorts | Opened on every 9:25/11:59 long exit — 5% cover target + UC-based stop-loss |
| Auth | Manual 24h access token paste — no OAuth handshake (see [Daily Operations](#daily-operations)) |

> **Why LIMIT, not MARKET**: Dhan/NSE silently apply their own price-protection band to a `MARKET` order — every order this pipeline placed as `orderType: MARKET` came back from Dhan's own order records as `orderType: LIMIT` near the submission price (confirmed 2026-08-17). That band is tight enough that ordinary same-second price movement (no circuit lock involved) produced 0-fills. Placing an explicit LIMIT 0.75% above live LTP gives wider, predictable headroom instead of relying on Dhan's undocumented band. Dhan's order API has no exposed market-protection/collar parameter (unlike Kite's `market_protection`).

### Entry — 3:21 PM (`--entry`)

Same `capital/4`-or-`n` allocation rule described in [Capital Allocation](#capital-allocation), against the ₹14.5L Dhan capital base. Per symbol: checks live leverage via `POST /margincalculator` (`productType=MTF`) — `product="MTF"` if leverage ≥2x, otherwise falls back to `product="CNC"` at **half** the capital base (`capital/2`, resized allocation/shares). Places a LIMIT buy 0.75% above live LTP (falls back to the reference price as the limit anchor if LTP is momentarily unavailable), polls for a broker-confirmed fill (never records a phantom fill on an unconfirmed timeout), and writes the position to `results/positions_dhan.json`.

> **MTF-ineligibility CNC retry**: `/margincalculator`'s pre-check isn't fully reliable — it can return a plausible leverage figure for a scrip that Dhan then genuinely rejects at order-placement time (`"Mtf Product Is Not Allowed For This Scrip"`). When that specific rejection is confirmed (not just any rejection — a circuit-limit rejection, for instance, retries nothing, since CNC would hit the same price band), the order is retried as CNC automatically, at the same quantity (no re-halving). This applies to entries, the 9:15 AM targets, both 9:25/11:59 exit branches, and the forced exit. **`MARGIN`/T+5 is never used as a product anywhere in this pipeline** — a T+5-settlement-lag sell rejection (`"No eligible T+5 quantity found"`) also retries as CNC, same mechanism.

### Profit Targets — 9:15 AM (`--place-targets`)

For every open long without one yet, places a resting LIMIT sell at **17% above the recorded fill price** (`target_order_id`/`target_price` saved on the position). This is live at the exchange one cron tick before the 9:25 exit check runs, so a target can fill on its own between checkpoints without the pipeline needing to be watching.

### Exit Check — 9:25 AM (`--exit-925`) / Forced Exit — 11:59 AM (`--exit-1159`)

Three branches (P&L-based exit / hold / no-data half-sell fallback), with one addition checked first: if the resting 17% target already filled, the position closes from **the target's own fill**, skipping the LTP check entirely. Otherwise the resting target is cancelled before whichever branch below fires:

- **Target already hit** → close from the target's fill, `status: exited_925` or `exited_1159`.
- **Live P&L > 0** (925 only) → cancel the resting target, MARKET sell the full position.
- **No LTP data** → cancel the resting target, sell half as a precaution (`partial_exit_925_nodata`), place a **fresh** target for the remainder at the same target price, remainder carries to 11:59.
- **P&L ≤ 0** (925 only) → hold for the forced exit; target stays live and untouched.
- **11:59 forced exit** → unconditional close of whatever's still open (blended P&L across any 925 partial + the 11:59 remainder).

Every long exit (full or partial) that actually fills — from any of the branches above — immediately opens a **mirrored intraday short** in the same symbol, same quantity (see below).

### Mirrored Intraday Shorts

Every time a long exit fills (925 full-sell, 925 no-data half-sell, or 1159 force-sell), `_open_short()` opens a same-quantity `productType=INTRADAY` short in the same symbol — skipped (never raising) if the margin check or balance comes up short, since the long exit that triggered it has already happened and is never reversed. Two protective orders go live immediately:

- **Cover target**: LIMIT buy at 5% below the short's entry price.
- **Stop-loss**: `STOP_LOSS_MARKET` buy, trigger price 0.5% below the day's upper circuit limit (fetched on-demand via `/marketfeed/quote`) — protects against the short being run over on a genuine breakout, since Dhan has no market-protection collar to fall back on for this leg either.

### Short Square-Off — 2:39 PM (`--square-off-239`)

Unconditional close of every open short, with full **OCO** (one-cancels-other) handling between the cover target and the stop-loss: whichever order shows `TRADED` closes the position from **its own fill** and cancels the other; if both somehow show `TRADED` (a race), the position is flagged for manual review rather than guessing; if neither filled, both are cancelled and the position is force-covered at MARKET, same unconditional shape as the 11:59 long exit.

### Live Monitor

```bash
python -m dhan.live_monitor
```

Runs 9:13 AM–3:40 PM (cron-managed), using the `dhanhq` package's MarketFeed WebSocket for ticks and this repo's own `dhan/auth.py`/`dhan/trade.py` for REST calls (circuit limits, symbol→securityId) — Telegram-only, no CSV log. Watches for the same qualifying conditions as the pipeline signal logic, but live and continuously:

- **`qualified`**: cumulative volume ≥ 6× the 36-day average *and* LTP ≥ prev-day VWAP × 1.05 — same thresholds as [Signal Logic](#signal-logic), evaluated tick-by-tick instead of once
- **`near_circuit`**: LTP within 1% of the upper circuit limit, only after a symbol has already qualified
- Both conditions stop evaluating at **3:00 PM** (signal_engine's STRICT run takes over from there)

### Positions JSON Schema (`results/positions_dhan.json`)

Longs and shorts share one file, distinguished by `direction`:

```json
{
  "broker": "dhan", "symbol": "TVSSRICHAK", "entry_date": "2026-08-18",
  "actual_fill_price": 4836.00, "actual_fill_quantity": 3,
  "entry_order_id": "...", "status": "exited_925", "product": "CNC",
  "target_order_id": "...", "target_price": 5658.12,
  "exit_price_925": 5658.12, "exit_order_id_925": "...",
  "realized_return_pct": 17.0, "realized_pnl": 2466.36
}
```

```json
{
  "broker": "dhan", "symbol": "TVSSRICHAK", "direction": "short",
  "entry_price": 5658.12, "quantity": 3, "product": "INTRADAY",
  "source_exit_stage": "925", "status": "short_closed",
  "cover_target_order_id": "...", "cover_target_price": 5375.21,
  "stop_order_id": "...", "stop_trigger_price": 5820.00,
  "exit_price_239": 5375.21, "exit_order_id_239": "...",
  "realized_return_pct": 5.0, "realized_pnl": 848.73
}
```

### UC-Based Staged Entry (Case A/B) — Off By Default

A second, independent entry mechanism (the "UC-based staged entry" section of `dhan/live_monitor.py`) that runs **alongside** the 3:21 PM entry, not in place of it — driven by live websocket ticks instead of a single end-of-day snapshot, so a strong stock can be bought earlier than 3:21 PM. **Off by default** behind `--enable-uc-staged-entry` on `dhan/live_monitor.py`; the launcher script and crontab don't pass it, so today's production behavior is unchanged until it's explicitly turned on.

- **Case A qualification filter**: a symbol is only eligible if it hit its upper circuit at some point *before* 2:30 PM, then is seen trading *off* that circuit at some point during the 2:30–3:18 PM window. Both are one-way latches, checked continuously from market open. A symbol that never locks at UC, or locks and never comes back off it, is never a Case A candidate — it just falls through untouched to the normal 3:21 PM entry.
- **Case A leg 1** (2:30–3:18 PM, qualified symbols only): LTP crosses up through `prev_close × 1.19` → buy 50% of `per_stock_capital`.
- **Case A leg 2** (same window, only after leg 1 has fired): LTP retraces back down to `prev_close × 1.17` → buy the other 50%, folded into the same position row. If it never retraces by 3:18 PM, the position is left `entry_status: partially_filled` — not abandoned, not treated as a fresh entry (see below).
- **Case B** (3:00–3:18 PM, non-qualified symbols only): LTP crosses `prev_close × 1.19` → buy 100% of `per_stock_capital` in one shot. No legs, no retrace, no UC-proximity gate.
- **Tie-break**: a symbol excluded the moment it's `case_a_qualified`, even before leg 1 has actually fired — Case A and Case B's windows overlap for the last 18 minutes, and a qualified symbol always resolves through the Case A path, never Case B.
- **`per_stock_capital`** = Total Capital ÷ number of symbols already "qualified" on the existing volume/VWAP screen (`live_monitor.py`'s own long-standing signal) — recomputed fresh every tick from 2:30 PM onward and read at the exact instant a Case A/B trigger actually fires, not a one-time snapshot (changed 2026-09-09: the earlier frozen-at-2:30-PM version gave a real Case B fire a stale divisor because 2 more symbols qualified between the 2:30 PM snapshot and that symbol's own later trigger).
- **3:21 PM entry priority** (the *only* change to the otherwise-untouched entry function): symbols left `partially_filled` by Case A are completed first (remaining rupee balance, same product as leg 1, no fresh margin check), symbols already `filled` via Case A/B are skipped entirely, everything else runs exactly as it always has.
- `prev_close` is read from this pipeline's own local candle history (`data/candles/<symbol>.csv`), not from Dhan's live feed — confirmed live that Dhan's `ohlc.close` tracks *today's* running price, not yesterday's close.
- Buy limit prices are capped just below the day's upper circuit (same fix already shipped for the sell-side 17% target), and a Case A/B fill that turns out MTF-ineligible retries as CNC exactly like the 3:21 PM entry does.

```bash
# Manual test — never places real orders
python -m dhan.live_monitor --enable-uc-staged-entry --dry-run
```

A step-by-step walkthrough with worked examples: `results/UC_Staged_Entry_Explained.docx`.

### Auth (Automated Daily Renewal, Manual Fallback)

Unlike Kite Connect, Dhan has no OAuth handshake for a personal account — the **very first** token still has to be generated by hand at **web.dhan.co → Profile → Access DhanHQ Trading APIs** (valid 24h):

```bash
python -m dhan.auth <ACCESS_TOKEN>
```

From then on, **`python -m dhan.auth --renew` renews it automatically**, twice a day (8:00 AM and 8:00 PM IST) via `GET /v2/RenewToken`, and verifies the new token with a real `/fundlimit` call before saving it — a failed renewal (or a failed verification) leaves the previous saved token completely untouched, so one bad attempt never leaves the system with no working token at all.

Two confirmed gotchas baked into `renew_access_token()`: the renew call **must be GET**, not POST/PUT (both return a misleading `DH-905` "missing fields" error); and renewing **invalidates the previous token immediately** — no overlap window — which is why both renewal times sit in the dead zone between the previous day's live-monitor `pkill` (3:40 PM) and the next launch (9:10 AM), and why it runs *twice* daily rather than once (a once-daily cron at a fixed clock time would leave only a second or two of buffer before the old token's real 24h expiry; twice-daily keeps a comfortable multi-hour buffer, and a failed attempt is caught 12 hours later instead of a full day later).

On a renewal failure, `dhan/auth.py`'s `--renew` entry point sends a Telegram alert (`notify.send_token_renewal_failed`, `errors` topic) — the same channel used for live-monitor start failures. Manual re-paste via `python -m dhan.auth <ACCESS_TOKEN>` is only needed if automated renewal has been failing long enough for the saved token to fully expire.

Saved to `dhan/.token.json` (gitignored) and reused for the rest of the day by every other Dhan script.

### Testing

All Dhan tests live in `dhan/test/` — standalone, fully mocked scripts (no pytest, no real network/file I/O, no real token file ever touched).

```bash
python dhan/test/test_all.py           # runs all 7 below, one consolidated pass/fail report
python dhan/test/test_all.py -v        #   ...same, but streams every file's full output live

python dhan/test/test_targets.py             # Profit targets, OCO short stop-loss, every exit-reason branch
python dhan/test/test_uc_staged_entry.py     # Case A/B qualification, legs, tie-break, capital snapshot, 3:21 priority
python dhan/test/test_auth_renew.py          # Token renewal: success, renew failure, verify failure, GET-not-POST
python dhan/test/test_batch_concurrency.py   # Wave-based batching, OCO status resolution from one Order Book snapshot
python dhan/test/test_parallel_orders.py     # RateLimiter sliding window, entry/exit parallel-phase timing
python dhan/test/test_exit_stage_timing.py   # 925/1159/239's :50-prep / :00-fire staging holds
python dhan/test/test_order_update_feed.py   # Order Update WebSocket shadow-mode validation layer
```

`test_all.py` runs each file as its own fresh subprocess rather than importing them together — several files apply process-wide mocks at import time that are only safe in isolation (see `test_all.py`'s own docstring).

---

## Repository Structure

```
volume-daily/
├── pipeline/
│   ├── main.py                # Daily orchestrator — called by cron at 3:06 PM
│   ├── fetch_market_cap.py    # Logs into Screener.in, exports market cap CSV
│   ├── data_loader.py         # Upstox V3 — historical + intraday + EOD fill + market cap loader
│   ├── signal_engine.py       # Signal conditions (market cap / volume / return) — STRICT + PRORATED modes
│   ├── notify.py              # All Telegram sends — pipeline success/failure, scan preview, trading-stage
│   │                          #   alerts (entry/exit/summary), and live-monitor alerts (start/qualified/etc.)
│   ├── requirements.txt       # pandas, requests, python-dotenv, kiteconnect, cffi, openpyxl
│   └── .env                   # ← NOT in git — credentials live here
│
├── scan/
│   └── scan_intraday.py       # Anytime preview scanner (PRORATED mode) — not read by any execution script
│
├── zerodha/                     # REMOVED 2026-09-10, pending a full rebuild (see git history for the
│                                #   pre-removal code) — the rest of this doc's Zerodha references
│                                #   describe that removed code, not anything currently runnable.
│                                #   results/positions_zerodha_long.json, positions_zerodha_short.json,
│                                #   and trade_book.csv/.xlsx trade history were kept, not deleted.
│
├── dhan/
│   ├── auth.py                  # Access-token save/reuse + automated renew_access_token() — dhan/.token.json
│   ├── cron_renew_token.py     # Cron wrapper — renews the token 8AM+8PM, Telegram-alerts on failure
│   ├── trade.py                 # buy(), sell(), place_order(), order_status(), cancel_order() via Dhan — CLI too
│   ├── instruments.py           # symbol → securityId resolution
│   ├── run_trades.py           # Entry / 17% targets / 925 & 1159 exits / mirrored shorts / 239 square-off
│   ├── uc_staged_entry.py      # UC-based staged entry (Case A/B) — off by default, see Dhan Pipeline section
│   ├── live_monitor.py         # MarketFeed WebSocket monitor, 9:13 AM–3:40 PM — Telegram-only
│   ├── test_targets.py         # Self-test — profit targets, OCO short stop-loss, all exit-reason branches
│   ├── test_uc_staged_entry.py # Self-test — Case A/B qualification, legs, tie-break, capital snapshot
│   └── test_auth_renew.py      # Self-test — token renewal success/failure paths, GET-not-POST guard
│
├── scripts/
│   ├── run_pipeline.sh         # Cron entry point — calls pipeline/main.py
│   ├── run_dhan_live_monitor.sh # Cron entry point — same, for dhan/live_monitor.py
│   ├── push_data_updates.sh    # Auto-commit + push data/results — cron'd 5:00 PM
│   └── setup_vm.sh             # One-time VM provisioning script
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
│   ├── positions_zerodha_long.json       # Zerodha longs, all-time — historical only, code removed
│   ├── positions_zerodha_short.json      # Zerodha mirrored shorts, all-time — historical only, code removed
│   ├── positions_dhan_long.json          # Dhan longs, all-time, all statuses
│   ├── positions_dhan_short.json         # Dhan mirrored shorts, all-time, all statuses
│   ├── trade_book.csv                    # Flattened per-position P&L view (regenerated daily at 4:30 PM)
│   ├── trade_book.xlsx                   # Day-boxed visual version of the same data
│   └── UC_Staged_Entry_Explained.docx    # Step-by-step walkthrough of Case A/B with worked examples
│
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

Then register all the cron jobs listed in [Cron Schedule Summary](#cron-schedule-summary) via `crontab -e`.

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
| `TELEGRAM_TOPIC_SIGNALS` | *(optional)* Forum topic ID for pipeline signal notifications |
| `TELEGRAM_TOPIC_ENTRIES_EXITS` | *(optional)* Forum topic ID for entry/exit/summary notifications |
| `TELEGRAM_TOPIC_LIVE_TRACKING` | *(optional)* Forum topic ID for qualified/near-circuit live-monitor alerts |
| `TELEGRAM_TOPIC_ERRORS` | *(optional)* Forum topic ID for pipeline failures + monitor start/stop/heartbeat |
| `TELEGRAM_TOPIC_PNL` | *(optional)* Forum topic ID for the daily P&L summary |
| `DHAN_CLIENT_ID` | Dhan client ID (from web.dhan.co) — the access token itself is *not* stored here, see [Dhan Pipeline auth](#auth-manual-daily-token) |

(`ZERODHA_API_KEY`/`ZERODHA_API_SECRET`/`ZERODHA_REDIRECT_URI` are unused for now — Zerodha's execution code was removed 2026-09-10, pending a rebuild.)

> **Security**: `pipeline/.env` is in `.gitignore`. Never commit credentials. Use `.env.example` as the key-name reference only.

---

## Daily Operations

### Morning Token Refresh (Required — Cannot Be Automated)

The **candle data token** (`UPSTOX_ACCESS_TOKEN`) is long-lived — update it in `.env` only when it eventually expires.

The **Dhan access token** expires every 24h, but **renews itself automatically** — `python -m dhan.auth --renew` runs at 8:00 AM and 8:00 PM daily and keeps it fresh with no manual step. A hand-generated token (**web.dhan.co → Profile → Access DhanHQ Trading APIs**) is only needed once, ever, or as a fallback if automated renewal has been failing:

```bash
python -m dhan.auth <ACCESS_TOKEN>
```

Saved to `dhan/.token.json`, reused for the rest of the day by every Dhan script. A renewal failure sends a Telegram alert (`errors` topic) rather than failing silently. See [Dhan Pipeline → Auth](#auth-automated-daily-renewal-manual-fallback).

### Monitoring

```bash
# Live pipeline log stream
tail -f ~/pipeline.log

# Live trades log
tail -f ~/trades_test.log

# Trade book / auto-push logs
tail -f ~/trade_book.log
tail -f ~/push_data_updates.log

# Dhan trades log + live monitor log
tail -f ~/dhan_trades.log
tail -f ~/dhan_live_monitor.log

# Verify all crons are registered
crontab -l

# Verify system time is IST
date

# Check today's open Dhan positions
python3 -m json.tool results/positions_dhan_long.json
python3 -m json.tool results/positions_dhan_short.json

# Check the flattened P&L view
column -s, -t results/trade_book.csv
```

### Pulling Updates

```bash
cd ~/volume-daily
git pull
```

Note: `run_pipeline.sh` does not `git pull` automatically — code updates pushed to GitHub do not reach the VM until you pull manually. (Data/results changes, in the other direction, *do* push automatically at 5:00 PM via `scripts/push_data_updates.sh`.)

---

## Manual Commands

### Pipeline

```bash
# Run the full pipeline manually
python3.11 pipeline/main.py

# Run only the EOD candle fill
python3.11 pipeline/data_loader.py --eod-fill

# Preview signals at any time of day (PRORATED mode — not the official 3:06 PM run)
python3.11 scan/scan_intraday.py
```

### Dhan Pipeline

```bash
# Entry — dry run / live
python dhan/run_trades.py --entry --dry-run
python dhan/run_trades.py --entry
python dhan/run_trades.py --entry --symbol RELIANCE --capital 5000 --dry-run   # single stock

# Profit targets, 9:15 AM
python dhan/run_trades.py --place-targets

# Exit check 9:25 AM / forced exit 11:59 AM
python dhan/run_trades.py --exit-925
python dhan/run_trades.py --exit-1159

# Mirrored short square-off, 2:39 PM
python dhan/run_trades.py --square-off-239

# Self-test (mocked, no real orders)
python dhan/test/test_all.py
```

### Auth

```bash
# Save today's Dhan access token (daily, before 9:13 AM — generate at web.dhan.co first)
python -m dhan.auth <ACCESS_TOKEN>
```

---

## Telegram Notifications

Every notification function in `pipeline/notify.py` is wired up and active — routed across Telegram forum topics (`signals`, `entries_exits`, `live_tracking`, `errors`, `pnl`) via `TELEGRAM_TOPIC_*` env vars, falling back to the group's General topic if a topic ID isn't set.

| Event | Sent by | When |
|---|---|---|
| Pipeline **success** | `pipeline/main.py` | After the 3:06 PM run completes — date, signal count, runtime, full trade table |
| Pipeline **failure** | `pipeline/main.py` | If any step crashes — date, failed step, runtime, error detail |
| Scan **preview** | `scan/scan_intraday.py` | Whenever run manually — clearly labeled as a preview, not the official signal |
| **Entry** / targets / exits / short-open / square-off / daily summary | `dhan/run_trades.py` | See [Dhan Pipeline](#dhan-pipeline-independent-parallel-broker) for each stage's exact trigger |
| Monitor **started** / **failed to start** / **qualified** / **near circuit** / **disconnect** / **reconnect** / **heartbeat** | `dhan/live_monitor.py` | Same events, Dhan side — see [Dhan Pipeline → Live Monitor](#live-monitor) |

Every trading-stage and live-monitor notification call is wrapped in its own `try/except` — a Telegram outage can never break order placement, position tracking, or the monitor loop itself; a failed send just prints to stderr (`[notify] ... failed: ...`) instead of raising.

### Setting Up on a New Bot or Group

1. Create a bot via `@BotFather` → copy the token → set `TELEGRAM_BOT_TOKEN` in `pipeline/.env`
2. Add the bot to your Telegram group
3. Send a message in the group, then run:
   ```bash
   curl "https://api.telegram.org/bot<TOKEN>/getUpdates"
   ```
4. Find `chat.id` in the response (negative integer for supergroups) → set `TELEGRAM_CHAT_ID` in `pipeline/.env`
5. *(Optional)* If the group has forum topics enabled, get each topic's `message_thread_id` the same way (send a message in that specific topic, then check the same `getUpdates` response) and set the matching `TELEGRAM_TOPIC_*` variable above. Any topic left unset just falls back to posting in General — nothing breaks either way.

---

## Cron Schedule Summary

All times IST (UTC+5:30), server timezone set to `Asia/Kolkata`. All jobs below are Mon–Fri only (`1-5`) unless noted.

**Zerodha's code was removed 2026-09-10, pending a full rebuild.** Its own entry/exit/live-monitor cron rows had already been unscheduled for weeks before removal — none of them were ever real. The rows below are the shared infrastructure that survives the removal (used by Dhan too, and by whatever replaces Zerodha later).

| Time (IST) | Cron | Command |
|---|---|---|
| 3:06 PM | `6 15 * * 1-5` | `scripts/run_pipeline.sh` |
| 4:00 PM | `0 16 * * 1-5` | `pipeline/data_loader.py --eod-fill` |
| 5:00 PM | `0 17 * * 1-5` | `scripts/push_data_updates.sh` |

### Dhan Pipeline Cron

Fully independent of the Zerodha cron lines above — separate log files, separate failure domain.

| Time (IST) | Cron | Command |
|---|---|---|
| 8:00 AM & 8:00 PM (every day) | `0 8,20 * * *` | `python -m dhan.auth --renew` — access-token auto-renewal (see [Auth](#auth-automated-daily-renewal-manual-fallback)) |
| 9:13 AM | `10 9 * * 1-5` | `scripts/run_dhan_live_monitor.sh` |
| 9:15 AM | `15 9 * * 1-5` | `dhan/run_trades.py --place-targets` |
| 9:24 AM | `24 9 * * 1-5` | `dhan/run_trades.py --exit-925` — one minute earlier than the actual decision point; the script itself holds internally at 09:24:50 (prep) and 09:25:00 (fire) — see the note below the table |
| 11:58 AM | `58 11 * * 1-5` | `dhan/run_trades.py --exit-1159` — holds internally at 11:58:50 (prep) and 11:59:00 (fire) |
| 2:38 PM | `38 14 * * 1-5` | `dhan/run_trades.py --square-off-239` — holds internally at 14:38:50 (prep) and 14:39:00 (fire) |
| 3:21 PM | `21 15 * * 1-5` | `dhan/run_trades.py --entry` |
| 3:40 PM | `40 15 * * 1-5` | `pkill -f 'dhan\.live_monitor'` |

> The token-renewal job runs **every day of the week**, not just Mon–Fri (see [Auth](#auth-automated-daily-renewal-manual-fallback) for why). Every other Dhan cron line stays Mon–Fri only. UC-based staged entry (`--enable-uc-staged-entry`) is **not** in any cron line above — `scripts/run_dhan_live_monitor.sh` still launches plain `python3.11 -u -m dhan.live_monitor` with no flags; the feature is tested manually (see [UC-Based Staged Entry](#uc-based-staged-entry-case-ab--off-by-default)) until explicitly wired in.
>
> **`--exit-925`/`--exit-1159`/`--square-off-239` cron times moved one minute earlier** (was 9:25/11:59/2:39) so each script has runway to reach two pinned wall-clock instants internally, same mechanism as `--entry`'s existing 15:20:57/15:21:00 staging hold (`_seconds_until`/`_hold_until` in `dhan/run_trades.py`): a prep-check 10 seconds before the real decision time (position load, Order Book snapshot, UC-cache read — none of it price-dependent) followed by the actual fire instant (fresh LTP, then the sell/cover decision). The stage names/flags/position-file field names (`exit_925`, `exit_1159`, `square_off_239`) are unchanged — only the cron trigger time and internal timing moved, not the decision logic itself.

---

*Pipeline runs Mon–Fri · DigitalOcean Ubuntu 22.04 · Python 3.11 · Upstox V3 API (data) · Dhan (execution, live monitoring) · Telegram alerts throughout · Dashboard published as a Claude Artifact*
