# NSE Volume Pipeline ("volume-daily-btst")

Automated NSE mid-cap momentum/gap-up scanner and live trading system, running unattended
on a DigitalOcean Ubuntu VM via cron. Scans the ₹1,500–5,000 Cr market-cap band for unusual
volume + a strong intraday gap, buys a basket near the close, holds overnight (**BTST** —
Buy Today, Sell Tomorrow), and exits the next trading morning on a schedule keyed to how
strong the original signal was. Candle data, live reference prices, and live order
execution all go through **Dhan**.

There is no human in the loop during market hours. Every decision point — signal
generation, entry, exit, mirrored short, square-off — is a scheduled script, each one
designed to **never guess**: if a broker confirmation is ambiguous (a fill poll times out,
an API call fails, charges aren't in the trade-book yet), the code either leaves the
position flagged for a later reconciliation pass or reads 0/pending rather than fabricating
a number. This principle recurs throughout the codebase's own comments and explains why
several mechanisms below exist (see [Project History](#project-history--issues-resolved)
for the real incidents that taught it).

**Zerodha's execution code was removed 2026-09-10, pending a full rebuild** that never
happened — it previously ran a parallel live trading pipeline against the same signals. Its
historical trade data (`results/positions_zerodha_long.json`,
`results/positions_zerodha_short.json`, `results/trade_book.xlsx`) was kept, not deleted,
but none of it is live.

---

## Table of Contents

- [Quick Facts](#quick-facts)
- [Signal Logic](#signal-logic)
- [Capital Allocation](#capital-allocation)
- [Complete Daily Flow](#complete-daily-flow)
- [Dhan Live Trading](#dhan-live-trading)
  - [Entry](#entry)
  - [Profit Targets & Return-Bucketed Exit Schedule](#profit-targets--return-bucketed-exit-schedule)
  - [Mirrored Intraday Shorts & Square-Off](#mirrored-intraday-shorts--square-off)
  - [Reconciliation](#reconciliation)
  - [Charges / Cost Model](#charges--cost-model)
  - [Live Monitor](#live-monitor)
  - [Positions JSON Schema](#positions-json-schema)
  - [Auth](#auth)
  - [Removed Features](#removed-features)
- [P&L Tracking](#pl-tracking)
- [Repository Structure](#repository-structure)
- [First-Time VM Setup](#first-time-vm-setup)
- [Configuration](#configuration)
- [Daily Operations](#daily-operations)
- [Manual Commands](#manual-commands)
- [Testing](#testing)
- [Telegram Notifications](#telegram-notifications)
- [Cron Schedule Summary](#cron-schedule-summary)
- [Project History / Issues Resolved](#project-history--issues-resolved)

---

## Quick Facts

| | |
|---|---|
| Universe | NSE EQ/BE, ₹1,500–5,000 Cr market cap |
| Signal conditions | 6× 36-day avg volume **and** ≥5% gap over prior-day VWAP |
| Live trading capital | **₹11,00,000** (`TOTAL_CAPITAL`, effective 2026-11-01) |
| Entry | Tranched LIMIT (~3:06–3:20 PM) + a MARKET safety-net fallback (3:20 PM) |
| Exit | Return-bucketed 3-stage schedule (5-10% / 10-15% / 15-20% buckets, each with its own exit/forced-exit/square-off times) |
| Broker (live) | Dhan — candle data, reference prices, and order execution all go through it |
| P&L tracking | One workbook, `results/volume-daily-btst-pnl.xlsx`, synced to Google Drive daily |
| Data source | Dhan Data API (`/charts/intraday`) — migrated off Upstox 2026-09-24 |

---

## Signal Logic

**All three conditions must pass** for a symbol to appear in the trade list. Implemented in
`pipeline/signal_engine.py`.

### 1. Market Cap Filter
Symbol must be present in today's Screener.in export (₹1,500–5,000 Cr band) — the first
gate; only stocks currently in the universe are evaluated.

### 2. Volume Condition
```
Cumulative volume (09:15–14:45) ≥ 6 × 36-day rolling average full-day volume
```
- Rolling window: 36 prior trading days, non-zero volume days only
- Symbols with fewer than 36 days of history are skipped

### 3. Return Condition
```
Reference candle close ≥ 5% above previous trading day's VWAP
```
- **STRICT mode** (the real 3:06 PM run): reference candle is fixed at 15:00 (falls back to
  the 14:45 close if 15:00 hasn't posted yet)
- **PRORATED mode** (`scan/scan_intraday.py`, anytime preview): reference candle is
  whatever's latest as of check time — a directional preview only, never read by any
  execution script

Previous-day VWAP is calculated from the prior trading day's 15:00 and 15:15 candles:
```
VWAP = Σ((H + L + C) / 3 × Volume) / Σ(Volume)
```

`return_pct` (how far above the 5% floor the signal actually closed) is fixed once at
signal time and later becomes the position's **return bucket** at exit time — see
[Profit Targets & Return-Bucketed Exit Schedule](#profit-targets--return-bucketed-exit-schedule).

---

## Capital Allocation

**Current live capital: ₹11,00,000**, effective 2026-11-01 (`TOTAL_CAPITAL` in
`dhan/run_trades.py`, `pipeline/main.py`, `pipeline/notify.py` — all three must stay in
sync). History: ₹5L (initial MTF test) → ₹14L → ₹14.5L → ₹15L (2026-08) → ₹12L
(2026-10-01) → **₹11L (2026-11-01)**.

**Two separate, independent allocations exist — don't confuse them:**

1. **`pipeline/main.py`'s own trade list** sizes the `shares`/`ref_price` columns written to
   `results/trades/trade_list_<date>.csv` against the live `TOTAL_CAPITAL` — this is
   display/notification sizing only, shown in the Telegram signal message.
2. **The actual live execution** (`dhan/run_trades.py`) ignores those CSV columns entirely
   — it only reads the `symbol` column, then recomputes quantity itself from the real
   capital pool and its own freshly-fetched reference price.

Both use the same tiered rule — max ₹ per position is `capital/4` (idle capital allowed
when signals ≤ 4); once signals hit 5+, the full capital splits equally instead of
over-allocating:

```python
allocation = capital / 4 if n <= 4 else capital / n
shares     = floor(allocation / ref_price)
```

**MTF vs CNC**: per symbol, `/margincalculator` is checked live. Leverage **≥2x** →
`product="MTF"` at the full allocation. Leverage <2x, or Dhan's margin-calculator was wrong
about eligibility and the live order gets rejected for it anyway → falls back to
`product="CNC"` at **half** the capital base (`capital/2`, resized allocation/shares).
`MARGIN`/T+5 is never used as a product anywhere in this pipeline.

> **MTF-ineligibility CNC retry**: `/margincalculator`'s pre-check isn't fully reliable — it
> can return a plausible leverage figure for a scrip that Dhan then genuinely rejects at
> order-placement time (`"Mtf Product Is Not Allowed For This Scrip"`). When that *specific*
> rejection is confirmed (not just any rejection — a circuit-limit rejection, for instance,
> retries nothing, since CNC would hit the same price band), the order is retried as CNC
> automatically at the same quantity. This applies to entries, the 9:15 AM targets, every
> exit branch, and the forced exit. A T+5-settlement-lag rejection (`"No eligible T+5
> quantity found"`) and a CNC settlement-lag rejection both also retry as CNC, same
> mechanism, with their own specific wait/retry sequencing.

---

## Complete Daily Flow

```
 9:10 AM  — [CRON] Live monitor starts (tick-by-tick qualifying-condition watch)
 9:12 AM  — [CRON] --reconcile: catches any position the broker already closed/filled
            that our own confirmation poll missed
 9:15 AM  — [CRON] --place-targets: 17% profit-target LIMIT sells for every open long
 9:15–9:44 AM  — [CRON] Exit checks (--exit-916), one per return bucket
10:17–11:59 AM — [CRON] Forced exits (--exit-1159), one per return bucket
 2:28–2:57 PM  — [CRON] Short square-offs (--square-off-239), one per return bucket
 3:10 PM  — [CRON] Signal pipeline: scans volume/return, writes trade list, notifies Telegram
 3:10–3:20 PM — Tranched LIMIT entry fires immediately after the pipeline (run_entry_limit)
 3:20 PM  — [CRON] MARKET-entry safety net for anything still unfilled (--entry)
 3:40 PM  — [CRON] Live monitor stopped
 3:45 PM  — [CRON] --sync-pnl: regenerates the P&L workbook, pushes it to Google Drive
 4:00 PM  — [CRON] EOD candle fill: corrects the in-progress 15:00 candle, adds the 15:15 one
 4:15 PM  — [CRON] Auto-push data/results changes to GitHub (includes the fresh P&L workbook)
─────────── overnight hold, cycle repeats ─────────────────────────────────────
```

See [Cron Schedule Summary](#cron-schedule-summary) for exact times (every exit-stage line
above is really 3 separate cron lines, one per return bucket).

### Signal Pipeline (3:10 PM) — `pipeline/main.py`

1. **Fetch market cap** — `data_loader.load_market_cap()` logs into Screener.in (Premium),
   runs the query `Market Capitalization > 1500 AND Market Capitalization < 5000`, exports
   to `data/market_cap_daily/market_cap_<date>.csv`. Falls back to the most recent prior
   export (with a warning) if the live fetch fails, or fails the whole run if there's no
   fallback at all.
2. **Update universe** — new symbols not previously seen are appended to
   `data/universe_combined.csv`.
3. **Candle data update** — for brand-new symbols: resolves a Dhan `securityId` and
   backfills 6 months of 15-min candles. For every tracked symbol: fetches today's intraday
   15-min candles (necessarily incomplete at fetch time until the 4:00 PM EOD fill corrects
   it).
4. **Signal check (STRICT mode)** — `signal_engine.get_signals()` applies all three
   conditions above.
5. **Write trade list** — `results/trades/trade_list_<date>.csv`
   (`symbol`/`shares`/`ref_price` — see the capital-allocation caveat; these sizing columns
   are display only, not what actually gets bought).
6. **Notify Telegram** — signal count + trade table, or a failure message naming the failed
   step.

### EOD Candle Fill (4:00 PM) — `pipeline/data_loader.py --eod-fill`

Fixes two real data-completeness gaps: the candle "in progress" when the 3:10 PM run
fetched intraday data is incomplete (collapsed OHLC, near-zero volume), and the session's
final 15:15 candle doesn't exist yet at 3:10 PM at all. Uses the **intraday** endpoint (the
historical one returns zero rows for same-day dates) and lets freshly fetched data
**overwrite** any existing row for the same timestamp. The file is re-sorted on every
write.

### Auto-Push Data & Results (4:15 PM) — `scripts/push_data_updates.sh`

Stages **only** `data/` and `results/` — never code, so an in-progress code edit can never
get swept into an unattended commit. Commits as `Data update — <date> candle + results
refresh` and pushes to `main`. Exits cleanly with no empty commit if nothing changed.

---

## Dhan Live Trading

Live execution — own capital pool, own position files, own live monitor, own auth model. It
reads the **same** `results/trades/trade_list_<date>.csv` the signal pipeline writes.

> **Why LIMIT, not MARKET, for entries**: Dhan/NSE silently apply their own
> price-protection band to a `MARKET` order — every order placed as `orderType: MARKET`
> came back from Dhan's own order records as `orderType: LIMIT` near the submission price
> (confirmed live 2026-08-17). That band was tight enough that ordinary same-second price
> movement (no circuit lock involved) produced 0-fills. An explicit LIMIT instead gives
> wider, predictable headroom. Dhan's order API has no exposed market-protection/collar
> parameter (unlike Kite's `market_protection`).

### Entry

Two mechanisms run back-to-back:

**1. Tranched limit entry — `run_entry_limit` (primary, fires ~3:10 PM, cutoff 3:20 PM)**.
Fires immediately after the signal pipeline finishes, for **every** symbol in the day's
trade list (routed through this path in full since 2026-10-08 — it used to be only the
least-liquid half). Places a LIMIT BUY at **best_bid + 1 tick**, showing a fixed tranche =
`floor(initial_desired / 10)` shares (fixed, not a shrinking remainder — gets real price
averaging as tranches fill at different bids over the window). Every **10 seconds**,
re-fetches best bids for every active symbol in one batched `/marketfeed/quote` call; if
the market has moved up, cancels and re-places at the new best_bid+tick. At **3:20 PM**:
cancels every still-open limit order, then MARKET-sweeps whatever remains unfilled.
Position records store `actual_fill_price` as the VWAP across every partial fill.

**2. Market-entry safety net — `run_entry_321` (fires ~3:20 PM, holds internally to
3:21:00)**. A pure safety net for any symbol `run_entry_limit` got zero fills on. Waits on
`run_entry_limit`'s own completion marker before reading positions (so it never
double-dedups mid-write), then enters anything still missing at a LIMIT price 0.75% above
live LTP (falls back to the reference price as the limit anchor if LTP is momentarily
unavailable). Every LIMIT order shows a **20% disclosed quantity**, hiding the rest from
the public book.

Every entry fill (either path) tags the position with its `return_pct` and the resulting
**return bucket** — there is no untagged/legacy fallback for anything entered after
2026-09-27.

### Profit Targets & Return-Bucketed Exit Schedule

**Profit targets — 9:15 AM (`--place-targets`)**: for every open long without one yet,
places a resting LIMIT sell at **17% above the recorded fill price** (capped at 0.5% below
the day's upper circuit if a plain 17% would exceed it).

Every long is classified at **entry** by its own signal's `return_pct` into one of three
buckets, and that bucket owns its own independent 3-stage exit schedule for the entire life
of the position:

| Bucket | `return_pct` range | Exit-check (916-style) | Forced exit (1159-style) | Short square-off (239-style) |
|---|---|---|---|---|
| **5-10** | 5.00 – 10.00% | 9:16 AM | 10:18 AM | 2:29 PM |
| **10-15** | 10.01 – 15.00% | 9:18 AM | 11:59 AM | 2:57 PM |
| **15-20** | 15.01%+ (no separate "20+" bucket) | 9:44 AM | 11:41 AM | 2:40 PM |

(This replaced one fixed 9:16/11:59/2:39 schedule for every position, 2026-09-26 — see
[Project History](#project-history--issues-resolved).)

Each stage's cron fires **one minute before** its real decision instant and holds
internally at two pinned wall-clock points: a `:50` prep check (position load, Order Book
snapshot, UC-cache read — none of it price-dependent, safe early) and the `:00` fire
instant (fresh LTP, then the actual decision) — every position in the same bucket decides
off the *exact same tick*.

**Exit-check (916-style) branches, in priority order:**
1. **17% target already hit** → close from the target's own fill, done.
2. **Live P&L > 0** → cancel the resting target, sell the full position at a fresh LIMIT.
3. **No LTP data** → cancel the resting target, sell **half** as a precaution
   (`partial_exit_916_nodata`), place a fresh target for the remainder, remainder carries to
   the forced-exit stage.
4. **P&L ≤ 0** → hold; target stays live untouched until the forced exit.

**Forced exit (1159-style)**: unconditional close of whatever's still open — no P&L gate.

### Mirrored Intraday Shorts & Square-Off

**Every long exit that actually fills** opens a same-quantity `productType=INTRADAY` short
in the same symbol — but **only if the long's own realized return was ≥3%**
(`LONG_RETURN_GATE_PCT` in `dhan/run_trades.py`; a weak/losing long exit doesn't get
mirrored). Two protective orders go live immediately: a 5%-below cover-target LIMIT buy,
and a `STOP_LOSS_MARKET` buy triggered 0.5% below the day's upper circuit.

**Short square-off (239-style)**: unconditional close of every open short in that bucket,
with full **OCO** (one-cancels-other) handling between the cover target and the stop-loss —
whichever shows `TRADED` closes the position from its own fill and cancels the other;
both-filled is flagged for manual review rather than guessed; neither-filled force-covers
at MARKET.

### Reconciliation

Two standing checks exist specifically to catch "the broker did something our own
confirmation poll didn't see in time" — both born from real incidents (see
[Project History](#project-history--issues-resolved)):

- **`--reconcile`** (9:12 AM cron, 3 min before `--place-targets`): for every open long,
  compares the broker's real holding qty to our local record. A mismatch means a sell
  filled after our poll gave up — pulls the real fill from `/trades` and closes the
  position for real, never guessing a price.
- **Short-open recovery** (called from every `square_off_239` bucket run, 3×/day, plus the
  same 9:12 AM slot): a short-open order whose poll timed out used to be silently
  discarded, leaving a real, unprotected, unmanaged short sitting at the broker. The order
  id is now preserved and re-checked — if it actually filled, the position is recovered
  with its real fill price and immediately given its missing cover-target + stop-loss.

Both refuse to guess on anything genuinely ambiguous (a broker qty *higher* than expected, a
`/trades` sum that doesn't exactly match the delta) — left flagged for manual review rather
than fabricated.

### Charges / Cost Model

Every closed position's P&L entry carries a real, **never-estimated** charges figure
(`dhan/charges.py`):
- **Entry/exit leg charges** come **only** from Dhan's real trade-book API — a rate-card
  estimate fallback existed early on and was deliberately removed. A same-day fill reads
  0/"pending" until the trade-book indexes it the next day.
- **DP/pledge charges**: fixed estimate (Dhan's ledger only exposes these as one combined
  daily total, with no way to split it back to individual positions).
- **MTF interest**: prefers a real-ledger-based allocation (scales every MTF position's
  formula-estimate weight so the whole billing period's positions sum to the real billed
  total) over the pure funded×rate×days formula, falling back to the formula only where the
  ledger doesn't cover a position yet.

### Live Monitor

```bash
python -m dhan.live_monitor
```

Runs 9:10 AM–3:40 PM (cron-managed), using the `dhanhq` package's MarketFeed WebSocket for
ticks — Telegram-only, no CSV log. Watches the same qualifying conditions as the signal
pipeline, but live and continuously:
- **`qualified`**: cumulative volume ≥ 6× the 36-day average *and* LTP ≥ prev-day VWAP ×
  1.05 — evaluated tick-by-tick instead of once
- **`near_circuit`**: LTP within 1% of the upper circuit limit, only after a symbol has
  already qualified
- Both stop evaluating at **3:00 PM** (the real STRICT pipeline run takes over from there)

### Positions JSON Schema

Longs (`results/positions_dhan_long.json`) and shorts (`results/positions_dhan_short.json`)
live in **separate files** (not one combined file with a `direction` field — a same-day
mirrored short used to silently block a fresh same-day long re-entry on the same symbol
when they shared a file; confirmed live 2026-08-20 on BAJAJHIND and ZAGGLE).

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

### Auth

Unlike Kite Connect, Dhan has no OAuth handshake for a personal account — the **very first**
token still has to be generated by hand at **web.dhan.co → Profile → Access DhanHQ Trading
APIs** (valid 24h):

```bash
python -m dhan.auth <ACCESS_TOKEN>
```

From then on, `python -m dhan.auth --renew` renews it automatically, twice a day (8:00 AM
and 8:00 PM IST) via `GET /v2/RenewToken`, verifying the new token with a real `/fundlimit`
call before saving — a failed renewal (or failed verification) leaves the previous saved
token completely untouched. Two confirmed gotchas: the renew call **must be GET**, not
POST/PUT; and renewing **invalidates the previous token immediately** — no overlap window —
which is why both renewal times sit in the dead zone between the live-monitor `pkill`
(3:40 PM) and the next launch (9:10 AM), and why it runs twice daily (a comfortable
multi-hour buffer either way, and a failed attempt is caught 12 hours later instead of a
full day later). Saved to `dhan/.token.json` (gitignored). A renewal failure sends a
Telegram alert (`errors` topic) rather than failing silently.

### Removed Features

- **UC-based staged entry (Case A/B)**: a second, tick-driven entry mechanism, live
  2026-09-09 through 2026-09-21, removed after a real bug (one branch re-fired every tick
  on the same symbol instead of latching once, burning real balance-check API calls in a
  tight loop). Never rebuilt; superseded in spirit by the tranched `run_entry_limit`
  mechanism above. Design writeup kept at `results/UC_Staged_Entry_Explained.docx`.
- **Zerodha execution** — removed 2026-09-10, pending a rebuild that never happened.
- **Google Sheets live dashboard** (webhook-based push) — removed; Google Drive-synced
  Excel (below) replaced it.

---

## P&L Tracking

**One workbook**, `results/volume-daily-btst-pnl.xlsx`, covering every trade since
2026-08-19, regenerated daily via `--sync-pnl` and pushed to Google Drive as a live-synced
Sheet (true in-place overwrite, same file ID/link). Eight sheets: Total PnL (dashboard),
Trade Log, Day Wise PnL, Position Type Stats, Bucket Stats, Company Stats, Charges, Monthly
& Weekly PnL.

Capital changed 3× across this history (₹15L → ₹12L → ₹11L), so only the Total PnL sheet is
capital-era-sensitive (its %-of-capital KPIs). All-time headline cards there show only
₹/count metrics (honest at any capital level); a separate **"By Capital Era"** table breaks
out Net P&L %/Gross P&L % per era, each against its own correct Base Capital. Every cell is
a live formula recomputed from `positions_dhan_long.json`/`positions_dhan_short.json` on
every run, not a stale cached number.

(Replaced 3 separate per-era files — `strategy_pnl_simple.xlsx`/`strategy_pnl_oct2026.xlsx`/
`strategy_pnl_nov2026.xlsx` — on 2026-11-02; see
[Project History](#project-history--issues-resolved).)

---

## Repository Structure

```
volume-daily/
├── pipeline/
│   ├── main.py                  # Daily orchestrator — called by cron at 3:10 PM
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
│   ├── charges.py                # Real trade-book charges + MTF-interest allocation (no estimates)
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
    ├── pending_short_verifications.json  # Short-open orders awaiting re-check (see Project History)
    ├── positions_zerodha_long.json       # Zerodha longs — historical only, code removed
    ├── positions_zerodha_short.json      # Zerodha mirrored shorts — historical only, code removed
    └── volume-daily-btst-pnl.xlsx        # The one consolidated P&L workbook
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
| `DHAN_CLIENT_ID` | Dhan client ID (from web.dhan.co) — the access token itself is *not* stored here, see [Auth](#auth) |

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
files apply process-wide mocks at import time that are only safe in isolation.

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
they come up — no manual cleanup needed afterward, the date simply stops matching);
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
> `run_entry_321`'s own 15:20:57/15:21:00 staging hold (`_hold_until` in
> `dhan/run_trades.py`). The token-renewal job runs **every day**, not just Mon–Fri — a
> token still needs renewing on a holiday so Monday's first real run doesn't start from an
> expired one.

---

## Project History / Issues Resolved

A thematic history of the real bugs, incidents, and design decisions behind this codebase —
the "why" behind several choices above. Not a line-by-line commit log (300+ commits as of
2026-11-02, most of them daily data refreshes) — the substantive history only.

### Broker and data-source migrations
- **Kite/Zerodha → removed, pending rebuild (2026-09-10)**. Historical position files kept,
  not deleted. Never rebuilt since.
- **Upstox → Dhan for candle data (2026-09-24)**. Upstox was never a trading broker here,
  only the candle/reference-price source — migrated to Dhan's own Data API so candle data
  and live execution share one API/one token.
- **MARKET → LIMIT orders (2026-08-17)** — see [Dhan Live Trading](#dhan-live-trading)
  above for why.

### The phantom-fill / reconciliation story

This is the throughline of the whole codebase. The founding incident: an entry poll timeout
was originally handled by falling back to a caller-supplied fallback price/qty **as if it
were a real fill** — i.e., guessing. Confirmed live: a position got recorded as filled at a
price it never actually traded at. Fixed by making every poll-timeout path return "not
confirmed" instead of guessing — but that correctly-cautious choice has a consequence: a
REST poll that gives up after ~15s doesn't mean the order is dead, and because the code
refuses to guess, nothing re-checked it afterward for a long time. This recurred,
hand-investigated, on NOVARTIND, RML, JINDRILL, 6 manual trades from 2026-09-21,
DPABHUSHAN, YUKEN, KOTIC, MMWL (twice), SUNCLAY, INDOKEM, and SIMPLEXINF before it was
recognized as systemic and automated away via `--reconcile` (`_reconcile_stale_exits`) —
deliberately its **own** CLI flag/cron slot rather than inlined into the hot exit-check
path, after inlining it first caused `test_targets.py` to balloon from ~9s to ~190s on
unmocked network calls.

The same gap existed on the **short-open side** and wasn't caught by that fix: FISCHER's
mirrored short (2026-10-26) filled 18 seconds after placement — 8 seconds past the poll's
budget — and was discarded with no cover-target, no stop-loss, no position record. The
naked short sat unmanaged for 5 hours until **Dhan's own RMS auto-covered it** before close
(NSE doesn't allow carrying a naked equity short overnight) at a price with zero input from
this system — a ₹4,584 avoidable loss. Fixed with the same pattern: the order id is now
preserved and re-checked (`_reconcile_pending_shorts`), recovering the position with its
real fill price and immediately placing the missing protective orders if it actually
traded.

### Rejection-retry logic

A cluster of fixes around Dhan's various RMS rejection reasons, each needing a different,
*specific* retry — MTF-ineligibility → retry as CNC; T+5/MARGIN settlement lag → retry as
CNC; MTF pledge-ledger lag → retry as MTF again (not CNC); CNC settlement lag → wait 30s,
retry as CNC, then MTF as a last resort. An earlier, cruder version retried on *any*
rejection rather than the confirmed specific one and was tightened multiple times.

### Exit schedule evolution

Exit stages were originally one fixed set of times (renamed 945→925→916 along the way).
**Return-bucketed exit schedule** (2026-09-26): replaced the single fixed schedule with
three independent ones, each position bucketed by its own entry signal's `return_pct` — no
default/untagged bucket exists since 2026-09-27. **Mirrored-short gate** (≥3% realized
return) added so a long that barely escaped flat doesn't get a short mirrored on it.

### Charges/cost model evolution

Started by always computing costs from a rate-card estimate, then preferring the real
trade-book API with an estimate fallback, then **removing the estimate fallback entirely**
(2026-09-15) — explicit instruction: show "no charge data yet" rather than a guessed
number, ever. A tranche-entry charge bug could strand entry charges at 0 if even one of
several order ids failed to resolve (fixed, backfilled). The trades API's pagination had no
retry-on-429 and discarded every already-fetched page on ANY failure — a transient 429 on a
late page was silently destroying real earlier-page data too (fixed with a rate-limited
retry wrapper).

**The VERANDA interest-allocation bug (2026-11-02)**: an audit of whether all closed trades
had their costs properly populated surfaced a bug bigger than the single trade that
triggered the question. The shared MTF-interest-allocation build does one `security_id()`
lookup per position inside a single call covering *every* MTF position at once (deliberately
— two separate trade-book calls back-to-back had already been confirmed to trip Dhan's rate
limit). A delisted symbol (VERANDA) raised `ValueError` there, which wasn't caught — it
propagated out of the whole function and was swallowed by a blanket exception handler,
**silently degrading every MTF position's interest charge from the accurate real-ledger
figure to the less-precise formula estimate, on every single sync run**, for as long as
VERANDA existed in the position file — not just VERANDA's own charge. Fixed by catching the
error at both the batch-build site and the per-position fallback site, scoping the failure
to just the one unresolvable symbol.

### Rate limiting and WS-first fill confirmation

Dhan's UC-fetch and Quote APIs both needed 429-retry/batching fixes over time. **WS-first
fill confirmation** (made permanent 2026-09-18): checks Dhan's real-time Order Update
WebSocket feed before falling back to REST polling — a time-boxed trial's shadow-logging
showed the WS feed confirmed every one of 66/66 compared fills before REST did (avg ~0.9s
lead). A separate precision bug surfaced inside this same fast path (PARACABLES,
2026-10-21): the WS feed's own `avg_price` field is rounded to 2 decimals versus the REST
endpoint's true higher precision — small but systemic; the one affected record was
corrected, and the broader fix (always re-pull the precise REST price) was deliberately not
applied since it would cost back the latency this path exists to save.

### P&L workbook evolution

Flattened CSV/XLSX → dashboard redesign with live-synced Trade Log → Charges
breakdown/bucket/monthly/weekly stats added → drawdown tracking added, then removed again
per user request. Google Sheets webhook push tried, then replaced with an rclone-synced
Google Drive Excel (true in-place overwrite). Capital-era split into 2 files (2026-10-01),
then 3 (2026-11-01) as capital changed — then **consolidated back into ONE file**
(2026-11-02) once it was recognized that only the Total PnL sheet's two %-of-capital cards
ever actually depended on Base Capital; every other sheet was capital-independent already.
Verified correct via an independent LibreOffice headless recalculation cross-checked
against a from-scratch Python aggregation, not just visual inspection of the formulas. The
Google Drive push itself was originally a separate timed cron line, folded into
`--sync-pnl` itself (2026-10-12) so a manual run also pushes, closing a drift gap that had
caused a "the Sheet looks stale" false alarm.

### Notifications

Order IDs removed from every Telegram message (internal detail, not useful to a reader).
Return-bucket tagging added to every entry and exit notification. The "target placed"
notification removed entirely (not an outcome worth a push alert). The `QUALIFIED`
live-monitor alert's volume figure was showing `cum_vol / threshold` (already-6×'d
denominator), understating the real ratio by ~6x — removed rather than leave two different
"ratio" meanings in the same alert type.

### Position-file structure

Positions were originally one file keyed by a `direction` field. Split into separate
long/short files after a real bug: a same-day mirrored short's `entry_date` made a *fresh*
same-day long re-entry on that same symbol silently skip itself (confirmed live 2026-08-20
on BAJAJHIND and ZAGGLE). Separate files make the collision structurally impossible rather
than relying on a smarter dedup check.

### Testing infrastructure

All Dhan tests consolidated into `dhan/test/` with one `test_all.py` runner — each file
still runs as its own subprocess, since several apply process-wide mocks at import time
that would otherwise leak across files sharing one process. A real isolation bug found
during the short-open reconcile work: an unconditional `import dhan.charges` at the top of
a function executed (and cached) even on paths that never used it, defeating
`unittest.mock.patch.dict(sys.modules, ...)` isolation across later test scenarios in the
same process — fixed by moving the import inside the branch that actually needs it.

### Holiday handling

Self-expiring one-line date checks chained with `||` as each new holiday comes up, applied
to every weekday cron line except the twice-daily token-renewal job, which deliberately
always runs.

---

*Pipeline runs Mon–Fri · DigitalOcean Ubuntu 22.04 · Python 3.11 · Dhan (data, execution,
live monitoring) · Telegram alerts throughout*
