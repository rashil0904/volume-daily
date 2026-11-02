# Strategy Reference

This is the single source of truth for **what this system does and why**, written so a
brand-new chat session (human or AI) can pick up full context without re-deriving it from
scratch. For the history of *bugs fixed and design decisions made along the way* — which
explains why many things below are built the specific way they are — see
[`ISSUES_RESOLVED.md`](ISSUES_RESOLVED.md). For setup/operations/cron, see
[`README.md`](README.md).

Last verified against the live code: **2026-11-02**.

---

## 1. What this is

A fully automated **NSE intraday momentum / gap-up strategy**, nicknamed **BTST** (Buy
Today, Sell Tomorrow) internally — it buys a basket of mid-cap stocks near the close on a
day they show unusual volume + a strong intraday gap, holds overnight, and exits the next
trading morning (or later the same morning if it hasn't moved yet). It runs unattended via
cron on a DigitalOcean Ubuntu VM, trades live through **Dhan** (broker), and reports every
action to Telegram.

There is no human in the loop during market hours. Every decision point — signal
generation, entry, exit, mirrored short, square-off — is a scheduled script, each one
designed to **never guess**: if a broker confirmation is ambiguous (a fill poll times out,
an API call fails, charges aren't in the trade-book yet), the code either leaves the
position flagged for a later reconciliation pass or reads 0/pending rather than fabricating
a number. This design principle recurs constantly in the codebase's own comments and is the
single most important thing to understand about how this system is built — see
`ISSUES_RESOLVED.md` §3 for the incidents that taught it.

---

## 2. Signal logic (`pipeline/signal_engine.py`)

A symbol enters the day's trade list only if **all three** conditions pass:

| # | Condition | Threshold |
|---|---|---|
| 1 | **Market cap** | ₹1,500–5,000 Cr (Screener.in export, `pipeline/fetch_market_cap.py`) |
| 2 | **Volume** | Cumulative volume (09:15–14:45) ≥ **6×** the 36-trading-day rolling average full-day volume (zero-volume days excluded; a symbol needs ≥36 days of history or it's skipped) |
| 3 | **Return** | Reference candle close ≥ **5%** above the *previous* trading day's VWAP |

- **Reference candle**: fixed at the **15:00** candle close in the real (STRICT) run; falls
  back to 14:45 if 15:00 hasn't posted yet. A separate PRORATED mode exists
  (`scan/scan_intraday.py`) for an anytime directional preview — it is **never** read by any
  execution script, display only.
- **Previous-day VWAP**: `Σ((H+L+C)/3 × Volume) / Σ(Volume)` over just the prior day's 15:00
  and 15:15 candles.
- `return_pct` (how far above that 5% floor the signal actually closed) is recorded on the
  signal and later becomes the position's **return bucket** at exit time (§5) — it is fixed
  once at signal time and never recomputed.

Candle data (15-min OHLCV) and live reference prices both come from **Dhan's own Data API**
(`/charts/intraday`) — migrated off Upstox 2026-09-24; see `ISSUES_RESOLVED.md` §1.

---

## 3. Capital allocation

**Current live capital: ₹11,00,000**, effective 2026-11-01 (`TOTAL_CAPITAL` in
`dhan/run_trades.py`, `pipeline/main.py`, `pipeline/notify.py` — all three must stay in
sync). History: ₹5L (initial MTF test) → ₹14L → ₹14.5L → ₹15L (2026-08) → ₹12L
(2026-10-01) → **₹11L (2026-11-01)**.

Sizing rule, applied identically to the signal list's display sizing and the real live
entry:

```python
allocation = capital / 4 if n_signals <= 4 else capital / n_signals
shares     = floor(allocation / ref_price)
```

i.e. never deploy more than a quarter of capital on one name even with few signals; once 5+
signals appear, split capital evenly instead of over-concentrating.

**Two independent sizing paths exist — don't confuse them:**
1. `pipeline/main.py`'s trade-list CSV (`shares`/`ref_price` columns) — **display/Telegram
   sizing only**, computed against `TOTAL_CAPITAL` at signal time.
2. `dhan/run_trades.py`'s real entry — ignores those CSV columns entirely except for the
   `symbol` column; it re-fetches a fresh reference price and recomputes quantity itself
   against the live capital pool at entry time.

**MTF vs CNC**: per symbol, `/margincalculator` is checked live. Leverage **≥2x** →
`product="MTF"` at the full allocation. Leverage <2x, or Dhan's margin-calculator was
simply wrong about eligibility and the live order gets rejected for it (confirmed, happens)
→ falls back to `product="CNC"` at **half** the capital base. `MARGIN`/T+5 is never used as
a product anywhere in this pipeline.

---

## 4. Entry — ~3:06 PM to 3:21 PM

Two mechanisms run back-to-back, not one:

### 4a. Tranched limit entry — `run_entry_limit` (primary, fires ~15:06, cutoff 15:20)

Fires immediately after the signal pipeline finishes. For **every** symbol in the day's
trade list (routed through this path in full since 2026-10-08 — see
`ISSUES_RESOLVED.md` §2; it used to be only the least-liquid half):

- Places a LIMIT BUY at **best_bid + 1 tick**, showing a fixed tranche =
  `floor(initial_desired / 10)` shares (fixed, not a shrinking remainder — this gets real
  price averaging as tranches fill at different bids over the window, rather than one
  single price for the whole position).
- Every **10 seconds**, re-fetches best bids for every active symbol in **one** batched
  `/marketfeed/quote` call. If the market has moved up, cancels and re-places at the new
  best_bid+tick; otherwise leaves the resting order alone.
- At **15:20** (`LIMIT_ENTRY_CUTOFF_HHMM`): cancels every still-open limit order, then
  MARKET-sweeps whatever quantity remains unfilled.
- Position records store `actual_fill_price` as the **VWAP across every partial fill**
  (limit tranches + the cutoff sweep), not a single price.
- Handles mid-loop MTF-ineligibility rejections (switches to CNC, resizes), zero-bid books
  (falls back to last_price), upper-circuit capping, and already-entered-today dedup.

### 4b. Market-entry safety net — `run_entry_321` (fires ~15:20, holds internally to 15:21:00)

A **pure safety net** for any symbol `run_entry_limit` got zero fills on (both the limit
phase and the cutoff sweep). It waits on `run_entry_limit`'s own completion marker before
reading positions (so it never double-dedups mid-write), then enters anything still
missing via the same CNC/MTF leverage-check logic as above, at a LIMIT price 0.75% above
live LTP (not MARKET — see `ISSUES_RESOLVED.md` §4 for why MARKET orders were abandoned
early on). Every LIMIT order shows a **20% disclosed quantity** (`disclosed_pct=0.20` in
`dhan/trade.py`), hiding the rest from the public book.

Every entry fill (either path) tags the position with its `return_pct` and the resulting
**return bucket** (next section) — there is no untagged/legacy fallback for anything
entered after 2026-09-27.

---

## 5. Exit — return-bucketed 3-stage schedule

Every long is classified at **entry** by its own signal's `return_pct` into one of three
buckets (`_return_bucket_for_pct` in `dhan/run_trades.py`), and that bucket owns its own
independent 3-stage exit schedule for the entire life of the position:

| Bucket | `return_pct` range | Exit-check (916-style) | Forced exit (1159-style) | Short square-off (239-style) |
|---|---|---|---|---|
| **5-10** | 5.00 – 10.00% | 9:16 AM | 10:18 AM | 2:29 PM |
| **10-15** | 10.01 – 15.00% | 9:18 AM | 11:59 AM | 2:57 PM |
| **15-20** | 15.01%+ (no separate "20+" bucket) | 9:44 AM | 11:41 AM | 2:40 PM |

(Historical note: these three stages were originally one fixed schedule for every
position — 9:16/11:59/2:39 — until 2026-09-26; see `ISSUES_RESOLVED.md` §5.)

Each stage's cron fires **one minute before** its real decision instant and the script
holds internally at two pinned wall-clock points: a `:50` prep check (position load, Order
Book snapshot, UC-cache read — none of it price-dependent, safe early) and the `:00` fire
instant (fresh LTP, then the actual sell/cover decision) — this guarantees every position
in the same bucket decides off the *exact same tick*, not whatever moment cron happened to
invoke the process.

### Exit-check (916-style) branches, in priority order
1. **17% profit target already hit** (resting LIMIT sell placed at 9:15 AM, capped at 0.5%
   below the day's upper circuit if a plain 17% would exceed it) → close from the target's
   own fill, done.
2. **Live P&L > 0** → cancel the resting target, sell the full position at a fresh LIMIT
   (LTP × 0.995).
3. **No LTP data available** → cancel the resting target, sell **half** as a precaution
   (`partial_exit_916_nodata`), place a fresh target for the remainder at the same price,
   remainder carries to the forced-exit stage.
4. **P&L ≤ 0** → hold; target stays live untouched until the forced exit.

### Forced exit (1159-style)
Unconditional close of whatever's still open at that bucket's forced-exit instant — no P&L
gate, blended P&L across any 916 partial + this remainder.

### Mirrored intraday shorts
**Every long exit that actually fills** (916 full-sell, 916 no-data half-sell, or forced
full-sell) opens a same-quantity `productType=INTRADAY` short in the same symbol — but
**only if the long's own realized return was ≥3%** (`LONG_RETURN_GATE_PCT`, added
2026-09-?? — see `ISSUES_RESOLVED.md` §5; a weak/losing long exit doesn't get mirrored).
Two protective orders go live immediately: a 5%-below cover-target LIMIT buy, and a
`STOP_LOSS_MARKET` buy triggered 0.5% below the day's upper circuit.

### Short square-off (239-style)
Unconditional close of every open short in that bucket, with full **OCO** handling between
the cover target and the stop-loss: whichever shows `TRADED` closes the position from its
own fill and cancels the other; both-filled is flagged for manual review rather than
guessed; neither-filled force-covers at MARKET.

---

## 6. Charges / cost model (`dhan/charges.py`)

Every closed position's P&L workbook entry carries a real, **never-estimated** charges
figure:

- **Entry/exit leg charges** (brokerage, STT, exchange, SEBI, stamp, GST) come **only** from
  Dhan's real trade-book API (`charges_index`, one `GET /trades/{from}/{to}/{page}` sweep
  per sync run, not per-leg) — a rate-card estimate fallback existed early on and was
  deliberately removed 2026-09-15. A same-day fill simply reads 0/"pending" until the
  trade-book indexes it the next day — that's expected, not an error.
- **DP/pledge charges**: fixed estimate (Dhan's ledger only exposes these as one combined
  daily total, with no way to split it back to individual positions) — applies to any
  delivery-style entry (CNC/MTF/MARGIN); MTF additionally gets the pledge/unpledge fee.
- **MTF interest**: prefers a real-ledger-based allocation (`mtf_interest_allocation_index`
  — scales every MTF position's formula-estimate weight so the whole billing period's
  positions sum to the REAL billed total) over the pure funded×rate×days formula. Falls
  back to the formula only for a position an ongoing/unposted billing period doesn't cover
  yet. See `ISSUES_RESOLVED.md` §7 for a real incident where one delisted symbol silently
  degraded *every* position to the formula fallback for months before being caught and
  fixed.

---

## 7. Reconciliation — the system's immune system

Two standing daily/per-run checks exist specifically to catch "the broker did something our
own confirmation poll didn't see in time" — both born from real incidents (full history in
`ISSUES_RESOLVED.md` §3):

- **`--reconcile`** (`_reconcile_stale_exits`, 9:12 AM cron, 3 min before `--place-targets`):
  for every open long, compares the broker's real holding qty to our local record. A
  mismatch means a sell filled after our poll gave up — pulls the real fill from `/trades`
  and closes the position for real, never guessing a price.
- **`_reconcile_pending_shorts`** (called from every `square_off_239` bucket run, 3×/day,
  plus the same 9:12 AM `--reconcile` slot): a short-open order whose poll timed out used
  to be silently discarded, leaving a real, unprotected, unmanaged short sitting at the
  broker. Now the order id is preserved and re-checked — if it actually filled, the
  position is recovered with its real fill price and immediately given its missing
  cover-target + stop-loss.

Both refuse to guess on anything genuinely ambiguous (a broker qty *higher* than expected, a
`/trades` sum that doesn't exactly match the delta) — they leave it flagged for manual
review rather than fabricating a number.

---

## 8. Live monitor (`dhan/live_monitor.py`)

Runs 9:10 AM–3:40 PM (cron-managed), watching the **same** qualifying conditions as the
signal pipeline (6× volume, LTP ≥ prev-day VWAP × 1.05) tick-by-tick via Dhan's MarketFeed
WebSocket, rather than once at 3:00 PM. Sends a `QUALIFIED` Telegram alert the moment a
symbol crosses both thresholds live, and a `near_circuit` alert once a qualified symbol gets
within 1% of its upper circuit. Both stop evaluating at 3:00 PM once the real STRICT pipeline
run takes over. Telegram-only, no CSV log.

---

## 9. P&L tracking (`results/build_pnl_simple.py`)

**One workbook**, `results/volume-daily-btst-pnl.xlsx` (consolidated 2026-11-02 from 3
separate per-era files — see `ISSUES_RESOLVED.md` §9), covering every trade since
2026-08-19, pushed to Google Drive as a live-synced Sheet on every `--sync-pnl` run. Eight
sheets: Total PnL (dashboard), Trade Log, Day Wise PnL, Position Type Stats, Bucket Stats,
Company Stats, Charges, Monthly & Weekly PnL.

Only the Total PnL sheet is capital-era-sensitive (%-of-capital KPIs) — capital changed 3×
across this history (₹15L → ₹12L → ₹11L), so blending them under one Base Capital figure
would misstate %-of-capital for whichever era didn't match. The fix: all-time headline cards
show only ₹/count metrics (honest at any capital level); a separate **"By Capital Era"**
table breaks out Net P&L %/Gross P&L % per era, each against its own correct base.

Regenerated once/day via `--sync-pnl` (after market close) — every cell is a live formula
recomputed from `positions_dhan_long.json`/`positions_dhan_short.json` on every run, not a
stale cached number.

---

## 10. Notifications (`pipeline/notify.py`)

Every trading action posts to Telegram, routed across forum topics
(`signals`/`entries_exits`/`live_tracking`/`errors`/`pnl`). Current conventions (both added
2026-10-xx, see `ISSUES_RESOLVED.md` §10):
- **No order IDs** in any message — internal detail, not useful to a human reading the
  channel.
- **Every entry and exit notification is tagged with its return bucket** (5-10/10-15/15-20)
  so a reader can tell which schedule a position is on without cross-referencing the
  position file.
- There is **no "target placed" notification** — removed as noise (resting-order placement
  isn't an outcome worth a push alert).
- `QUALIFIED` live-monitor alerts show the real volume ratio (`cum_vol / avg_36`), not a
  figure measured against the already-6×'d threshold (which understated the true ratio by
  ~6x before the fix).

---

## 11. Broker / auth

**Dhan** is the only live broker (Zerodha's execution code was removed 2026-09-10, pending a
rebuild that never happened — its historical position files are kept, not deleted; Upstox
was never a trading broker, only a candle-data source, also since migrated off). Access
token is valid 24h and **auto-renews itself** twice daily (8 AM + 8 PM, `python -m
dhan.auth --renew`) — no OAuth handshake exists for a personal Dhan account, so the very
first token ever still has to be generated by hand once at web.dhan.co.

---

## 12. Holiday handling

Market-holiday crontab guards are self-expiring one-line date checks
(`[ "$(date +\%Y-\%m-\%d)" = "2026-10-02" ] || <command>`), chained with `||` for each
additional holiday as they come up — added ahead of a known holiday, never need manual
removal afterward since the date simply stops matching. The twice-daily token-renewal cron
is deliberately **never** guarded (tokens still need renewing on a holiday so Monday's first
real run isn't starting from an expired one).

---

## Open / historically-removed features

- **UC-based staged entry (Case A/B)**: a second, tick-driven entry mechanism, live
  2026-09-09 through 2026-09-21, removed after a real bug (one branch re-fired every tick
  on the same symbol instead of latching once). Never rebuilt; superseded in spirit by the
  tranched `run_entry_limit` mechanism in §4a.
- **Zerodha execution**: removed 2026-09-10, pending a full rebuild. Historical position
  files kept.
- **Google Sheets live dashboard** (the old `sheets/apps_script/` webhook push): removed;
  Google Drive-synced Excel (§9) replaced it.
