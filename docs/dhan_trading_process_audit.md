# Dhan Trading Process — Full Audit (as of 2026-09-07)

**Scope note:** everything in this document was produced by reading the current
code in this repo directly (`dhan/run_trades.py`, `dhan/live_monitor.py`,
`dhan/trade.py`, `dhan/auth.py`, `dhan/charges.py`, `results/build_pnl_simple.py`,
crontab, and `/root/dhan_trades.log`), not from prior session notes, comments, or
docstrings — those are cited only where explicitly marked. Where a docstring's
claim didn't match the code next to it, both are reported and the discrepancy is
flagged. Two named design docs from an earlier request in this session (a
"wave-based redesign spec" and a `uc-ladder-entry-spec.md`) still do not exist
anywhere in git history — confirmed again via `git log --all --diff-filter=A
--name-only`.

Rate-limit classes cited below are Dhan's four published categories: **Non-Trading
20/sec, Order 10/sec, Data 5/sec, Quote 1/sec (up to ~900–1000 instruments/call)**.
Dhan's own docs don't enumerate which category every endpoint falls under —
where a call's class is inferred rather than stated outright by Dhan (e.g.
`/fundlimit`, `/margincalculator`), it's marked *(inferred)*.

---

## PART 1 — FULL PROCESS MAP

### 1. `place_targets_915` — 9:15am

**Trigger:** cron `15 9 * * 1-5`, no `--dry-run` in production.
**Reads:** `positions_dhan_long.json` (all `status in ("open",
"partial_exit_925_nodata")` rows).
**Writes:** `positions_dhan_long.json` (target_order_id/target_price),
`results/dhan_uc_cache.json`.
**Notifies:** `send_target_placed` per position.
**Hands off to:** `check_exit_925` ten minutes later — the ordering dependency is
real (925 reads `target_order_id` expecting it to already be resting), but
there's no live race: these are two separate cron-launched processes ten
minutes apart, not concurrent.

| Step | Function | API call | Rate class | Latency source | Batchable? |
|---|---|---|---|---|---|
| Load positions | `_load_long_pos` | file read | — | disk I/O | n/a |
| Fetch UC for every open position | `_fetch_upper_circuit_batch` | `POST /marketfeed/quote` | Quote | 1 network round-trip (chunked ≤900) | Already batched |
| Persist UC cache | `_save_uc_cache` | file write | — | disk I/O | n/a — this is what lets 925/1159 skip a live UC fetch later |
| Per position: place target sell | `_sell_margin_safe` → `sell()` | `POST /orders` | Order | 1 RT, **sequential, one position at a time** | **Not batched/parallelized** — the one stage in this pipeline that still does its Order-API calls one-by-one in a loop, unlike 925/1159/239's wave/chunk design |
| Per position (MTF/MARGIN only, on rejection): ledger-lag check | `_dhan_order_status` | `GET /orders/{id}` | Order, but **not rate-limited client-side** — see finding below | fixed `time.sleep(2)` + 1 RT | n/a |
| Per position (MTF ledger-lag confirmed): retry sell | `sell()` | `POST /orders` | Order | 1 RT + a fixed 30s `time.sleep` before it | n/a |
| Per position: save | `_save_long_pos` | file write | — | disk I/O, called **once per successfully-targeted position**, not once for the whole run | Batchable — see Part 4 |
| Per position: notify | `send_target_placed` | Telegram HTTP | — | 1 RT | Low-value to batch |

**Finding (new, not previously flagged):** `_dhan_order_status` and
`_dhan_get_orders` (the two read-only order endpoints) are the *only* two
Order-API-class functions in `dhan/trade.py` that never call
`rate_limiter.acquire()` — only `place_order` and `cancel_order` do (confirmed by
reading `dhan/trade.py:230-367` directly: `rate_limiter.acquire()` appears at
line 298 inside `place_order` and line 331 inside `cancel_order`, nowhere in
`order_status`/`get_orders`). In practice this hasn't caused a visible problem
because these two calls are now made at most once per stage-run (since the
recent batching change) or throttled by `_poll_fill`'s own 1s sleep between
attempts — but it means nothing client-side would stop a burst of concurrent
`_dhan_order_status` calls (e.g. several `_poll_fill` loops running in the same
Wave-1 chunk) from exceeding Order API's 10/sec ceiling if chunk sizes ever grew
past `MAX_ORDER_CALLS_PER_SECOND` (currently 5).

---

### 2. `run_entry_321` — cron 15:20, fires at 15:21:00 IST

**Trigger:** cron `20 15 * * 1-5`, fires the actual orders at a hardcoded
wall-clock instant, not at cron-start.
**Reads:** `data/candles/<symbol>.csv` (15:20/15:19 close for sizing),
`positions_dhan_long.json` (Step 1/2/3 restructure for Case-A/B partial
completions), local scrip master (`security_id`/`tick_size`).
**Writes:** `positions_dhan_long.json` (one save for the whole batch),
`results/trades/dhan_entries_<date>.csv`.
**Notifies:** `send_entry` / `send_entry_failed`.

Three phases, confirmed to still match this shape exactly:

**Phase 1 (sequential)** — per candidate symbol: reference price (local file,
no API), share sizing, `security_id` lookup (local), `_margin_check`
(`POST /margincalculator`, Data *(inferred)*), one shared `_available_balance`
fetch (`GET /fundlimit`, Non-Trading *(inferred)*) reused across every symbol —
confirmed still fetched exactly once for the whole run, not per-symbol (test
`(bal-entry)` in `dhan/test_targets.py` asserts this).

**Between Phase 1 and Phase 2 — pricing/UC step, just restructured this
session:**
1. Right after Phase 1 (≈15:20:00–15:20:5x, before any hold), one batched
   `_fetch_upper_circuit_batch` call (Quote) — moved here specifically so it
   doesn't compete with the LTP fetch for the same 1 req/1.5s quote limiter in
   the tight pre-fire window.
2. Hold until 15:20:57.
3. One batched `get_ltp_batch` call (Quote).
4. Per symbol: if `LTP >= UC`, LIMIT price = UC itself (tick-rounded); else
   LIMIT price = `LTP × 1.005` (tick-rounded) — see Part 3 for why this exists.
5. Hold until exactly 15:21:00.

**Phase 2 (parallel, 4-worker pool)** — per symbol: `buy()` (`POST /orders`,
Order), then `_poll_fill_strict` (up to 12× `GET /orders/{id}`, Order, 1s
apart, never guesses a fill on timeout). On a confirmed MTF-ineligibility
rejection (`"mtf product is not allow"` **or**, as of this session,
`"buy back is not allowed"` — RML, 2026-09-07), retries once as CNC at half
capital.

**Phase 3 (sequential apply)** — per result: log to CSV, `send_entry`/
`send_entry_failed`, fold into `positions`. **One `_save_long_pos` call for the
whole batch** — already optimal, no per-symbol write here (unlike
`place_targets_915` above).

**Hands off to:** `check_exit_925` the next trading morning — positions written
here are `status: "open"`, which is exactly what `_open_pos()` filters for.

`_sync_pnl_workbook()` runs at the end of every stage including this one — see
Part 2 for what that actually costs.

---

### 3. `check_exit_925` — 9:25am

**Trigger:** cron `25 9 * * 1-5`.
**Reads:** `positions_dhan_long.json`.
**Writes:** `positions_dhan_long.json` (up to 2 saves this run — Phase 1
target-hit updates, then Wave 1 sell updates), `positions_dhan_short.json` (up
to 2 saves — Wave 2 short-open rows, then Wave 3 protect updates).
**Notifies:** `send_target_hit`, `send_exit_925`, `send_exit_925_nodata`,
`send_short_open`, `send_circuit_fetch_failed`.

| Step | Function | API call | Rate class | Latency source | Batchable? |
|---|---|---|---|---|---|
| LTP for every open position | `get_ltp_batch` | `POST /marketfeed/ltp` | Quote | 1 RT | Already batched |
| Target-status pre-check **(this session's change)** | `_dhan_get_orders` | `GET /orders` | Order (unthrottled, see Part 1.1 finding) | 1 RT | Already batched — was N sequential `_dhan_order_status` calls before `4cb68ec7` |
| Phase 1 (sequential): resolve target-hits from the pre-fetched snapshot, decide no-data/P&L-gate per position | none (pure lookup) | — | — | negligible now — measured `0.000s for 6 positions` in a real dry-run, down from several seconds pre-fix | n/a |
| UC lookup for mirrored-short candidates | `_circuit_cache_for` | file read first; `_fetch_upper_circuit_batch` (Quote) only for any gap | Quote (only on cache miss) | near-zero in the normal case — 9:15's cache covers every open position | Already optimal |
| One-time short balance fetch | `_available_balance` | `GET /fundlimit` | Non-Trading *(inferred)* | 1 RT | Already batched (once per run, thread-shared via `_BalanceTracker`) |
| **Wave 1**, chunked at `MAX_ORDER_CALLS_PER_SECOND` (5): cancel stale target | `_dhan_cancel_order` | `DELETE /orders/{id}` | Order | 1 RT per position, concurrent within chunk | Already batched-concurrent |
| **Wave 1** sell sub-step (only after that chunk's cancels finish) | `_sell_margin_safe`→`sell()` + `_poll_fill_safe` | `POST /orders` + up to 12×`GET /orders/{id}` | Order | poll loop dominates — up to ~11-12s worst case per position, but concurrent within the chunk | Already batched-concurrent |
| **Wave 2**, chunked: open mirrored short | `_open_short_place`→`sell()` + `_poll_fill_safe` | `POST /orders` + poll | Order | same poll-loop shape | Already batched-concurrent — **uses the LTP fetched at the very top of the function, before Wave 1 — see Part 3** |
| Settle buffer | `time.sleep(SHORT_SETTLE_BUFFER_SECONDS)` | — | — | fixed 2.5s, paid once total (not per chunk) | n/a, deliberate |
| **Wave 3**, chunked: cover-target + UC stop-loss | `_open_short_protect`→`buy()`×2 | `POST /orders`×2 | Order | 1 RT each, concurrent within chunk | Already batched-concurrent; UC read from cache, not fetched live |
| `_sync_pnl_workbook` | see Part 2 | `POST /margincalculator` per open MTF position | Data *(inferred)* | see Part 2 finding | Not currently cached across the 5x/day it's called |

**Divergence from `force_exit_1159`:** 925 has a P&L gate (`pnl_live > 0` queues
a real sell; `pnl_live <= 0` holds for 11:59) and a no-data fallback that
half-sells + re-targets the remainder. 1159 has neither — every remaining
position force-sells unconditionally, full quantity, no fallback branching.

---

### 4. `force_exit_1159` — 11:59am

Same shape as `check_exit_925` (same target-status pre-check via
`_dhan_get_orders`, same Wave 1/2/3 structure, same `_run_exit_wave1` helper),
minus the P&L gate — see the divergence note above. Additionally: if
`open_ps` is empty, sends `send_nothing_open_at_1159` and still calls
`_daily_summary`. Ends every run (empty or not) with `_daily_summary` (prints
+ `send_daily_summary`) and `_sync_pnl_workbook`.

**Hands off to:** `square_off_239` — any mirrored short opened here is
`status: "short_open"`, exactly what `_open_short_pos()` filters for.

---

### 5. `square_off_239` — 2:39pm

**Trigger:** cron `39 14 * * 1-5`.
**Reads/writes:** `positions_dhan_short.json` only — never touches the long
file.

| Step | Function | API call | Rate class | Latency source | Batchable? |
|---|---|---|---|---|---|
| Cover LTP for every open short | `get_ltp_batch` | `POST /marketfeed/ltp` | Quote | 1 RT | Already batched |
| OCO status pre-check | `_dhan_get_orders` | `GET /orders` | Order (unthrottled) | 1 RT | Already batched — this is the ORIGINAL pattern 925/1159 were ported from |
| Classify every short from the one snapshot | none | — | — | negligible | n/a |
| Chunked (5): cancel the losing side of the OCO | `_dhan_cancel_order` | `DELETE /orders/{id}` | Order | concurrent per chunk | Already batched |
| Chunked: force-cover (`neither_filled` only) | `buy()` + `_poll_fill_safe` | `POST /orders` + poll | Order | poll-loop-bound | Already batched — **but see Part 3: this buy's limit price has no UC check at all** |
| Per-chunk save | `_save_short_pos` | file write | — | disk I/O, **once per chunk, not once for the whole run** (deliberate — see code comment: avoids a half-cancelled state on interruption) | n/a, intentional tradeoff |
| `_sync_pnl_workbook` | — | — | — | — | — |

Note the per-chunk save here is a **different, deliberate** choice from
925/1159's single end-of-run save — the code comment explains it's specifically
to avoid ending up with some positions cancelled-but-not-yet-closed if the run
is interrupted mid-way. Not an inconsistency to "fix."

---

### 6. UC-staged entry — Case A / Case B (`dhan/live_monitor.py`)

**This corrects the "wired but off by default" premise from last session — that
was true of the code's own default, but not of the actual deployed
configuration.**

Reading `scripts/run_dhan_live_monitor.sh` (the actual script cron launches
daily at 9:13am):

```bash
# UC-based staged entry -- DRY-RUN only, every day. No orders are actually
# placed (--dry-run); this just keeps generating real signal/fill logs so we
# can evaluate the feature before ever turning it live.
UC_FLAGS="--enable-uc-staged-entry --dry-run"
```

**The feature is enabled and running live every trading day**, not merely
present-but-disabled. What `--dry-run` actually gates, read precisely from the
code:

- `evaluate_tick`/`uc_evaluate_tick`/`update_case_a_qualification` run on
  **every tick, every symbol, all day** — pure in-memory dataclass mutation, no
  I/O, negligible cost, dry-run or not.
- The instant a symbol's trigger condition is met, `_fire_uc_staged` dispatches
  to a 4-worker `ThreadPoolExecutor` (never runs inline on the websocket
  callback thread) → `execute_case_a_leg1`/`execute_case_a_leg2`/
  `execute_case_b` → `_place_staged_buy`.
- `_place_staged_buy` makes **two genuinely real API calls regardless of
  `dry_run`**: `_margin_check` (`POST /margincalculator`, Data *(inferred)*)
  and `_available_balance` (`GET /fundlimit`, Non-Trading *(inferred)*) — both
  fire live every time a trigger condition is met, dry-run or not, since
  leverage/balance must be evaluated for real to produce a meaningful log.
- Only the actual order call is gated: `buy(..., dry_run=True)` short-circuits
  inside `dhan/trade.py:284-291` before any `session.post` — confirmed by
  reading `place_order` directly, it returns the literal string `"DRY_RUN"`
  with zero network calls. `_poll_fill_strict` is never even invoked in this
  path (an earlier `if dry_run: return {...}` in `_place_staged_buy` returns
  before the poll call).
- Every position-file write in `execute_case_a_leg1/leg2`/`execute_case_b` is
  behind an explicit `if not dry_run: _save_long_pos(...)` — confirmed no
  write occurs under today's config.

**Net effect today:** zero orders placed, zero position-file writes, but real
margin-check and balance-check API traffic every time a symbol crosses its
trigger — worth knowing since "dry-run" can misleadingly suggest zero live API
activity.

**Trigger logic, read directly from `dhan/live_monitor.py` (this does NOT match
the plan file's revised spec currently sitting in
`~/.claude/plans/wondrous-inventing-frost.md` — see the discrepancy note
below):**

- `_CASE_A_START = 14:30`, `_CASE_A_END = 15:18` — **leg 1 and leg 2 share the
  exact same window**, per the code's own comment: `"leg 1 and leg 2 share
  this window (widened -- both used to differ)"`. The plan document describes
  leg 1 as 14:30–15:00 and leg 2 as a separately-wider 14:30–15:18 — the actual
  code has already unified them into one window, and does it with a single
  `_in_case_a_window()` function, not the plan's three separate window
  functions (`_in_leg1_window`/`_in_leg2_window`/`_in_case_b_window`).
- `_CASE_B_START = 15:00`, `_CASE_B_END = 15:18`.
- Leg 1 fires when `case_a_qualified` AND in the Case A window AND
  `LTP >= prev_close × 1.19`.
- **`case_a_qualified` is a qualification gate the plan document never
  mentions at all**: `hit_uc_before_1430` (LTP touched UC at any point before
  14:30) AND `off_uc_in_window` (LTP later dropped back below UC during the
  Case A window) — both one-way latches. This means leg 1 doesn't fire on a
  bare 19%-above-prev-close cross; it additionally requires the stock to have
  already hit its upper circuit earlier in the day and come back off it. This
  is real, implemented logic with its own docstring, not a stub.
- Leg 2 fires when `entry_status == "partially_filled"` and
  `case_a_leg == "leg1_filled_watching_retrace"` and still in the Case A
  window and `LTP <= prev_close × 1.17`.
- Case B fires when NOT `case_a_qualified` (the tie-break — a qualified symbol
  never falls through to Case B) and in the Case B window and
  `LTP >= prev_close × 1.19`. No separate near-UC gate exists for Case B in
  the current code, consistent with the plan's decision to drop it, but the
  code's tie-break condition is "not case_a_qualified," not literally "case
  never fired Case A" as one might assume from the plan text — functionally
  overlapping but worth citing precisely.
- `capital_base`/`per_stock_capital` snapshot: confirmed matches the plan on
  this specific point — `TOTAL_CAPITAL / max(n_qualified, 1)` where
  `n_qualified` is `_SymState.qualified` (the existing volume+VWAP screen),
  snapshotted exactly once at the first tick at/after 14:30, read back as a
  plain parameter afterward, never recomputed.

**⚠️ Discrepancy to flag explicitly, per your instruction not to assume prior
findings hold:** the plan file currently on disk describes a revision to a
file (`dhan/uc_staged_entry.py`) that **does not exist** — the entire
UC-staged-entry state machine lives inline in `dhan/live_monitor.py`. The
live code's qualification-gate mechanism (`hit_uc_before_1430`/
`off_uc_in_window`) is not described anywhere in that plan. Either the plan
predates a further round of changes not reflected in the plan file, or the
plan was never the thing actually implemented. **I did not attempt to
reconcile which is "correct" — I only report what the running code does
today**, since that's what this audit was scoped to.

**Position rows this creates carry `case`, `entry_status`, `case_a_leg`,
`filled_amount`, `capital_base`** alongside the ordinary `status` field —
confirmed additive, not a replacement: `status` still flips to `"open"` on a
completed fill, which is all `place_targets_915`/`check_exit_925`/
`force_exit_1159` (`_open_pos()`) key off. **Confirmed zero changes needed in
any exit-side function for this to work** — matches the plan's point 7
exactly, one of the few points that does match.

`run_entry_321` reads any `entry_status == "partially_filled"` row as a
Step-1-priority completion (uses `capital_base - filled_amount` for sizing,
reuses the existing product, folds into the same row) before Step 3's
untouched fresh-entry loop — confirmed present in the code at
`run_trades.py:1332-1368` and matches the plan's Step 1/2/3 shape.

---

### 7. `live_monitor.py` — continuous monitoring, 9:13am–3:40pm

**Confirmed: the "normal" (non-UC) path never touches position files or
places orders.** `_fire_qualified` and `_fire_near_circuit` (the two events
`evaluate_tick` can return outside the UC state machine) only print and call
`notify.send_monitor_qualified`/implicitly via the near-circuit branch —
neither reads nor writes `positions_dhan_*.json`, neither calls `buy`/`sell`.
Confirmed by reading both functions directly (`live_monitor.py:922-953`).

Startup (`setup()`): one batched `_fetch_circuit_limits` call (Quote, chunked)
covering every trackable symbol — confirmed batched once at startup, not
per-tick.

Per-tick (`_on_message`): parses one instrument's Quote Data packet, updates
in-memory `_SymState`/`UCState` under `self._lock`, dispatches any fired event.
No API calls in the hot tick path itself except the (rare, trigger-only) UC
fire dispatch described above.

**Resource contention with cron-triggered scripts:** the UC-staged-entry write
path (when `dry_run=False`, not today's config) and the cron scripts
(`run_entry_321`, etc.) share the same unlocked `_load_long_pos`/
`_save_long_pos` functions — see Part 3's locking-gap item for the concrete
timing analysis of whether this currently matters.

---

## PART 2 — COST MODEL

All cost computation lives in exactly **one** place:
`dhan/charges.py:estimate_trade_charges()` /
`dhan/charges.py:position_charge_summary()`. `results/build_pnl_simple.py`
imports `dhan.charges` directly and calls
`position_charge_summary(position)["total_charges"]` — confirmed the P&L
workbook has no separate/duplicate cost logic of its own.

### MTF (delivery) long entry/exit — rates read directly from `charges.py:166-216`

| Charge | Rate | Side | Verified against |
|---|---|---|---|
| Brokerage | `min(₹20, 0.03% of turnover)` | both | Dhan's published rate card, confirmed live 2026-08-20 |
| STT | 0.1% | both sides | same |
| Exchange transaction charge | 0.0030699% | both | same, NSE rate |
| SEBI turnover fee | 0.0001% | both | same |
| Stamp duty | 0.015% | **buy side only** | same |
| GST | 18% of (brokerage + exchange + SEBI) — **not** on STT/stamp | both | same, matches Dhan's own GST-ability convention |
| DP charge | fixed ₹14.75 | entry only, one-time per position | leg-classification confirmed via `is_delivery_buy()` |
| Pledge/unpledge | fixed ₹35.40 | entry only, MTF only | same |
| **MTF interest** | slab-rate on funded amount, see below | daily accrual | see finding below |

**MTF interest — your instruction asked whether the rate is looked up or just
estimated. Answer: it's a real lookup, but not by "leverage assignment" the way
the question framed it.** `funded_amount()` (`charges.py:340-363`) makes a
real `POST /margincalculator` call per position to get the actual borrowed
rupee amount (trade value minus Dhan's reported `totalMargin`), and
`mtf_daily_rate_pct()` maps that real amount to Dhan's published slab table:

```
Up to ₹500                    : no interest
₹500.01     – ₹5,00,000       : 12.49% p.a. (0.0342%/day)
₹5,00,000.01 – ₹10,00,000     : 13.49% p.a. (0.0369%/day)
₹10,00,000.01 – ₹25,00,000    : 14.49% p.a. (0.0397%/day)
₹25,00,000.01 – ₹5,00,00,000  : 15.49% p.a. (0.0425%/day)
```

The whole funded amount is taxed at whichever single slab it falls into (not
progressive/marginal). So: the *rate* varies by funded-amount **size**, looked
up from a real API call, not from a flat assumed multiple — but it's not
"per-symbol leverage assignment" varying the rate; it's the rupee size of the
loan. `calendar_days_held()` is a plain calendar-day difference, weekends
included.

### Intraday (MIS/INTRADAY) short entry/exit — same function, `product == "INTRADAY"` branch

| Charge | Rate | Side |
|---|---|---|
| Brokerage | `min(₹20, 0.03% of turnover)` | both — same formula as MTF |
| STT | 0.025% | **sell side only** (vs MTF/CNC's 0.1% both sides) |
| Exchange transaction charge | 0.0030699% | both, same as MTF |
| SEBI turnover fee | 0.0001% | both, same |
| Stamp duty | 0.003% | **buy side only**, vs MTF/CNC's 0.015% |
| GST | 18% of (brokerage + exchange + SEBI) | both |
| DP charge | **never** — `is_delivery_buy()` explicitly excludes INTRADAY | — |
| MTF interest | **never** — the `product == "MTF"` gate excludes shorts entirely; shorts are always `INTRADAY` per `_open_short_place`'s hardcoded `product="INTRADAY"` | — |

### Mismatch check between what's charged and what's recorded

**Found one, but it's a stale comment, not a functional bug.**
`results/build_pnl_simple.py:187-191`'s own docstring says `_live_cost()` is
"fetched live via `dhan.charges.position_charge_summary()`, which itself calls
Dhan's trade-book API." That description is now stale:
`position_charge_summary()`'s own docstring (`charges.py:399-408`) explicitly
says entry/exit leg charges come from `estimate_trade_charges()` (the
published rate-card formula) **unconditionally, not as an outage fallback** —
verified live 2026-08-20 against a real BAJAJHIND fill to within ₹0.05 on a
₹378.78 leg (~0.01% error). The trade-book API (`get_trades`/
`trade_by_order_id`) is defined in the same file and still used by the
standalone `python -m dhan.charges` CLI report, but **not** by
`position_charge_summary()`, and therefore not by the P&L workbook. The actual
numbers in the workbook are correct and centralized; the comment describing
where they come from in `build_pnl_simple.py` is just out of date.

**Finding (new): the P&L workbook sync is called after every single stage —
`place_targets_915`, `run_entry_321`, `check_exit_925`, `force_exit_1159`,
`square_off_239`, all five call `_sync_pnl_workbook()` — and each invocation
of `position_charge_summary()` for every currently-open MTF position makes a
fresh, uncached `POST /margincalculator` call for that position's
`funded_amount()`.** On a day with, say, 6 open MTF positions, that's up to
30 real margin-calculator calls across the day for numbers that don't change
intraday for a position that hasn't been re-sized (the funded amount only
changes when the position's own qty/price does). This is a genuine, currently
unaddressed redundant-API-call opportunity — see Part 4.

---

## PART 3 — KNOWN OPEN ITEMS (verified fresh, not assumed)

### Cross-process file locking gap — **confirmed still present, structurally, with a nuance on current live risk**

`_load_json_positions`/`_save_json_positions` (`run_trades.py:569-580`) are a
plain `path.read_text()`/`path.write_text(json.dumps(...))` — no `flock`, no
atomic rename, no read-modify-write compare-and-swap. `live_monitor.py` imports
and calls the exact same `_load_long_pos`/`_save_long_pos` functions
(`live_monitor.py:102`) for Case A/B's writes. This is a real, unaddressed gap
structurally.

**But**, checking actual timing overlap against the crontab: Case A/B's write
window closes at 15:18 (both `_CASE_A_END` and `_CASE_B_END`); the only
cron-triggered stage that writes `positions_dhan_long.json` anywhere near that
time is `run_entry_321`, whose cron starts at 15:20 and whose one save happens
after firing at 15:21+ — a roughly 2-minute gap with no scheduled overlap
today. `square_off_239` (14:39, squarely inside the Case A/B window) writes
`positions_dhan_short.json` only, a different file, so no collision there
either. **Net: the locking mechanism genuinely doesn't exist, but today's
specific window boundaries happen to leave ~2 minutes of buffer — this is a
timing coincidence of the current config, not a designed guarantee, and would
need re-checking any time either the cron schedule or the Case A/B window
constants change.** Moot in practice today regardless, since the write path
itself is dry-run-gated off (see Part 1.6).

### `_poll_fill_safe` phantom-fill-on-timeout — **confirmed still open**

Read directly (`run_trades.py:263-272`): on any exception during the fill
poll (including a plain timeout with no confirmed status), it returns
`(fallback_price, fallback_qty)` — i.e. reports a fill that was never actually
confirmed. This is the exact STYLEBAAZA-class bug (2026-08-14) that
`_poll_fill_strict` was built to close for entries (`_poll_fill_strict` returns
`(0.0, 0, ...)` on both a genuine rejection and an unconfirmed timeout —
never guesses). `_poll_fill_safe` is still used throughout every exit-side
call site: `check_exit_925`/`force_exit_1159`'s Wave 1 sell, Wave 2 short-open,
and `square_off_239`'s force-cover. Confirmed still open, matching this
session's earlier note that it was explicitly deferred by you ("let it be,
will do it later").

### Mirrored-short LTP staleness in Wave 2 — **still theoretical, not measured, but now bounded precisely**

No log evidence or committed measurement exists of how stale the LTP actually
is by the time Wave 2 fires (grepped `run_trades.py` for "stale" — the only
references are to unrelated concerns). Structurally confirmed: `ltp_cache` is
fetched once at the very top of `check_exit_925`/`force_exit_1159`, before
Phase 1, and passed unchanged into `_open_short_place` for Wave 2 — no
re-fetch anywhere in between. This is intentional and unit-tested as such
(`test_batch_concurrency.py`'s `test_short_anchor_uses_batched_ltp_cache`,
scenario STALE1, explicitly asserts the batched value is used and `get_ltp()`
is never called).

A theoretical worst-case bound, derived from the code's own constants (not
measured live): `_poll_fill`'s 12 retries × 1s delay ≈ 11-12s worst case per
position; Wave 1 sells run concurrently within a chunk of
`MAX_ORDER_CALLS_PER_SECOND` (5) but chunks themselves are sequential with a
1.0s `BATCH_SLEEP_SECONDS` pause between. For today's real 6-open-position
count (2 chunks: 5 + 1), a worst-case run where every fill needs the full poll
timeout could see Wave 2 fire roughly 25-30 seconds after the LTP it's still
using was fetched. Typical runs are much faster (most fills confirm within the
first 1-2 poll attempts, and today's dry-run Phase 1 measured `0.000s`), but
the *ceiling* is real and this specific number has not been captured from an
actual live run.

### Test-hygiene leaks — **confirmed still present, re-verified by running the suite just now**

```
Scenario (a)/(a2)/(a3) — place_targets_915, symbols ALPHA/OMICRON/PI
Scenario (g2) — _open_short skip-when-disabled
```
Re-ran `python3.11 dhan/test_targets.py` and grepped for `"Reusing valid
token"` — all 4 scenarios still leak a real authenticated call (root cause:
`_sell_margin_safe`'s own internal MTF/MARGIN ledger-lag check calls the real
`_dhan_order_status`/`time.sleep(2)`, unmocked in these specific scenarios; g2
leaks via `_open_short`'s unconditional `_available_balance()` call before its
own skip-check even runs). Never fixed — correctly left alone per this
session's explicit scope restriction (`place_targets_915`/`_open_short` were
out of bounds for the tasks that touched nearby code).

### Entry order pricing near upper circuit — mapped exactly, per your request not to fix it here

Every `* 1.005`-style reference-price-buffer LIMIT price in the codebase,
found via `grep -rn "\* 1\.005"` across `dhan/run_trades.py` and
`dhan/live_monitor.py`:

| Location | Function | Order side | UC-validated? |
|---|---|---|---|
| `run_trades.py:1486` | `run_entry_321`'s 3:21pm entry | BUY | **Yes — fixed this session.** If `LTP >= UC`, bids at UC instead; this line only runs in the not-at-UC branch. |
| `run_trades.py:2587` | `square_off_239`'s force-cover buy | BUY | **No. Confirmed unprotected** — `buy_limit = _tick_round(sym, cover_ltp * 1.005)` unconditionally, no UC/circuit comparison anywhere nearby. This is the highest-severity instance of the three: a short that's run hard enough to be near its UC by 2:39pm is exactly the scenario where this buy could get "Rate Not Within Ckt Limit"-rejected, and unlike a rejected entry (a missed trade), a rejected force-cover leaves a short **stuck open past the stage that's supposed to unconditionally close every short**. |
| `live_monitor.py:521-530` (`_capped_limit_price`, used by all three of `execute_case_a_leg1`/`execute_case_a_leg2`/`execute_case_b`) | UC-staged entry | BUY | **Yes, pre-existing** — `min(trigger*1.005, UC*0.995)`. This was the original place this exact fix pattern shipped, predating today's `run_entry_321` fix. |

(For completeness: sell-side `* 0.995` prices — `check_exit_925`/
`force_exit_1159`'s exit sells, the no-data fallback, `square_off_239`'s
mirrored-short open — carry the symmetric *lower*-circuit risk instead, not
in scope for this question but structurally the same shape.)

**Rejection frequency, estimated from `/root/dhan_trades.log`:** grepped for
the literal RMS rejection text `"Rate Not Within Ckt Limit"` across the log's
full history (`2026-08-14` through `2026-09-07`, 15 logged entry-stage runs,
69 total logged `"LIMIT buy @ ... 0.5% above LTP"` attempts):

- **9 of 69 entry attempts (~13%)** were rejected for exactly this reason.
- Occurred on **8 of the 15** trading days with a logged entry run (~53% of
  days) — SHANTIGEAR alone appears twice (2026-08-21, 2026-08-24).
- Every single one of the 9 rejections traces back to `run_entry_321`'s
  buy — confirmed by checking the log lines immediately preceding each
  rejection (all show `"LIMIT buy @ ... 0.5% above LTP"` or
  `"placing MTF LIMIT BUY ..."` right before the reject). **Zero observed
  rejections from `square_off_239`'s force-cover** in this log — but that's
  because the log predates today's session (the force-cover gap has existed
  the whole time; it's simply never been exercised by a short running hard
  enough toward UC by 2:39pm in this specific window, not because it's
  protected).

This is real, not projected: the `run_entry_321` fix implemented this session
addresses a failure mode that hit roughly 1 in 8 entry attempts historically.
The identical, still-open gap in `square_off_239` hasn't been observed yet in
this log, which is a fact about small-sample luck, not about the code being
safe.

---

## PART 4 — OPTIMIZATION BACKLOG

Ranked safety/correctness first, then latency/efficiency, per your instruction.

| # | Item | Stage | API calls saved | Latency impact | Correctness/safety impact | Risk to implement | Status |
|---|---|---|---|---|---|---|---|
| 1 | `square_off_239` force-cover buy has no UC check — can get rejected exactly like the entry-side bug did, leaving a short stuck open past the stage meant to unconditionally close it | `square_off_239` | none | none | **High** — an unprotected short surviving past 2:39pm is a real, uncapped-duration open-risk position, worse than a missed entry | Low — same pattern already proven twice (`run_entry_321`, `live_monitor.py`) | Not started |
| 2 | `_poll_fill_safe` phantom-fill-on-timeout can silently record an unconfirmed sell/cover/short-open as filled at a guessed price | `check_exit_925`/`force_exit_1159`/`square_off_239` (all exit-side fill polling) | none | none | **High** — incorrect P&L, incorrect position state, on a specific already-reproduced failure mode (STYLEBAAZA) | Medium — `_poll_fill_strict` already exists and is proven on the entry side; porting changes behavior on timeout (position stays "not filled" instead of auto-closing), needs a decision on what should happen to a stuck exit | Deferred by you earlier this session ("let it be, will do it later") |
| 3 | Cross-process locking gap between `live_monitor.py`'s Case A/B writes and cron writes to `positions_dhan_long.json` | `live_monitor.py` + all long-file writers | none | none | Medium, currently dormant — only matters the moment `--dry-run` is dropped from the launcher, and today's window timing leaves ~2 min buffer that isn't a designed guarantee | Medium — needs a real file lock (e.g. `fcntl.flock`) around read-modify-write, touches every writer | Not started |
| 4 | `place_targets_915` places target orders sequentially, one position at a time — the one stage never ported to the wave/chunk pattern | `place_targets_915` | 0 (same total calls) | Real — N sequential RTs instead of concurrent chunks, same shape 925/1159 already fixed | None — pure speed, no behavior change | Low — direct application of the existing `_run_in_chunks` helper | Not started |
| 5 | `_sync_pnl_workbook` re-fetches `funded_amount` (`POST /margincalculator`) for every open MTF position, uncached, on every one of the 5 stage-runs/day | all 5 stages | Up to ~24-30/day saved on a 6-position day (5 calls → 1 if cached until a position's qty/price changes) | Minor per-call, adds up across the day | None — funded amount is stable intraday for an unchanged position | Low — simple day-scoped cache keyed on symbol+qty+price | Not started |
| 6 | `_dhan_order_status`/`_dhan_get_orders` bypass `rate_limiter` entirely (only `place_order`/`cancel_order` call `.acquire()`) | all stages that poll fills or fetch the Order Book | 0 | None currently observed | Low today (chunk sizes stay under Order API's 10/sec even unthrottled), but latent — a future chunk-size increase could silently exceed the ceiling with no client-side guard | Low — one-line addition to both functions | Not started |
| 7 | `place_targets_915` saves the position file once per successfully-targeted position instead of once per run | `place_targets_915` | 0 API calls, N-1 fewer file writes | Minor (local disk I/O) | None — the current design is actually a slightly *safer* default (partial progress survives a mid-run crash), a genuine tradeoff not just an oversight | Low, but changes crash-recovery behavior — worth deciding deliberately, not silently | Not started |
| 8 | Temporary `TIMING` print statements left in `check_exit_925`/`force_exit_1159` after this session's Phase-1 batching verification | `check_exit_925`/`force_exit_1159` | 0 | 0 | None | Trivial | **Implemented-uncommitted** — you haven't yet said whether to keep or remove them |
| 9 | Mirrored-short LTP staleness in Wave 2 (Part 3) — no fix proposed, since re-fetching would reintroduce the exact rate-limit collision the batching was built to avoid; flagged for awareness only | `check_exit_925`/`force_exit_1159` Wave 2 | n/a | n/a (currently favors safety over freshness, deliberately) | Low-to-medium, unquantified — bounded above at ~25-30s worst case, not measured live | n/a — no fix recommended, this is an awareness item | Not started (by design, not neglect) |

**Two items from this session already ship in the "committed"/"implemented-uncommitted"
status, for cross-reference against the table above:**

- `check_exit_925`/`force_exit_1159` GET /orders batching — **committed**
  (`4cb68ec7`).
- `run_entry_321`'s "Buy back is not allowed" → CNC retry — **committed**
  (`2e25c28d`).
- `run_entry_321`'s UC-at-entry pricing fix (item's cousin to #1 above, already
  done on the entry side) — **implemented-uncommitted** as of this document
  (`git status` shows `dhan/run_trades.py`, `dhan/test_parallel_orders.py`,
  `dhan/test_targets.py`, `dhan/test_uc_staged_entry.py` modified, not yet
  committed).

---

## What's verified-from-code vs. cited-from-comments

Everything in Parts 1, 2, and the "confirmed" language in Part 3 was read
directly from the current source (line numbers cited throughout). Historical
incident details (SHANTIGEAR, KLBRENG-B, OPTIEMUS, RML, STYLEBAAZA, GOPAL,
BAJAJHIND, THOMASCOOK dates) are cited from code comments/docstrings, not
independently re-verified against raw broker records beyond what
`/root/dhan_trades.log` could confirm directly (the 9 UC-rejection log lines
in Part 3 were independently grepped, not taken from a comment). The one
place a docstring was found to be stale against its own neighboring code is
called out explicitly in Part 2. The UC-staged-entry section's discrepancy
against the plan file is reported as an open discrepancy, not resolved one way
or the other, since resolving it wasn't in scope for this audit.
