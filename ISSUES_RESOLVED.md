# Issues Resolved — Project History

A thematic history of the real bugs, incidents, and design decisions behind this codebase,
so a new chat session understands **why** things are built the specific way they are, not
just what the code currently does (that's [`STRATEGY.md`](STRATEGY.md)). Organized by
theme, roughly chronological within each. Section numbers match the `§` references in
`STRATEGY.md`.

This is not a line-by-line commit log (306 commits as of 2026-11-02, most of them daily data
refreshes) — it's the substantive history, with enough detail on each incident to
understand the fix without re-reading the original bug report.

---

## §1 — Broker and data-source migrations

- **Kite/Zerodha → removed, pending rebuild (2026-09-10)**. Zerodha originally ran a full
  parallel execution pipeline alongside Dhan (same signals, separate capital pool, separate
  position files). Removed entirely rather than left half-working; its historical position
  files (`positions_zerodha_long.json`, `positions_zerodha_short.json`, `trade_book.csv/.xlsx`)
  were kept, not deleted. Never rebuilt since.
- **Upstox → Dhan for candle data (2026-09-24)**. Upstox was never a trading broker in this
  system, only the candle/reference-price data source. Migrated to Dhan's own
  `/charts/intraday` endpoint so candle data and live execution share one API/one token,
  removing an entire second-vendor dependency.
- **MARKET → LIMIT orders (early, 2026-08-17)**. Every order placed as `orderType: MARKET`
  came back from Dhan's own order records as `orderType: LIMIT` near the submission price —
  Dhan/NSE silently apply an undocumented price-protection band to MARKET orders. That band
  was tight enough that ordinary same-second price movement (no circuit lock involved)
  produced 0-fills. Switched to an explicit LIMIT 0.75% above live LTP for predictable
  headroom instead. Dhan's order API has no exposed market-protection/collar parameter
  (unlike Kite's `market_protection`).

## §2 — Entry mechanism evolution

- Started as a single fixed-time MARKET/LIMIT entry at 3:21 PM.
- **UC-based staged entry (Case A/B)**: added 2026-09-09, a second tick-driven entry
  mechanism running in parallel with the fixed-time entry. **Removed 2026-09-21** after a
  real live bug — Case B kept re-firing on the same symbol every tick instead of latching
  once a fire attempt was decided/skipped, burning real balance-check API calls in a tight
  loop for the rest of the session. Never rebuilt; design writeup kept at
  `results/UC_Staged_Entry_Explained.docx` for reference.
- **Tranched limit entry (`run_entry_limit`)** brought in from a PR the same day the UC
  staged entry was removed (2026-09-21) — semi-aggressive software-side tranching: LIMIT at
  best_bid+1 tick, recalibrated every 10s, MARKET-sweeping anything unfilled at a 15:20
  cutoff. Originally sized to a liquidity-ranked **half** of each day's trade list (the
  least-liquid half, reasoning being the more-liquid half didn't need the gentler tranching).
  **Changed 2026-10-08** to route the **full** trade list through tranched entry — per user
  decision, every signal gets the gradual mechanism rather than splitting by liquidity.
- `run_entry_321` (the original fixed-time entry) is now purely the **safety net** for
  anything `run_entry_limit` got zero fills on — it waits on a completion marker from
  `run_entry_limit` before touching positions, so the two never race on the same write.
- A genuinely tricky coordination bug here: `run_entry_321`'s cron and `run_entry_limit`'s
  internal cutoff used to sit ~1 minute apart (15:19 vs 15:20); moved to the exact same
  clock minute (2026-09-22) once it became clear the overlap was the common case, not a rare
  edge — `run_entry_321`'s bounded wait on the marker (deadline 15:21:30) now reliably blocks
  for a few real seconds on every normal run.

## §3 — Phantom fills and the reconciliation philosophy

This is the throughline of the whole codebase and worth understanding as one story, not a
list of isolated bugs.

**The founding incident (2026-08-xx, very early)**: an entry poll timeout was originally
handled by falling back to a caller-supplied `(fallback_price, fallback_qty)` **as if it
were a real fill** — i.e., guessing. Confirmed live: a position got recorded as filled at a
price it never actually traded at. Fixed by making every poll-timeout path return `(0, 0)`
instead (`_poll_fill_safe`'s current behavior) — a real rejection and a genuine ambiguous
timeout are deliberately handled identically at this layer (both "not confirmed filled"),
with a different log message so a human can tell which actually happened.

**The recurring consequence**: a REST poll that returns "not filled" because it gave up
after ~15s doesn't mean the order is dead — Dhan sometimes fills it seconds-to-minutes
later, and because the code correctly refuses to guess, nothing re-checks it afterward. This
exact failure mode recurred individually, by hand-investigation, many times before it was
finally automated away:

| Symbol(s) | What happened | How it was found/fixed |
|---|---|---|
| NOVARTIND | Short stopped out at 10:35am, left recorded as open | Manual reconciliation |
| RML | Manual exit after two automated CNC sell rejections | Manual reconciliation |
| JINDRILL | `_broker_qty()` false mismatch on a partial fallback sell | Fixed root cause + reconciled |
| 6 symbols, 2026-09-21 | Manual trades never entered into position tracking at all | Manual reconciliation |
| DPABHUSHAN, YUKEN | Stuck at `status=open` | Manually traced via broker `/trades` API, corrected |
| KOTIC, MMWL (10-09), SUNCLAY | Same poll-timeout-then-late-fill pattern | Manually reconciled |
| MMWL (10-12), INDOKEM, SIMPLEXINF | Same pattern again | Manually reconciled — this is the point the recurrence was recognized as systemic |

**The systemic fix (`--reconcile`, `b97dd6a9`)**: rather than keep hand-fixing individual
symbols, `_reconcile_stale_exits()` was built — compares every open long's local qty against
the broker's real holding; on a mismatch, pulls the real fill from Dhan's `/trades` API
(never estimates) and closes the position properly. Deliberately **not** inlined into
`place_targets_915`/`check_exit_916`/`force_exit_1159` — doing that first was tried and
caused `test_targets.py` to balloon from ~9s to ~190s (real unmocked network calls hit by
every pre-existing mismatch-test fixture) and would have meant 9 redundant re-sweeps/day.
Reverted to its own dedicated `--reconcile` CLI flag + a single 9:12 AM cron slot instead.

**The short-side gap (FISCHER, 2026-10-26, `a9af87ab`)**: the systemic fix above only
covered long exits. A mirrored short-open order filled 18 seconds after placement — 8
seconds past the poll's budget — and because the code (correctly) treats a timeout as
"not confirmed," it discarded the order id entirely, with **no cancel, no position record,
no cover-target, no stop-loss**. The naked short sat completely unmanaged for 5 hours until
**Dhan's own RMS auto-covered it** before close (NSE doesn't allow carrying a naked equity
short overnight) at a price with zero input from this system — a ₹4,584 avoidable loss.
Fixed with the same pattern as the long side: `_record_pending_short_verification`
preserves the order id instead of discarding it, and `_reconcile_pending_shorts` (called
from every `square_off_239` bucket run, 3×/day, plus the 9:12 AM slot) re-checks it against
Dhan's real order status — recovering the position with its real fill price and
immediately placing the missing protective orders if it actually traded.

**The design lesson repeated across both fixes**: a reconciliation sweep belongs in its own
dedicated, infrequent entry point — never inlined into a timing-sensitive, already-tested
hot path.

## §4 — Rejection-retry logic

A cluster of fixes around Dhan's various RMS rejection reasons, each requiring a different,
specific retry:
- **MTF-ineligibility** (`"Mtf Product Is Not Allowed For This Scrip"`) → retry as CNC, same
  quantity, no re-halving. Only on this *specific* rejection — a circuit-limit rejection
  retries nothing, since CNC would hit the identical price band.
- **T+5/MARGIN settlement lag** (`"No eligible T+5 quantity found"`) → retry as CNC.
- **MTF pledge-ledger lag** → retry as MTF again (not CNC) — a different lag than the T+5
  case, needing a different retry target.
- **CNC settlement-lag rejection** (confirmed live on PARACABLES, 2026-10-21) → wait 30s,
  retry as CNC again; if that also fails, fall through to MTF once before giving up.
- Earlier, cruder version of this logic over-retried on *any* rejection, not just the
  specific genuine one — tightened multiple times (`617e1470`, `f815d1bf`) to only retry on
  a confirmed, specific rejection reason.

## §5 — Exit schedule evolution

- Exit stages were originally named/timed `945`/`1200`, renamed to `925`/`1159` (`269a714a`),
  then the first stage moved again to `916` (`943985bf`, 2026-09-18) with the parallel order
  cap raised 5→7 concurrent.
- **Return-bucketed exit schedule** (`240b4f52`, 2026-09-26, times revised `3819ac5f`
  2026-09-27): replaced one fixed 3-stage schedule for every position with three independent
  schedules (5-10%/10-15%/15-20%), each position bucketed by its own entry signal's
  `return_pct`. The legacy untagged/"no bucket" fallback path was removed entirely
  2026-09-27 once every open position had been backfilled with a bucket — there is no
  default bucket now; every entry path requires one.
- **Mirrored-short gate** (`b775a46d`): mirrored shorts used to open on every long exit
  regardless of how marginal the win was. Gated to only open when the long's realized return
  was ≥3% — a long that barely escaped flat doesn't get a short mirrored on it.
- Mirrored shorts themselves went through: added → stop-loss added (`c5daf69c`) → removed
  entirely for a period (`f6d5de4c`) → re-added with the UC-based stop-loss design that
  persists today.

## §6 — Charges/cost model evolution

- Started by **always computing costs from a rate-card estimate** (`665a5cb8`), then
  switched to **preferring the real trade-book API, falling back to the estimate**
  (`dd943277`), then finally **removed the estimate fallback entirely** (`644dd778`,
  2026-09-15) — explicit instruction: show "no charge data yet" (0/pending) rather than a
  guessed number, ever, for entry/exit leg charges. A same-day fill simply isn't in Dhan's
  trade-book yet and reads 0 until the next day's run — expected, not a bug.
- **Tranche-entry charge bug** (`c0e8f60b`): a tranched entry (multiple separate LIMIT fills)
  could get its entry charges permanently stranded at 0 if even one of its several order ids
  failed to resolve — fixed, backfilled 10 affected positions.
- **Rate-limiting on the trades API** (`0c142fbd`): `get_trades()`'s pagination had no
  retry-on-429, and since it discards every already-fetched page on ANY failure (not a
  partial-result return), a transient 429 on a late page was silently destroying every
  earlier page's real data too — not just failing to fetch the rest. Fixed with a
  rate-limited retry-with-backoff wrapper.

## §7 — The VERANDA interest-allocation bug (2026-11-02)

Investigating a user request to audit whether all closed trades had their costs properly
populated surfaced a real, higher-impact bug than the single trade that triggered the
question. `mtf_interest_allocation_index()` builds ONE shared real-ledger interest
allocation across **every** MTF position in a single call (deliberately — two separate
`get_trades()` calls back-to-back had already been confirmed to trip Dhan's rate limit).
Inside that shared build, a per-position `funded_amount()` call does a `security_id()`
lookup that raises `ValueError` for a delisted/renamed symbol (VERANDA, no longer in Dhan's
current scrip master). That `ValueError` was **not** caught — only `(RuntimeError,
EnvironmentError)` were — so it propagated out of the whole function, was swallowed by a
blanket `except Exception` in the caller, and **silently degraded every MTF position's
interest charge from the accurate real-ledger figure to the less-precise formula estimate,
on every single sync run**, for as long as VERANDA has existed in the position file — not
just VERANDA's own charge. Fixed by catching `ValueError` at both the batch-build site and
the per-position formula-fallback site, scoping the failure to just the one unresolvable
symbol. Verified via a real 3-workbook rebuild and confirmed every other closed trade stayed
on the real-ledger path.

## §8 — Rate limiting and API reliability

- **Dhan UC-fetch 429s** (`260fe6ba`): persisted the 9:15 AM circuit cache, widened the
  quote rate limiter, added retry-on-429.
- **Quote API batching** (`e8ebe442`): batched LTP fetches into one call instead of
  per-symbol, to stay under Dhan's 1 req/sec Quote API limit.
- **WS-first fill confirmation** (`074fdfbc`, made permanent 2026-09-18): a time-boxed trial
  (2026-09-09 → 09-13) checking Dhan's real-time Order Update WebSocket feed before falling
  back to REST polling. [WS_VALIDATION] shadow-logging ran the whole trial independently and
  showed the WS feed confirmed every one of 66/66 compared fills before REST did (avg ~0.9s
  lead, up to ~5s) — made permanent on that data. Every path that doesn't get an explicit
  WS-confirmed TRADED within the budget falls straight through to the exact same REST
  `_poll_fill_safe()` call as before, so a disconnected/stale feed can only ever cost a few
  wasted checks, never produce a wrong price.
- A real, separate precision bug surfaced inside this same fast path 2026-10-21 (PARACABLES):
  the WS feed's own `avg_price` field is rounded to 2 decimals, while Dhan's REST order-detail
  endpoint carries the true higher-precision average — a small (~₹9 on that trade) but
  systemic overstatement on every WS-confirmed fill. Identified and the one affected record
  corrected; the broader fix (always re-pull the precise REST price after a WS confirm) was
  flagged but intentionally not applied, since it would cost back the latency the WS-first
  path exists to save.

## §9 — P&L workbook evolution

- Started as a flattened `trade_book.csv`/`.xlsx`, then a full "Total PnL" dashboard redesign
  with live-synced Trade Log (`e93361b9`), then a Charges breakdown, bucket/monthly/weekly
  stats, and drawdown tracking added (`9d14207b`) — drawdown later removed again
  (`bffc202c`) per user request (not currently tracked).
- **Google Sheets live-push** (webhook-based, `sheets/apps_script/`) was tried, then removed
  in favor of an **rclone-synced Google Drive Excel** instead — true in-place overwrite (same
  file ID/link), converted to a native Sheet on upload.
- **Capital-era split** (2026-10-01, `8e7dfa7e`): once capital was reduced ₹15L→₹12L, mixing
  both eras' trades under one Base Capital figure would've made %-of-capital misleading for
  whichever era didn't match — became 2 separate workbooks (one frozen, one live), then 3
  once ₹12L→₹11L happened (2026-11-01).
- **Consolidated back into ONE file** (2026-11-02): realized only the Total PnL sheet's two
  %-of-capital cards ever actually depended on Base Capital — every other sheet (Trade Log,
  Day Wise, Position Stats, Bucket Stats, Company Stats, Charges, Monthly/Weekly) was
  already capital-independent the whole time. Replaced the 3-file split with one
  `volume-daily-btst-pnl.xlsx` covering every trade since 2026-08-19; the Total PnL sheet now
  shows all-time ₹/count-only cards plus a "By Capital Era" table breaking out %-of-capital
  per era against its own correct base. Verified correct via an independent LibreOffice
  headless recalculation cross-checked against a from-scratch Python aggregation — not just
  visual inspection of the formulas.
- The Google Drive push itself was originally a separate timed cron line (15:50); **folded
  into `_sync_pnl_workbook()` itself** (`86a354ec`, 2026-10-12) so a manual `--sync-pnl` run
  also pushes, closing a drift gap that had just caused a "the Sheet looks stale" complaint
  (it wasn't actually stale — a manual fix hadn't been re-synced to Drive yet).

## §10 — Notifications

- **Order IDs removed from every Telegram message** — internal detail, not useful to a human
  reading the channel.
- **Return-bucket tagging added to every entry and exit notification** — lets a reader tell
  which of the 3 exit schedules a position is on without opening the position file.
- **"Target placed" notification removed entirely** — a resting order being placed isn't an
  outcome worth a push alert; kept only as a console log line.
- **Dead notifiers removed**: `send_zerodha_token_renewal_needed` (referenced a `zerodha.auth`
  module that no longer exists since Zerodha's removal) and the `send_target_placed` function
  itself.
- **`QUALIFIED` live-monitor alert volume figure fixed**: was showing `cum_vol / threshold`
  (already-6×'d denominator), understating the real ratio by ~6x versus the intended
  `cum_vol / raw_36day_avg`. Removed the misleading figure from the message rather than
  silently leave two different "ratio" meanings in the same alert type.

## §11 — Testing infrastructure

- All Dhan tests consolidated into `dhan/test/` with one `test_all.py` runner
  (`5706e3e4`) — each test file still runs as its **own subprocess**, not imported together,
  since several files apply process-wide mocks at import time that would otherwise leak
  across files sharing one process.
- A real isolation bug found during the short-open reconcile work (2026-10-26): an
  unconditional `import dhan.charges` at the top of a function executed (and cached) even on
  code paths that never used it, defeating `unittest.mock.patch.dict(sys.modules, ...)`
  isolation across later test scenarios in the same process. Fixed by moving the import
  inside the specific branch that actually needs it — a general gotcha worth remembering for
  any future test using `patch.dict(sys.modules)`.

## §12 — Position-file structure

Positions were originally one file keyed by a `direction` field (long vs short). **Split
into separate long/short files** (`078edfe5` for Dhan, `89338729` for Zerodha) after a real
bug: a same-day mirrored short's `entry_date` made a *fresh* same-day long re-entry on that
same symbol silently skip itself (the "already entered today" dedup check scanned for any
same-day row regardless of direction). Confirmed live 2026-08-20 on BAJAJHIND and ZAGGLE.
Separate files make the collision structurally impossible rather than relying on a smarter
dedup check.

## §13 — Capital changes

₹5L (initial MTF test) → ₹14L → ₹14.5L → **₹15L** (2026-08) → **₹12L** (2026-10-01) →
**₹11L** (2026-11-01, current). Each change is a 3-file sync
(`pipeline/main.py`/`pipeline/notify.py`/`dhan/run_trades.py`'s `TOTAL_CAPITAL` constants)
plus — since the ₹15L→₹12L cutover — a matching capital-era entry in the P&L workbook (see
§9).

## §14 — Holiday handling

Market-holiday cron guards are self-expiring one-line date checks chained with `||` as each
new holiday comes up (`[ "$(date +\%Y-\%m-\%d)" = "2026-10-02" ] || [ "$(date +\%Y-\%m-\%d)" =
"2026-10-20" ] || <command>`) — applied to every weekday (`1-5`) cron line except the
twice-daily token-renewal job, which deliberately always runs (a token still needs renewing
on a holiday so Monday's first real run doesn't start from an expired one). No manual
cleanup needed afterward; the date simply stops matching once it passes.
