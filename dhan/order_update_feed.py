"""
dhan/order_update_feed.py -- Phase 1 shadow-mode observation layer for Dhan's
Live Order Update WebSocket feed (dhanhq.OrderUpdate, wss://api-order-update.
dhan.co).

SCOPE (Phase 1 only): this module OBSERVES. It runs a second, independent
WebSocket connection alongside live_monitor.py's existing MarketFeed
connection, logs every order-update message it receives into an in-memory
cache (periodically persisted to results/order_update_cache.json), and --
when explicitly enabled -- logs a comparison between when this feed first
saw an order go TRADED and when the EXISTING polling path
(_poll_fill_strict/_poll_fill_safe in dhan/run_trades.py) confirmed the same
fill. Nothing here changes what either poll function returns, how long they
take to return, or any downstream order-placement/exit decision -- see
enable_validation_logging()'s own docstring for exactly how that's kept true.

Independence from the market-feed connection (per research on dhanhq's
WebSocket clients): this runs its own thread, its own asyncio event loop
(via dhanhq.OrderUpdate.connect_to_dhan_websocket_sync(), which creates and
owns its own loop), and its own reconnect/backoff state -- never shared with
LiveMonitor's MarketFeed thread/loop or its UC-staged-entry
ThreadPoolExecutor. A disconnect on one feed has no way to touch the other's
state.

Known gap, by design: dhanhq's OrderUpdate does not replay missed updates on
reconnect -- if the feed is down when an order fills, that update is gone
for good, not queued. This module logs every reconnect explicitly (see
_on_connect_ok) specifically so that gap is visible in the validation data as
a real WEBSOCKET_MISSED case, not silently absorbed into "the WS just never
mentioned it."

Phase 2 (NOT built here): actually wiring this feed's cache into
_poll_fill_strict/_poll_fill_safe as a REST-fallback-backed primary source.
That decision -- and what fallback timeout to use -- should be made from the
[WS_VALIDATION] log data this phase produces, not guessed in advance.
"""

import json
import logging
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_logger = logging.getLogger(__name__)   # see FileBackedOrderCache.get_cached's own note
                                        # on why this is the one place in this module
                                        # using logging instead of print

_IST = ZoneInfo("Asia/Kolkata")
_RESULTS_DIR  = Path(__file__).resolve().parent.parent / "results"
_CACHE_FILE   = _RESULTS_DIR / "order_update_cache.json"

_RECONNECT_BACKOFF_INITIAL = 2     # seconds
_RECONNECT_BACKOFF_MAX     = 60    # seconds -- independent of MarketFeed's own backoff constants
_PERSIST_EVERY_SECONDS     = 5     # throttle for the periodic cache-file flush

# How long the validation-logging wrapper waits for a TRADED update to show
# up in this feed's cache before giving up and logging WEBSOCKET_MISSED --
# NOT the same instant polling itself confirms (that would misclassify
# "WS is just 300ms behind on a healthy connection" as a miss). Module-level
# so tests can shrink it instead of sleeping for real seconds.
_VALIDATION_GRACE_SECONDS = 5.0
_VALIDATION_POLL_INTERVAL = 0.25


class OrderUpdateFeed:
    """Independent WebSocket connection to Dhan's Live Order Update feed.

    Phase 1: shadow mode only. Maintains order_id -> {status, filled_qty,
    avg_price, received_at_wallclock, first_traded_at_wallclock,
    last_updated_at_wallclock} under its own lock (see get_cached/snapshot),
    periodically persisted to results/order_update_cache.json. Nothing reads
    that file yet -- it exists so a future cron-triggered process COULD, per
    the Phase 1 spec, not because anything does today."""

    def __init__(self, client_id: str, access_token: str):
        self._client_id    = client_id
        self._access_token = access_token
        self._lock  = threading.Lock()
        self._cache: dict[str, dict] = {}
        self._stop  = threading.Event()
        self._last_persist_at = 0.0
        self._connect_count = 0
        self._last_connect_at: float | None = None
        self._reconnect_backoff = _RECONNECT_BACKOFF_INITIAL

    # ── Cache access (thread-safe) ──────────────────────────────────────────

    def get_cached(self, order_id: str) -> dict | None:
        with self._lock:
            entry = self._cache.get(str(order_id))
            return dict(entry) if entry is not None else None

    def snapshot(self) -> dict:
        with self._lock:
            return {k: dict(v) for k, v in self._cache.items()}

    # ── Message handling ─────────────────────────────────────────────────────

    def _handle_message(self, message: dict) -> None:
        """Bound as the dhanhq OrderUpdate client's on_update callback --
        called synchronously from that client's own asyncio loop/thread, so
        this must stay fast and never raise (wrapped in its own try/except,
        matching every other broker-facing callback in this codebase)."""
        try:
            data = message.get("Data", message) if isinstance(message, dict) else {}
            order_id = str(data.get("orderNo") or data.get("orderId") or "").strip()
            if not order_id:
                print(f"[order_update_feed]   message with no orderNo/orderId: {message}")
                return

            status     = str(data.get("status") or data.get("orderStatus") or "").upper()
            filled_qty = data.get("filledQty") or data.get("tradedQty") or 0
            avg_price  = data.get("averageTradedPrice") or data.get("avgTradedPrice") or 0.0
            now        = datetime.now(_IST)
            now_iso    = now.isoformat()

            with self._lock:
                entry = self._cache.setdefault(order_id, {
                    "status": None, "filled_qty": 0, "avg_price": 0.0,
                    "received_at_wallclock": now_iso,
                    "first_traded_at_wallclock": None,
                    "last_updated_at_wallclock": None,
                })
                entry["status"]                  = status
                entry["filled_qty"]              = filled_qty
                entry["avg_price"]                = avg_price
                entry["last_updated_at_wallclock"] = now_iso
                if status == "TRADED" and entry["first_traded_at_wallclock"] is None:
                    entry["first_traded_at_wallclock"] = now_iso

            print(f"[order_update_feed]   {order_id}  {status}  qty={filled_qty}  "
                  f"avg_price={avg_price}  at {now_iso}")
            self._maybe_persist()
        except Exception as exc:
            print(f"[order_update_feed]   !! failed to handle message: {exc} -- raw={message}",
                  file=sys.stderr)

    def _maybe_persist(self) -> None:
        now = time.monotonic()
        if now - self._last_persist_at < _PERSIST_EVERY_SECONDS:
            return
        self._last_persist_at = now
        self._persist()

    def _persist(self) -> None:
        try:
            snap = self.snapshot()
            _RESULTS_DIR.mkdir(parents=True, exist_ok=True)
            _CACHE_FILE.write_text(json.dumps(snap, indent=2))
        except Exception as exc:
            print(f"[order_update_feed]   !! failed to persist cache: {exc}", file=sys.stderr)

    # ── Connection lifecycle -- own independent reconnect/backoff, entirely
    # separate from live_monitor.py's MarketFeed reconnect logic. ──────────────

    def _on_connect_ok(self) -> None:
        """Note: dhanhq's OrderUpdate.connect_to_dhan_websocket_sync() catches
        its OWN internal exceptions and just returns (see its source) --
        it does NOT re-raise on a genuine connection failure, so this
        process's own `except Exception` around it will rarely fire; a
        return from that call can mean either a clean disconnect OR a
        failure, indistinguishable from here. So backoff is judged by how
        LONG the previous connection actually held (>=30s = treat as
        stable, reset backoff), same heuristic live_monitor.py's own
        _throttle_reconnect_ already uses for the market feed -- NOT by
        whether an exception was raised, and NOT shared state with that
        other reconnect logic, just the same reasoning applied
        independently."""
        self._connect_count += 1
        now = time.monotonic()
        if self._last_connect_at is not None and (now - self._last_connect_at) >= 30:
            self._reconnect_backoff = _RECONNECT_BACKOFF_INITIAL
        if self._connect_count == 1:
            print("[order_update_feed] Connected (order update feed).")
        else:
            print(f"[order_update_feed]   !! RECONNECTED (attempt {self._connect_count}) -- "
                  f"any order update sent while disconnected was NOT replayed by Dhan's feed. "
                  f"Validation entries whose fill happened during that gap will read "
                  f"WEBSOCKET_MISSED -- that's the real gap, not a bug in the comparison.")
        self._last_connect_at = now

    def run(self) -> None:
        """Blocks forever via its own reconnect loop. Call this from its own
        background daemon thread -- never the main thread (that's
        LiveMonitor's MarketFeed loop) and never live_monitor's
        ThreadPoolExecutor (that's for UC-staged-entry order dispatch)."""
        from dhanhq import DhanContext, OrderUpdate

        print("[order_update_feed] Starting Dhan Order Update feed…")
        while not self._stop.is_set():
            try:
                dhan_context = DhanContext(self._client_id, self._access_token)
                client = OrderUpdate(dhan_context)
                client.on_update = self._handle_message
                self._on_connect_ok()
                client.connect_to_dhan_websocket_sync()   # blocks; returns on disconnect/error,
                                                           # does NOT auto-retry internally
            except Exception as exc:
                print(f"[order_update_feed]   !! connection error: {exc}", file=sys.stderr)

            if self._stop.is_set():
                break
            print(f"[order_update_feed]   disconnected -- reconnecting in "
                  f"{self._reconnect_backoff}s…")
            time.sleep(self._reconnect_backoff)
            self._reconnect_backoff = min(self._reconnect_backoff * 2, _RECONNECT_BACKOFF_MAX)

        self._persist()   # best-effort final flush -- see module note on daemon-thread shutdown

    def stop(self) -> None:
        self._stop.set()


class FileBackedOrderCache:
    """Read-only stand-in for OrderUpdateFeed.get_cached(), for use by
    processes that don't hold a live WebSocket connection of their own.

    IMPORTANT, read before wiring anything to this: run_entry_321/
    check_exit_925/force_exit_1159/square_off_239 each run as their OWN
    separate cron-triggered `python3.11 dhan/run_trades.py --...` process --
    NOT inside live_monitor.py's process. enable_validation_logging()
    monkeypatches dhan.run_trades's module-level names IN WHICHEVER PROCESS
    CALLS IT -- calling it inside live_monitor.py only affects polling calls
    made from live_monitor.py's own process (today, realistically just the
    UC-staged-entry path, itself dry-run-gated before it ever reaches
    _poll_fill_strict in production). It does NOT reach into a separate,
    already-running cron process's memory.

    This class exists so that gap is closeable later with a small,
    separately-reviewed addition to dhan/run_trades.py's own entrypoint
    (reading the same results/order_update_cache.json live_monitor.py's
    feed writes) -- deliberately NOT wired in this pass, since that touches
    order-placement-adjacent code this task scoped out. Re-reads the file on
    every call (cheap -- one small JSON file, at most once per filled order
    per run) rather than caching a snapshot that could go stale across a
    run spanning several minutes."""

    def get_cached(self, order_id: str) -> dict | None:
        """Missing file, empty file, or a torn/truncated read (live_monitor.
        py's feed rewrites this file every _PERSIST_EVERY_SECONDS from a
        SEPARATE process, so a read here landing mid-write is an expected,
        harmless timing race, not a real error) all fall into the same
        except below and resolve to "no cached data available" -- never
        raised up into the calling run_trades.py stage. A live exit/entry
        stage must never crash or stall over a torn read on this file.
        Logged at debug level only (not a warning) since this is routine,
        not something worth surfacing by default -- this is the one place in
        this module using `logging` instead of `print`, specifically so it
        stays silent unless someone explicitly turns on DEBUG-level output
        to investigate read-race frequency."""
        try:
            data = json.loads(_CACHE_FILE.read_text())
        except Exception as exc:
            _logger.debug("FileBackedOrderCache: read/parse of %s failed (%s) -- "
                          "treating as no cached data for order_id=%s",
                          _CACHE_FILE, exc, order_id)
            return None
        entry = data.get(str(order_id))
        return dict(entry) if entry is not None else None


# ── Validation logging: compare this feed's TRADED timestamp against the
# EXISTING polling path's confirmation timestamp for the same order_id ──────
#
# The wrapper below is a PURE pass-through: it calls the real
# _poll_fill_strict/_poll_fill_safe unchanged, returns its exact result
# unchanged, and only adds a side-effect (a deferred, background-thread log
# comparison) AFTER the real call has already returned to its caller. Nothing
# about order placement, exit decisions, or position state can depend on this
# feed, because nothing here ever touches the return value, and the
# comparison itself runs in a throwaway daemon thread that the wrapped
# function's caller never waits on.

def _log_not_filled(feed: OrderUpdateFeed, order_id: str, poll_fn: str) -> None:
    entry = feed.get_cached(order_id)
    ws_status = entry.get("status") if entry else "NO_DATA"
    print(f"[WS_VALIDATION] order_id={order_id} poll_fn={poll_fn} "
          f"polling=NOT_FILLED ws_status={ws_status}")


def _log_validation_deferred(feed: OrderUpdateFeed, order_id: str,
                             poll_wall_after: datetime, poll_fn: str) -> None:
    """Runs in its own throwaway daemon thread -- see the module note above.
    Waits up to _VALIDATION_GRACE_SECONDS for the WS feed to report this
    order_id as TRADED before concluding WEBSOCKET_MISSED, so a healthy feed
    that's merely a few hundred ms behind polling isn't misclassified as a
    miss."""
    deadline = time.monotonic() + _VALIDATION_GRACE_SECONDS
    entry = feed.get_cached(order_id)
    while not (entry and entry.get("first_traded_at_wallclock")) and time.monotonic() < deadline:
        time.sleep(_VALIDATION_POLL_INTERVAL)
        entry = feed.get_cached(order_id)

    ws_traded_at_str = entry.get("first_traded_at_wallclock") if entry else None
    if not ws_traded_at_str:
        print(f"[WS_VALIDATION] order_id={order_id} poll_fn={poll_fn} "
              f"polling_confirmed_at={poll_wall_after.isoformat()} result=WEBSOCKET_MISSED")
        return

    ws_traded_at = datetime.fromisoformat(ws_traded_at_str)
    delta  = (poll_wall_after - ws_traded_at).total_seconds()   # + -> WS was faster
    faster = "WEBSOCKET" if delta > 0 else ("POLLING" if delta < 0 else "TIE")
    print(f"[WS_VALIDATION] order_id={order_id} poll_fn={poll_fn} "
          f"polling_confirmed_at={poll_wall_after.isoformat()} "
          f"ws_confirmed_at={ws_traded_at.isoformat()} "
          f"delta_seconds={delta:+.3f} faster={faster}")


def enable_validation_logging(feed: OrderUpdateFeed):
    """Monkeypatches dhan.run_trades._poll_fill_strict/_poll_fill_safe with
    pass-through wrappers that add a [WS_VALIDATION] log comparison as a
    side effect, without touching dhan/run_trades.py's source or altering
    either function's return value/timing as seen by their callers -- see
    the module note above for exactly why the comparison itself runs off
    the critical path in a background thread.

    Returns a restore() callable that puts the two original functions back
    -- production code (live_monitor.py) can ignore it (this only ever gets
    enabled once, for the life of the process); tests use it to undo the
    patch between scenarios.

    Call this ONCE per process, after `feed` has started connecting --
    calling it twice double-wraps and double-logs."""
    import dhan.run_trades as rt

    real_strict = rt._poll_fill_strict
    real_safe   = rt._poll_fill_safe

    def wrapped_strict(order_id):
        result = real_strict(order_id)
        poll_wall_after = datetime.now(_IST)
        price, qty = result[0], result[1]
        if qty <= 0:
            _log_not_filled(feed, order_id, "strict")
        else:
            threading.Thread(target=_log_validation_deferred,
                             args=(feed, order_id, poll_wall_after, "strict"),
                             daemon=True).start()
        return result

    def wrapped_safe(order_id, fallback_price, fallback_qty):
        result = real_safe(order_id, fallback_price, fallback_qty)
        poll_wall_after = datetime.now(_IST)
        price, qty = result[0], result[1]
        if qty <= 0:
            _log_not_filled(feed, order_id, "safe")
        else:
            threading.Thread(target=_log_validation_deferred,
                             args=(feed, order_id, poll_wall_after, "safe"),
                             daemon=True).start()
        return result

    rt._poll_fill_strict = wrapped_strict
    rt._poll_fill_safe   = wrapped_safe

    def restore():
        rt._poll_fill_strict = real_strict
        rt._poll_fill_safe   = real_safe

    return restore
