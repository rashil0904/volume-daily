#!/usr/bin/env python3
"""
test_state_machine.py — standalone state machine verifier for live_monitor.py

Imports evaluate_tick() and _SymState directly from live_monitor.py (no mock,
no stub — the exact same logic the live script runs). Feeds synthetic tick
sequences and asserts correct one-time-transition behaviour.

Usage:
    python test_state_machine.py

Exit 0 on all-pass, exit 1 on any failure.
"""

import sys
from pathlib import Path

# live_monitor.py is at the project root; add root to path so we can import it
# without triggering its networking imports (IPv4 monkeypatch and kiteconnect
# are module-level, so we must handle them before the import).
import socket as _socket
_orig = _socket.getaddrinfo
def _v4(*a, **k): return [r for r in _orig(*a, **k) if r[0] == _socket.AF_INET]
_socket.getaddrinfo = _v4

# Stub kiteconnect so test_state_machine.py has zero network dependency
import types, unittest.mock as _mock
_kc_stub = types.ModuleType("kiteconnect")
_kc_stub.KiteConnect = _mock.MagicMock
_kc_stub.KiteTicker  = _mock.MagicMock
sys.modules["kiteconnect"] = _kc_stub

# Stub pipeline modules (loaded at import time by live_monitor)
import os
sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))
for _m in ("data_loader", "notify"):
    sys.modules.setdefault(_m, types.ModuleType(_m))

# Now import the exact objects under test
sys.path.insert(0, str(Path(__file__).parent))
from unittest.mock import patch
from datetime import datetime
import live_monitor
from live_monitor import _SymState, evaluate_tick, _RETURN_MULT, _CIRCUIT_WARN_PCT, _IST

# ─────────────────────────────────────────────────────────────────────────────

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
failures = 0


def check(label: str, condition: bool, detail: str = "") -> None:
    global failures
    if condition:
        print(f"  {PASS}  {label}")
    else:
        failures += 1
        print(f"  {FAIL}  {label}" + (f"  [{detail}]" if detail else ""))


def make_state(vol_threshold=1_000_000, prev_vwap=100.0,
               upper_circuit=130.0, lower_circuit=70.0) -> _SymState:
    return _SymState(
        symbol        = "TEST",
        vol_threshold = vol_threshold,
        prev_vwap     = prev_vwap,
        upper_circuit = upper_circuit,
        lower_circuit = lower_circuit,
    )


def fake_now(hhmm: int):
    """Return a datetime that reports the given HHMM in IST."""
    h, m = divmod(hhmm, 100)
    return datetime(2026, 8, 1, h, m, 0, tzinfo=_IST)


def tick(state, ltp, cum_vol, hhmm: int = 1000):
    """Feed one tick at the given HHMM (default 10:00, well before cutoff)."""
    with patch.object(live_monitor, "datetime", wraps=live_monitor.datetime) as mock_dt:
        mock_dt.now.return_value = fake_now(hhmm)
        return evaluate_tick(state, ltp, cum_vol)


# ─────────────────────────────────────────────────────────────────────────────
# Scenario A — Happy path: gradual build-up to all 3 conditions, then circuit
# ─────────────────────────────────────────────────────────────────────────────

print("\nScenario A — Happy path: conditions met gradually\n")
print(f"  Parameters: vol_threshold=1,000,000  prev_vwap=100.00  "
      f"upper_circuit=130.00  RETURN_MULT={_RETURN_MULT}  CIRCUIT_WARN_PCT={_CIRCUIT_WARN_PCT}")
print()

s = make_state(vol_threshold=1_000_000, prev_vwap=100.0, upper_circuit=130.0)
vwap_target = 100.0 * _RETURN_MULT  # 105.0

# Tick 1: volume below threshold, price above VWAP target — should not qualify
e = tick(s, ltp=106.0, cum_vol=500_000)
check("Tick 1 — vol low, price OK → no event", e == set(),
      f"got {e}")

# Tick 2: volume above threshold, price just below VWAP target — should not qualify
e = tick(s, ltp=104.9, cum_vol=1_200_000)
check("Tick 2 — vol OK, price low → no event", e == set(),
      f"got {e}")

# Tick 3: volume above threshold, price at exactly vwap_target — should qualify
e = tick(s, ltp=vwap_target, cum_vol=1_500_000)
check("Tick 3 — both conditions met → qualified fires", "qualified" in e,
      f"got {e}")
check("Tick 3 — near_circuit does NOT fire (price far from upper)", "near_circuit" not in e,
      f"got {e}")
check("Tick 3 — state.qualified is True", s.qualified)

# Tick 4: same conditions still true, re-fire check
e = tick(s, ltp=vwap_target + 1, cum_vol=2_000_000)
check("Tick 4 — qualified already set → does NOT re-fire", "qualified" not in e,
      f"got {e}")

# Tick 5: price climbs to within 0.5% of upper circuit (130.0)
# pct_to_upper = (130 - 129.35) / 129.35 * 100 = 0.502% → below CIRCUIT_WARN_PCT
near_ltp = 129.35
pct = (130.0 - near_ltp) / near_ltp * 100
e = tick(s, ltp=near_ltp, cum_vol=2_500_000)
check(f"Tick 5 — pct_to_upper={pct:.2f}% (≤{_CIRCUIT_WARN_PCT}%) → near_circuit fires",
      "near_circuit" in e, f"got {e}")
check("Tick 5 — state.near_circuit is True", s.near_circuit)

# Tick 6: still near circuit — near_circuit must NOT re-fire
e = tick(s, ltp=129.50, cum_vol=2_700_000)
check("Tick 6 — near_circuit already set → does NOT re-fire", "near_circuit" not in e,
      f"got {e}")
check("Tick 6 — no events at all", e == set(), f"got {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Scenario B — Inverse: conditions never fully met → qualified never fires
# ─────────────────────────────────────────────────────────────────────────────

print("\nScenario B — Inverse: never all 3 conditions → no events\n")
s2 = make_state(vol_threshold=1_000_000, prev_vwap=100.0, upper_circuit=130.0)

# Vol high, price always below VWAP target
for i, (ltp, vol) in enumerate([
    (103.0, 2_000_000),   # vol OK but ltp < 105
    (104.9, 3_000_000),   # still short
    (104.99, 5_000_000),  # 1 pip below
], start=1):
    e = tick(s2, ltp=ltp, cum_vol=vol)
    check(f"Tick B{i} — price={ltp} < vwap_target=105 → no event", e == set(),
          f"got {e}")

check("Scenario B — state.qualified remains False", not s2.qualified)
check("Scenario B — state.near_circuit remains False", not s2.near_circuit)

# ─────────────────────────────────────────────────────────────────────────────
# Scenario C — near_circuit cannot fire before qualified
# ─────────────────────────────────────────────────────────────────────────────

print("\nScenario C — near_circuit cannot precede qualified\n")
s3 = make_state(vol_threshold=1_000_000, prev_vwap=100.0, upper_circuit=130.0)

# Drive price right to the circuit edge but volume is too low to qualify
e = tick(s3, ltp=129.5, cum_vol=100_000)  # vol below threshold
check("Tick C1 — near circuit price, insufficient volume → neither fires", e == set(),
      f"got {e}")
check("Scenario C — state.qualified=False, state.near_circuit=False",
      not s3.qualified and not s3.near_circuit)

# Now meet both volume AND price at once — qualified should fire first, then
# near_circuit in the same tick (because pct_to_upper is already ≤ 1%)
e = tick(s3, ltp=129.5, cum_vol=1_500_000)
check("Tick C2 — both threshold met at once → qualified fires", "qualified" in e,
      f"got {e}")
check("Tick C2 — near_circuit fires same tick (already in circuit zone)", "near_circuit" in e,
      f"got {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Scenario D — Zero / edge values don't crash or spuriously fire
# ─────────────────────────────────────────────────────────────────────────────

print("\nScenario D — Edge values: zero ltp / zero volume / zero circuit\n")
s4 = make_state(vol_threshold=1_000_000, prev_vwap=100.0, upper_circuit=0.0)

e = tick(s4, ltp=0.0, cum_vol=0.0)
check("Tick D1 — ltp=0, vol=0 → no event", e == set(), f"got {e}")

e = tick(s4, ltp=106.0, cum_vol=2_000_000)
check("Tick D2 — vol+price OK, qualified fires", "qualified" in e, f"got {e}")
# upper_circuit=0 → pct_to_upper check skipped → near_circuit should NOT fire
check("Tick D2 — upper_circuit=0 → near_circuit does NOT fire", "near_circuit" not in e,
      f"got {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Scenario E — 15:00 cutoff: no new events after cutoff, ltp/cum_vol still updated
# ─────────────────────────────────────────────────────────────────────────────

print("\nScenario E — 15:00 cutoff: evaluation suppressed at/after 15:00 IST\n")

s5 = make_state(vol_threshold=1_000_000, prev_vwap=100.0, upper_circuit=130.0)

# Tick at 14:59 — both conditions met, should qualify
e = tick(s5, ltp=106.0, cum_vol=1_500_000, hhmm=1459)
check("Tick E1 — 14:59, conditions met → qualified fires", "qualified" in e, f"got {e}")

# Tick at exactly 15:00 — cutoff is inclusive (>=), should produce no events
e = tick(s5, ltp=129.5, cum_vol=2_000_000, hhmm=1500)
check("Tick E2 — 15:00 exactly → no events (cutoff is >=)", e == set(), f"got {e}")
check("Tick E2 — near_circuit NOT set despite being in circuit zone", not s5.near_circuit)

# Tick at 15:30 — well past cutoff, still no events
e = tick(s5, ltp=129.9, cum_vol=3_000_000, hhmm=1530)
check("Tick E3 — 15:30 → no events", e == set(), f"got {e}")

# But ltp and cum_vol must still be updated (heartbeat reads them)
check("Tick E3 — state.ltp updated to 129.9 despite cutoff", s5.ltp == 129.9)
check("Tick E3 — state.cum_vol updated to 3,000,000 despite cutoff", s5.cum_vol == 3_000_000)

# ─────────────────────────────────────────────────────────────────────────────
# Final tally
# ─────────────────────────────────────────────────────────────────────────────

print(f"\n{'─' * 55}")
if failures == 0:
    print(f"\033[32mAll scenarios PASSED\033[0m")
    sys.exit(0)
else:
    print(f"\033[31m{failures} assertion(s) FAILED\033[0m")
    sys.exit(1)
