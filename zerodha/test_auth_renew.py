#!/usr/bin/env python3
"""
test_auth_renew.py -- standalone verifier for zerodha/auth.py's Telegram
renewal-needed alert. Unlike Dhan, Zerodha has no automated renewal --
every point that discovers the token is missing/expired/rejected must
notify a human to run `python -m zerodha.auth` manually. Covers:
  (1) no saved token file
  (2) unreadable/corrupt token file
  (3) expired token
  (4) valid token -> notify must NOT fire
  (5) mid-run 401 whose re-authentication attempt also fails

Mocks pipeline/notify.py's send_zerodha_token_renewal_needed (patched on the
shared `notify` module object -- auth.py's local `import notify` resolves to
the same cached module instance) and redirects zerodha.auth._TOKEN_FILE to a
temp file for the duration of each scenario -- the REAL zerodha/.token.json
is never touched, and no real Telegram/network call is made. Mirrors
dhan/test_auth_renew.py's standalone script style (no pytest in this repo).

Usage:
    python zerodha/test_auth_renew.py

Exit 0 on all-pass, exit 1 on any failure.
"""

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "pipeline"))

import zerodha.auth as auth   # noqa: E402
import notify                 # noqa: E402 -- same module object auth.py's local import resolves to

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


def seed_token_file(path: Path, access_token: str, expires_at: str) -> None:
    path.write_text(json.dumps({
        "access_token": access_token,
        "user_id":      "AB1234",
        "issued_at":    "2026-08-27T08:00:00+05:30",
        "expires_at":   expires_at,
    }))


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (1) — No saved token file -> notify fires, returns None\n")
# ─────────────────────────────────────────────────────────────────────────────

tmp1 = Path(tempfile.mktemp())   # deliberately never created
alerts1 = []

with patch.object(auth, "_TOKEN_FILE", str(tmp1)), \
     patch.object(notify, "send_zerodha_token_renewal_needed",
                  lambda reason: alerts1.append(reason)):
    result1 = auth._load_valid_token()

check("(1) returns None when no token file exists", result1 is None)
check("(1) notify fired exactly once", len(alerts1) == 1, str(alerts1))
check("(1) reason mentions no saved token file", "No saved token file" in alerts1[0], alerts1)


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (2) — Corrupt token file -> notify fires, returns None\n")
# ─────────────────────────────────────────────────────────────────────────────

tmp2 = Path(tempfile.mkstemp()[1])
tmp2.write_text("{not valid json")
alerts2 = []

with patch.object(auth, "_TOKEN_FILE", str(tmp2)), \
     patch.object(notify, "send_zerodha_token_renewal_needed",
                  lambda reason: alerts2.append(reason)):
    result2 = auth._load_valid_token()

check("(2) returns None on a corrupt token file", result2 is None)
check("(2) notify fired exactly once", len(alerts2) == 1, str(alerts2))
check("(2) reason mentions the read failure", "Could not read token file" in alerts2[0], alerts2)
tmp2.unlink()


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (3) — Expired token -> notify fires, returns None\n")
# ─────────────────────────────────────────────────────────────────────────────

tmp3 = Path(tempfile.mkstemp()[1])
seed_token_file(tmp3, "OLD_TOKEN", "2020-01-01T06:00:00+05:30")   # long past
alerts3 = []

with patch.object(auth, "_TOKEN_FILE", str(tmp3)), \
     patch.object(notify, "send_zerodha_token_renewal_needed",
                  lambda reason: alerts3.append(reason)):
    result3 = auth._load_valid_token()

check("(3) returns None for an expired token", result3 is None)
check("(3) notify fired exactly once", len(alerts3) == 1, str(alerts3))
check("(3) reason mentions the token expired", "expired" in alerts3[0].lower(), alerts3)
tmp3.unlink()


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (4) — Valid token -> notify must NOT fire\n")
# ─────────────────────────────────────────────────────────────────────────────

tmp4 = Path(tempfile.mkstemp()[1])
seed_token_file(tmp4, "GOOD_TOKEN", "2099-01-01T06:00:00+05:30")   # far future
alerts4 = []

with patch.object(auth, "_TOKEN_FILE", str(tmp4)), \
     patch.object(notify, "send_zerodha_token_renewal_needed",
                  lambda reason: alerts4.append(reason)):
    result4 = auth._load_valid_token()

check("(4) returns the valid (token, user_id) pair", result4 == ("GOOD_TOKEN", "AB1234"), str(result4))
check("(4) notify NEVER fired for a still-valid token", alerts4 == [], str(alerts4))
tmp4.unlink()


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (5) — Mid-run 401 whose re-authentication also fails -> "
      "notify fires, RuntimeError raised\n")
# ─────────────────────────────────────────────────────────────────────────────
# _KiteSession.request() intercepts a 401, deletes the token file, and tries
# _login() (the interactive browser flow) -- which cannot succeed in a
# headless/cron context. Confirms that failure path also alerts, not just
# the startup-time _load_valid_token() checks above.

tmp5 = Path(tempfile.mkstemp()[1])
seed_token_file(tmp5, "STALE_TOKEN", "2099-01-01T06:00:00+05:30")
alerts5 = []


class Fake401Response:
    status_code = 401


def fake_base_request(self, method, url, **kwargs):
    return Fake401Response()


with patch.object(auth, "_TOKEN_FILE", str(tmp5)), \
     patch.object(auth.requests.Session, "request", fake_base_request), \
     patch.object(auth, "_login", MagicMock(side_effect=RuntimeError("browser flow unavailable"))), \
     patch.object(notify, "send_zerodha_token_renewal_needed",
                  lambda reason: alerts5.append(reason)):
    session = auth._KiteSession()
    try:
        session.request("GET", "https://api.kite.trade/orders")
        raised = None
    except RuntimeError as exc:
        raised = exc

check("(5) RuntimeError raised when re-authentication fails after a 401",
      raised is not None and "re-authentication failed" in str(raised), str(raised))
check("(5) notify fired exactly once", len(alerts5) == 1, str(alerts5))
check("(5) reason mentions the 401 and the re-auth failure",
      "401" in alerts5[0] and "re-authentication failed" in alerts5[0], alerts5)
if tmp5.exists():
    tmp5.unlink()


print("\n" + "─" * 60)
if failures:
    print(f"\033[31m{failures} scenario(s) FAILED\033[0m")
    sys.exit(1)
else:
    print("\033[32mAll scenarios PASSED\033[0m")
    sys.exit(0)
