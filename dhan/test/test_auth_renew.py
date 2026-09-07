#!/usr/bin/env python3
"""
test_auth_renew.py -- standalone verifier for dhan/auth.py's
renew_access_token() (automated daily Dhan token renewal) AND
_load_valid_token()'s Telegram renewal-needed alert (fired whenever ANY
script's get_session() call discovers there's no usable token at all --
distinct from send_token_renewal_failed, which only fires from the dedicated
8am/8pm --renew cron when its own renewal attempt fails).

Mocks every network call (requests.get/requests.post on the module dhan.auth
imports) and redirects dhan.auth._TOKEN_FILE to a temp file for the duration
of each scenario -- the REAL dhan/.token.json (holding today's actual live
token) is never touched. Mirrors dhan/test_targets.py's standalone script
style (no pytest in this repo).

Usage:
    python dhan/test_auth_renew.py

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

import dhan.auth as auth   # noqa: E402
import notify              # noqa: E402 -- same module object auth.py's local import resolves to

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


class FakeResponse:
    def __init__(self, status_code: int, json_body: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._json_body  = json_body or {}
        self.text        = text or json.dumps(self._json_body)

    def json(self):
        return self._json_body


def seed_token_file(path: Path, access_token: str) -> None:
    path.write_text(json.dumps({
        "access_token": access_token,
        "issued_at":  "2026-08-25T08:00:00+05:30",
        "expires_at": "2026-08-26T08:00:00+05:30",
    }))


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (1) — Successful renew + verify + save\n")
# ─────────────────────────────────────────────────────────────────────────────

tmp1 = Path(tempfile.mkstemp()[1])
seed_token_file(tmp1, "OLD_TOKEN")


def fake_get_1(url, headers=None, timeout=None):
    if url.endswith("/RenewToken"):
        check("(1) RenewToken called with the OLD token in the access-token header",
              headers.get("access-token") == "OLD_TOKEN")
        return FakeResponse(200, {"token": "NEW_TOKEN", "createTime": "x", "expiryTime": "y"})
    if url.endswith("/fundlimit"):
        check("(1) /fundlimit verification called with the NEW token, not the old one",
              headers.get("access-token") == "NEW_TOKEN")
        return FakeResponse(200, {"dhanClientId": "123", "availabelBalance": 100000.0})
    raise AssertionError(f"unexpected URL: {url}")


with patch.object(auth, "_TOKEN_FILE", str(tmp1)), \
     patch.object(auth, "_CLIENT_ID", "CID123"), \
     patch.object(auth.requests, "get", fake_get_1), \
     patch.object(auth.requests, "post", MagicMock(side_effect=AssertionError("must not use POST"))):
    ok, msg = auth.renew_access_token()

check("(1) renew_access_token() returns success", ok is True, msg)
check("(1) success message mentions the new expiry", "renewed successfully" in msg, msg)
saved1 = json.loads(tmp1.read_text())
check("(1) token file now holds the NEW token", saved1["access_token"] == "NEW_TOKEN", str(saved1))
tmp1.unlink()


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (2) — RenewToken call itself fails -> old token preserved\n")
# ─────────────────────────────────────────────────────────────────────────────

tmp2 = Path(tempfile.mkstemp()[1])
seed_token_file(tmp2, "OLD_TOKEN")


def fake_get_2(url, headers=None, timeout=None):
    if url.endswith("/RenewToken"):
        return FakeResponse(400, {"errorType": "Order_Error", "errorCode": "DH-906",
                                  "errorMessage": "Invalid Token"})
    raise AssertionError(f"unexpected URL called after RenewToken failed: {url}")


with patch.object(auth, "_TOKEN_FILE", str(tmp2)), \
     patch.object(auth, "_CLIENT_ID", "CID123"), \
     patch.object(auth.requests, "get", fake_get_2), \
     patch.object(auth.requests, "post", MagicMock(side_effect=AssertionError("must not use POST"))):
    ok2, msg2 = auth.renew_access_token()

check("(2) renew_access_token() returns failure", ok2 is False)
check("(2) failure message includes the HTTP status", "400" in msg2, msg2)
saved2 = json.loads(tmp2.read_text())
check("(2) token file STILL holds the OLD token -- untouched by a failed renewal",
      saved2["access_token"] == "OLD_TOKEN", str(saved2))
tmp2.unlink()


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (3) — Renew succeeds but /fundlimit verification fails -> old token preserved\n")
# ─────────────────────────────────────────────────────────────────────────────
# The old token is already dead on Dhan's side by this point (renewal already
# succeeded) -- but the file must still show the OLD token, not the unverified
# NEW one, per the explicit "don't save an unverified token" requirement.

tmp3 = Path(tempfile.mkstemp()[1])
seed_token_file(tmp3, "OLD_TOKEN")


def fake_get_3(url, headers=None, timeout=None):
    if url.endswith("/RenewToken"):
        return FakeResponse(200, {"token": "UNVERIFIED_NEW_TOKEN"})
    if url.endswith("/fundlimit"):
        return FakeResponse(400, {"errorType": "Order_Error", "errorCode": "DH-906",
                                  "errorMessage": "Invalid Token"})
    raise AssertionError(f"unexpected URL: {url}")


with patch.object(auth, "_TOKEN_FILE", str(tmp3)), \
     patch.object(auth, "_CLIENT_ID", "CID123"), \
     patch.object(auth.requests, "get", fake_get_3), \
     patch.object(auth.requests, "post", MagicMock(side_effect=AssertionError("must not use POST"))):
    ok3, msg3 = auth.renew_access_token()

check("(3) renew_access_token() returns failure (verification failed)", ok3 is False)
check("(3) failure message mentions verification", "verification" in msg3.lower(), msg3)
saved3 = json.loads(tmp3.read_text())
check("(3) token file STILL holds the OLD token -- the unverified new token was NEVER saved",
      saved3["access_token"] == "OLD_TOKEN", str(saved3))
tmp3.unlink()


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (4) — Must use GET, not POST (fails loudly if changed back)\n")
# ─────────────────────────────────────────────────────────────────────────────
# requests.post is wired to blow up if called at all. If a future edit
# switches RenewToken (or the verification call) to POST, this scenario
# turns from a clean pass into an explicit, readable failure instead of
# silently doing the wrong thing.

tmp4 = Path(tempfile.mkstemp()[1])
seed_token_file(tmp4, "OLD_TOKEN")

mock_post_4 = MagicMock(side_effect=AssertionError(
    "POST must never be used for RenewToken/fundlimit -- Dhan requires GET "
    "(confirmed live: POST/PUT both return DH-905)"))


def fake_get_4(url, headers=None, timeout=None):
    if url.endswith("/RenewToken"):
        return FakeResponse(200, {"token": "NEW_TOKEN"})
    if url.endswith("/fundlimit"):
        return FakeResponse(200, {"dhanClientId": "123", "availabelBalance": 100000.0})
    raise AssertionError(f"unexpected URL: {url}")


with patch.object(auth, "_TOKEN_FILE", str(tmp4)), \
     patch.object(auth, "_CLIENT_ID", "CID123"), \
     patch.object(auth.requests, "get", fake_get_4), \
     patch.object(auth.requests, "post", mock_post_4):
    ok4, msg4 = auth.renew_access_token()

check("(4) renew_access_token() succeeds using GET only", ok4 is True, msg4)
check("(4) requests.post was NEVER called -- proves the implementation uses GET, "
      "not POST, for both RenewToken and the verification call",
      mock_post_4.call_count == 0, f"post called {mock_post_4.call_count}x")
tmp4.unlink()


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (5) — No saved token file -> renewal-needed notify fires, "
      "_load_valid_token() returns None\n")
# ─────────────────────────────────────────────────────────────────────────────

tmp5 = Path(tempfile.mktemp())   # deliberately never created
alerts5 = []

with patch.object(auth, "_TOKEN_FILE", str(tmp5)), \
     patch.object(notify, "send_dhan_token_renewal_needed",
                  lambda reason: alerts5.append(reason)):
    result5 = auth._load_valid_token()

check("(5) returns None when no token file exists", result5 is None)
check("(5) notify fired exactly once", len(alerts5) == 1, str(alerts5))
check("(5) reason mentions no saved token file", "No saved token file" in alerts5[0], alerts5)


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (6) — Corrupt token file -> notify fires, returns None\n")
# ─────────────────────────────────────────────────────────────────────────────

tmp6 = Path(tempfile.mkstemp()[1])
tmp6.write_text("{not valid json")
alerts6 = []

with patch.object(auth, "_TOKEN_FILE", str(tmp6)), \
     patch.object(notify, "send_dhan_token_renewal_needed",
                  lambda reason: alerts6.append(reason)):
    result6 = auth._load_valid_token()

check("(6) returns None on a corrupt token file", result6 is None)
check("(6) notify fired exactly once", len(alerts6) == 1, str(alerts6))
check("(6) reason mentions the read failure", "Could not read token file" in alerts6[0], alerts6)
tmp6.unlink()


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (7) — Expired token -> notify fires, returns None\n")
# ─────────────────────────────────────────────────────────────────────────────

tmp7 = Path(tempfile.mkstemp()[1])
tmp7.write_text(json.dumps({
    "access_token": "OLD_TOKEN",
    "issued_at":  "2020-01-01T08:00:00+05:30",
    "expires_at": "2020-01-02T08:00:00+05:30",   # long past
}))
alerts7 = []

with patch.object(auth, "_TOKEN_FILE", str(tmp7)), \
     patch.object(notify, "send_dhan_token_renewal_needed",
                  lambda reason: alerts7.append(reason)):
    result7 = auth._load_valid_token()

check("(7) returns None for an expired token", result7 is None)
check("(7) notify fired exactly once", len(alerts7) == 1, str(alerts7))
check("(7) reason mentions the token expired", "expired" in alerts7[0].lower(), alerts7)
tmp7.unlink()


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (8) — Valid token -> notify must NOT fire\n")
# ─────────────────────────────────────────────────────────────────────────────

tmp8 = Path(tempfile.mkstemp()[1])
tmp8.write_text(json.dumps({
    "access_token": "GOOD_TOKEN",
    "issued_at":  "2026-08-27T08:00:00+05:30",
    "expires_at": "2099-01-01T06:00:00+05:30",   # far future -- avoids depending on real "now"
}))
alerts8 = []

with patch.object(auth, "_TOKEN_FILE", str(tmp8)), \
     patch.object(notify, "send_dhan_token_renewal_needed",
                  lambda reason: alerts8.append(reason)):
    result8 = auth._load_valid_token()

check("(8) returns the valid access_token", result8 == "GOOD_TOKEN", str(result8))
check("(8) notify NEVER fired for a still-valid token", alerts8 == [], str(alerts8))
tmp8.unlink()


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (9) — --renew CLI success -> confirmation notify fires\n")
# ─────────────────────────────────────────────────────────────────────────────

tmp9 = Path(tempfile.mkstemp()[1])
seed_token_file(tmp9, "OLD_TOKEN")
success_alerts9 = []
failure_alerts9 = []

with patch.object(auth, "_TOKEN_FILE", str(tmp9)), \
     patch.object(auth, "_CLIENT_ID", "CID123"), \
     patch.object(auth.requests, "get", fake_get_1), \
     patch.object(auth.requests, "post", MagicMock(side_effect=AssertionError("must not use POST"))), \
     patch.object(notify, "send_token_renewal_succeeded",
                  lambda message: success_alerts9.append(message)), \
     patch.object(notify, "send_token_renewal_failed",
                  lambda message: failure_alerts9.append(message)):
    exit_code9 = auth._run_renew_cli()

check("(9) --renew exits 0 on success", exit_code9 == 0, str(exit_code9))
check("(9) success notify fired exactly once", len(success_alerts9) == 1, str(success_alerts9))
check("(9) success message mentions the new expiry",
      "renewed successfully" in success_alerts9[0], success_alerts9)
check("(9) failure notify never fired on a successful renewal",
      failure_alerts9 == [], str(failure_alerts9))
tmp9.unlink()


# ─────────────────────────────────────────────────────────────────────────────
print("\nScenario (10) — --renew CLI failure -> failure notify fires, "
      "NOT the success one\n")
# ─────────────────────────────────────────────────────────────────────────────

tmp10 = Path(tempfile.mkstemp()[1])
seed_token_file(tmp10, "OLD_TOKEN")
success_alerts10 = []
failure_alerts10 = []

with patch.object(auth, "_TOKEN_FILE", str(tmp10)), \
     patch.object(auth, "_CLIENT_ID", "CID123"), \
     patch.object(auth.requests, "get", fake_get_2), \
     patch.object(auth.requests, "post", MagicMock(side_effect=AssertionError("must not use POST"))), \
     patch.object(notify, "send_token_renewal_succeeded",
                  lambda message: success_alerts10.append(message)), \
     patch.object(notify, "send_token_renewal_failed",
                  lambda message: failure_alerts10.append(message)):
    exit_code10 = auth._run_renew_cli()

check("(10) --renew exits 1 on failure", exit_code10 == 1, str(exit_code10))
check("(10) failure notify fired exactly once", len(failure_alerts10) == 1, str(failure_alerts10))
check("(10) success notify never fired on a failed renewal",
      success_alerts10 == [], str(success_alerts10))
tmp10.unlink()


print("\n" + "─" * 60)
if failures:
    print(f"\033[31m{failures} scenario(s) FAILED\033[0m")
    sys.exit(1)
else:
    print("\033[32mAll scenarios PASSED\033[0m")
    sys.exit(0)
