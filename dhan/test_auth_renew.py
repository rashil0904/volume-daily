#!/usr/bin/env python3
"""
test_auth_renew.py -- standalone verifier for dhan/auth.py's
renew_access_token() (automated daily Dhan token renewal).

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


print("\n" + "─" * 60)
if failures:
    print(f"\033[31m{failures} scenario(s) FAILED\033[0m")
    sys.exit(1)
else:
    print("\033[32mAll scenarios PASSED\033[0m")
    sys.exit(0)
