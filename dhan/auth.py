"""
Dhan (DhanHQ API v2) authentication.

Unlike Kite Connect, Dhan has no request_token/OAuth handshake for a single
personal account -- you generate an access token by hand at
https://web.dhan.co (Profile -> "Access DhanHQ Trading APIs"), valid 24h.
There's nothing for this script to automate about that step; it only saves
the token you paste it and reuses it for the rest of the day.

Usage from any script:
    from dhan.auth import get_session

    session, client_id = get_session()
    resp = session.get(f"{BASE_URL}/fundlimit")

Run directly to save a freshly generated token:
    python -m dhan.auth <ACCESS_TOKEN>

Token lifecycle (per DhanHQ docs):
    Access tokens are valid 24h from generation. Paste a new one each
    morning before trading -- every other script reuses the saved token
    for the rest of the day.
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

_ENV_FILE = Path(__file__).resolve().parent.parent / "pipeline" / ".env"
load_dotenv(_ENV_FILE)

_BASE_URL   = "https://api.dhan.co/v2"
_TOKEN_FILE = os.path.join(os.path.dirname(__file__), ".token.json")
_IST        = ZoneInfo("Asia/Kolkata")

_CLIENT_ID = (os.getenv("DHAN_CLIENT_ID") or "").strip()


# ── Token file helpers ─────────────────────────────────────────────────────────

def _save_token(access_token: str) -> dict:
    issued  = datetime.now(_IST)
    expires = issued + timedelta(hours=24)
    payload = {
        "access_token": access_token,
        "issued_at":    issued.isoformat(),
        "expires_at":   expires.isoformat(),
    }
    with open(_TOKEN_FILE, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"[dhan] Token saved → {_TOKEN_FILE}  (expires {expires.strftime('%Y-%m-%d %H:%M %Z')})")
    return payload


def _load_valid_token() -> str | None:
    """Returns access_token if the saved token is still valid, else None."""
    if not os.path.exists(_TOKEN_FILE):
        print("[dhan] No saved token found — run `python -m dhan.auth <ACCESS_TOKEN>`.")
        return None
    try:
        with open(_TOKEN_FILE) as f:
            data = json.load(f)
        expires_at = datetime.fromisoformat(data["expires_at"])
    except Exception as exc:
        print(f"[dhan] Could not read token file ({exc}) — new token required.")
        return None

    if datetime.now(_IST) >= expires_at:
        print(f"[dhan] Token expired at {expires_at.strftime('%Y-%m-%d %H:%M %Z')} — "
              "generate a new one at web.dhan.co and run `python -m dhan.auth <ACCESS_TOKEN>`.")
        return None

    issued = data.get("issued_at", "")[:16]
    print(f"[dhan] Reusing valid token (issued {issued}, expires {expires_at.strftime('%H:%M %Z')})")
    return data["access_token"]


# ── Public API ─────────────────────────────────────────────────────────────────

class DhanAuthError(RuntimeError):
    """Raised when there's no valid token and no way to get one automatically."""


def get_session() -> tuple[requests.Session, str]:
    """
    Returns (session, client_id).

    - Reuses today's saved token if not expired.
    - Raises DhanAuthError if no valid token exists (there's no browser flow
      to fall back to -- unlike Kite, Dhan's direct-token model requires a
      human to paste a freshly generated token; see module docstring).

    Usage:
        session, client_id = get_session()
        resp = session.get(f"{BASE_URL}/fundlimit")
    """
    if not _CLIENT_ID:
        raise EnvironmentError("[dhan] DHAN_CLIENT_ID is not set in pipeline/.env")

    access_token = _load_valid_token()
    if access_token is None:
        raise DhanAuthError(
            "[dhan] No valid Dhan token. Generate one at web.dhan.co "
            "(Profile → Access DhanHQ Trading APIs) and run:\n"
            "  python -m dhan.auth <ACCESS_TOKEN>"
        )

    session = requests.Session()
    session.headers.update({
        "access-token": access_token,
        "dhanClientId": _CLIENT_ID,
        "Content-Type": "application/json",
        "Accept":       "application/json",
    })
    return session, _CLIENT_ID


BASE_URL = _BASE_URL


# ── CLI entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"[dhan] BASE_URL = {_BASE_URL}")
    print()

    if len(sys.argv) < 2:
        print("Usage: python -m dhan.auth <ACCESS_TOKEN>", file=sys.stderr)
        print("Generate a token at web.dhan.co → Profile → Access DhanHQ Trading APIs", file=sys.stderr)
        sys.exit(1)

    if not _CLIENT_ID:
        print("Error: DHAN_CLIENT_ID is not set in pipeline/.env", file=sys.stderr)
        sys.exit(1)

    access_token = sys.argv[1].strip()
    _save_token(access_token)

    session = requests.Session()
    session.headers.update({
        "access-token": access_token,
        "dhanClientId": _CLIENT_ID,
        "Content-Type": "application/json",
        "Accept":       "application/json",
    })

    print("\n[dhan] Verifying via fund limit endpoint...")
    resp = session.get(f"{_BASE_URL}/fundlimit", timeout=10)

    if resp.ok:
        data = resp.json()
        print("[dhan] Login verified.")
        print(f"  Client ID          : {data.get('dhanClientId', _CLIENT_ID)}")
        print(f"  Available balance   : ₹{data.get('availabelBalance', 'n/a')}")
        print(f"  SOD limit           : ₹{data.get('sodLimit', 'n/a')}")
        print(f"  Utilized amount     : ₹{data.get('utilizedAmount', 'n/a')}")
    else:
        print(f"[dhan] Fund limit check failed: HTTP {resp.status_code}", file=sys.stderr)
        print(resp.text, file=sys.stderr)
        print(
            "\n[dhan] If this is a 401/403, the header name may need adjusting "
            "(dhanClientId vs client-id) -- check the response body above.",
            file=sys.stderr,
        )
        sys.exit(1)
