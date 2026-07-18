"""
Upstox API authentication — supports sandbox and live modes.

Usage:
    from upstox.auth import get_session

    session, base_url = get_session()           # defaults to UPSTOX_MODE in .env
    session, base_url = get_session("sandbox")  # force sandbox
    session, base_url = get_session("live")     # force live

Run directly to verify auth or trigger OAuth login for live:
    python -m upstox.auth           # check auth using UPSTOX_MODE from .env
    python -m upstox.auth live      # OAuth login for live mode
    python -m upstox.auth sandbox   # verify sandbox token
    python -m upstox.auth live "http://127.0.0.1/?code=..." # non-interactive live login

.env keys required:
    Live:    UPSTOX_LIVE_API_KEY, UPSTOX_LIVE_API_SECRET, UPSTOX_LIVE_REDIRECT_URI
    Sandbox: UPSTOX_SANDBOX_ACCESS_TOKEN  (static token from developer portal)
    Default: UPSTOX_MODE=live

Token lifecycle:
    Live tokens expire at midnight IST — run `python -m upstox.auth live` each morning.
    Sandbox token is long-lived (~30 days) — update UPSTOX_SANDBOX_ACCESS_TOKEN when it expires.
"""

import json
import os
import sys
import webbrowser
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

_ENV_FILE = Path(__file__).resolve().parent.parent / "pipeline" / ".env"
load_dotenv(_ENV_FILE)

_TOKEN_DIR  = Path(__file__).resolve().parent
_IST        = ZoneInfo("Asia/Kolkata")

LIVE_BASE_URL    = "https://api.upstox.com/v2"
SANDBOX_BASE_URL = "https://api-sandbox.upstox.com/v2"

_SANDBOX_STATIC_TOKEN = (os.getenv("UPSTOX_SANDBOX_ACCESS_TOKEN") or "").strip()

_CREDS = {
    "live": {
        "api_key":      (os.getenv("UPSTOX_LIVE_API_KEY")      or "").strip(),
        "api_secret":   (os.getenv("UPSTOX_LIVE_API_SECRET")   or "").strip(),
        "redirect_uri": (os.getenv("UPSTOX_LIVE_REDIRECT_URI") or "http://127.0.0.1/").strip(),
        "base_url":     LIVE_BASE_URL,
        "token_file":   _TOKEN_DIR / ".token_live.json",
    },
    "sandbox": {
        "base_url":   SANDBOX_BASE_URL,
    },
}

_DEFAULT_MODE = (os.getenv("UPSTOX_MODE") or "live").strip().lower()


# ── Token persistence ──────────────────────────────────────────────────────────

def _token_file(mode: str) -> Path:
    return _CREDS[mode]["token_file"]


def _save_token(mode: str, access_token: str):
    now_ist = datetime.now(_IST)
    # Upstox trading tokens expire at midnight IST
    midnight_ist = (now_ist + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    payload = {
        "access_token": access_token,
        "mode":         mode,
        "issued_at":    now_ist.isoformat(),
        "expires_at":   midnight_ist.isoformat(),
    }
    _token_file(mode).write_text(json.dumps(payload, indent=2))
    print(f"[upstox/{mode}] Token saved — expires at midnight IST.")


def _load_valid_token(mode: str) -> str | None:
    tf = _token_file(mode)
    if not tf.exists():
        return None
    try:
        data = json.loads(tf.read_text())
        expires_at = datetime.fromisoformat(data["expires_at"])
        if datetime.now(_IST) < expires_at:
            return data["access_token"]
        print(f"[upstox/{mode}] Saved token expired.")
    except Exception:
        pass
    return None


# ── OAuth flow ─────────────────────────────────────────────────────────────────

def _exchange_code(mode: str, auth_code: str) -> str:
    creds = _CREDS[mode]
    payload = {
        "code":          auth_code,
        "client_id":     creds["api_key"],
        "client_secret": creds["api_secret"],
        "redirect_uri":  creds["redirect_uri"],
        "grant_type":    "authorization_code",
    }
    resp = requests.post(
        f"{creds['base_url']}/login/authorization/token",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )
    body = resp.json()
    if not resp.ok or "access_token" not in body:
        raise RuntimeError(
            f"[upstox/{mode}] Token exchange failed: {body}"
        )
    return body["access_token"]


def _extract_code_from_url(url: str) -> str | None:
    try:
        qs = parse_qs(urlparse(url).query)
        codes = qs.get("code", [])
        return codes[0] if codes else None
    except Exception:
        return None


def _login(mode: str) -> str:
    creds = _CREDS[mode]
    if not creds["api_key"]:
        key = "UPSTOX_LIVE_API_KEY" if mode == "live" else "UPSTOX_SANDBOX_API_KEY"
        raise EnvironmentError(f"[upstox/{mode}] {key} is not set in pipeline/.env")
    if not creds["api_secret"]:
        key = "UPSTOX_LIVE_API_SECRET" if mode == "live" else "UPSTOX_SANDBOX_API_SECRET"
        raise EnvironmentError(f"[upstox/{mode}] {key} is not set in pipeline/.env")

    login_url = (
        f"{creds['base_url']}/login/authorization/dialog"
        f"?response_type=code&client_id={creds['api_key']}"
        f"&redirect_uri={quote(creds['redirect_uri'], safe='')}"
    )

    print(f"\n[upstox/{mode}] Open this URL in your browser:")
    print(f"  {login_url}\n")
    webbrowser.open(login_url)

    print(f"[upstox/{mode}] After logging in, copy the redirect URL and paste it here.")
    redirect_url = input("Redirect URL: ").strip()
    code = _extract_code_from_url(redirect_url)
    if not code:
        raise ValueError(f"[upstox/{mode}] Could not extract 'code' from: {redirect_url!r}")

    print(f"[upstox/{mode}] Exchanging code for access token ...")
    access_token = _exchange_code(mode, code)
    _save_token(mode, access_token)
    return access_token


# ── Public API ─────────────────────────────────────────────────────────────────

def get_session(mode: str | None = None) -> tuple[requests.Session, str]:
    """
    Returns (session, base_url) for the given mode (live/sandbox).
    Reuses saved token if valid; prompts for OAuth login otherwise.
    """
    if mode is None:
        mode = _DEFAULT_MODE
    if mode not in ("live", "sandbox"):
        raise ValueError(f"mode must be 'live' or 'sandbox', got: {mode!r}")

    if mode == "sandbox":
        # Sandbox uses a static access token from the Upstox developer portal
        if not _SANDBOX_STATIC_TOKEN:
            raise EnvironmentError(
                "[upstox/sandbox] UPSTOX_SANDBOX_ACCESS_TOKEN is not set in pipeline/.env\n"
                "Get it from: https://upstox.com/developer/api-documentation/sandbox/"
            )
        token = _SANDBOX_STATIC_TOKEN
    else:
        token = _load_valid_token(mode)
        if not token:
            print(f"[upstox/{mode}] No valid token — running login flow.")
            token = _login(mode)

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    })
    return session, _CREDS[mode]["base_url"]


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = sys.argv[1:]

    # Direct URL paste: python -m upstox.auth sandbox "https://...?code=..."
    mode = _DEFAULT_MODE
    url_arg = None
    for a in args:
        if a in ("live", "sandbox"):
            mode = a
        elif a.startswith("http"):
            url_arg = a

    if url_arg:
        if mode == "sandbox":
            print("[upstox/sandbox] Sandbox uses a static token — no URL paste needed.")
            sys.exit(0)
        creds = _CREDS[mode]
        code = _extract_code_from_url(url_arg)
        if not code:
            print(f"[upstox/{mode}] No 'code' found in URL: {url_arg!r}")
            sys.exit(1)
        try:
            token = _exchange_code(mode, code)
            _save_token(mode, token)
            print(f"[upstox/{mode}] Login complete.")
        except RuntimeError as e:
            print(e)
            sys.exit(1)
    else:
        try:
            session, base_url = get_session(mode)
            resp = session.get(f"{base_url}/user/profile", timeout=10)
            body = resp.json()
            if resp.ok:
                d = body.get("data", {})
                print(f"[upstox/{mode}] Authenticated as: {d.get('name')} ({d.get('email')})")
            else:
                print(f"[upstox/{mode}] Auth check failed: {body}")
                sys.exit(1)
        except (EnvironmentError, RuntimeError, ValueError) as e:
            print(e)
            sys.exit(1)
