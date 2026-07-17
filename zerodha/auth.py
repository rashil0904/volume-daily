"""
Zerodha Kite Connect authentication.

Usage from any script:
    from zerodha.auth import get_session

    session, api_key = get_session()
    resp = session.get("https://api.kite.trade/user/profile")

Run directly to do the one-time daily login:
    python -m zerodha.auth

Token lifecycle (per Kite Connect docs):
    Access tokens expire at 6:00 AM IST the following day (regulatory requirement).
    Run this script once each morning before trading — every other script reuses
    the saved token for the rest of the day with no browser prompt.
"""

import hashlib
import json
import os
import sys
import threading
import webbrowser
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

load_dotenv()

_BASE_URL    = "https://api.kite.trade"
_LOGIN_URL   = "https://kite.zerodha.com/connect/login"
_TOKEN_FILE  = os.path.join(os.path.dirname(__file__), ".token.json")
_IST         = ZoneInfo("Asia/Kolkata")

_API_KEY    = (os.getenv("ZERODHA_API_KEY") or "").strip()
_API_SECRET = (os.getenv("ZERODHA_API_SECRET") or "").strip()
_REDIRECT   = (os.getenv("ZERODHA_REDIRECT_URI") or "http://localhost:5005/callback").strip()


# ── Token file helpers ─────────────────────────────────────────────────────────

def _token_expiry() -> datetime:
    """Tokens expire at 6:00 AM IST the following day (Kite Connect docs)."""
    now    = datetime.now(_IST)
    expiry = now.replace(hour=6, minute=0, second=0, microsecond=0)
    if now >= expiry:
        expiry += timedelta(days=1)
    return expiry


def _save_token(access_token: str, user_id: str) -> None:
    expiry = _token_expiry()
    payload = {
        "access_token": access_token,
        "user_id":      user_id,
        "issued_at":    datetime.now(_IST).isoformat(),
        "expires_at":   expiry.isoformat(),
    }
    with open(_TOKEN_FILE, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"[zerodha] Token saved → {_TOKEN_FILE}  (expires {expiry.strftime('%Y-%m-%d %H:%M %Z')})")


def _load_valid_token() -> tuple[str, str] | None:
    """Returns (access_token, user_id) if saved token is still valid, else None."""
    if not os.path.exists(_TOKEN_FILE):
        print("[zerodha] No saved token found — login required.")
        return None
    try:
        with open(_TOKEN_FILE) as f:
            data = json.load(f)
        expires_at = datetime.fromisoformat(data["expires_at"])
    except Exception as exc:
        print(f"[zerodha] Could not read token file ({exc}) — login required.")
        return None

    if datetime.now(_IST) >= expires_at:
        print(f"[zerodha] Token expired at {expires_at.strftime('%Y-%m-%d %H:%M %Z')} — login required.")
        return None

    issued = data.get("issued_at", "")[:16]
    print(f"[zerodha] Reusing valid token (issued {issued}, expires {expires_at.strftime('%H:%M %Z')})")
    return data["access_token"], data.get("user_id", "")


# ── OAuth callback server (localhost redirect) ─────────────────────────────────

def _capture_callback_local(port: int, timeout: int = 120) -> dict:
    captured: dict = {}
    done = threading.Event()

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            params = parse_qs(urlparse(self.path).query)
            captured.update({k: v[0] for k, v in params.items()})
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"<h2>Zerodha login successful.</h2>"
                b"<p>You may close this tab and return to the terminal.</p>"
            )
            done.set()

        def log_message(self, *args):
            pass

    server = HTTPServer(("localhost", port), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    if not done.wait(timeout=timeout):
        server.shutdown()
        raise RuntimeError(
            f"[zerodha] Login callback not received within {timeout}s. "
            "Ensure your browser completed the login and the redirect URI matches."
        )
    server.shutdown()
    return captured


def _capture_callback_manual() -> dict:
    """For external redirect URIs (e.g. https://kite.trade/): prompt for pasted URL."""
    print("\n[zerodha] After login, your browser will redirect to a URL like:")
    print("  https://kite.trade/?request_token=XXXXX&action=login&status=success")
    print("\nCopy the full URL from your browser's address bar and paste it here:")
    url = input("> ").strip()
    params = {k: v[0] for k, v in parse_qs(urlparse(url).query).items()}
    if not params:
        raise RuntimeError("[zerodha] Could not parse URL — paste the full redirect URL.")
    return params


def _is_localhost_redirect(redirect: str) -> bool:
    host = urlparse(redirect).hostname or ""
    return host in ("localhost", "127.0.0.1")


# ── Login flow ─────────────────────────────────────────────────────────────────

def _login() -> tuple[str, str]:
    if not _API_KEY:
        raise EnvironmentError("[zerodha] ZERODHA_API_KEY is not set in .env")
    if not _API_SECRET:
        raise EnvironmentError("[zerodha] ZERODHA_API_SECRET is not set in .env")

    login_url = f"{_LOGIN_URL}?v=3&api_key={_API_KEY}"
    print(f"[zerodha] Opening browser for Kite login...")
    print(f"[zerodha] If browser doesn't open, visit:\n  {login_url}")
    webbrowser.open(login_url)

    if _is_localhost_redirect(_REDIRECT):
        port = urlparse(_REDIRECT).port or 5005
        print(f"[zerodha] Waiting for callback on port {port} (timeout 120s)...")
        params = _capture_callback_local(port=port)
    else:
        params = _capture_callback_manual()

    if params.get("status") == "error" or "error_message" in params:
        raise RuntimeError(f"[zerodha] Login failed: {params.get('error_message', params)}")
    if params.get("status") != "success":
        raise RuntimeError(f"[zerodha] Unexpected callback params: {params}")

    request_token = params.get("request_token")
    if not request_token:
        raise RuntimeError(f"[zerodha] No request_token in callback: {params}")

    print("[zerodha] Request token received — exchanging for access token...")

    # Checksum = SHA-256(api_key + request_token + api_secret)
    checksum = hashlib.sha256(
        (_API_KEY + request_token + _API_SECRET).encode()
    ).hexdigest()

    resp = requests.post(
        f"{_BASE_URL}/session/token",
        data={
            "api_key":       _API_KEY,
            "request_token": request_token,
            "checksum":      checksum,
        },
        headers={"X-Kite-Version": "3"},
        timeout=30,
    )

    if not resp.ok:
        raise RuntimeError(
            f"[zerodha] Token exchange failed: HTTP {resp.status_code}\n{resp.text}"
        )

    data         = resp.json().get("data", resp.json())
    access_token = data.get("access_token")
    user_id      = data.get("user_id", "")

    if not access_token:
        raise RuntimeError(f"[zerodha] No access_token in response: {data}")

    _save_token(access_token, user_id)
    return access_token, user_id


# ── Session with 401 handling ──────────────────────────────────────────────────

class _KiteSession(requests.Session):
    """Session that intercepts 401s and auto-retries once with a fresh login."""

    def request(self, method, url, **kwargs):
        resp = super().request(method, url, **kwargs)
        if resp.status_code != 401:
            return resp

        print("[zerodha] 401 Unauthorized — token rejected. Re-authenticating...")
        try:
            os.remove(_TOKEN_FILE)
        except FileNotFoundError:
            pass

        try:
            access_token, _ = _login()
        except Exception as exc:
            raise RuntimeError(
                f"[zerodha] 401 received and re-authentication failed: {exc}\n"
                "Re-run `python zerodha_auth.py` manually."
            ) from exc

        self.headers.update({"Authorization": f"token {_API_KEY}:{access_token}"})
        retry = super().request(method, url, **kwargs)
        if retry.status_code == 401:
            raise RuntimeError(
                "[zerodha] Still 401 after fresh login — check your API key and secret."
            )
        return retry


# ── Public API ─────────────────────────────────────────────────────────────────

def get_session() -> tuple[_KiteSession, str]:
    """
    Returns (session, api_key).

    - Reuses today's saved token if not expired (no browser, no prompts).
    - Runs the Kite OAuth browser flow if no valid token exists.
    - Auto-retries once on 401.

    Usage:
        session, api_key = get_session()
        resp = session.get(f"{BASE_URL}/orders")
    """
    result = _load_valid_token()

    if result is None:
        print("[zerodha] No valid token — running login flow.")
        access_token, _ = _login()
    else:
        access_token, _ = result

    session = _KiteSession()
    session.headers.update({
        "X-Kite-Version": "3",
        "Authorization":  f"token {_API_KEY}:{access_token}",
    })
    return session, _API_KEY


BASE_URL = _BASE_URL


# ── CLI entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"[zerodha] BASE_URL = {_BASE_URL}")
    print()

    # Allow passing the redirect URL directly:
    #   python zerodha_auth.py "https://kite.trade/?request_token=XXX&status=success"
    if len(sys.argv) > 1:
        redirect_url = sys.argv[1].strip()
        params = {k: v[0] for k, v in parse_qs(urlparse(redirect_url).query).items()}
        if not params.get("request_token"):
            print("Error: no request_token found in the provided URL.", file=sys.stderr)
            sys.exit(1)
        try:
            request_token = params["request_token"]
            checksum = hashlib.sha256((_API_KEY + request_token + _API_SECRET).encode()).hexdigest()
            resp = requests.post(
                f"{_BASE_URL}/session/token",
                data={"api_key": _API_KEY, "request_token": request_token, "checksum": checksum},
                headers={"X-Kite-Version": "3"},
                timeout=30,
            )
            if not resp.ok:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
            data = resp.json().get("data", resp.json())
            access_token = data.get("access_token")
            if not access_token:
                raise RuntimeError(f"No access_token in response: {data}")
            _save_token(access_token, data.get("user_id", ""))
            session = _KiteSession()
            session.headers.update({"X-Kite-Version": "3",
                                    "Authorization": f"token {_API_KEY}:{access_token}"})
        except Exception as exc:
            print(f"\nError: {exc}", file=sys.stderr)
            sys.exit(1)
    else:
        try:
            session, _ = get_session()
        except (EnvironmentError, RuntimeError) as exc:
            print(f"\n{exc}", file=sys.stderr)
            sys.exit(1)

    print("\n[zerodha] Verifying via profile endpoint...")
    resp = session.get(f"{_BASE_URL}/user/profile", timeout=10)

    if resp.ok:
        profile = resp.json().get("data", {})
        print("[zerodha] Login verified.")
        print(f"  Name   : {profile.get('user_name', 'n/a')}")
        print(f"  Email  : {profile.get('email', 'n/a')}")
        print(f"  User ID: {profile.get('user_id', 'n/a')}")
        print(f"  Broker : {profile.get('broker', 'n/a')}")
    else:
        print(f"[zerodha] Profile check failed: HTTP {resp.status_code}", file=sys.stderr)
        print(resp.text, file=sys.stderr)
        sys.exit(1)
