#!/usr/bin/env python3
"""
Garmin session refresher — Mac mini only.

Maintains a valid Garmin Connect session using Camoufox browser automation
and uploads the session cookies + CSRF token to GCS for Cloud Run to consume.
Runs in a loop, refreshing every REFRESH_INTERVAL_MINUTES (default: 10).

Requirements (Mac mini only, not in Docker):
    pip install camoufox playwright google-cloud-storage python-dotenv
    playwright install firefox
    camoufox fetch

.env file (or environment variables):
    GARMIN_USERNAME=your@email.com
    GARMIN_PASSWORD=yourpassword
    GARMIN_TOKENSTORE_GCS=gs://your-bucket/garmin/tokens

Usage:
    python scripts/garmin_session_refresh.py
    # Runs forever, refresh every 10 min. Ctrl+C to stop.
"""

import json
import logging
import os
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

SSO_LOGIN_URL = (
    "https://sso.garmin.com/portal/sso/en-US/sign-in"
    "?clientId=GarminConnect"
    "&service=https%3A%2F%2Fconnect.garmin.com%2Fapp"
)

LOCAL_SESSION_FILE = Path.home() / ".garmin-client" / "camoufox_session.json"


# -----------------------------------------------------------------------------
# Session persistence helpers
# -----------------------------------------------------------------------------

def _load_local_session() -> dict | None:
    """Load local session file if it exists and is less than 23 hours old."""
    if not LOCAL_SESSION_FILE.exists():
        return None
    try:
        data = json.loads(LOCAL_SESSION_FILE.read_text())
        age_h = (time.time() - data.get("saved_at", 0)) / 3600
        if age_h > 23:
            logger.info(f"Local session is {age_h:.1f}h old, will re-login")
            return None
        return data
    except Exception as e:
        logger.debug(f"Could not load local session: {e}")
        return None


def _save_local_session(data: dict) -> None:
    LOCAL_SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
    import os as _os
    fd = _os.open(
        str(LOCAL_SESSION_FILE),
        _os.O_WRONLY | _os.O_CREAT | _os.O_TRUNC,
        0o600,
    )
    with _os.fdopen(fd, "w") as f:
        json.dump(data, f, indent=2)
    logger.info(f"Session saved locally: {LOCAL_SESSION_FILE}")


def _upload_to_gcs(data: dict, gcs_uri: str) -> None:
    from google.cloud import storage

    gcs_path = gcs_uri[len("gs://"):]
    bucket_name, prefix = gcs_path.split("/", 1)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"{prefix.rstrip('/')}/garmin_session.json")
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type="application/json",
    )
    logger.info(
        f"Session uploaded to gs://{bucket_name}/{prefix.rstrip('/')}/garmin_session.json"
        f" ({len(data.get('cookies', []))} cookies)"
    )


# -----------------------------------------------------------------------------
# Browser auth
# -----------------------------------------------------------------------------

def _extract_session(page) -> dict:
    """Extract CSRF token, display name and cookies from the current page."""
    # Ensure we're on /modern/ where the CSRF meta tag lives
    if "/modern/" not in page.url:
        try:
            page.goto(
                "https://connect.garmin.com/modern/",
                wait_until="domcontentloaded",
            )
            time.sleep(3)
        except Exception:
            pass

    setup = page.evaluate("""
        async () => {
            const csrf = document.querySelector(
                'meta[name="csrf-token"], meta[name="_csrf"]'
            )?.content;
            const h = {'connect-csrf-token': csrf || ''};
            const resp = await fetch(
                '/gc-api/userprofile-service/socialProfile',
                {credentials: 'include', headers: h}
            );
            const profile = resp.status === 200 ? await resp.json() : null;
            return {csrf, displayName: profile?.displayName};
        }
    """)

    csrf = setup.get("csrf")
    display_name = setup.get("displayName")

    if not csrf:
        raise RuntimeError("Could not extract CSRF token — session may be invalid")

    cookies = page.context.cookies()
    garmin_cookies = [
        c for c in cookies
        if "garmin" in c.get("domain", "") or "cloudflare" in c.get("domain", "")
    ]

    return {
        "cookies": garmin_cookies,
        "csrf_token": csrf,
        "display_name": display_name,
        "saved_at": time.time(),
    }


def _do_login(page, context, email: str, password: str) -> None:
    """Perform SSO login with credentials."""
    logger.info("Logging in to Garmin Connect...")
    context.clear_cookies()

    for attempt in range(3):
        try:
            page.goto(SSO_LOGIN_URL, wait_until="domcontentloaded")
            break
        except Exception as e:
            logger.debug(f"SSO nav attempt {attempt + 1}: {e}")
            time.sleep(3)

    time.sleep(2)

    try:
        email_input = page.locator('input[name="email"]').first
        email_input.wait_for(timeout=15000)
    except Exception:
        raise RuntimeError(
            f"Login form not found at {page.url}. "
            "Try running once with headless=False to solve any CAPTCHA."
        )

    email_input.click()
    page.keyboard.type(email, delay=30)

    pwd_input = page.locator('input[name="password"]').first
    pwd_input.wait_for(timeout=5000)
    pwd_input.click()
    page.keyboard.type(password, delay=30)

    # Check "Remember Me"
    try:
        page.evaluate("""
            () => {
                const cb = document.querySelector(
                    'input[name="remember"], input[id="remember"]'
                );
                if (cb && !cb.checked) cb.click();
            }
        """)
    except Exception:
        pass

    submit = page.locator('button[type="submit"], button:has-text("Sign In")').first
    submit.click()
    logger.info("Credentials submitted, waiting for redirect...")

    # Poll until redirect to connect.garmin.com
    for _ in range(120):
        time.sleep(1)
        url = page.url
        if "connect.garmin.com" in url and "sso.garmin.com" not in url:
            logger.info(f"Login successful: {url}")
            return

    raise RuntimeError(f"Login timeout — still on: {page.url}")


def refresh_session(email: str, password: str, gcs_uri: str) -> None:
    """
    Launch Camoufox, restore or create a Garmin session, upload to GCS.
    """
    from camoufox.sync_api import Camoufox

    existing = _load_local_session()
    has_session = existing is not None

    with Camoufox(headless=True) as browser:
        page = browser.new_page()
        context = page.context

        # Load existing cookies into browser
        if has_session and existing.get("cookies"):
            context.add_cookies(existing["cookies"])
            logger.info(f"Loaded {len(existing['cookies'])} existing cookies")

        # Try navigating to the app
        try:
            page.goto(
                "https://connect.garmin.com/modern/",
                wait_until="domcontentloaded",
            )
            time.sleep(3)
        except Exception as e:
            logger.debug(f"Initial navigation error: {e}")

        url = page.url
        need_login = "sso.garmin.com" in url or "sign-in" in url.lower()

        if not need_login:
            # Quick CSRF check to confirm session is still valid
            csrf = page.evaluate("""
                () => document.querySelector(
                    'meta[name="csrf-token"], meta[name="_csrf"]'
                )?.content
            """)
            if not csrf:
                logger.info("Session expired (no CSRF), re-logging in")
                need_login = True
            else:
                logger.info("Session still valid, refreshing...")

        if need_login:
            _do_login(page, context, email, password)
            time.sleep(3)

        session_data = _extract_session(page)

    _save_local_session(session_data)

    if gcs_uri:
        _upload_to_gcs(session_data, gcs_uri)
    else:
        logger.warning("GARMIN_TOKENSTORE_GCS not set — session not uploaded to GCS")


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------

REFRESH_INTERVAL_MINUTES = 10


def main() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    email = os.environ.get("GARMIN_USERNAME")
    password = os.environ.get("GARMIN_PASSWORD")
    gcs_uri = os.environ.get("GARMIN_TOKENSTORE_GCS", "")

    if not email or not password:
        logger.error("GARMIN_USERNAME and GARMIN_PASSWORD must be set")
        sys.exit(1)

    logger.info(f"Starting Garmin session daemon (refresh every {REFRESH_INTERVAL_MINUTES} min)")

    while True:
        try:
            refresh_session(email, password, gcs_uri)
        except Exception as e:
            logger.error(f"Session refresh failed: {e}")

        logger.info(f"Next refresh in {REFRESH_INTERVAL_MINUTES} min...")
        time.sleep(REFRESH_INTERVAL_MINUTES * 60)


if __name__ == "__main__":
    main()
