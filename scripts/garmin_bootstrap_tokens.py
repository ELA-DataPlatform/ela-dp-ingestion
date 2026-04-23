#!/usr/bin/env python3
"""
garmin_bootstrap_tokens.py — run once to create initial Garmin OAuth tokens in GCS.

Uses the garminconnect mobile SSO flow (no browser required) to authenticate
and stores the resulting DI OAuth tokens in GCS. Cloud Run jobs will load these
tokens autonomously and auto-refresh them on each run.

Re-run this script if:
  - First-time setup
  - Tokens are revoked by Garmin (rare)
  - Authentication errors appear in Cloud Run logs

Usage:
    python scripts/garmin_bootstrap_tokens.py

Requires .env with:
    GARMIN_USERNAME=your@email.com
    GARMIN_PASSWORD=yourpassword
    GARMIN_TOKENSTORE_GCS=gs://your-bucket/garmin/tokens
"""

import logging
import os
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

TMP_DIR = Path("/tmp/garmin_bootstrap")
TOKEN_FILE = "garmin_tokens.json"


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
        logger.error("GARMIN_USERNAME and GARMIN_PASSWORD must be set in .env")
        sys.exit(1)

    if not gcs_uri:
        logger.error("GARMIN_TOKENSTORE_GCS must be set in .env")
        sys.exit(1)

    from garminconnect import Garmin, GarminConnectAuthenticationError, GarminConnectConnectionError

    logger.info(f"Logging in as {email} via mobile SSO...")
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    try:
        garmin = Garmin(email=email, password=password)
        garmin.login(str(TMP_DIR))
    except GarminConnectAuthenticationError as e:
        logger.error(f"Authentication failed: {e}")
        sys.exit(1)
    except GarminConnectConnectionError as e:
        logger.error(f"Connection error: {e}")
        sys.exit(1)

    logger.info(f"Logged in as {garmin.display_name}")

    token_file = TMP_DIR / TOKEN_FILE
    if not token_file.exists():
        logger.error("Token file was not created after login — unexpected error")
        sys.exit(1)

    token_json = garmin.client.dumps()

    gcs_path = gcs_uri[len("gs://"):]
    bucket_name, prefix = gcs_path.split("/", 1)

    from google.cloud import storage
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(f"{prefix.rstrip('/')}/{TOKEN_FILE}")
    blob.upload_from_string(token_json, content_type="application/json")

    logger.info(f"Tokens uploaded to {gcs_uri}/{TOKEN_FILE}")
    logger.info("Bootstrap complete. Cloud Run jobs can now authenticate autonomously.")

    token_file.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
