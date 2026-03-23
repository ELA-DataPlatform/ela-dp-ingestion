"""
Refresh Garmin OAuth tokens and upload them to GCS.

Usage:
    python scripts/refresh_garmin_tokens.py --env dev
    python scripts/refresh_garmin_tokens.py --env prd

Required env vars: GARMIN_USERNAME, GARMIN_PASSWORD
"""

import argparse
import logging
import os
import sys
import tempfile

from google.cloud import storage
from garminconnect import Garmin

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

BUCKET_PATTERN = "ela-source-{env}"
TOKEN_GCS_PREFIX = "garmin/tokens"
TOKEN_FILES = ["oauth1_token.json", "oauth2_token.json"]


def download_tokens(bucket_name: str, local_dir: str) -> bool:
    """Download garth token files from GCS into a local directory."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    found = False
    for filename in TOKEN_FILES:
        blob = bucket.blob(f"{TOKEN_GCS_PREFIX}/{filename}")
        if blob.exists():
            blob.download_to_filename(os.path.join(local_dir, filename))
            logger.info("Downloaded gs://%s/%s/%s", bucket_name, TOKEN_GCS_PREFIX, filename)
            found = True
        else:
            logger.warning("Not found: gs://%s/%s/%s", bucket_name, TOKEN_GCS_PREFIX, filename)
    return found


def upload_tokens(bucket_name: str, local_dir: str) -> None:
    """Upload garth token files from a local directory to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    for filename in TOKEN_FILES:
        local_path = os.path.join(local_dir, filename)
        if os.path.exists(local_path):
            blob = bucket.blob(f"{TOKEN_GCS_PREFIX}/{filename}")
            blob.upload_from_filename(local_path)
            logger.info("Uploaded gs://%s/%s/%s", bucket_name, TOKEN_GCS_PREFIX, filename)


def refresh(env: str) -> None:
    """Refresh Garmin tokens for the given environment."""
    username = os.environ.get("GARMIN_USERNAME")
    password = os.environ.get("GARMIN_PASSWORD")
    if not username or not password:
        logger.error("GARMIN_USERNAME and GARMIN_PASSWORD must be set")
        sys.exit(1)

    bucket_name = BUCKET_PATTERN.format(env=env)

    with tempfile.TemporaryDirectory() as token_dir:
        garmin = Garmin(username, password)

        # Try cached tokens first
        if download_tokens(bucket_name, token_dir):
            try:
                garmin.login(tokenstore=token_dir)
                logger.info("Authenticated via cached tokens")
                garmin.garth.dump(token_dir)
                upload_tokens(bucket_name, token_dir)
                return
            except Exception as e:
                logger.warning("Cached tokens invalid, falling back to login: %s", e)

        # Full login with credentials
        try:
            garmin.login()
            logger.info("Authenticated via username/password")
            garmin.garth.dump(token_dir)
            upload_tokens(bucket_name, token_dir)
        except Exception as e:
            logger.error("Authentication failed: %s", e)
            sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Refresh Garmin OAuth tokens")
    parser.add_argument("--env", required=True, choices=["dev", "prd"], help="Target environment")
    args = parser.parse_args()
    refresh(args.env)
