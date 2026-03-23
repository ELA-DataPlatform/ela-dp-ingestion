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
TOKEN_BLOB = "garmin/tokens/garmin_tokens.json"


def download_token(bucket_name: str, local_path: str) -> bool:
    """Download garmin_tokens.json from GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(TOKEN_BLOB)
    if not blob.exists():
        logger.warning("No tokens found in gs://%s/%s", bucket_name, TOKEN_BLOB)
        return False
    blob.download_to_filename(local_path)
    logger.info("Downloaded tokens from gs://%s/%s", bucket_name, TOKEN_BLOB)
    return True


def upload_token(bucket_name: str, local_path: str) -> None:
    """Upload garmin_tokens.json to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(TOKEN_BLOB)
    blob.upload_from_filename(local_path)
    logger.info("Uploaded tokens to gs://%s/%s", bucket_name, TOKEN_BLOB)


def refresh(env: str) -> None:
    """Refresh Garmin tokens for the given environment."""
    username = os.environ.get("GARMIN_USERNAME")
    password = os.environ.get("GARMIN_PASSWORD")
    if not username or not password:
        logger.error("GARMIN_USERNAME and GARMIN_PASSWORD must be set")
        sys.exit(1)

    bucket_name = BUCKET_PATTERN.format(env=env)

    with tempfile.TemporaryDirectory() as token_dir:
        token_path = os.path.join(token_dir, "garmin_tokens.json")
        garmin = Garmin(username, password)

        # Try cached tokens first
        if download_token(bucket_name, token_path):
            try:
                garmin.login(tokenstore=token_path)
                logger.info("Authenticated via cached tokens")
                garmin.client.dump(token_path)
                upload_token(bucket_name, token_path)
                return
            except Exception as e:
                logger.warning("Cached tokens invalid, falling back to login: %s", e)

        # Full login with credentials
        try:
            garmin.login()
            logger.info("Authenticated via username/password")
            garmin.client.dump(token_path)
            upload_token(bucket_name, token_path)
        except Exception as e:
            logger.error("Authentication failed: %s", e)
            sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Refresh Garmin OAuth tokens")
    parser.add_argument("--env", required=True, choices=["dev", "prd"], help="Target environment")
    args = parser.parse_args()
    refresh(args.env)
