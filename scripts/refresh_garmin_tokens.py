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
TOKEN_PREFIX = "garmin/tokens/"


def download_tokens(bucket_name: str, local_dir: str) -> bool:
    """Download garth token files from GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=TOKEN_PREFIX))
    if not blobs:
        logger.warning("No tokens found in gs://%s/%s", bucket_name, TOKEN_PREFIX)
        return False
    for blob in blobs:
        filename = blob.name[len(TOKEN_PREFIX):]
        if not filename:
            continue
        local_path = os.path.join(local_dir, filename)
        blob.download_to_filename(local_path)
    logger.info("Downloaded %d token files from gs://%s/%s", len(blobs), bucket_name, TOKEN_PREFIX)
    return True


def upload_tokens(bucket_name: str, local_dir: str) -> None:
    """Upload garth token files to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    count = 0
    for f in os.listdir(local_dir):
        filepath = os.path.join(local_dir, f)
        if os.path.isfile(filepath):
            blob = bucket.blob(TOKEN_PREFIX + f)
            blob.upload_from_filename(filepath)
            count += 1
    logger.info("Uploaded %d token files to gs://%s/%s", count, bucket_name, TOKEN_PREFIX)


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

        # Full SSO login
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
