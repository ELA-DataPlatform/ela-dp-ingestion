"""
Refresh Garmin tokens and upload them to GCS.

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

from garminconnect import Garmin

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

BUCKET_PATTERN = "ela-source-{env}"
TOKEN_GCS_PREFIX = "garmin/tokens"
TOKEN_FILE = "garmin_tokens.json"


def download_tokens(bucket_name: str, local_dir: str) -> bool:
    """Download garmin_tokens.json from GCS into a local directory."""
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"{TOKEN_GCS_PREFIX}/{TOKEN_FILE}")
    if blob.exists():
        blob.download_to_filename(os.path.join(local_dir, TOKEN_FILE))
        logger.info("Downloaded gs://%s/%s/%s", bucket_name, TOKEN_GCS_PREFIX, TOKEN_FILE)
        return True
    logger.warning("Not found: gs://%s/%s/%s", bucket_name, TOKEN_GCS_PREFIX, TOKEN_FILE)
    return False


def upload_tokens(bucket_name: str, local_dir: str) -> None:
    """Upload garmin_tokens.json from a local directory to GCS."""
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    local_path = os.path.join(local_dir, TOKEN_FILE)
    if os.path.exists(local_path):
        blob = bucket.blob(f"{TOKEN_GCS_PREFIX}/{TOKEN_FILE}")
        blob.upload_from_filename(local_path)
        logger.info("Uploaded gs://%s/%s/%s", bucket_name, TOKEN_GCS_PREFIX, TOKEN_FILE)


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
                # Set browser headers — the library skips this when loading
                # from tokenstore, causing Cloudflare to reject requests.
                garmin.client.cs.headers.update({
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                })
                garmin.login(tokenstore=token_dir)
                logger.info("Authenticated via cached tokens")
                garmin.client.dump(token_dir)
                upload_tokens(bucket_name, token_dir)
                return
            except Exception as e:
                logger.warning("Cached tokens invalid, falling back to login: %s", e)

        # Full login with credentials
        try:
            garmin.login()
            logger.info("Authenticated via username/password")
            garmin.client.dump(token_dir)
            upload_tokens(bucket_name, token_dir)
        except Exception as e:
            logger.error("Authentication failed: %s", e)
            sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Refresh Garmin OAuth tokens")
    parser.add_argument("--env", required=True, choices=["dev", "prd"], help="Target environment")
    args = parser.parse_args()
    refresh(args.env)
