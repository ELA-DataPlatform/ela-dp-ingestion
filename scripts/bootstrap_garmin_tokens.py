"""
Bootstrap Garmin tokens locally.

Run once from a residential/office IP to avoid 429 rate limits,
then upload the generated token files to GCS:

    python scripts/bootstrap_garmin_tokens.py
    gcloud storage cp garmin_tokens/oauth1_token.json gs://ela-source-dev/garmin/tokens/oauth1_token.json
    gcloud storage cp garmin_tokens/oauth2_token.json gs://ela-source-dev/garmin/tokens/oauth2_token.json
"""

import os
import sys

from garminconnect import Garmin

OUTPUT_DIR = "garmin_tokens"


def main():
    username = os.environ.get("GARMIN_USERNAME")
    password = os.environ.get("GARMIN_PASSWORD")

    if not username or not password:
        print("Error: GARMIN_USERNAME and GARMIN_PASSWORD must be set")
        sys.exit(1)

    print(f"Logging in as {username}...")
    garmin = Garmin(username, password)
    garmin.login()
    garmin.garth.dump(OUTPUT_DIR)
    print(f"Tokens saved to {OUTPUT_DIR}/")
    print()
    print("Upload to GCS with:")
    print(f"  gcloud storage cp {OUTPUT_DIR}/oauth1_token.json gs://ela-source-dev/garmin/tokens/oauth1_token.json")
    print(f"  gcloud storage cp {OUTPUT_DIR}/oauth2_token.json gs://ela-source-dev/garmin/tokens/oauth2_token.json")


if __name__ == "__main__":
    main()
